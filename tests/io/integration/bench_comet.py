# -*- coding: utf-8 -*-
"""Benchmark do DataFusion Comet: quanto o mesmo pipeline ganha, medido.

`test_comet_spark.py` prova **correção** (mesmas linhas) e **ativação** (nós
`Comet` no plano). Não prova velocidade, que é o motivo de existir do plugin.
Este arquivo mede velocidade — e por isso não é um teste: tempo de parede depende
de máquina, volume e cache, então uma asserção sobre ele quebraria no CI sem
nenhum defeito no framework.

Metodologia, e cada escolha existe para não medir a coisa errada:

  * **Dados gerados uma vez**, num processo próprio e sem Comet, gravados em
    Parquet. Se cada configuração gerasse os seus, mediríamos a escrita também.
  * **Duas JVMs**, uma por configuração — o plugin é por JVM, como no teste.
  * **Aquecimento antes de cronometrar**: a primeira execução paga JIT, leitura
    de metadado do Parquet e page cache frio. Ela é descartada.
  * **N repetições, e o relatório traz mediana e mínimo.** Média com N pequeno
    numa máquina compartilhada mede o vizinho, não o plugin.
  * **Duas formas de consulta**, porque o ganho não é um número único:
    `agregacao` (filtro + `group_by`) e `filtro_contagem` (filtro seletivo +
    contagem), que é ligada a varredura.
  * **O pipeline é o do framework** (`run_from_dict`), não SQL solto: o que se
    quer saber é se o ganho aparece no caminho que o usuário usa.
  * A cronometragem termina numa **ação** (`collect` do agregado, `count` na
    varredura). Sem isso o Spark é preguiçoso e mediríamos planejamento.

O relatório também imprime os nós `Comet` do plano de cada forma: número de
aceleração sem prova de que o operador era nativo não vale nada.

Custo que o número não mostra: Comet exige off-heap dimensionado (aqui 4 GB), que
é memória a mais do que a execução sem plugin usa, e o jar tem 88 MB.

Uso (Linux, com o jar do Comet — mesmos pré-requisitos do teste):

    SPARQUET_IT_COMET_JAR=/caminho/comet.jar python tests/io/integration/bench_comet.py

    # mais volume, mais repetições:
    SPARQUET_IT_COMET_JAR=... python tests/io/integration/bench_comet.py --linhas 80000000 --repeticoes 5
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

_RAIZ = Path(__file__).resolve().parents[3]
_MARCA = "BENCH_COMET "

#: Configuração comum às duas execuções — o que muda entre elas é só o Comet.
_BASE = {
    "master": "local[4]",
    "configs": {
        "spark.sql.shuffle.partitions": "8",
        "spark.ui.enabled": "false",
        "spark.sql.adaptive.enabled": "true",
    },
}

_CONFIGS_COMET = {
    "spark.plugins": "org.apache.spark.CometPlugin",
    "spark.shuffle.manager": (
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
    ),
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "4g",
}


def _bloco(nome: str, com_comet: bool) -> dict:
    bloco = {
        "app_name": nome,
        "master": _BASE["master"],
        "configs": dict(_BASE["configs"]),
    }
    if com_comet:
        bloco["configs"].update(_CONFIGS_COMET)
    return bloco


# ------------------------------------------------------------------- consultas


def _pipelines(dados: Path) -> dict:
    """As duas formas medidas, como JSON de pipeline do framework."""
    entrada = {"format": "parquet", "path": dados.as_posix()}
    return {
        # Filtro + agregação com duas chaves: o caminho onde Comet substitui
        # scan, filtro, agregação parcial/final e o shuffle entre elas.
        "agregacao": {
            "name": "bench-agregacao",
            "input": entrada,
            "transformations": [
                {"type": "filter", "condition": "valor > 0"},
                {
                    "type": "group_by",
                    "by": ["nome", "categoria"],
                    "agg": ["sum(valor) as total", "count(1) as linhas"],
                },
            ],
            # `cache: false` é obrigatório aqui: o ViewWriter, por default, faz
            # `cache()` + `count()`, e aí a segunda repetição leria memória em vez
            # de recomputar — mediria o cache do Spark, não o Comet.
            "output": {
                "format": "view",
                "path": "bench_agregacao",
                "options": {"cache": "false"},
            },
        },
        # Filtro seletivo + contagem: quase todo o tempo é varredura de Parquet,
        # sem shuffle. Mede a outra ponta do ganho.
        "filtro_contagem": {
            "name": "bench-filtro",
            "input": entrada,
            "transformations": [
                {
                    "type": "filter",
                    "condition": "valor between 10 and 40 and nome <> 'nome-3'",
                },
            ],
            "output": {
                "format": "view",
                "path": "bench_filtro",
                "options": {"cache": "false"},
            },
        },
    }


# ---------------------------------------------------------------- preparação


def _prepara(dados: Path, linhas: int) -> None:
    """Gera o Parquet de entrada. Processo próprio, sem Comet, uma única vez."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("bench-prepara")
        .master("local[4]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    (
        spark.range(0, linhas)
        .selectExpr(
            "id",
            "concat('nome-', cast(pmod(hash(id), 8) as string)) as nome",
            "concat('cat-', cast(pmod(hash(id * 31), 50) as string)) as categoria",
            "cast(pmod(id, 1000) as double) / 7 - 20 as valor",
        )
        # 16 arquivos: paralelismo de leitura sem cair no problema de small files.
        .repartition(16)
        .write.mode("overwrite")
        .parquet(dados.as_posix())
    )
    tamanho = sum(f.stat().st_size for f in dados.rglob("*.parquet"))
    print(_MARCA + json.dumps({"linhas": linhas, "bytes": tamanho}))
    spark.stop()


# ------------------------------------------------------------------ medição


def _mede(com_comet: bool, dados: Path, repeticoes: int) -> None:
    """Roda as duas formas nesta JVM e imprime os tempos em JSON."""
    sys.path.insert(0, str(_RAIZ))
    from sparquet import Sparquet

    app = Sparquet(spark=_bloco("bench-" + ("comet" if com_comet else "plain"), com_comet))
    saida: dict = {"comet": com_comet, "formas": {}}

    for forma, spec in _pipelines(dados).items():
        view = spec["output"]["path"]

        def uma_volta() -> float:
            inicio = time.perf_counter()
            resultado = app.run_from_dict(spec)
            if not resultado.success:
                raise RuntimeError(f"{forma}: {resultado.error}")
            df = app.spark.table(view)
            # A ação é o que força execução; a agregação devolve poucas linhas,
            # a varredura devolve muitas, então cada forma tem a sua.
            if forma == "agregacao":
                df.collect()
            else:
                df.count()
            return time.perf_counter() - inicio

        aquecimento = uma_volta()
        tempos = [uma_volta() for _ in range(repeticoes)]

        plano = app.spark.table(view)._jdf.queryExecution().executedPlan().toString()
        saida["formas"][forma] = {
            "aquecimento": round(aquecimento, 3),
            "tempos": [round(t, 3) for t in tempos],
            "mediana": round(statistics.median(tempos), 3),
            "minimo": round(min(tempos), 3),
            "nos_comet": sorted(
                {p.strip("(),*+- ") for p in plano.split() if p.startswith("Comet")}
            ),
        }

    print(_MARCA + json.dumps(saida))


# -------------------------------------------------------------------- relatório


def _roda_filho(args: list, jar: str | None, timeout: int = 3600) -> dict:
    ambiente = dict(os.environ)
    if jar:
        # Mesmo motivo do teste: o plugin é instanciado durante a construção do
        # SparkContext, então o jar entra na linha de comando da JVM. A memória
        # de driver é igual nas duas execuções, para não medir GC de uma só.
        ambiente["PYSPARK_SUBMIT_ARGS"] = (
            f"--jars {jar} --driver-class-path {jar} --driver-memory 6g pyspark-shell"
        )
    else:
        ambiente["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 6g pyspark-shell"
    ambiente["PYTHONPATH"] = os.pathsep.join(
        [str(_RAIZ), ambiente.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    processo = subprocess.run(
        [sys.executable, "-u", str(Path(__file__).resolve()), *args],
        cwd=str(_RAIZ),
        env=ambiente,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    for linha in processo.stdout.splitlines():
        if linha.startswith(_MARCA):
            return json.loads(linha[len(_MARCA):])
    cauda = (processo.stdout[-2000:] + processo.stderr[-2000:]).strip()
    raise SystemExit(f"filho {args} não devolveu resultado (exit {processo.returncode}):\n{cauda}")


def _relatorio(prep: dict, com: dict, sem: dict, repeticoes: int) -> None:
    mb = prep["bytes"] / 1024 / 1024
    print()
    print(f"dados: {prep['linhas']:,} linhas, {mb:,.0f} MB em Parquet".replace(",", "."))
    print(f"plataforma: {platform.platform()} | repetições cronometradas: {repeticoes}")
    print()
    print(f"{'forma':<18}{'sem Comet':>12}{'com Comet':>12}{'ganho':>10}")
    for forma in com["formas"]:
        a, b = sem["formas"][forma], com["formas"][forma]
        ganho = a["mediana"] / b["mediana"] if b["mediana"] else float("nan")
        print(f"{forma:<18}{a['mediana']:>11.2f}s{b['mediana']:>11.2f}s{ganho:>9.2f}x")
    print()
    for forma in com["formas"]:
        a, b = sem["formas"][forma], com["formas"][forma]
        print(f"{forma}:")
        print(f"  sem Comet  mediana {a['mediana']}s  mínimo {a['minimo']}s  tempos {a['tempos']}")
        print(f"  com Comet  mediana {b['mediana']}s  mínimo {b['minimo']}s  tempos {b['tempos']}")
        print(f"  nós Comet no plano: {', '.join(b['nos_comet']) or 'NENHUM (plugin inativo!)'}")
        if a["nos_comet"]:
            print(f"  !! a execução sem Comet trouxe nós Comet: {a['nos_comet']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark do DataFusion Comet")
    parser.add_argument("--linhas", type=int, default=40_000_000)
    parser.add_argument("--repeticoes", type=int, default=3)
    parser.add_argument("--dados", default="/tmp/sparquet-bench-comet")
    parser.add_argument("--regerar", action="store_true", help="apaga e gera os dados de novo")
    # Usados internamente pelos processos filhos.
    parser.add_argument("--prepara", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--child", choices=("comet", "plain"), help=argparse.SUPPRESS)
    args = parser.parse_args()

    dados = Path(args.dados) / f"linhas-{args.linhas}.parquet"

    if args.prepara:
        _prepara(dados, args.linhas)
        return
    if args.child:
        _mede(args.child == "comet", dados, args.repeticoes)
        return

    jar = os.environ.get("SPARQUET_IT_COMET_JAR", "").strip()
    if not jar or not Path(jar).is_file():
        raise SystemExit(
            "aponte SPARQUET_IT_COMET_JAR para o jar do Comet da linha do seu Spark "
            "(ver tests/io/integration/test_comet_spark.py)"
        )
    if platform.system() != "Linux":
        raise SystemExit(
            f"o binário nativo do Comet só existe para Linux (aqui: {platform.system()}); "
            "em outro sistema o plugin se desabilita em silêncio e o benchmark mediria "
            "duas execuções idênticas"
        )

    if args.regerar and dados.exists():
        import shutil

        shutil.rmtree(dados)
    if dados.exists():
        prep = {
            "linhas": args.linhas,
            "bytes": sum(f.stat().st_size for f in dados.rglob("*.parquet")),
        }
        print(f"dados reaproveitados de {dados}")
    else:
        print(f"gerando {args.linhas:,} linhas em {dados}".replace(",", "."))
        prep = _roda_filho(
            ["--prepara", "--linhas", str(args.linhas), "--dados", args.dados], None
        )

    comuns = ["--linhas", str(args.linhas), "--dados", args.dados,
              "--repeticoes", str(args.repeticoes)]
    print("medindo sem Comet…")
    sem = _roda_filho([*comuns, "--child", "plain"], jar)
    print("medindo com Comet…")
    com = _roda_filho([*comuns, "--child", "comet"], jar)
    _relatorio(prep, com, sem, args.repeticoes)


if __name__ == "__main__":
    main()
