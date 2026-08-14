import traceback

from sparquet import SparkFramework

# ---------------------------------------------------------------------------
# Mapeamento declarativo dos registros: cada (tipo_ativo, registradora) aponta
# para a conf de registro e, por fluxo, o tópico Kafka de destino. Substitui as
# funções Python do antigo FLUXOS_OPERACOES — o Python só orquestra as confs.
#
# Fluxo de cada (tipo_ativo, registradora, fluxo):
#   1) conf de registro  → filtra o fluxo, monta o payload e grava no staging
#                          genérico view_registro_staging (não escreve destinos).
#   2) conf_commit_registro (genérica) → lê o staging, verifica os contratos
#      (validations) e grava Kafka + silver_registro_contratos + silver_registro_parcelas.
# ---------------------------------------------------------------------------
CONFS_REGISTRO = {
    ("NOTA_COMERCIAL", "B3"): {
        "conf": "conf_registro_b3_nota_comercial.json",
        "fluxos": {"REGISTRO": "vertc-registro-b3-nc-registro-requisicao"},
    },
    ("CPR", "B3"): {
        "conf": "conf_registro_b3_cpr.json",
        "fluxos": {"REGISTRO": "vertc-registro-b3-cpr-instrument-requisicao"},
    },
    ("DUPLICATA", "VERT"): {
        "conf": "conf_registro_vert_duplicata.json",
        "fluxos": {"EMISSAO": "vertc-document-duplicata"},
    },
    # Duplicata/CERC: uma única conf para os 3 fluxos. O branching é feito DENTRO da
    # conf via skip_if_false sobre {fluxo_operacao} — o orquestrador passa só o fluxo.
    ("DUPLICATA", "CERC"): {
        "conf": "conf_registro_cerc_duplicata.json",
        "fluxos": {
            "REGISTRO":           "vertc-registro-cerc-duplicatas-registro-requisicao",
            "EMISSAO":            "vertc-registro-cerc-duplicatas-formalizacao-requisicao",
            "EMISSAO_E_REGISTRO": "vertc-registro-cerc-duplicatas-formalizacao-registro-requisicao",
        },
    },
    # CCB/CERC entra aqui conforme for migrado de função Python para conf.
}

CONF_BASE = "conf_cessoes_pendentes_registro.json"
CONF_COMMIT = "conf_commit_registro.json"


# ---------------------------------------------------------------------------
# ETAPA 1 — Parâmetros do job
# ---------------------------------------------------------------------------
try:
    processar_somente_cessoes_pendentes = True

    param_lista_cessoes   = validar_param_lista("cessoes_lista_string", str)
    param_lista_operacoes = validar_param_lista("operacoes_lista_int", int)
    param_tipo_ativo      = validar_param_unico("tipo_ativo_string", str)
    param_registradora    = validar_param_unico("registradora_string", str)
    param_lista_contratos = validar_param_lista("contratos_lista_string", str)

    if param_lista_cessoes or param_lista_contratos:
        processar_somente_cessoes_pendentes = False

    if param_tipo_ativo and param_registradora:
        chave = (param_tipo_ativo, param_registradora)
        if chave not in CONFS_REGISTRO:
            raise ValueError(
                f"ERRO: NÃO EXISTE CONF DE REGISTRO MAPEADA PARA {param_tipo_ativo}-{param_registradora}"
            )
        confs_a_processar = {chave: CONFS_REGISTRO[chave]}
        print(f"✅ TIPO DE ATIVO E REGISTRADORA FILTRADOS: {param_tipo_ativo}-{param_registradora}")
    else:
        confs_a_processar = CONFS_REGISTRO
        print("ℹ️ NÃO EXISTEM FILTROS PARA TIPO DE ATIVO E REGISTRADORA")

except Exception as e:
    detalhes_erro = traceback.format_exc()
    raise RuntimeError(
        f"ERRO ETAPA 1 - AO PROCESSAR PARAMETROS DO JOB PIPELINE \n{detalhes_erro}"
    ) from e


fw = SparkFramework()


# ---------------------------------------------------------------------------
# ETAPA 2 — Base de cessões pendentes de registro (view_cessoes_pendentes)
# ---------------------------------------------------------------------------
r_base = fw.run(
    f"{BASE_PATH}/{CONF_BASE}",
    params={
        "processar_somente_cessoes_pendentes": processar_somente_cessoes_pendentes,
        "lista_contratos": param_lista_contratos,
        "lista_cessoes":   param_lista_cessoes,
        "lista_operacoes": param_lista_operacoes,
    },
)
print(r_base.summary())

if not r_base.success:
    raise RuntimeError(f"ERRO ETAPA 2 - FALHA NA BASE DE CESSÕES PENDENTES: {r_base.error}")

if r_base.rows_written == 0:
    print("ℹ️ NÃO EXISTEM CESSÕES PENDENTES DE REGISTRO - ENCERRANDO PIPELINE...")
    dbutils.notebook.exit("SEM_DADOS")

print("✅ CONTRATOS FILTRADOS:", param_lista_contratos) if param_lista_contratos else print("ℹ️ Sem filtros para contratos")
print("✅ CESSÕES FILTRADAS:",   param_lista_cessoes)   if param_lista_cessoes   else print("ℹ️ Sem filtros para cessões")
print("✅ OPERAÇÕES FILTRADAS:", param_lista_operacoes) if param_lista_operacoes else print("ℹ️ Sem filtros para operações")


# ---------------------------------------------------------------------------
# ETAPA 3 — Por fluxo: monta o registro (staging) e faz o commit (verifica + grava).
# ---------------------------------------------------------------------------
for (tipo_ativo, registradora), spec in confs_a_processar.items():
    tipo_contrato = CODIGOS_TIPO_CONTRATO[tipo_ativo]

    for tipo_fluxo, topico in spec["fluxos"].items():
        print(f"\n⌛ PROCESSANDO REGISTROS - {tipo_ativo}-{registradora} ({tipo_fluxo})")

        params_fluxo = {
            "tipo_ativo":     tipo_ativo,
            "registradora":   registradora,
            "tipo_contrato":  tipo_contrato,
            "fluxo_operacao": tipo_fluxo,
        }

        # 1) Conf de registro: monta o payload e grava no staging genérico.
        r_reg = fw.run(f"{BASE_PATH}/{spec['conf']}", params=params_fluxo)
        print(r_reg.summary())

        if not r_reg.success:
            print(f"❌ ERRO AO MONTAR REGISTRO - {tipo_ativo}-{registradora} ({tipo_fluxo}): {r_reg.error}")
            continue
        if r_reg.skipped:
            print(f"ℹ️ NÃO EXISTEM CESSÕES PARA - {tipo_ativo}-{registradora} ({tipo_fluxo})")
            continue

        # 2) Conf de commit (genérica): verifica os contratos (validations) e grava
        #    Kafka + silver_registro_contratos + silver_registro_parcelas.
        r_commit = fw.run(
            f"{BASE_PATH}/{CONF_COMMIT}",
            params={**params_fluxo, "topico": topico, "kafka_broker": KAFKA_BROKER},
        )
        print(r_commit.summary())

        if not r_commit.success:
            print(f"❌ ERRO NO COMMIT - {tipo_ativo}-{registradora} ({tipo_fluxo}): {r_commit.error}")
            continue

        # Verificação de perda (data quality) — feita na conf de commit via validations.
        for v in r_commit.validation_results:
            if v.passed:
                print(f"✅ VERIFICAÇÃO OK ({v.rule_type}) - {tipo_ativo}-{registradora} ({tipo_fluxo})")
            else:
                print(f"⚠️❌ {tipo_ativo}-{registradora} ({tipo_fluxo}): {v.message} (falhas={v.failed_count})")

fw.stop()
