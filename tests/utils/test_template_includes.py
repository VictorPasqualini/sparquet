"""As duas etapas que reescrevem o JSON antes do parse: `{param}` e `$include`.

São as primeiras coisas que rodam num pipeline (`apply_template` sobre o texto
bruto, depois `resolve_includes` sobre o dict) e as duas são puras — nada de
Spark, nada de IO além de ler o arquivo incluído. Um erro aqui não aparece como
erro: ele muda o SQL que vai para o cluster, e o pipeline roda com um filtro a
menos ou uma lista vazia onde deveria haver três valores.

O que este arquivo trava:

  formatação    a tabela de `apply_template` — bool, lista de texto, lista de
                número, lista vazia — que é o contrato de quem escreve
                `IN ({param})` e `skip_if_false`.
  literal       chave sem valor em `params` fica como está, sem quebrar.
  `{{var}}`     a interação com a variável de runtime das transformações, que
                usa chaves duplas e passa por este mesmo regex.
  $include      substituição inline em `transformations`, arquivo com um objeto
                ou com uma lista, `params` valendo dentro do incluído, e os dois
                limites conhecidos: não é recursivo e só vale em
                `transformations`.

    python tests/utils/test_template_includes.py
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sparquet.utils.includes import resolve_includes  # noqa: E402
from sparquet.utils.template import apply_template  # noqa: E402


class ApplyTemplateTest(unittest.TestCase):
    def test_texto_e_numero_entram_como_str(self) -> None:
        self.assertEqual(apply_template("{nome}", {"nome": "cessao"}), "cessao")
        self.assertEqual(apply_template("{n}", {"n": 42}), "42")
        self.assertEqual(apply_template("{n}", {"n": 1.5}), "1.5")

    def test_bool_vira_true_ou_vazio(self) -> None:
        """O par que `skip_if_false` lê: "true" é truthy, "" é falsy. Um `False`
        que virasse a string "false" seria truthy e ligaria a etapa que o
        pipeline pediu para pular."""
        self.assertEqual(apply_template("{ativo}", {"ativo": True}), "true")
        self.assertEqual(apply_template("{ativo}", {"ativo": False}), "")

    def test_lista_de_texto_sai_pronta_para_um_IN(self) -> None:
        self.assertEqual(
            apply_template("tipo IN ({tipos})", {"tipos": ["NC", "CCB"]}),
            "tipo IN ('NC', 'CCB')",
        )

    def test_lista_de_numero_sai_sem_aspas(self) -> None:
        self.assertEqual(
            apply_template("id IN ({ids})", {"ids": [1, 2, 3]}), "id IN (1, 2, 3)"
        )

    def test_lista_vazia_vira_vazio(self) -> None:
        """Falsy de propósito: uma lista vazia não deve virar `IN ()`, que é erro
        de sintaxe — vira "" e a etapa é pulada por `skip_if_false`."""
        self.assertEqual(apply_template("{tipos}", {"tipos": []}), "")

    def test_o_tipo_do_primeiro_item_decide_a_lista_inteira(self) -> None:
        """Limite conhecido: a formatação olha `value[0]`. Uma lista mista sai
        com o critério do primeiro item, não item a item."""
        self.assertEqual(apply_template("{x}", {"x": ["a", 2]}), "'a', '2'")
        self.assertEqual(apply_template("{x}", {"x": [2, "a"]}), "2, a")

    def test_chave_sem_valor_fica_literal(self) -> None:
        """Não levanta: um JSON pode ter `{param}` que só um outro chamador
        preenche, e quebrar aqui impediria de rodar o resto."""
        self.assertEqual(apply_template("{ausente}", {}), "{ausente}")
        self.assertEqual(
            apply_template("{a} e {b}", {"a": "1"}), "1 e {b}"
        )

    def test_so_substitui_chave_de_palavra(self) -> None:
        """O padrão é `\\w+`: qualquer coisa com espaço, ponto ou hífen dentro
        das chaves não é um parâmetro e fica intocada."""
        self.assertEqual(apply_template("{a-b}", {"a-b": "z"}), "{a-b}")
        self.assertEqual(apply_template("{a.b}", {"a.b": "z"}), "{a.b}")
        self.assertEqual(apply_template("{ }", {" ": "z"}), "{ }")

    def test_a_variavel_de_runtime_de_chave_dupla_e_alcancada(self) -> None:
        """Gotcha real, não detalhe: `{{var}}` das transformações contém `{var}`,
        e este regex casa o de dentro. Um `params` com uma chave de mesmo nome de
        uma variável de runtime reescreve a referência ANTES de o
        TransformationEngine vê-la — o `{{var}}` some e vira `{valor}`.
        Nomes de param e de variável de runtime precisam ser distintos."""
        self.assertEqual(apply_template("{{var}}", {"var": "x"}), "{x}")
        # Sem a colisão de nome, a referência atravessa intacta.
        self.assertEqual(apply_template("{{var}}", {"outro": "x"}), "{{var}}")

    def test_o_valor_entra_cru_no_texto(self) -> None:
        """Não há escape: o valor é interpolado como veio. Aspas dentro de um
        param viram aspas dentro do JSON — quem passa valor de fora precisa saber
        que isto é substituição de texto, não bind de SQL."""
        self.assertEqual(apply_template('"{x}"', {"x": "o'brien"}), '"o\'brien"')

    def test_o_json_continua_valido_depois_da_substituicao(self) -> None:
        cru = '{"name": "p", "transformations": [{"type": "filter", "condition": "tipo IN ({tipos})"}]}'
        pronto = json.loads(apply_template(cru, {"tipos": ["A", "B"]}))
        self.assertEqual(
            pronto["transformations"][0]["condition"], "tipo IN ('A', 'B')"
        )


class ResolveIncludesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = Path(tempfile.mkdtemp(prefix="sparquet-includes-"))
        self.addCleanup(shutil.rmtree, self.dir, True)

    def escrever(self, nome: str, conteudo) -> str:
        (self.dir / nome).write_text(
            json.dumps(conteudo, ensure_ascii=False), encoding="utf-8"
        )
        return nome

    def test_arquivo_com_um_objeto_entra_no_lugar_da_diretiva(self) -> None:
        self.escrever("filtro.json", {"type": "filter", "condition": "ativo = true"})
        data = {
            "transformations": [
                {"$include": "filtro.json"},
                {"type": "select", "columns": ["id"]},
            ]
        }

        resolvido = resolve_includes(data, self.dir)

        self.assertEqual(
            resolvido["transformations"],
            [
                {"type": "filter", "condition": "ativo = true"},
                {"type": "select", "columns": ["id"]},
            ],
        )

    def test_arquivo_com_uma_lista_entra_expandido_e_na_ordem(self) -> None:
        """A ordem importa: `filter`/`select` antes de join/group_by é o que
        mantém o volume baixo, e um include que embaralhasse a sequência mudaria
        o plano de execução sem avisar."""
        self.escrever(
            "duas.json",
            [
                {"type": "filter", "condition": "a = 1"},
                {"type": "select", "columns": ["a"]},
            ],
        )
        data = {"transformations": [{"$include": "duas.json"}, {"type": "debug"}]}

        tipos = [t["type"] for t in resolve_includes(data, self.dir)["transformations"]]

        self.assertEqual(tipos, ["filter", "select", "debug"])

    def test_params_valem_dentro_do_arquivo_incluido(self) -> None:
        """É o que torna o include reutilizável: o mesmo arquivo compartilhado
        serve a vários pipelines porque `{tipos}` é resolvido com os params de
        quem incluiu."""
        self.escrever("param.json", {"type": "filter", "condition": "tipo IN ({tipos})"})
        data = {"transformations": [{"$include": "param.json"}]}

        resolvido = resolve_includes(data, self.dir, {"tipos": ["NC", "CCB"]})

        self.assertEqual(
            resolvido["transformations"][0]["condition"], "tipo IN ('NC', 'CCB')"
        )

    def test_sem_diretiva_o_dict_volta_igual(self) -> None:
        data = {"transformations": [{"type": "filter", "condition": "a = 1"}]}
        self.assertIs(resolve_includes(data, self.dir), data)

    def test_sem_transformations_nao_quebra(self) -> None:
        data = {"name": "p", "input": {"format": "csv", "path": "x.csv"}}
        self.assertEqual(resolve_includes(data, self.dir), data)

    def test_o_caminho_e_relativo_ao_json_principal(self) -> None:
        pasta = self.dir / "compartilhado"
        pasta.mkdir()
        (pasta / "f.json").write_text(
            json.dumps({"type": "filter", "condition": "b = 2"}), encoding="utf-8"
        )
        data = {"transformations": [{"$include": "compartilhado/f.json"}]}

        resolvido = resolve_includes(data, self.dir)

        self.assertEqual(resolvido["transformations"][0]["condition"], "b = 2")

    def test_arquivo_inexistente_levanta_dizendo_o_caminho(self) -> None:
        data = {"transformations": [{"$include": "nao-existe.json"}]}
        with self.assertRaises(FileNotFoundError) as capturado:
            resolve_includes(data, self.dir)
        self.assertIn("nao-existe.json", str(capturado.exception))

    def test_include_dentro_de_include_nao_e_expandido(self) -> None:
        """Limite documentado: um nível só. O include interno chega como está e
        vira uma transformação de tipo desconhecido mais adiante — não silêncio,
        mas também não recursão."""
        self.escrever("interno.json", {"type": "filter", "condition": "c = 3"})
        self.escrever("externo.json", [{"$include": "interno.json"}])
        data = {"transformations": [{"$include": "externo.json"}]}

        resolvido = resolve_includes(data, self.dir)

        self.assertEqual(resolvido["transformations"], [{"$include": "interno.json"}])

    def test_a_diretiva_so_vale_em_transformations(self) -> None:
        """Fora de `transformations` a diretiva é ignorada em silêncio — inclusive
        no bloco `spark`, que é justamente onde compartilhar configuração seria
        mais útil (jars de conector). Ver o item de `$include` genérico no
        BACKLOG."""
        self.escrever("spark.json", {"configs": {"spark.jars.packages": "x:y:1"}})
        data = {"spark": {"$include": "spark.json"}, "transformations": []}

        resolvido = resolve_includes(data, self.dir)

        self.assertEqual(resolvido["spark"], {"$include": "spark.json"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
