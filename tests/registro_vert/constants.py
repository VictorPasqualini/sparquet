"""Constantes do orquestrador de registro de cessões.

Mapeamento FLUXOS_OPERACOES totalmente declarativo. Sem self-join na cessoes_pendentes:
todos os fluxos usam a mesma `cessoes_pendentes.json` genérica, que filtra por
(tipo_ativo, registradora, fluxo_operacao) e por compatibilidade multi_ativos via
param_codigo_tipo_contrato. Cada payload absorve seus joins específicos.

Duplicata é o único caso especial: tem 3 subfluxos (REGISTRO, EMISSAO, EMISSAO_E_REGISTRO)
compartilhando o mesmo enriquecimento (bronze_remessa + lastros_relacionamento + parcela).
Por isso há uma `duplicata_cerc/enriquecimento.json` intermediária.
"""
from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Conexão Kafka e tópicos
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("kafka_broker", "localhost:9092")

TOPICO_CCB_CERC               = "vertc-registro-lastro-ccb"
TOPICO_CPR_B3                 = "vertc-registro-b3-cpr-instrument-requisicao"
TOPICO_NOTA_COMERCIAL_B3      = "vertc-registro-b3-nc-registro-requisicao"
TOPICO_DUPLICATA_CERC_REGISTRO            = "vertc-registro-cerc-duplicatas-registro-requisicao"
TOPICO_DUPLICATA_CERC_EMISSAO             = "vertc-registro-cerc-duplicatas-formalizacao-requisicao"
TOPICO_DUPLICATA_CERC_EMISSAO_E_REGISTRO  = "vertc-registro-cerc-duplicatas-formalizacao-registro-requisicao"

# ---------------------------------------------------------------------------
# Código tipo_contrato por tipo_ativo (param para filtro multi_ativos)
# ---------------------------------------------------------------------------
CODIGOS_TIPO_CONTRATO = {
    "CCB":            "0",
    "DUPLICATA":      "1",
    "CPR":            "4",
    "NOTA_COMERCIAL": "8",
}

# ---------------------------------------------------------------------------
# FLUXOS_OPERACOES
# ---------------------------------------------------------------------------
# Cada entrada: (tipo_ativo, registradora) → {tipo_fluxo: attrs}
# attrs:
#   conf_enriquecimento – conf intermediária OPCIONAL (apenas Duplicata; outros
#                         ativos pulam direto para payload)
#   conf_payload        – conf final: joins do ativo + struct + 4 outputs
#   topico              – tópico Kafka (injetado via ${param_topico!raw})
#   view_payload        – nome da view criada pelo payload (anti-join no orquestrador)
# ---------------------------------------------------------------------------
FLUXOS_OPERACOES = {
    ("CCB", "CERC"): {
        "REGISTRO": {
            "conf_enriquecimento": None,
            "conf_payload":        "ccb_cerc/payload.json",
            "topico":               TOPICO_CCB_CERC,
            "view_payload":         "ccb_cerc_payload",
        },
    },
    ("DUPLICATA", "CERC"): {
        "REGISTRO": {
            "conf_enriquecimento": "duplicata_cerc/enriquecimento.json",
            "conf_payload":        "duplicata_cerc/payload_registro.json",
            "topico":               TOPICO_DUPLICATA_CERC_REGISTRO,
            "view_payload":         "duplicata_cerc_payload",
        },
        "EMISSAO": {
            "conf_enriquecimento": "duplicata_cerc/enriquecimento.json",
            "conf_payload":        "duplicata_cerc/payload_emissao.json",
            "topico":               TOPICO_DUPLICATA_CERC_EMISSAO,
            "view_payload":         "duplicata_cerc_payload",
        },
        "EMISSAO_E_REGISTRO": {
            "conf_enriquecimento": "duplicata_cerc/enriquecimento.json",
            "conf_payload":        "duplicata_cerc/payload_emissao_e_registro.json",
            "topico":               TOPICO_DUPLICATA_CERC_EMISSAO_E_REGISTRO,
            "view_payload":         "duplicata_cerc_payload",
        },
    },
    ("CPR", "B3"): {
        "REGISTRO": {
            "conf_enriquecimento": None,
            "conf_payload":        "cpr_b3/payload.json",
            "topico":               TOPICO_CPR_B3,
            "view_payload":         "cpr_b3_payload",
        },
    },
    ("NOTA_COMERCIAL", "B3"): {
        "REGISTRO": {
            "conf_enriquecimento": None,
            "conf_payload":        "nota_comercial_b3/payload.json",
            "topico":               TOPICO_NOTA_COMERCIAL_B3,
            "view_payload":         "nota_comercial_b3_payload",
        },
    },
}

# Temp views criadas pelo pipeline (limpas no fim)
VIEWS_INTERMEDIARIAS = [
    "cessoes_base",
    "cessoes_pendentes",
    "duplicata_cerc_enriquecido",
    "ccb_cerc_payload",
    "duplicata_cerc_payload",
    "cpr_b3_payload",
    "nota_comercial_b3_payload",
]
