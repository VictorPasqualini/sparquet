"""Constantes do orquestrador de registro de cessões.

Mapeamento FLUXOS_OPERACOES totalmente declarativo: cada fluxo aponta para
2 confs JSON (cessoes_pendentes + payload). Para Duplicata, que tem 3
subfluxos compartilhando o enriquecimento, cessoes_pendentes é a mesma e
muda apenas a conf de payload.
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
# FLUXOS_OPERACOES
# ---------------------------------------------------------------------------
# Cada entrada: (tipo_ativo, registradora) → {tipo_fluxo: attrs}
# attrs:
#   conf_cessoes_pendentes – conf JSON que filtra e enriquece com tabelas do ativo
#   conf_payload           – conf JSON que monta payload + 4 outputs (Kafka + 3 Deltas)
#   topico                 – tópico Kafka (injetado via ${param_topico} no payload)
#   view_payload           – nome da view escrita pela payload.json (para anti-join no orquestrador)
# ---------------------------------------------------------------------------
FLUXOS_OPERACOES = {
    ("CCB", "CERC"): {
        "REGISTRO": {
            "conf_cessoes_pendentes": "ccb_cerc/cessoes_pendentes.json",
            "conf_payload":           "ccb_cerc/payload.json",
            "topico":                 TOPICO_CCB_CERC,
            "view_payload":           "ccb_cerc_payload",
        },
    },
    ("DUPLICATA", "CERC"): {
        "REGISTRO": {
            "conf_cessoes_pendentes": "duplicata_cerc/cessoes_pendentes.json",
            "conf_payload":           "duplicata_cerc/payload_registro.json",
            "topico":                 TOPICO_DUPLICATA_CERC_REGISTRO,
            "view_payload":           "duplicata_cerc_payload",
        },
        "EMISSAO": {
            "conf_cessoes_pendentes": "duplicata_cerc/cessoes_pendentes.json",
            "conf_payload":           "duplicata_cerc/payload_emissao.json",
            "topico":                 TOPICO_DUPLICATA_CERC_EMISSAO,
            "view_payload":           "duplicata_cerc_payload",
        },
        "EMISSAO_E_REGISTRO": {
            "conf_cessoes_pendentes": "duplicata_cerc/cessoes_pendentes.json",
            "conf_payload":           "duplicata_cerc/payload_emissao_e_registro.json",
            "topico":                 TOPICO_DUPLICATA_CERC_EMISSAO_E_REGISTRO,
            "view_payload":           "duplicata_cerc_payload",
        },
    },
    ("CPR", "B3"): {
        "REGISTRO": {
            "conf_cessoes_pendentes": "cpr_b3/cessoes_pendentes.json",
            "conf_payload":           "cpr_b3/payload.json",
            "topico":                 TOPICO_CPR_B3,
            "view_payload":           "cpr_b3_payload",
        },
    },
    ("NOTA_COMERCIAL", "B3"): {
        "REGISTRO": {
            "conf_cessoes_pendentes": "nota_comercial_b3/cessoes_pendentes.json",
            "conf_payload":           "nota_comercial_b3/payload.json",
            "topico":                 TOPICO_NOTA_COMERCIAL_B3,
            "view_payload":           "nota_comercial_b3_payload",
        },
    },
}

# Temp views criadas pelo pipeline (limpas no fim)
VIEWS_INTERMEDIARIAS = [
    "cessoes_base",
    "ccb_cerc_pendentes",         "ccb_cerc_payload",
    "duplicata_cerc_pendentes",   "duplicata_cerc_payload",
    "cpr_b3_pendentes",           "cpr_b3_payload",
    "nota_comercial_b3_pendentes", "nota_comercial_b3_payload",
]
