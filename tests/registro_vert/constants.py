"""Constantes do orquestrador de registro de cessões.

Define o mapeamento FLUXOS_OPERACOES (tipo_ativo, registradora) → fluxos,
sem nenhuma função Python — apenas caminhos para confs JSON. Toda a lógica
de enriquecimento e montagem de payload é declarativa via JSON.
"""
from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Conexão Kafka e tópicos
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("kafka_broker", "localhost:9092")

TOPICO_REGISTRO_LASTRO_REQUISICAO_CCB_CERC = "vertc-registro-lastro-ccb"
TOPICO_REGISTRO_LASTRO_REQUISICAO_CPR_B3 = "vertc-registro-b3-cpr-instrument-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_NOTA_COMERCIAL_B3 = "vertc-registro-b3-nc-registro-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_REGISTRO_DUPLICATA_CERC = "vertc-registro-cerc-duplicatas-registro-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_DUPLICATA_CERC = "vertc-registro-cerc-duplicatas-formalizacao-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_REGISTRO_DUPLICATA_CERC = "vertc-registro-cerc-duplicatas-formalizacao-registro-requisicao"

# ---------------------------------------------------------------------------
# Códigos / status
# ---------------------------------------------------------------------------
CODIGOS_TIPO_CONTRATO = {
    "CCB": "0",
    "DUPLICATA": "1",
    "CPR": "4",
    "NOTA_COMERCIAL": "8",
}

ENVIADO = "ENVIADO_PARA_REGISTRO"

# ---------------------------------------------------------------------------
# FLUXOS_OPERACOES — totalmente declarativo
# ---------------------------------------------------------------------------
# Cada fluxo aponta para 2 confs JSON:
#   conf_enriquecimento – filtra cessões + joins específicos do tipo + regras
#                         de negócio. Grava na view 'cessoes_<tipo>_enriquecido'.
#   conf_payload        – monta o struct 'payload' aninhado. Grava na view
#                         'cessoes_<tipo>_payload'.
#   topico              – tópico Kafka destino (passado via columns para envio.json).
#   view_payload        – nome da view criada pela conf_payload (lida pelo envio).
# ---------------------------------------------------------------------------
FLUXOS_OPERACOES = {
    ("CCB", "CERC"): {
        "REGISTRO": {
            "conf_enriquecimento": "ccb_cerc/enriquecimento.json",
            "conf_payload":        "ccb_cerc/payload.json",
            "topico":              TOPICO_REGISTRO_LASTRO_REQUISICAO_CCB_CERC,
            "view_payload":        "cessoes_ccb_cerc_payload",
        },
    },
    ("DUPLICATA", "CERC"): {
        "REGISTRO": {
            "conf_enriquecimento": "duplicata_cerc/enriquecimento.json",
            "conf_payload":        "duplicata_cerc/payload_registro.json",
            "topico":              TOPICO_REGISTRO_LASTRO_REQUISICAO_REGISTRO_DUPLICATA_CERC,
            "view_payload":        "cessoes_duplicata_cerc_payload",
        },
        "EMISSAO": {
            "conf_enriquecimento": "duplicata_cerc/enriquecimento.json",
            "conf_payload":        "duplicata_cerc/payload_emissao.json",
            "topico":              TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_DUPLICATA_CERC,
            "view_payload":        "cessoes_duplicata_cerc_payload",
        },
        "EMISSAO_E_REGISTRO": {
            "conf_enriquecimento": "duplicata_cerc/enriquecimento.json",
            "conf_payload":        "duplicata_cerc/payload_emissao_e_registro.json",
            "topico":              TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_REGISTRO_DUPLICATA_CERC,
            "view_payload":        "cessoes_duplicata_cerc_payload",
        },
    },
    ("CPR", "B3"): {
        "REGISTRO": {
            "conf_enriquecimento": "cpr_b3/enriquecimento.json",
            "conf_payload":        "cpr_b3/payload.json",
            "topico":              TOPICO_REGISTRO_LASTRO_REQUISICAO_CPR_B3,
            "view_payload":        "cessoes_cpr_b3_payload",
        },
    },
    ("NOTA_COMERCIAL", "B3"): {
        "REGISTRO": {
            "conf_enriquecimento": "nota_comercial_b3/enriquecimento.json",
            "conf_payload":        "nota_comercial_b3/payload.json",
            "topico":              TOPICO_REGISTRO_LASTRO_REQUISICAO_NOTA_COMERCIAL_B3,
            "view_payload":        "cessoes_nota_comercial_b3_payload",
        },
    },
}

# Views intermediárias usadas pelo orquestrador (dropadas no fim do job)
VIEWS_INTERMEDIARIAS = [
    "cessoes_base",
    "cessoes_para_processar",
    "cessoes_ccb_cerc_enriquecido",
    "cessoes_duplicata_cerc_enriquecido",
    "cessoes_cpr_b3_enriquecido",
    "cessoes_nota_comercial_b3_enriquecido",
    "cessoes_ccb_cerc_payload",
    "cessoes_duplicata_cerc_payload",
    "cessoes_cpr_b3_payload",
    "cessoes_nota_comercial_b3_payload",
]
