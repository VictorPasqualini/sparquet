import os
from typing import Any, Dict

KAFKA_BROKER = os.getenv("kafka_broker")
TOPICO_REGISTRO_LASTRO_REQUISICAO_CCB_CERC = "vertc-registro-lastro-ccb"
TOPICO_REGISTRO_LASTRO_REQUISICAO_CPR_B3 = "vertc-registro-b3-cpr-instrument-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_NOTA_COMERCIAL_B3 = "vertc-registro-b3-nc-registro-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_REGISTRO_DUPLICATA_CERC = "vertc-registro-cerc-duplicatas-registro-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_DUPLICATA_CERC = "vertc-registro-cerc-duplicatas-formalizacao-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_REGISTRO_DUPLICATA_CERC = "vertc-registro-cerc-duplicatas-formalizacao-registro-requisicao"
TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_DUPLICATA_VERT = "vertc-document-duplicata"
TOPICO_REGISTRO_LASTRO_REQUISICAO_BAIXA_CPR_B3 = "vertc-registro-b3-cpr-withdrawal-requisicao"

ENVIADO = "ENVIADO_PARA_REGISTRO"
SUCESSO = "SUCCESS"
ENVIADO_CANCELAMENTO = "ENVIADO CANCELAMENTO"
CANCELADO = "CANCELED"
PAGA_PARCIALMENTE = "PAGA_PARCIALMENTE"
PAGA = "PAGA"
ERRO = "ERROR"
REJEITADA = "REJEITADA"
PREJUIZO = "PREJUIZO"
ENVIADO_PARA_BAIXA = "ENVIADO_PARA_BAIXA"

REGISTROS_POR_GRUPO = 300
CHECKPOINT_BAIXAS_PARCELAS_REGISTRO = "/tmp/checkpoint/baixas_parcelas_registro"

CODIGOS_TIPO_CONTRATO = {
    "CCB": "0",
    "DUPLICATA": "1",
    "CPR": "4",
    "NOTA_COMERCIAL": "8",
}

FLUXOS_OPERACOES = {
    ("CCB", "CERC"): {
        "REGISTRO": {
            "funcao_preparacao_objeto": preparar_objeto_cessao_ccb,
            "funcao_construcao_payload": cessao_registro_lastro_ccb,
            "topico": TOPICO_REGISTRO_LASTRO_REQUISICAO_CCB_CERC,
        }
    },
    
    ("DUPLICATA", "CERC"): {
        "REGISTRO": {
            "funcao_preparacao_objeto": preparar_objeto_cessao_duplicata,
            "funcao_construcao_payload": cessao_registro_lastro_duplicata,
            "topico": TOPICO_REGISTRO_LASTRO_REQUISICAO_REGISTRO_DUPLICATA_CERC,
        },
        "EMISSAO": {
            "funcao_preparacao_objeto": preparar_objeto_cessao_duplicata,
            "funcao_construcao_payload": cessao_registro_lastro_duplicata,
            "topico": TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_DUPLICATA_CERC,
        },
        "EMISSAO_E_REGISTRO": {
            "funcao_preparacao_objeto": preparar_objeto_cessao_duplicata,
            "funcao_construcao_payload": cessao_registro_lastro_duplicata,
            "topico": TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_REGISTRO_DUPLICATA_CERC,
        }
    },

    ("DUPLICATA", "VERT"): {
        "EMISSAO": {
            "funcao_preparacao_objeto": preparar_objeto_cessao_duplicata_vert,
            "funcao_construcao_payload": cessao_registro_lastro_duplicata_vert,
            "topico": TOPICO_REGISTRO_LASTRO_REQUISICAO_FORMALIZACAO_DUPLICATA_VERT,
        },
    },
    
    ("CPR", "B3"): {
        "REGISTRO": {
            "funcao_preparacao_objeto": preparar_objeto_cessao_cpr,
            "funcao_construcao_payload": cessao_registro_lastro_cpr,
            "topico": TOPICO_REGISTRO_LASTRO_REQUISICAO_CPR_B3,
        }
    },

    ("NOTA_COMERCIAL", "B3"): {
        "REGISTRO": {
            "funcao_preparacao_objeto": preparar_objeto_cessao_nota_comercial,
            "funcao_construcao_payload": cessao_registro_lastro_nota_comercial,
            "topico": TOPICO_REGISTRO_LASTRO_REQUISICAO_NOTA_COMERCIAL_B3,
        }
    },
}