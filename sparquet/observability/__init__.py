"""Observabilidade opcional: o que aconteceu numa execução, para fora do processo.

Nada aqui é ligado por padrão. O framework roda em qualquer ambiente sem depender
de nada, e isso não muda: sem configuração explícita, nenhum sink é criado e
`Pipeline.run` segue idêntico.

Ver `sparquet.observability.history` para o histórico de execução (o que o Studio
grava quando é ele quem executa, disponível também para quem roda via
`sparquet.cli`, Airflow, Databricks ou EMR).
"""
from sparquet.observability.history import (
    HistorySink,
    HttpHistorySink,
    RunRecorder,
    clear_sinks,
    register_sink,
)

__all__ = [
    "HistorySink",
    "HttpHistorySink",
    "RunRecorder",
    "clear_sinks",
    "register_sink",
]
