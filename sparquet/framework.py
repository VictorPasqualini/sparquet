from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Union

from pyspark.sql import DataFrame

from sparquet.core.config import PipelineConfig, SparkConfig
from sparquet.utils.template import apply_template
from sparquet.utils.includes import resolve_includes
from sparquet.core.context import SparkContextManager
from sparquet.core.pipeline import Pipeline, PipelineResult
from sparquet.io.factory import ReaderFactory, WriterFactory
from sparquet.io.base import BaseReader, BaseWriter
from sparquet.transform.base import BaseTransformation
from sparquet.transform.engine import TransformationEngine
from sparquet.validation.base import BaseValidator
from sparquet.validation.engine import ValidationEngine


class Sparquet:
    """Ponto de entrada principal para uso do framework como biblioteca.

    Gerencia uma única SparkSession compartilhada entre múltiplas execuções.
    Permite registrar formatos, transformações e validators customizados que
    ficam disponíveis em todos os pipelines executados pela mesma instância.

    Uso básico:
        fw = Sparquet()
        fw.run("pipeline_clientes.json")
        fw.run("pipeline_pedidos.json")
        fw.stop()

    Com configurações Spark personalizadas:
        fw = Sparquet(spark={"master": "yarn", "app_name": "MeuJob"})

    Com extensões customizadas:
        fw = Sparquet()
        fw.register_reader("delta", DeltaReader)
        fw.register_transformation("normalize", NormalizeTransformation)
        fw.run("config.json")
    """

    def __init__(
        self,
        spark: Optional[Dict[str, Any]] = None,
        input_view: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> None:
        self._spark_config = SparkConfig.from_dict(spark or {})
        self._transform_engine = TransformationEngine()
        self._validation_engine = ValidationEngine()
        # Registra (e cacheia) o df de entrada como temp view em toda execução, para
        # permitir self-join / SQL sobre a entrada sem reler a base. Ver `run()`.
        self._input_view = input_view
        SparkContextManager.get_or_create(self._spark_config)

    # ------------------------------------------------------------------
    # Execução de pipelines
    # ------------------------------------------------------------------

    def run(
        self,
        config_path: str,
        input_df: Optional[DataFrame] = None,
        columns: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        input_view: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> PipelineResult:
        """Executa um pipeline a partir de um arquivo JSON.

        Args:
            config_path: caminho para o JSON de configuração.
            input_df:    DataFrame de entrada; quando fornecido substitui o 'input'
                         declarado no JSON e não adiciona ingestion_ts automaticamente.
            columns:     Colunas literais a injetar no df antes das transformações,
                         ex: {"param_tipo_ativo": "NC", "param_registradora": "CERC"}.
            params:      Valores de runtime substituídos como {chave} no JSON antes do
                         parse. Listas viram SQL IN (ex: 'a', 'b'); booleanos viram
                         "true"/"" (vazio = falsy dispara skip_if_false).
            input_view:  se informado, registra (e cacheia) o df de entrada como uma
                         temp view antes das transformações. Permite self-join / SQL
                         sobre a entrada sem reler a base (ex: um `join` com
                         `{"format":"view","path":"<input_view>"}`). Sobrepõe o default
                         passado no construtor. Aceita:
                         - uma **string** (nome da view, escopo "session"), ou
                         - um **dict** `{"name": "<nome>", "type": "session"|"global"}`
                           — use `"type": "global"` para uma global temp view visível a
                           toda a aplicação (lida como `global_temp.<nome>`).
        """
        config = self._load_config(config_path, params)
        self._apply_spark_override(config)
        return self._execute(
            config, input_df=input_df, columns=columns, input_view=input_view,
        )

    def run_from_dict(
        self,
        config: Dict[str, Any],
        input_df: Optional[DataFrame] = None,
        columns: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        input_view: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> PipelineResult:
        """Executa um pipeline a partir de um dicionário Python."""
        pipeline_config = self._load_config_from_dict(config, params)
        self._apply_spark_override(pipeline_config)
        return self._execute(
            pipeline_config, input_df=input_df, columns=columns, input_view=input_view,
        )

    # ------------------------------------------------------------------
    # Registro de extensões
    # ------------------------------------------------------------------

    def register_reader(self, format_name: str, reader_cls: type[BaseReader]) -> None:
        """Registra um leitor customizado para um novo formato."""
        ReaderFactory.register(format_name, reader_cls)

    def register_writer(self, format_name: str, writer_cls: type[BaseWriter]) -> None:
        """Registra um escritor customizado para um novo formato."""
        WriterFactory.register(format_name, writer_cls)

    def register_transformation(
        self, name: str, transformation_cls: type[BaseTransformation]
    ) -> None:
        """Registra uma transformação customizada disponível via JSON."""
        self._transform_engine.register(name, transformation_cls)

    def register_validator(
        self, name: str, validator_cls: type[BaseValidator]
    ) -> None:
        """Registra um validator customizado disponível via JSON."""
        self._validation_engine.register(name, validator_cls)

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Encerra a SparkSession."""
        SparkContextManager.stop()

    # ------------------------------------------------------------------
    # Interno
    # ------------------------------------------------------------------

    def _execute(
        self,
        config: PipelineConfig,
        input_df: Optional[DataFrame] = None,
        columns: Optional[Dict[str, Any]] = None,
        input_view: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> PipelineResult:
        pipeline = Pipeline(
            config,
            transform_engine=self._transform_engine,
            validation_engine=self._validation_engine,
            input_df=input_df,
            columns=columns,
            input_view=input_view if input_view is not None else self._input_view,
        )
        return pipeline.run()

    def _load_config(self, path: str, params: Optional[Dict[str, Any]]) -> PipelineConfig:
        raw = Path(path).read_text(encoding="utf-8")
        if params:
            raw = apply_template(raw, params)
        data = json.loads(raw)
        data = resolve_includes(data, Path(path).parent, params)
        return PipelineConfig.from_dict(data)

    def _load_config_from_dict(
        self, config: Dict[str, Any], params: Optional[Dict[str, Any]]
    ) -> PipelineConfig:
        if params:
            raw = apply_template(json.dumps(config), params)
            config = json.loads(raw)
        config = resolve_includes(config, Path.cwd(), params)
        return PipelineConfig.from_dict(config)

    def _apply_spark_override(self, config: PipelineConfig) -> None:
        """Garante que configs Spark do framework prevalecem sobre o JSON."""
        if self._spark_config.app_name != "Sparquet":
            config.spark.app_name = self._spark_config.app_name
        if self._spark_config.master != "local[*]":
            config.spark.master = self._spark_config.master
        config.spark.configs.update(self._spark_config.configs)
