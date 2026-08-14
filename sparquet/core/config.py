from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class SparkConfig:
    app_name: str = "SparkFramework"
    master: str = "local[*]"
    configs: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SparkConfig:
        return cls(
            app_name=data.get("app_name", "SparkFramework"),
            master=data.get("master", "local[*]"),
            configs=data.get("configs", {}),
        )


@dataclass
class InputConfig:
    """Configuração da fonte de dados principal do pipeline."""

    format: str
    path: str
    options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> InputConfig:
        return cls(
            format=data["format"].lower(),
            path=data["path"],
            options=data.get("options", {}),
        )


_TRANSFORMATION_META_KEYS = {"type", "skip_if_false"}


@dataclass
class TransformationConfig:
    type: str
    params: Dict[str, Any] = field(default_factory=dict)
    skip_if_false: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TransformationConfig:
        return cls(
            type=data["type"],
            skip_if_false=data.get("skip_if_false"),
            params={k: v for k, v in data.items() if k not in _TRANSFORMATION_META_KEYS},
        )


@dataclass
class ValidationRule:
    type: str
    params: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ValidationRule:
        return cls(
            type=data["type"],
            params={k: v for k, v in data.items() if k != "type"},
        )


@dataclass
class ValidationConfig:
    on_failure: str = "fail"  # fail | warn | skip
    rules: List[ValidationRule] = field(default_factory=list)
    # Destino opcional para gravar o resultado das validações (uma linha por regra:
    # pipeline, rule_type, passed, failed_count, message, validated_at). Serve para
    # análise/observabilidade de qualidade. Aceita qualquer formato de saída.
    report: Optional["OutputConfig"] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ValidationConfig:
        return cls(
            on_failure=data.get("on_failure", "fail"),
            rules=[ValidationRule.from_dict(r) for r in data.get("rules", [])],
            report=(
                OutputConfig.from_dict(data["report"]) if data.get("report") else None
            ),
        )


@dataclass
class OutputConfig:
    """Configuração de um destino de escrita do pipeline.

    O campo `columns` permite selecionar quais colunas serão escritas
    neste destino específico, sem alterar o DataFrame das demais saídas.
    Se omitido, todas as colunas são escritas.

    O campo `transformations` aplica transformações próprias deste destino sobre
    o DataFrame transformado, antes da projeção de `columns` e da escrita — sem
    afetar as demais saídas. Permite gravar formas diferentes (ex: explode,
    to_json, join, colunas extras) a partir do mesmo df. Aceita todos os tipos
    de transformação do TransformationEngine (inclusive {{var}} de runtime).
    """

    format: str
    path: str
    mode: str = "overwrite"  # append | overwrite | merge
    partition_by: List[str] = field(default_factory=list)
    columns: Optional[List[str]] = None
    options: Dict[str, Any] = field(default_factory=dict)
    transformations: List["TransformationConfig"] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> OutputConfig:
        return cls(
            format=data["format"].lower(),
            path=data["path"],
            mode=data.get("mode", "overwrite"),
            partition_by=data.get("partition_by", []),
            columns=data.get("columns"),
            options=data.get("options", {}),
            transformations=[
                TransformationConfig.from_dict(t)
                for t in data.get("transformations", [])
            ],
        )


@dataclass
class PipelineConfig:
    name: str
    input: InputConfig
    outputs: List[OutputConfig]
    description: str = ""
    spark: SparkConfig = field(default_factory=SparkConfig)
    transformations: List[TransformationConfig] = field(default_factory=list)
    validations: ValidationConfig = field(default_factory=ValidationConfig)

    @classmethod
    def from_file(cls, path: str) -> PipelineConfig:
        from sparquet.utils.includes import resolve_includes
        content = Path(path).read_text(encoding="utf-8")
        data = json.loads(content)
        data = resolve_includes(data, Path(path).parent)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PipelineConfig:
        # Aceita "output" (objeto único) ou "outputs" (lista)
        if "outputs" in data:
            outputs = [OutputConfig.from_dict(o) for o in data["outputs"]]
        elif "output" in data:
            outputs = [OutputConfig.from_dict(data["output"])]
        else:
            raise ValueError(
                "O JSON do pipeline precisa ter 'output' (objeto) ou 'outputs' (lista)."
            )

        return cls(
            name=data["name"],
            description=data.get("description", ""),
            spark=SparkConfig.from_dict(data.get("spark", {})),
            input=InputConfig.from_dict(data["input"]),
            transformations=[
                TransformationConfig.from_dict(t)
                for t in data.get("transformations", [])
            ],
            validations=ValidationConfig.from_dict(data.get("validations", {})),
            outputs=outputs,
        )
