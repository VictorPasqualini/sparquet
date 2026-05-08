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


@dataclass
class TransformationConfig:
    type: str
    params: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TransformationConfig:
        return cls(
            type=data["type"],
            params={k: v for k, v in data.items() if k != "type"},
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

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ValidationConfig:
        return cls(
            on_failure=data.get("on_failure", "fail"),
            rules=[ValidationRule.from_dict(r) for r in data.get("rules", [])],
        )


@dataclass
class OutputConfig:
    """Configuração de um destino de escrita do pipeline.

    O campo `columns` permite selecionar quais colunas serão escritas
    neste destino específico, sem alterar o DataFrame das demais saídas.
    Se omitido, todas as colunas são escritas.
    """

    format: str
    path: str
    mode: str = "overwrite"  # append | overwrite | merge
    partition_by: List[str] = field(default_factory=list)
    columns: Optional[List[str]] = None
    options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> OutputConfig:
        return cls(
            format=data["format"].lower(),
            path=data["path"],
            mode=data.get("mode", "overwrite"),
            partition_by=data.get("partition_by", []),
            columns=data.get("columns"),
            options=data.get("options", {}),
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
        content = Path(path).read_text(encoding="utf-8")
        return cls.from_dict(json.loads(content))

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
