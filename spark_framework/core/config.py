from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


# ${name} ou ${name!modifier} (modifier opcional: 'raw')
_PARAM_PATTERN = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)(?:!(\w+))?\}")


def _format_sql_value(value: Any) -> str:
    """Formata um valor Python como literal SQL adequado para uso em expressões.

    Mapping:
      None              → NULL
      bool              → true | false
      int/float         → 123  (sem aspas)
      str               → 'value'  (com aspas, escapando aspas internas)
      list/tuple não-vazia → array(item1, item2, ...)  (cada item formatado SQL)
      list/tuple vazia  → NULL

    Útil para uso direto em transformations:
      "condition": "tipo = ${param_tipo}"         → tipo = 'NC'
      "condition": "array_contains(${param_lista}, id)" → array_contains(array('a','b'), id)
      "skip_if":   "${param_lista} IS NULL"       → array(...) IS NULL  (false → não skipa)
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, (list, tuple)):
        if not value:
            return "NULL"
        return "array(" + ", ".join(_format_sql_value(v) for v in value) + ")"
    # fallback: trata como string
    return _format_sql_value(str(value))


def _format_raw_value(value: Any) -> str:
    """Formata um valor como literal raw (sem escape SQL).

    Útil para uso em paths, options e outros campos não-SQL:
      "path": "${param_topico!raw}"  + param_topico="vertc-topic" → "vertc-topic"

    Listas são joined com vírgula (raro, mas suportado).
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return ",".join(_format_raw_value(v) for v in value)
    return str(value)


def substitute_params(data: Any, params: Dict[str, Any]) -> Any:
    """Substitui ocorrências de ${name} (e ${name!raw}) em strings do dict.

    Aplicado recursivamente em dicts, lists e strings.

    Sintaxe:
      ${name}      → SQL value escapado (default, para uso em expressions)
      ${name!raw}  → valor literal sem escape (para paths/options)

    Se o param não está em 'params', mantém o placeholder literal (não substitui).
    """
    if isinstance(data, dict):
        return {k: substitute_params(v, params) for k, v in data.items()}
    if isinstance(data, list):
        return [substitute_params(item, params) for item in data]
    if isinstance(data, str):
        def _replace(match: "re.Match[str]") -> str:
            name = match.group(1)
            modifier = match.group(2)
            if name not in params:
                return match.group(0)
            value = params[name]
            if modifier == "raw":
                return _format_raw_value(value)
            return _format_sql_value(value)
        return _PARAM_PATTERN.sub(_replace, data)
    return data


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

    Campos opcionais para projeção/transformação por output:
      columns         – projeta apenas estas colunas (após transformations).
      transformations – lista de transformações aplicadas ao df ANTES de
                        escrever neste destino. Útil quando o mesmo pipeline
                        grava em destinos com granularidades diferentes (ex:
                        Kafka 1-msg-por-contrato + Delta parcelas via explode).
                        As transformações suportam todos os types builtin
                        (filter, with_column, select, drop, explode-via-with_column, etc.)
                        e são aplicadas em um clone do df principal, sem
                        afetar outros outputs.
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
    params: List[str] = field(default_factory=list)
    """Lista declarativa de params de runtime esperados (recebidos via columns={}).

    Quando definido, o framework valida que todos os params declarados foram
    fornecidos (com warning se algum estiver faltando). Documenta a interface
    da conf — quem chama sabe que params precisa passar."""

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
            params=list(data.get("params", [])),
        )
