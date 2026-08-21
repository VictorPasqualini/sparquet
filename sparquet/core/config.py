from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class SparkConfig:
    app_name: str = "Sparquet"
    master: str = "local[*]"
    configs: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SparkConfig:
        return cls(
            app_name=data.get("app_name", "Sparquet"),
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


# Chaves que só existem numa quarentena (`validations.outputs`). Fora dela não há
# linha rejeitada para escopar nem para rotular, então valem como erro de config em
# vez de virarem chave morta no JSON.
_QUARANTINE_ONLY_KEYS = ("rules", "annotate")


def _quarantine_rules(value: Any) -> Optional[List[str]]:
    """`rules` de uma quarentena: lista de códigos de regra."""
    if value is None:
        return None
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        raise ValueError(
            "validations.outputs.invalid.rules precisa ser uma LISTA de códigos de regra "
            "(ex: ['AGE_RANGE', 'not_null(email)']), não "
            f"{type(value).__name__}."
        )
    codes = [str(code).strip() for code in value]
    if any(not code for code in codes):
        raise ValueError(
            "validations.outputs.invalid.rules tem um código vazio. Cada item é o `code` "
            "de uma regra, ou a expressão dela quando o `code` é omitido."
        )
    return codes


def _annotate_column(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "validations.outputs.invalid.annotate precisa ser o NOME da coluna de "
            "códigos (string não vazia)."
        )
    return value.strip()


def _reject_quarantine_keys(data: Dict[str, Any], where: str, reason: str) -> None:
    """Recusa `rules`/`annotate` num destino que não é quarentena.

    Deixar a chave passar em silêncio é pior do que falhar: o usuário fica com um JSON
    que promete rastreabilidade e um destino que nunca a recebe.
    """
    for key in _QUARANTINE_ONLY_KEYS:
        if not isinstance(data, dict) or data.get(key) is None:
            continue
        raise ValueError(
            f"'{key}' só existe na quarentena `validations.outputs.invalid`, e "
            f"apareceu em {where}. {reason} "
            "`annotate` nomeia a coluna array<string> com os códigos das regras que "
            "rejeitaram a linha e `rules` restringe o split a alguns códigos — as duas "
            "só existem onde há linha rejeitada."
        )


@dataclass
class ValidationConfig:
    on_failure: str = "fail"  # fail | warn | skip
    rules: List[ValidationRule] = field(default_factory=list)
    # Destino opcional para gravar o resultado das validações (uma linha por regra:
    # pipeline, rule_type, passed, failed_count, message, validated_at). Serve para
    # análise/observabilidade de qualidade. Aceita qualquer formato de saída.
    report: Optional["OutputConfig"] = None
    # Roteamento de LINHAS (quarentena) — apartado da(s) saída(s) principal(is). Chaves
    # reconhecidas: "valid" e "invalid" (uma linha é inválida quando viola qualquer
    # check row-level: not_null, range, regex, unique, e o `check` de missing/invalid).
    # Cada valor é um destino de escrita completo (format/path/mode/columns/options),
    # mais duas chaves exclusivas do lado `invalid`:
    #   • `rules`    – lista de CÓDIGOS de regra: só elas alimentam esta quarentena
    #                  (ausente = todas as row-level, o comportamento histórico);
    #   • `annotate` – nome da coluna array<string> com os códigos das regras que
    #                  rejeitaram cada linha.
    outputs: Dict[str, "OutputConfig"] = field(default_factory=dict)
    # Materializa o df antes de validar. Cada regra dispara a própria action e, sem
    # cache, todas recomputam a linhagem desde a fonte. Default True porque quase
    # sempre é o certo; desligue se o df for grande demais para caber em memória +
    # disco do executor e você preferir pagar as releituras.
    cache: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ValidationConfig:
        return cls(
            on_failure=data.get("on_failure", "fail"),
            cache=bool(data.get("cache", True)),
            rules=[ValidationRule.from_dict(r) for r in data.get("rules", [])],
            report=(
                _validation_report(data["report"]) if data.get("report") else None
            ),
            outputs={
                key: _quarantine_output(key, out)
                for key, out in data.get("outputs", {}).items()
            },
        )


def _validation_report(data: Dict[str, Any]) -> "OutputConfig":
    _reject_quarantine_keys(
        data,
        "validations.report",
        "O relatório tem schema fixo — uma linha por REGRA, nunca uma linha de dados.",
    )
    return OutputConfig.from_dict(data)


def _quarantine_output(key: str, data: Dict[str, Any]) -> "OutputConfig":
    """Um destino de `validations.outputs` — só o `invalid` escopa e rotula.

    O lado válido é definido por exclusão (não violou NADA), então nem código para
    rotular nem escopo para restringir fazem sentido nele.
    """
    if key != "invalid":
        _reject_quarantine_keys(
            data,
            f"`validations.outputs.{key}`",
            "Uma linha válida não violou nenhuma regra — não há código para rotulá-la "
            "nem violação para escopar.",
        )
    return OutputConfig.from_dict(data)


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

    `rules` e `annotate` só valem na quarentena `validations.outputs.invalid` — ver
    `ValidationConfig`, que recusa os dois em qualquer outro destino.
    """

    format: str
    path: str
    mode: str = "overwrite"  # append | overwrite | merge
    partition_by: List[str] = field(default_factory=list)
    columns: Optional[List[str]] = None
    options: Dict[str, Any] = field(default_factory=dict)
    transformations: List["TransformationConfig"] = field(default_factory=list)
    # Quarentena `invalid`: restringe o destino às regras cujos CÓDIGOS estão na lista
    # (`code` da regra, ou a expressão dela quando o `code` é omitido — ver
    # `BaseCheck.code()` no sparquet_cola). None = todas as regras row-level.
    rules: Optional[List[str]] = None
    # Quarentena `invalid`: nome da coluna `array<string>` com os códigos das regras
    # que rejeitaram cada linha. Só faz sentido no lado inválido.
    annotate: Optional[str] = None

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
            rules=_quarantine_rules(data.get("rules")),
            annotate=_annotate_column(data.get("annotate")),
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
        main_reason = (
            "As saídas principais recebem o df COMPLETO — válidas e inválidas juntas — "
            "então não há linha rejeitada para rotular nem para escopar nelas."
        )
        if "outputs" in data:
            for index, out in enumerate(data["outputs"]):
                _reject_quarantine_keys(out, f"outputs[{index}]", main_reason)
            outputs = [OutputConfig.from_dict(o) for o in data["outputs"]]
        elif "output" in data:
            _reject_quarantine_keys(data["output"], "output", main_reason)
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
