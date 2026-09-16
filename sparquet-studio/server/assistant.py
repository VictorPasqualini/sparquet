"""The assistant, answered by the runner instead of by the browser.

Studio has always been able to talk to a model: `src/lib/ai/` calls Anthropic,
OpenAI or Gemini straight from the page with a key the user pastes into
Settings. That is the right default for somebody trying the Studio out, and it
is the wrong one for a team, for three reasons that all point here:

* **The key.** A key in browser storage is a key on every machine that opens the
  Studio. A team that has one account wants the account on the server.
* **The tools.** A model in the browser can only see what the page pasted into
  the prompt. A model on the runner can call the runner: parse a configuration
  the way the framework parses it, list the formats this install really has,
  read the catalog. That is the difference between a chat about Sparquet and an
  assistant that can check its own answer.
* **The bill.** Nothing in the browser is metered. Everything here is.

So this module is a seam with two runtimes behind it:

`ollama`
    A model on this machine, over Ollama's HTTP API. No key, no account, no
    request that leaves the host — and therefore no cost, which is the whole
    reason it is the default. The tool loop is ours: Ollama speaks the
    OpenAI-shaped function-calling format, so the specs in `TOOLS` are handed to
    it verbatim and the calls come back as `message.tool_calls`.

`omnigent`
    The same tools, driven by Omnigent's executor instead of by our loop, which
    is what buys sub-agents, policies and MCP servers. Optional: Omnigent is a
    thirteen-megabyte wheel with forty dependencies and a Python 3.12 floor,
    none of which a runner should carry to answer "why did my join lose rows".
    It is imported lazily and the endpoint says so clearly when it is absent.

Both are metered the same way, through `credits.record_assist`, and a turn whose
model ran on this machine is recorded at a cost of zero rather than not recorded
at all — see the `assist_usage` table for why that distinction is the point.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence

#: Where Ollama listens when nobody has said otherwise. `127.0.0.1` rather than
#: `localhost` so the runner never waits on an IPv6 resolution that nothing is
#: listening on — a well-known way for a local model to look like a hung one.
DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434"

#: The model asked for when the operator has not chosen. Small enough for the
#: laptop of somebody who just wanted to try it, and — the part that is not
#: obvious — one that actually emits tool calls. A code-shaped model reads and
#: writes JSON better, but `qwen2.5-coder:7b` answers a tool question by printing
#: the call as prose while reporting `tools` in `ollama show`: nothing runs, the
#: user reads a JSON blob, and the turn is metered with zero tool calls. Tools
#: are what this backend is for, so the default is the model measured using them.
#: Free text, like every model id in this product: a model released tomorrow
#: works by typing its name.
DEFAULT_MODEL = "llama3.1:8b"

#: Long enough for a cold model to load off disk, short enough that a runner does
#: not hold a connection open all afternoon for a model nobody is serving.
CONNECT_TIMEOUT = 10.0
STREAM_TIMEOUT = 300.0

#: A model that decides to keep calling tools forever is a model that has
#: misunderstood the question. It gets this many rounds before the loop stops and
#: says so, which is a better answer than an endless one.
MAX_TOOL_ROUNDS = 6


class AssistantUnavailable(RuntimeError):
    """The assistant cannot answer, and it is a configuration fact rather than a
    failure of the turn.

    `hint` is the command or the setting that would fix it. It is separate from
    the message because the API returns both and the screen renders them
    differently — one is what happened, the other is what to do.
    """

    def __init__(self, message: str, *, hint: str = "") -> None:
        super().__init__(message)
        self.hint = hint


# ------------------------------------------------------------------ settings


def backend_name() -> str:
    """Which runtime answers. `ollama` unless the operator says `omnigent`.

    `off` is a real value: a runner that should not answer questions at all, only
    run pipelines. The routes then 503 with that said plainly instead of the
    Studio showing an assistant that silently never replies.
    """
    chosen = os.getenv("SPARQUET_STUDIO_ASSISTANT", "").strip().lower()
    if chosen in {"off", "none", "disabled"}:
        return "off"
    if chosen in {"omnigent", "agent"}:
        return "omnigent"
    return "ollama"


def ollama_url() -> str:
    return (os.getenv("SPARQUET_STUDIO_OLLAMA_URL", "").strip() or DEFAULT_OLLAMA_URL).rstrip("/")


def default_model() -> str:
    return os.getenv("SPARQUET_STUDIO_ASSISTANT_MODEL", "").strip() or DEFAULT_MODEL


def omnigent_agent_path() -> str:
    """The YAML agent definition Omnigent should load, when there is one.

    Empty means "build one from the prompt and tools in this module", which is
    what a runner that has never been configured wants. A team that outgrows that
    writes its own YAML and points here.
    """
    return os.getenv("SPARQUET_STUDIO_OMNIGENT_AGENT", "").strip()


# -------------------------------------------------------------------- prompt


SYSTEM_PROMPT = """You are the assistant inside Sparquet Studio.

Sparquet is a JSON-driven framework on PySpark. One JSON file describes one job:
an `input`, an optional `input_view`, a list of `transformations`, a
`validations` block, and one or more `outputs`. The framework reads that file and
runs it; nothing else is written in Python. In the Studio that file is called a
Job, a Pipeline is an ordered sequence of Jobs, and the folder holding them is a
Workflow — the framework's own API calls the file a pipeline, and that difference
is deliberate, so use the Studio's words when talking about the screen and the
framework's words when talking about the Python API.

You have tools that talk to this installation. Use them rather than guessing:
`list_formats` says which readers and writers this install really has, and
`validate_config` parses a configuration exactly as the framework will. When you
produce a configuration of any size, validate it before you present it.

Two things that are easy to get wrong here, and worth saying plainly when they
come up: transformations change the data while validations only report on it, so
to find out which rows failed a rule you need the quarantine with `annotate` and
not the aggregate; and `filter` and `select` belong at the front of the
transformation chain, before any join, struct or group_by.

Answer in the language the question was asked in. Be concrete: show the JSON.
"""


#: Characters of caller instructions kept. The Studio's canvas prompt is built
#: from its catalog and runs to a few thousand; a caller sending more than this
#: is sending a document, and the tail of a document is not guidance.
MAX_INSTRUCTIONS = 24_000


def prompt_with(instructions: str = "") -> str:
    """The runner's prompt, plus whatever the caller wants the model to also do.

    Appended rather than replaced, and appended *after*, so a caller cannot drop
    the part that describes the tools this installation has — that section is the
    only reason to ask the runner instead of a vendor, and a browser cannot write
    it because it does not know what is installed.

    The Studio's canvas panel is why this exists: its prompt is generated from
    the catalog that drives the forms, and it asks for a very specific JSON
    envelope back. Without it, choosing the runner would quietly turn the panel
    into a chat that cannot propose anything.
    """
    extra = (instructions or "").strip()[:MAX_INSTRUCTIONS]
    if not extra:
        return SYSTEM_PROMPT
    return f"{SYSTEM_PROMPT}\n\nThe caller added these instructions:\n\n{extra}"


# --------------------------------------------------------------------- tools


@dataclass
class Tool:
    """One thing the assistant can do to this runner.

    `spec` is the OpenAI-shaped function schema, which is what Ollama and
    Omnigent both take. `run` is the Python behind it. Both live in one object so
    a tool cannot be advertised without being implemented.
    """

    name: str
    spec: Dict[str, Any]
    run: Callable[[Dict[str, Any]], Any]


def _tool_list_formats(_: Dict[str, Any]) -> Dict[str, Any]:
    """Which formats this install can actually read and write.

    Read from the registries rather than from a list in this file, because a
    registry is extensible at runtime: a team that registered its own reader
    should see it here, and a model told about a format the install does not have
    will confidently write a configuration that cannot run.
    """
    try:
        from sparquet.io.factory import ReaderFactory, WriterFactory  # type: ignore
    except Exception as error:  # pragma: no cover - depends on the install
        return {"error": f"The framework is not importable here: {error}"}
    return {
        "read": sorted(ReaderFactory._registry),
        "write": sorted(WriterFactory._registry),
    }


def _tool_validate_config(args: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a configuration the way the framework parses it.

    The same code path `/validate` uses, so an answer that passes here passes
    there. Nothing is executed and no session is started: this is `from_dict`,
    not a run.
    """
    config = args.get("config")
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError as error:
            return {"valid": False, "error": f"That is not valid JSON: {error}"}
    if not isinstance(config, dict):
        return {"valid": False, "error": "`config` must be a JSON object."}
    try:
        from sparquet.core.config import PipelineConfig  # type: ignore
    except Exception as error:  # pragma: no cover - depends on the install
        return {"valid": False, "error": f"The framework is not importable here: {error}"}
    try:
        PipelineConfig.from_dict(config)
    except Exception as error:
        return {"valid": False, "error": f"{type(error).__name__}: {error}"}
    return {"valid": True}


def _function(name: str, description: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {"name": name, "description": description, "parameters": parameters},
    }


def _flat(spec: Dict[str, Any]) -> Dict[str, Any]:
    """The same tool, with the `function` envelope unwrapped.

    Omnigent reads `name`, `description` and `parameters` off the top of each
    spec. Handed the OpenAI-shaped one it finds no name, and a spec with no name
    is skipped rather than rejected — so the symptom is not an error, it is an
    assistant that answers from memory because it was registered with no tools
    at all. Converting here keeps `TOOLS` in the shape Ollama wants, which is
    also the shape the docs and the tests describe.
    """
    inner = spec.get("function", spec)
    return {
        "name": inner.get("name", ""),
        "description": inner.get("description", ""),
        "parameters": inner.get("parameters", {"type": "object", "properties": {}}),
    }


TOOLS: List[Tool] = [
    Tool(
        name="list_formats",
        spec=_function(
            "list_formats",
            "List the input and output formats this Sparquet installation supports. "
            "Call this before naming a format you are not certain about.",
            {"type": "object", "properties": {}},
        ),
        run=_tool_list_formats,
    ),
    Tool(
        name="validate_config",
        spec=_function(
            "validate_config",
            "Parse a Sparquet job configuration exactly as the framework parses it "
            "and report whether it is valid, with the error if it is not. Nothing "
            "is executed. Call this on every configuration you are about to show.",
            {
                "type": "object",
                "properties": {
                    "config": {
                        "type": "object",
                        "description": "The whole job configuration, as a JSON object.",
                    }
                },
                "required": ["config"],
            },
        ),
        run=_tool_validate_config,
    ),
]

TOOLS_BY_NAME: Dict[str, Tool] = {tool.name: tool for tool in TOOLS}

#: What goes on the wire. Kept apart from `TOOLS` so a caller can hand the specs
#: to a runtime that does its own dispatch without also handing it our callables.
TOOL_SPECS: List[Dict[str, Any]] = [tool.spec for tool in TOOLS]

#: The same tools in Omnigent's flat shape. Two lists rather than one converted
#: at the call site, because which shape a runtime wants is a fact about that
#: runtime and belongs next to the specs it describes.
FLAT_TOOL_SPECS: List[Dict[str, Any]] = [_flat(tool.spec) for tool in TOOLS]


def call_tool(name: str, args: Dict[str, Any]) -> Any:
    """Run one tool by name, turning every failure into a result.

    A tool that raises must not end the turn: the model asked for something and
    is owed an answer, even when the answer is "that did not work". An exception
    escaping here would abort a stream that was about to be useful.
    """
    tool = TOOLS_BY_NAME.get(name)
    if tool is None:
        return {"error": f"There is no tool called {name!r}."}
    try:
        return tool.run(args if isinstance(args, dict) else {})
    except Exception as error:  # pragma: no cover - defensive
        return {"error": f"{type(error).__name__}: {error}"}


# -------------------------------------------------------------------- events


@dataclass
class Usage:
    """What one turn consumed, as the runtime reported it.

    `local` is the field the billing screen turns into money: it is the runtime's
    statement that the tokens were produced on this machine. Token counts are
    best-effort — several providers report none — which is why the cost is per
    turn and these are only for the reading.
    """

    model: str
    provider: str
    local: bool
    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    duration_ms: int = 0


@dataclass
class Event:
    """One thing to send to the browser.

    `delta` carries text as it arrives; `tool` says a tool was called and what it
    answered, so the transcript can show the work instead of a pause; `done`
    closes the turn and carries the `Usage`; `error` ends it with a reason.
    """

    kind: str
    text: str = ""
    name: str = ""
    args: Dict[str, Any] = field(default_factory=dict)
    result: Any = None
    usage: Optional[Usage] = None

    def payload(self) -> Dict[str, Any]:
        if self.kind == "delta":
            return {"text": self.text}
        if self.kind == "tool":
            return {"name": self.name, "args": self.args, "result": self.result}
        if self.kind == "error":
            return {"message": self.text}
        usage = self.usage
        return {
            "model": usage.model if usage else "",
            "provider": usage.provider if usage else "",
            "local": usage.local if usage else True,
            "inputTokens": usage.input_tokens if usage else 0,
            "outputTokens": usage.output_tokens if usage else 0,
            "toolCalls": usage.tool_calls if usage else 0,
            "durationMs": usage.duration_ms if usage else 0,
        }


def turns_of(messages: Sequence[Any]) -> List[Dict[str, str]]:
    """Normalize whatever the request carried into `{role, content}` pairs.

    Anything that is not a user or assistant turn with text is dropped rather
    than rejected: the transcript belongs to the browser, and a turn the browser
    invented a new shape for should degrade to a shorter conversation, not a 422.
    """
    out: List[Dict[str, str]] = []
    for message in messages or []:
        role = getattr(message, "role", None)
        content = getattr(message, "content", None)
        if role is None and isinstance(message, dict):
            role, content = message.get("role"), message.get("content")
        if role not in {"user", "assistant"} or not isinstance(content, str):
            continue
        if content.strip():
            out.append({"role": role, "content": content})
    return out


# ------------------------------------------------------------------- ollama


def _get_json(url: str, *, timeout: float) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


class OllamaBackend:
    """A model on this machine, over Ollama's `/api/chat`.

    Ollama rather than an OpenAI-compatible gateway even though Ollama offers
    one, because `/api/chat` reports `prompt_eval_count` and `eval_count` on the
    final frame and `/v1/chat/completions` does not report usage at all when
    streaming. The metering is the reason this class exists in the first place.

    No key, no account, no egress. Every turn it answers is free, and it says so
    to the ledger by reporting `local=True`.
    """

    id = "ollama"

    def __init__(self, base_url: Optional[str] = None, model: Optional[str] = None) -> None:
        self.base_url = (base_url or ollama_url()).rstrip("/")
        self.model = model or default_model()

    # ---- description -----------------------------------------------------

    def models(self) -> List[str]:
        """What is pulled on this host. Empty when Ollama is not answering."""
        try:
            body = _get_json(f"{self.base_url}/api/tags", timeout=CONNECT_TIMEOUT)
        except Exception:
            return []
        models = body.get("models") if isinstance(body, dict) else None
        if not isinstance(models, list):
            return []
        names = [str(entry.get("name")) for entry in models if isinstance(entry, dict)]
        return sorted(name for name in names if name and name != "None")

    def describe(self) -> Dict[str, Any]:
        models = self.models()
        available = bool(models)
        detail: Dict[str, Any] = {
            "backend": self.id,
            "available": available,
            "local": True,
            "baseUrl": self.base_url,
            "model": self.model,
            "models": models,
            "tools": [tool.name for tool in TOOLS],
        }
        if not available:
            detail["hint"] = (
                f"No model answered at {self.base_url}. Start Ollama and pull one: "
                f"`ollama pull {self.model}`."
            )
        elif self.model not in models:
            # Not an error: Ollama pulls on demand, so a model that is not here
            # yet is a download and not a mistake. Worth saying, though, because
            # the first turn will then take minutes instead of seconds.
            detail["hint"] = (
                f"`{self.model}` is not pulled yet — the first turn will download it. "
                f"`ollama pull {self.model}` does it now instead."
            )
        return detail

    # ---- the turn --------------------------------------------------------

    def stream(
        self, turns: Sequence[Dict[str, str]], *, system: str = SYSTEM_PROMPT,
        model: Optional[str] = None,
    ) -> Iterator[Event]:
        """One turn, with the tool loop, as a stream of `Event`.

        The loop is here rather than in the model because Ollama dispatches
        nothing: it reports that a tool should be called and waits for the result
        to come back as another message. Each round appends the assistant's call
        and our answer to the conversation and asks again, until the model
        answers with text or `MAX_TOOL_ROUNDS` is spent.
        """
        chosen = (model or self.model).strip() or self.model
        conversation: List[Dict[str, Any]] = [{"role": "system", "content": system}]
        conversation.extend(dict(turn) for turn in turns)

        started = time.perf_counter()
        input_tokens = output_tokens = tool_calls = 0

        for _ in range(MAX_TOOL_ROUNDS):
            text_parts: List[str] = []
            pending: List[Dict[str, Any]] = []
            try:
                for frame in self._frames(chosen, conversation):
                    message = frame.get("message") if isinstance(frame, dict) else None
                    if isinstance(message, dict):
                        chunk = message.get("content")
                        if isinstance(chunk, str) and chunk:
                            text_parts.append(chunk)
                            yield Event(kind="delta", text=chunk)
                        calls = message.get("tool_calls")
                        if isinstance(calls, list):
                            pending.extend(call for call in calls if isinstance(call, dict))
                    if frame.get("done"):
                        input_tokens += int(frame.get("prompt_eval_count") or 0)
                        output_tokens += int(frame.get("eval_count") or 0)
            except AssistantUnavailable as error:
                yield Event(kind="error", text=str(error))
                return

            if not pending:
                yield Event(
                    kind="done",
                    usage=Usage(
                        model=chosen, provider=self.id, local=True,
                        input_tokens=input_tokens, output_tokens=output_tokens,
                        tool_calls=tool_calls,
                        duration_ms=int((time.perf_counter() - started) * 1000),
                    ),
                )
                return

            conversation.append(
                {
                    "role": "assistant",
                    "content": "".join(text_parts),
                    "tool_calls": pending,
                }
            )
            for call in pending:
                function = call.get("function")
                function = function if isinstance(function, dict) else {}
                name = str(function.get("name") or "")
                args = function.get("arguments")
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except json.JSONDecodeError:
                        args = {}
                args = args if isinstance(args, dict) else {}
                result = call_tool(name, args)
                tool_calls += 1
                yield Event(kind="tool", name=name, args=args, result=result)
                conversation.append(
                    {
                        "role": "tool",
                        "name": name,
                        "content": json.dumps(result, ensure_ascii=False, default=str),
                    }
                )

        yield Event(
            kind="error",
            text=(
                f"The model called tools {MAX_TOOL_ROUNDS} times without answering. "
                f"Try asking something narrower, or a larger model."
            ),
        )

    def _frames(
        self, model: str, conversation: Sequence[Dict[str, Any]]
    ) -> Iterator[Dict[str, Any]]:
        """Ollama's newline-delimited JSON, one object per line."""
        body = {
            "model": model,
            "messages": list(conversation),
            "stream": True,
            "tools": TOOL_SPECS,
        }
        request = urllib.request.Request(
            f"{self.base_url}/api/chat",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            response = urllib.request.urlopen(request, timeout=STREAM_TIMEOUT)
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", "replace")[:400]
            raise AssistantUnavailable(
                f"Ollama refused the request ({error.code}): {detail}",
                hint=f"`ollama pull {model}` if the model is the problem.",
            ) from error
        except Exception as error:
            raise AssistantUnavailable(
                f"No model answered at {self.base_url}: {error}",
                hint="Start Ollama, then pull a model.",
            ) from error

        with response:
            for line in response:
                line = line.strip()
                if not line:
                    continue
                try:
                    frame = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                if isinstance(frame, dict):
                    if isinstance(frame.get("error"), str):
                        raise AssistantUnavailable(str(frame["error"]))
                    yield frame


# ----------------------------------------------------------------- omnigent


_USAGE_ASKED = False


def _ask_for_usage() -> None:
    """Make the Agents SDK request token counts from a non-OpenAI endpoint.

    The SDK only sends `stream_options: {"include_usage": true}` when it
    recognises the endpoint as OpenAI's own — `ChatCmplHelpers.is_openai` is a
    prefix test against `https://api.openai.com` — and Omnigent never sets
    `include_usage` itself. Point the same client at Ollama and no streamed
    chunk carries a usage block, so `TurnComplete.usage` is empty and Billing
    records a turn that read and wrote nothing. Ollama does report usage; it
    only has to be asked, which is what this does.

    Narrow on purpose. `is_openai` also decides `store`, and asking a local
    server to store the conversation is an OpenAI-side feature it would reject,
    so only the stream options are touched — and only when nothing else already
    decided them.
    """
    global _USAGE_ASKED
    if _USAGE_ASKED:
        return
    _USAGE_ASKED = True
    try:
        from agents.models.chatcmpl_helpers import ChatCmplHelpers
    except ImportError:  # pragma: no cover - the SDK moved or is absent
        return
    original = ChatCmplHelpers.get_stream_options_param.__func__

    def with_usage(cls: Any, client: Any, model_settings: Any, stream: bool) -> Any:
        chosen = original(cls, client, model_settings, stream)
        if stream and chosen is None:
            return {"include_usage": True}
        return chosen

    ChatCmplHelpers.get_stream_options_param = classmethod(with_usage)


def _omnigent() -> Any:
    """The package, or a refusal that says how to get it.

    Imported here and nowhere else. A runner that will never use this backend
    must not pay for the import — it is thirteen megabytes of wheel and pulls in
    two agent SDKs of its own.
    """
    try:
        import omnigent  # type: ignore
    except ImportError as error:
        raise AssistantUnavailable(
            "Omnigent is not installed on this runner.",
            hint=(
                "`pip install omnigent` (Python 3.12 or newer), or leave "
                "SPARQUET_STUDIO_ASSISTANT unset to use Ollama directly."
            ),
        ) from error
    return omnigent


def _omnigent_version(module: Any) -> str:
    """Which Omnigent this is, asked of the package rather than the module.

    0.14.0 exports no `__version__`, so reading the attribute reports an empty
    string on a perfectly healthy install — and "available, version unknown" is
    the shape of a report nobody trusts. The distribution metadata is there
    either way.
    """
    declared = getattr(module, "__version__", "")
    if declared:
        return str(declared)
    try:
        from importlib import metadata

        return metadata.version("omnigent")
    except Exception:  # pragma: no cover - a source checkout has no metadata
        return ""


class OmnigentBackend:
    """The same tools, driven by Omnigent's executor.

    What it buys over calling Ollama directly is everything above the single
    turn: sub-agents, policies that can refuse a tool call, MCP servers, and a
    YAML file the team can edit without touching this code. What it costs is the
    dependency, which is why it is opt-in.

    It is pointed at the same local Ollama by default, through the OpenAI Agents
    SDK executor and an OpenAI-compatible base URL — so choosing this backend
    changes the agent loop and does not, on its own, start spending money. When
    an operator points it somewhere else, `local` goes false here and the turn
    reaches the invoice through `credits.record_assist` like any other cost.
    """

    id = "omnigent"

    def __init__(
        self, base_url: Optional[str] = None, model: Optional[str] = None,
        *, agent_path: Optional[str] = None,
    ) -> None:
        # The OpenAI-compatible face of the same Ollama the other backend uses,
        # because that is the endpoint an agent SDK knows how to speak to.
        self.base_url = f"{(base_url or ollama_url()).rstrip('/')}/v1"
        self.model = model or default_model()
        self.agent_path = agent_path if agent_path is not None else omnigent_agent_path()

    @property
    def local(self) -> bool:
        """Whether the model is on this machine, decided by the address.

        A loopback base URL is the definition of "did not leave the host", and it
        is what the ledger is told. Anything else is charged, including a model
        on the next desk — the ledger's question is whether this runner paid for
        the compute, and it did not.
        """
        host = self.base_url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0]
        return host in {"127.0.0.1", "localhost", "::1", "[::1]"}

    def models(self) -> List[str]:
        """What this endpoint will serve, asked in OpenAI's own vocabulary.

        `GET /v1/models` rather than Ollama's `/api/tags`, because the base URL
        here is only promised to be OpenAI-compatible — llama.cpp and vLLM
        answer it too, and the point of this backend is that the server behind
        it is interchangeable.

        Local only. A hosted endpoint charges for the call in some plans, its
        catalogue is thousands of entries long, and neither belongs in a health
        report; the picker there is the operator's own list. Empty on any
        failure: the model still runs, the picker just falls back to typing.
        """
        if not self.local:
            return []
        try:
            body = _get_json(f"{self.base_url}/models", timeout=CONNECT_TIMEOUT)
        except Exception:
            return []
        entries = body.get("data") if isinstance(body, dict) else None
        if not isinstance(entries, list):
            return []
        names = [str(entry.get("id")) for entry in entries if isinstance(entry, dict)]
        return sorted(name for name in names if name and name != "None")

    def describe(self) -> Dict[str, Any]:
        detail: Dict[str, Any] = {
            "backend": self.id,
            "local": self.local,
            "baseUrl": self.base_url,
            "model": self.model,
            "models": self.models(),
            "agent": self.agent_path or "(built in)",
            "tools": [tool.name for tool in TOOLS],
        }
        try:
            module = _omnigent()
        except AssistantUnavailable as error:
            detail["available"] = False
            detail["hint"] = error.hint
            detail["error"] = str(error)
            return detail
        detail["available"] = True
        detail["version"] = _omnigent_version(module)
        return detail

    def _executor(self) -> Any:
        """The executor, pointed at our model and wired to our tools.

        Three things here are decided by what the package actually does rather
        than by what its agent YAML describes:

        * `auth: {type: api_key, …}` is the *spec* syntax. The constructor takes
          `api_key` and `base_url_override` as plain keywords, and rejects an
          `auth` keyword outright.
        * `use_responses` defaults to True, which is OpenAI's `/responses`
          endpoint. Ollama does not implement it — the default would make every
          turn fail against the very backend this is pointed at by default.
        * Tools are dispatched through `_tool_executor`, which Omnigent's own
          runtime adapter assigns directly. Without it the executor answers
          every call with "no tool executor", and the model reasons on from an
          error it cannot fix.
        """
        module = _omnigent()
        try:
            executor_cls = module.OpenAIAgentsSDKExecutor
        except AttributeError as error:  # pragma: no cover - version drift
            raise AssistantUnavailable(
                "This Omnigent does not expose OpenAIAgentsSDKExecutor.",
                hint="Install omnigent>=0.14.",
            ) from error
        # The key is a placeholder because Ollama does not check one and the SDK
        # refuses to build a client without something in the field.
        key = os.getenv("SPARQUET_STUDIO_ASSISTANT_KEY", "") or "sparquet-local"
        try:
            executor = executor_cls(
                api_key=key, base_url_override=self.base_url, use_responses=False,
            )
        except TypeError as error:  # pragma: no cover - version drift
            raise AssistantUnavailable(
                "This Omnigent's executor does not take an OpenAI-compatible base URL.",
                hint="Install omnigent>=0.14,<0.15.",
            ) from error
        executor._tool_executor = lambda name, args: call_tool(name, args)
        _ask_for_usage()
        return executor

    def stream(
        self, turns: Sequence[Dict[str, str]], *, system: str = SYSTEM_PROMPT,
        model: Optional[str] = None,
    ) -> Iterator[Event]:
        """One turn, translated from Omnigent's events into ours.

        `run_turn` is an async iterator and this route is a synchronous
        generator, so the loop below pumps it one event at a time on a private
        event loop. A private one rather than the ambient one: this generator is
        consumed from a worker thread, where there is no running loop to join.
        """
        module = _omnigent()
        chosen = (model or self.model).strip() or self.model
        started = time.perf_counter()
        tool_calls = 0
        streamed = False
        usage_report: Dict[str, Any] = {}

        # Dicts, not `omnigent.Message`, despite the `list[Message]` annotation
        # on `run_turn`: the executor reads `message.get("role")` and
        # `message["session_id"]` off them, and a dataclass instance dies on the
        # first `.get`. The annotation describes the protocol, the code describes
        # the contract, and the contract is what runs.
        messages = [dict(turn) for turn in turns]
        if messages:
            # A turn of its own. Omnigent keys an SDK session off this and
            # replays that session's history into the next turn; ours already
            # carries the whole transcript from the browser, so a shared key
            # would show the model its own past twice — and, on a runner with
            # more than one user, somebody else's.
            messages[-1]["session_id"] = f"sparquet-{uuid.uuid4().hex}"
        config = module.ExecutorConfig(model=chosen)

        loop = asyncio.new_event_loop()
        try:
            stream = self._executor().run_turn(
                messages, list(FLAT_TOOL_SPECS), system, config,
            )
            while True:
                try:
                    event = loop.run_until_complete(stream.__anext__())
                except StopAsyncIteration:
                    break
                except Exception as error:
                    yield Event(kind="error", text=f"{type(error).__name__}: {error}")
                    return

                name = type(event).__name__
                if name == "TextChunk":
                    streamed = True
                    yield Event(kind="delta", text=str(getattr(event, "text", "")))
                elif name == "ToolCallRequest":
                    tool_calls += 1
                    yield Event(
                        kind="tool",
                        name=str(getattr(event, "name", "")),
                        args=dict(getattr(event, "args", {}) or {}),
                    )
                elif name == "ToolCallComplete":
                    yield Event(
                        kind="tool",
                        name=str(getattr(event, "name", "")),
                        result=getattr(event, "result", None),
                    )
                elif name == "ExecutorError":
                    yield Event(
                        kind="error",
                        text=str(getattr(event, "message", None) or "The agent failed."),
                    )
                    return
                elif name == "TurnComplete":
                    reported = getattr(event, "usage", None)
                    if isinstance(reported, dict):
                        usage_report = reported
                    # A harness that streamed nothing still has to say something,
                    # so the final response is emitted when no delta preceded it.
                    final = getattr(event, "response", None)
                    if not streamed and isinstance(final, str) and final:
                        streamed = True
                        yield Event(kind="delta", text=final)
                    if not getattr(event, "continue_turn", False):
                        break
        finally:
            try:
                loop.close()
            except Exception:  # pragma: no cover - defensive
                pass

        yield Event(
            kind="done",
            usage=Usage(
                model=chosen, provider=self.id, local=self.local,
                input_tokens=int(usage_report.get("input_tokens") or 0),
                output_tokens=int(usage_report.get("output_tokens") or 0),
                tool_calls=tool_calls,
                duration_ms=int((time.perf_counter() - started) * 1000),
            ),
        )


# ------------------------------------------------------------------ the seam


def build(name: Optional[str] = None) -> Any:
    """The backend this runner answers with.

    Built per request rather than held as a singleton: every knob here is an
    environment variable, and a test that changes one should not have to reach
    into module state to be believed.
    """
    chosen = (name or backend_name()).strip().lower()
    if chosen == "off":
        raise AssistantUnavailable(
            "The assistant is turned off on this runner.",
            hint="Unset SPARQUET_STUDIO_ASSISTANT to turn it back on.",
        )
    if chosen == "omnigent":
        return OmnigentBackend()
    if chosen == "ollama":
        return OllamaBackend()
    raise AssistantUnavailable(
        f"There is no assistant backend called {chosen!r}.",
        hint="SPARQUET_STUDIO_ASSISTANT takes `ollama`, `omnigent` or `off`.",
    )


def describe() -> Dict[str, Any]:
    """What `/assistant` answers: what is configured, and whether it can answer."""
    chosen = backend_name()
    if chosen == "off":
        return {
            "backend": "off",
            "available": False,
            "local": True,
            "tools": [],
            "hint": "Unset SPARQUET_STUDIO_ASSISTANT to turn the assistant back on.",
        }
    return build(chosen).describe()
