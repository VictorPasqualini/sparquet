"""The Omnigent backend against the Omnigent that is actually installed.

`test_assistant.py` drives the adapter with a fake module whose classes carry
the real names. That catches an event this adapter forgot to handle; it cannot
catch a constructor keyword the package never had, a tool spec whose shape the
package silently ignores, or a message type its executor cannot read — and all
three of those were wrong until somebody installed it and ran a turn.

So this file installs nothing and fakes nothing about Omnigent. It skips when
the package is absent, which is the normal case: Omnigent needs Python 3.12 and
the framework supports 3.9, so a runner that never asked for an agent loop will
never have it.

What *is* faked is the model, because a model is weights and a test is not
allowed to need eight gigabytes of them. A stub HTTP server answers the
OpenAI-compatible chat-completions stream that Ollama's `/v1` route answers,
asks for `validate_config` on the first turn and replies in text once the tool
result comes back. Everything between that stub and this test — the executor,
the OpenAI Agents SDK underneath it, the tool bridge, the event classes, our
adapter, and the framework the tool calls into — is the real thing.

    python server/test_omnigent_live.py
"""
from __future__ import annotations

import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import assistant  # noqa: E402

try:  # pragma: no cover - the skip is the point
    import omnigent  # type: ignore # noqa: F401

    HAVE_OMNIGENT = True
except ImportError:
    HAVE_OMNIGENT = False


#: What the stub reports for every turn. Real numbers rather than zeros: the
#: question this file answers about usage is whether it survives the trip from
#: the SDK through `TurnComplete` into our `done` event, and a zero cannot tell
#: an answer that arrived from one that did not.
USAGE = {"prompt_tokens": 137, "completion_tokens": 24, "total_tokens": 161}


def _frame(delta: dict, finish: str | None = None, usage: dict | None = None) -> bytes:
    body: dict = {
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "stub-model",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    if usage is not None:
        body["usage"] = usage
    return f"data: {json.dumps(body)}\n\n".encode("utf-8")


class _StubModel(BaseHTTPRequestHandler):
    """One tool call, then one answer, in OpenAI's streaming shape."""

    #: Every request body the SDK sent, so a test can ask what the model saw.
    seen: list = []

    def log_message(self, *_args):  # noqa: D102 - silence the default access log
        pass

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's spelling
        length = int(self.headers.get("content-length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        _StubModel.seen.append(body)
        answered = any(m.get("role") == "tool" for m in body.get("messages", []))

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        if not answered:
            self.wfile.write(_frame({
                "role": "assistant",
                "tool_calls": [{
                    "index": 0, "id": "call_1", "type": "function",
                    "function": {"name": "validate_config", "arguments": ""},
                }],
            }))
            self.wfile.write(_frame({
                "tool_calls": [{
                    "index": 0,
                    "function": {"arguments": json.dumps({"config": {"name": "x"}})},
                }],
            }))
            self.wfile.write(_frame({}, finish="tool_calls", usage=USAGE))
        else:
            for piece in ("That config ", "is not valid."):
                self.wfile.write(_frame({"role": "assistant", "content": piece}))
            self.wfile.write(_frame({}, finish="stop", usage=USAGE))
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


@unittest.skipUnless(HAVE_OMNIGENT, "omnigent is not installed here")
class OmnigentLiveTests(unittest.TestCase):
    """One real turn, start to finish."""

    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), _StubModel)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

        _StubModel.seen = []
        backend = assistant.OmnigentBackend(
            base_url=f"http://127.0.0.1:{cls.port}", model="stub-model",
        )
        cls.backend = backend
        cls.events = list(backend.stream(
            [{"role": "user", "content": 'is {"name": "x"} a valid pipeline?'}]
        ))
        cls.sent = list(_StubModel.seen)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_the_turn_finished_instead_of_failing(self):
        errors = [event.text for event in self.events if event.kind == "error"]
        self.assertEqual(errors, [], f"the turn reported: {errors}")
        self.assertEqual(self.events[-1].kind, "done")

    def test_the_installed_package_reports_itself(self):
        detail = self.backend.describe()
        self.assertTrue(detail["available"])
        # 0.14.0 exports no `__version__`; the metadata is what answers.
        self.assertTrue(detail["version"], "an installed Omnigent reported no version")

    def test_the_model_was_offered_the_runners_tools(self):
        offered = [
            tool.get("function", {}).get("name")
            for tool in self.sent[0].get("tools", [])
        ]
        # Omnigent reads `name` off the top of our flat spec and rebuilds the
        # nested one for the wire. A spec it cannot read a name from is dropped
        # in silence, so an empty list here is the bug this test exists for.
        self.assertEqual(sorted(offered), ["list_formats", "validate_config"])

    def test_the_tool_really_ran_and_the_model_saw_the_answer(self):
        calls = [event for event in self.events if event.kind == "tool"]
        self.assertTrue(calls, "no tool call reached the caller")
        self.assertEqual(calls[0].name, "validate_config")

        results = [
            message for message in self.sent[-1].get("messages", [])
            if message.get("role") == "tool"
        ]
        self.assertTrue(results, "the tool result never went back to the model")
        # The framework's own refusal, not a shape this file invented: a stub
        # that answered for the tool would prove only that the stub works.
        self.assertIn("valid", str(results[0].get("content")))

    def test_the_answer_was_streamed(self):
        text = "".join(event.text for event in self.events if event.kind == "delta")
        self.assertEqual(text, "That config is not valid.")

    def test_the_local_server_was_asked_for_its_token_counts(self):
        """An OpenAI-compatible server reports usage only when asked to.

        The SDK asks on its own only when the base URL is OpenAI's own, and
        Omnigent never sets `include_usage`, so without `_ask_for_usage` no
        chunk carries a usage block and every local turn is metered at zero.
        This is the only test that runs the real SDK, so it is the only place
        the request body can be checked rather than a fake's stand-in for it.
        """
        self.assertEqual(self.sent[0].get("stream_options"), {"include_usage": True})

    def test_usage_survives_the_trip_and_is_free(self):
        usage = self.events[-1].usage
        self.assertEqual(usage.provider, "omnigent")
        self.assertEqual(usage.model, "stub-model")
        self.assertTrue(usage.local, "a loopback model is not somebody else's compute")
        self.assertGreater(usage.input_tokens, 0)
        self.assertGreater(usage.output_tokens, 0)
        self.assertEqual(usage.tool_calls, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
