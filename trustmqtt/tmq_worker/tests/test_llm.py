import sys
import types as pytypes

from tmq_worker.llm import generate_report, render_fallback_report

SUMMARY = {
    "client_id": "client-abcd",
    "peak_level": 3,
    "peak_score": 0.81,
    "reason": "fsm=0.9 drift=0.4 fleet=0.0",
    "fsm_diff": [{"from": "CONNECT", "to": "SUB(⟦redacted⟧)", "learned_p": 0.001}],
}

BASE_URL = "https://integrate.api.nvidia.com/v1"


def test_fallback_report_contains_key_facts():
    report = render_fallback_report(SUMMARY)
    assert "client-abcd" in report
    assert "0.81" in report
    assert "CONNECT" in report


def test_generate_report_uses_fallback_when_disabled():
    report = generate_report(SUMMARY, api_key="whatever", model="meta/llama-3.1-8b-instruct",
                              timeout_s=10, enabled=False, base_url=BASE_URL)
    assert "deterministic fallback" in report


def test_generate_report_uses_fallback_when_no_api_key():
    report = generate_report(SUMMARY, api_key=None, model="meta/llama-3.1-8b-instruct",
                              timeout_s=10, enabled=True, base_url=BASE_URL)
    assert "deterministic fallback" in report


def _install_fake_openai(monkeypatch, create_impl):
    fake_openai = pytypes.ModuleType("openai")

    class _FakeCompletions:
        def create(self, model, messages, temperature, max_tokens):
            return create_impl(model, messages, temperature, max_tokens)

    class _FakeChat:
        def __init__(self):
            self.completions = _FakeCompletions()

    class _FakeClient:
        def __init__(self, api_key=None, base_url=None, timeout=None):
            self.chat = _FakeChat()

    fake_openai.OpenAI = _FakeClient
    monkeypatch.setitem(sys.modules, "openai", fake_openai)


def test_generate_report_returns_nvidia_text_on_success(monkeypatch):
    class _Message:
        content = "NVIDIA-written incident summary."

    class _Choice:
        message = _Message()

    class _Resp:
        choices = [_Choice()]

    _install_fake_openai(monkeypatch, lambda model, messages, temperature, max_tokens: _Resp())

    report = generate_report(SUMMARY, api_key="nvapi-fake", model="meta/llama-3.1-8b-instruct",
                              timeout_s=10, enabled=True, base_url=BASE_URL)
    assert report == "NVIDIA-written incident summary."


def test_generate_report_falls_back_on_api_error(monkeypatch):
    def _raise(model, messages, temperature, max_tokens):
        raise RuntimeError("401 unauthorized")

    _install_fake_openai(monkeypatch, _raise)

    report = generate_report(SUMMARY, api_key="nvapi-fake", model="meta/llama-3.1-8b-instruct",
                              timeout_s=10, enabled=True, base_url=BASE_URL)
    assert "deterministic fallback" in report


def test_generate_report_falls_back_when_response_has_no_content(monkeypatch):
    class _Message:
        content = None

    class _Choice:
        message = _Message()

    class _Resp:
        choices = [_Choice()]

    _install_fake_openai(monkeypatch, lambda model, messages, temperature, max_tokens: _Resp())

    report = generate_report(SUMMARY, api_key="nvapi-fake", model="meta/llama-3.1-8b-instruct",
                              timeout_s=10, enabled=True, base_url=BASE_URL)
    assert "deterministic fallback" in report
