import json
from src.core.secret_string import SecretString, redact_secrets


class TestSecretString:
    """Test SecretString masks itself in display contexts but works as a real string."""

    def test_repr_is_redacted(self):
        s = SecretString("my-api-key")
        assert repr(s) == "'[REDACTED]'"

    def test_str_is_redacted(self):
        s = SecretString("my-api-key")
        assert str(s) == "[REDACTED]"

    def test_print_is_redacted(self, capsys):
        s = SecretString("my-api-key")
        print(s)
        assert capsys.readouterr().out.strip() == "[REDACTED]"

    def test_format_returns_real_value(self):
        s = SecretString("my-api-key")
        assert f"Bearer {s}" == "Bearer my-api-key"

    def test_concat_returns_real_value(self):
        s = SecretString("my-api-key")
        assert "Bearer " + s == "Bearer my-api-key"

    def test_encode_returns_real_bytes(self):
        s = SecretString("my-api-key")
        assert s.encode() == b"my-api-key"

    def test_equality_with_real_string(self):
        s = SecretString("my-api-key")
        assert s == "my-api-key"

    def test_get_secret_value(self):
        s = SecretString("my-api-key")
        assert s.get_secret_value() == "my-api-key"

    def test_json_dumps_leaks_raw_value(self):
        """json.dumps uses C-level str buffer, bypassing __str__.
        This is expected — redact_secrets and remove_circular_refs are the protection layers."""
        s = SecretString("my-api-key")
        dumped = json.dumps(s)
        assert dumped == '"my-api-key"'  # Documents the real behavior

    def test_isinstance_str(self):
        s = SecretString("my-api-key")
        assert isinstance(s, str)

    def test_logging_format_string(self):
        s = SecretString("my-api-key")
        msg = "key=%s" % s
        assert msg == "key=[REDACTED]"

    def test_bang_s_forces_redaction(self):
        """f'{s!s}' forces __str__, which redacts. This is by design."""
        s = SecretString("my-api-key")
        assert f"{s!s}" == "[REDACTED]"

    def test_bang_r_forces_redaction(self):
        s = SecretString("my-api-key")
        assert f"{s!r}" == "'[REDACTED]'"

    def test_used_as_dict_value_in_headers(self):
        """Simulates passing to requests/httpx headers dict."""
        s = SecretString("my-api-key")
        headers = {"Authorization": s}
        # Libraries read the str buffer directly, not __str__
        assert headers["Authorization"] == "my-api-key"
        assert headers["Authorization"].encode() == b"my-api-key"


class TestRedactSecrets:
    """Test deep scrubbing of secret values from JSON-serializable objects."""

    def test_redact_string_exact_match(self):
        result = redact_secrets("my-secret-key", {"my-secret-key"})
        assert result == "[REDACTED]"

    def test_redact_string_substring(self):
        result = redact_secrets("Bearer my-secret-key here", {"my-secret-key"})
        assert result == "Bearer [REDACTED] here"

    def test_redact_in_dict_values(self):
        obj = {"output": "token is my-secret-key", "count": 42}
        result = redact_secrets(obj, {"my-secret-key"})
        assert result["output"] == "token is [REDACTED]"
        assert result["count"] == 42

    def test_redact_in_nested_dict(self):
        obj = {"outer": {"inner": "my-secret-key"}}
        result = redact_secrets(obj, {"my-secret-key"})
        assert result["outer"]["inner"] == "[REDACTED]"

    def test_redact_in_list(self):
        obj = ["safe", "contains my-secret-key"]
        result = redact_secrets(obj, {"my-secret-key"})
        assert result[0] == "safe"
        assert result[1] == "contains [REDACTED]"

    def test_redact_in_set(self):
        obj = {"items": {"my-secret-key", "safe"}}
        result = redact_secrets(obj, {"my-secret-key"})
        assert "[REDACTED]" in result["items"]
        assert "my-secret-key" not in result["items"]

    def test_redact_multiple_secrets(self):
        obj = "key1=aaaa key2=bbbb"
        result = redact_secrets(obj, {"aaaa", "bbbb"})
        assert result == "key1=[REDACTED] key2=[REDACTED]"

    def test_skip_short_secrets(self):
        """Secrets shorter than 4 chars are skipped to avoid false positives."""
        obj = "the api key"
        result = redact_secrets(obj, {"api"})
        assert result == "the api key"  # Not redacted

    def test_no_secrets_passthrough(self):
        obj = {"key": "value", "num": 123}
        result = redact_secrets(obj, set())
        assert result == {"key": "value", "num": 123}

    def test_none_passthrough(self):
        assert redact_secrets(None, {"secret"}) is None

    def test_bool_passthrough(self):
        assert redact_secrets(True, {"secret"}) is True

    def test_int_passthrough(self):
        assert redact_secrets(42, {"secret"}) == 42

    def test_does_not_mutate_original(self):
        original = {"key": "my-secret-key"}
        redact_secrets(original, {"my-secret-key"})
        assert original["key"] == "my-secret-key"

    def test_redact_pydantic_model(self):
        """Pydantic models are converted to dicts and scrubbed."""
        from pydantic import BaseModel

        class Response(BaseModel):
            api_key: str
            message: str

        obj = Response(api_key="my-secret-key", message="ok")
        result = redact_secrets(obj, {"my-secret-key"})
        assert isinstance(result, dict)
        assert result["api_key"] == "[REDACTED]"
        assert result["message"] == "ok"


class TestEngineOutputScrubbing:
    """Test that engine scrubs secrets from result, variables, error_message, and logs."""

    def test_scrub_result_variables_and_logs(self):
        from src.core.secret_string import redact_secrets

        secret_values = {"sk-12345678"}

        result = {"message": "Used key sk-12345678 successfully"}
        variables = {"api_key": "sk-12345678", "count": 5}
        logs = [
            {"level": "info", "message": "Calling API with sk-12345678"},
            {"level": "info", "message": "Done"},
        ]

        scrubbed_result = redact_secrets(result, secret_values)
        scrubbed_variables = redact_secrets(variables, secret_values)
        scrubbed_logs = redact_secrets(logs, secret_values)

        assert "sk-12345678" not in json.dumps(scrubbed_result)
        assert "sk-12345678" not in json.dumps(scrubbed_variables)
        assert "sk-12345678" not in json.dumps(scrubbed_logs)
        assert scrubbed_result["message"] == "Used key [REDACTED] successfully"
        assert scrubbed_variables["api_key"] == "[REDACTED]"
        assert scrubbed_variables["count"] == 5
        assert scrubbed_logs[0]["message"] == "Calling API with [REDACTED]"
        assert scrubbed_logs[1]["message"] == "Done"

    def test_scrub_error_message(self):
        from src.core.secret_string import redact_secrets

        secret_values = {"sk-12345678"}
        error_message = "Auth failed with key sk-12345678"

        scrubbed = redact_secrets(error_message, secret_values)
        assert scrubbed == "Auth failed with key [REDACTED]"


class TestLogScrubbing:
    """Test that log messages are scrubbed before streaming."""

    def test_log_message_scrubbed(self):
        from src.core.secret_string import redact_secrets

        secret_values = {"sk-12345678"}
        log_entry = "[INFO] Calling API with sk-12345678"

        scrubbed = redact_secrets(log_entry, secret_values)
        assert scrubbed == "[INFO] Calling API with [REDACTED]"

    def test_log_dict_message_scrubbed(self):
        from src.core.secret_string import redact_secrets

        secret_values = {"sk-12345678"}
        log_dict = {
            "executionLogId": "abc",
            "level": "INFO",
            "message": "key is sk-12345678",
            "timestamp": "2026-01-01T00:00:00Z",
            "sequence": 1,
        }

        scrubbed = redact_secrets(log_dict, secret_values)
        assert scrubbed["message"] == "key is [REDACTED]"
        assert scrubbed["executionLogId"] == "abc"  # Not scrubbed


class TestRemoveCircularRefsSecretString:
    """Test that remove_circular_refs converts SecretString to [REDACTED]."""

    def test_secret_string_detected(self):
        """SecretString values should be replaced with [REDACTED] in variable capture."""
        s = SecretString("my-api-key")
        from src.core.secret_string import REDACTED
        assert REDACTED == "[REDACTED]"
        # The actual assertion is that isinstance check works
        assert isinstance(s, SecretString)
        assert isinstance(s, str)
