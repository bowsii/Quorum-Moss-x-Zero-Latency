"""tests/test_settings.py
------------------------
Unit tests for configuration validation in config/settings.py.
"""
import pytest
from pydantic import ValidationError
from config.settings import Settings


def test_settings_rejects_default_secret_in_production(monkeypatch):
    """Production environment must reject the default development SECRET_KEY."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    with pytest.raises(ValidationError) as excinfo:
        Settings()
    assert "SECRET_KEY must be overridden" in str(excinfo.value)


def test_settings_accepts_custom_secret_in_production(monkeypatch):
    """Production environment allows startup if a custom SECRET_KEY is supplied."""
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("SECRET_KEY", "a_very_secure_custom_production_secret_key_12345")
    s = Settings()
    assert s.ENVIRONMENT == "production"
    assert s.SECRET_KEY == "a_very_secure_custom_production_secret_key_12345"


def test_settings_rejects_invalid_environment_literal(monkeypatch):
    """Typo in ENVIRONMENT (e.g. 'produciton') must fail validation immediately."""
    monkeypatch.setenv("ENVIRONMENT", "produciton")
    with pytest.raises(ValidationError):
        Settings()
