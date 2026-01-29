"""Tests for icestac config module."""

import os

import pytest
from pydantic import ValidationError

from icestac.config import IcebergCatalogConfig


class TestIcebergCatalogConfig:
    """Test Iceberg catalog settings configuration."""

    def test_default_settings(self, monkeypatch, tmp_path):
        """Test default settings values with minimal valid configuration."""
        # Clear any existing environment variables
        for key in os.environ.copy():
            if key.startswith("ICESTAC_"):
                monkeypatch.delenv(key, raising=False)

        # Set minimal required configuration for SQL catalog (the default)
        catalog_db = tmp_path / "catalog.db"
        warehouse_path = tmp_path / "warehouse"
        warehouse_path.mkdir()

        monkeypatch.setenv("ICESTAC_CATALOG_URI", f"sqlite:///{catalog_db}")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", str(warehouse_path))

        # Disable .env file reading to avoid pollution from project .env file
        settings = IcebergCatalogConfig(_env_file=None)

        # Verify defaults are applied
        assert settings.catalog_name == "default"
        assert settings.catalog_type == "sql"
        assert settings.sql_echo is False
        assert settings.aws_region is None
        assert settings.rest_token is None

    def test_sql_catalog_properties(self, monkeypatch):
        """Test SQL catalog configuration."""
        monkeypatch.setenv("ICESTAC_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "sql")
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")

        settings = IcebergCatalogConfig()

        assert settings.catalog_name == "test_catalog"
        assert settings.catalog_type == "sql"
        assert settings.catalog_uri == "sqlite:///catalog.db"
        assert settings.warehouse_path == "/tmp/warehouse"

        properties = settings.get_catalog_properties()
        assert properties["type"] == "sql"
        assert properties["uri"] == "sqlite:///catalog.db"
        assert properties["warehouse"] == "/tmp/warehouse"
        assert properties["echo"] == "false"

    def test_rest_catalog_properties(self, monkeypatch):
        """Test REST catalog configuration."""
        monkeypatch.setenv("ICESTAC_CATALOG_NAME", "rest_catalog")
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "rest")
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "https://rest.example.com")
        monkeypatch.setenv("ICESTAC_REST_TOKEN", "my-token")

        settings = IcebergCatalogConfig()

        assert settings.catalog_type == "rest"
        assert settings.rest_token == "my-token"

        properties = settings.get_catalog_properties()
        assert properties["type"] == "rest"
        assert properties["uri"] == "https://rest.example.com"
        assert properties["token"] == "my-token"

    def test_glue_catalog_properties(self, monkeypatch):
        """Test AWS Glue catalog configuration."""
        monkeypatch.setenv("ICESTAC_CATALOG_NAME", "glue_catalog")
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "glue")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "s3://bucket/warehouse")
        monkeypatch.setenv("ICESTAC_AWS_REGION", "us-west-2")

        settings = IcebergCatalogConfig()

        assert settings.catalog_type == "glue"
        assert settings.aws_region == "us-west-2"

        properties = settings.get_catalog_properties()
        assert properties["type"] == "glue"
        assert properties["warehouse"] == "s3://bucket/warehouse"
        assert properties["region"] == "us-west-2"

    def test_sql_catalog_requires_uri(self, monkeypatch):
        """Test that SQL catalog requires a URI."""
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "sql")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")

        with pytest.raises(ValidationError) as exc_info:
            IcebergCatalogConfig(_env_file=None)

        assert "catalog_uri is required" in str(exc_info.value)

    def test_sql_catalog_requires_warehouse_path(self, monkeypatch):
        """Test that SQL catalog requires a warehouse path."""
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "sql")
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")

        with pytest.raises(ValidationError) as exc_info:
            IcebergCatalogConfig(_env_file=None)

        assert "warehouse_path is required" in str(exc_info.value)

    def test_rest_catalog_requires_uri(self, monkeypatch):
        """Test that REST catalog requires a URI."""
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "rest")

        with pytest.raises(ValidationError) as exc_info:
            IcebergCatalogConfig(_env_file=None)

        assert "catalog_uri is required" in str(exc_info.value)

    def test_case_insensitive_env_vars(self, monkeypatch):
        """Test that environment variables are case-insensitive."""
        monkeypatch.setenv("icestac_catalog_name", "test")
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "sql")
        monkeypatch.setenv("icestac_catalog_uri", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")

        settings = IcebergCatalogConfig()

        assert settings.catalog_name == "test"

    def test_sql_echo_enabled(self, monkeypatch):
        """Test SQL echo setting."""
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "sql")
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")
        monkeypatch.setenv("ICESTAC_SQL_ECHO", "true")

        settings = IcebergCatalogConfig()

        assert settings.sql_echo is True

        properties = settings.get_catalog_properties()
        assert properties["echo"] == "true"

    def test_load_catalog(self, tmp_path, monkeypatch):
        """Test loading a PyIceberg catalog from settings."""
        catalog_db = tmp_path / "catalog.db"
        warehouse_path = tmp_path / "warehouse"
        warehouse_path.mkdir()

        monkeypatch.setenv("ICESTAC_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICESTAC_CATALOG_TYPE", "sql")
        monkeypatch.setenv("ICESTAC_CATALOG_URI", f"sqlite:///{catalog_db}")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", str(warehouse_path))

        settings = IcebergCatalogConfig()
        catalog = settings.load_catalog()

        # Verify catalog is created successfully
        assert catalog is not None
        assert catalog.name == "test_catalog"

        # Test basic catalog operations
        catalog.create_namespace("test")
        namespaces = catalog.list_namespaces()
        assert ("test",) in namespaces
