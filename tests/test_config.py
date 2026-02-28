"""Tests for icestac config module."""

import os

import pytest
from pydantic import ValidationError

from icestac.config import (
    GlueCatalogConfig,
    HiveCatalogConfig,
    RestCatalogConfig,
    SqlCatalogConfig,
)


class TestIcestacCatalogConfig:
    """Test Iceberg catalog settings configuration."""

    def test_default_settings(self, monkeypatch, tmp_path):
        """Test default settings values with minimal valid SQL configuration."""
        for key in os.environ.copy():
            if key.startswith("ICESTAC_"):
                monkeypatch.delenv(key, raising=False)

        catalog_db = tmp_path / "catalog.db"
        warehouse_path = tmp_path / "warehouse"
        warehouse_path.mkdir()

        monkeypatch.setenv("ICESTAC_CATALOG_URI", f"sqlite:///{catalog_db}")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", str(warehouse_path))

        settings = SqlCatalogConfig.model_validate({})

        assert settings.catalog_name == "default"
        assert settings.catalog_type == "sql"
        assert settings.sql.echo is False

    def test_sql_catalog_properties(self, monkeypatch):
        """Test SQL catalog configuration."""
        monkeypatch.setenv("ICESTAC_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")

        settings = SqlCatalogConfig.model_validate(
            {},
        )

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
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "https://rest.example.com")
        monkeypatch.setenv("ICESTAC_REST__TOKEN", "my-token")

        settings = RestCatalogConfig.model_validate(
            {},
        )

        assert settings.catalog_type == "rest"
        assert settings.rest.token == "my-token"

        properties = settings.get_catalog_properties()
        assert properties["type"] == "rest"
        assert properties["uri"] == "https://rest.example.com"
        assert properties["token"] == "my-token"

    def test_glue_catalog_properties(self, monkeypatch):
        """Test AWS Glue catalog configuration."""
        monkeypatch.setenv("ICESTAC_CATALOG_NAME", "glue_catalog")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "s3://bucket/warehouse")
        monkeypatch.setenv("ICESTAC_GLUE__REGION", "us-west-2")

        settings = GlueCatalogConfig()

        assert settings.catalog_type == "glue"
        assert settings.glue.region == "us-west-2"

        properties = settings.get_catalog_properties()
        assert properties["type"] == "glue"
        assert properties["warehouse"] == "s3://bucket/warehouse"
        assert properties["region"] == "us-west-2"

    def test_sql_catalog_requires_uri(self, monkeypatch):
        """Test that SQL catalog requires a URI."""
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")

        with pytest.raises(ValidationError) as exc_info:
            SqlCatalogConfig.model_validate(
                {},
            )

        assert "catalog_uri" in str(exc_info.value)

    def test_sql_catalog_requires_warehouse_path(self, monkeypatch):
        """Test that SQL catalog requires a warehouse path."""
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")

        with pytest.raises(ValidationError) as exc_info:
            SqlCatalogConfig.model_validate(
                {},
            )

        assert "warehouse_path" in str(exc_info.value)

    def test_rest_catalog_requires_uri(self, monkeypatch):
        """Test that REST catalog requires a URI."""
        with pytest.raises(ValidationError) as exc_info:
            RestCatalogConfig.model_validate(
                {},
            )

        assert "catalog_uri" in str(exc_info.value)

    def test_case_insensitive_env_vars(self, monkeypatch):
        """Test that environment variables are case-insensitive."""
        monkeypatch.setenv("icestac_catalog_name", "test")
        monkeypatch.setenv("icestac_catalog_uri", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")

        settings = SqlCatalogConfig.model_validate(
            {},
        )

        assert settings.catalog_name == "test"

    def test_sql_echo_enabled(self, monkeypatch):
        """Test SQL echo setting."""
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")
        monkeypatch.setenv("ICESTAC_SQL__ECHO", "true")

        settings = SqlCatalogConfig.model_validate(
            {},
        )

        assert settings.sql.echo is True

        properties = settings.get_catalog_properties()
        assert properties["echo"] == "true"

    def test_s3_endpoint_properties(self, monkeypatch):
        """Test that S3 endpoint and path-style access are included in properties."""
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")
        monkeypatch.setenv("ICESTAC_S3__ENDPOINT", "http://minio:9000")
        monkeypatch.setenv("ICESTAC_S3__PATH_STYLE_ACCESS", "true")

        settings = SqlCatalogConfig.model_validate({})
        properties = settings.get_catalog_properties()

        assert properties["s3.endpoint"] == "http://minio:9000"
        assert properties["s3.path-style-access"] == "true"

    def test_s3_credentials_properties(self, monkeypatch):
        """Test that S3 access key and secret are included in properties."""
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")
        monkeypatch.setenv("ICESTAC_S3__ACCESS_KEY_ID", "mykey")
        monkeypatch.setenv("ICESTAC_S3__SECRET_ACCESS_KEY", "mysecret")

        settings = SqlCatalogConfig.model_validate({})
        properties = settings.get_catalog_properties()

        assert properties["s3.access-key-id"] == "mykey"
        assert properties["s3.secret-access-key"] == "mysecret"

    def test_extra_properties_passthrough(self, monkeypatch):
        """Test that extra fields passed to the model are included in catalog properties."""
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "sqlite:///catalog.db")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "/tmp/warehouse")

        settings = SqlCatalogConfig.model_validate({"some_extra_key": "extra_value"})
        properties = settings.get_catalog_properties()

        assert properties["some_extra_key"] == "extra_value"

    def test_rest_catalog_with_warehouse_and_credential(self, monkeypatch):
        """Test REST catalog with optional warehouse path and credential."""
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "https://rest.example.com")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "s3://bucket/warehouse")
        monkeypatch.setenv("ICESTAC_REST__CREDENTIAL", "client_id:client_secret")

        settings = RestCatalogConfig.model_validate({})
        properties = settings.get_catalog_properties()

        assert properties["warehouse"] == "s3://bucket/warehouse"
        assert properties["credential"] == "client_id:client_secret"

    def test_glue_catalog_with_uri(self, monkeypatch):
        """Test AWS Glue catalog with optional catalog URI."""
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "glue://my-catalog")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "s3://bucket/warehouse")

        settings = GlueCatalogConfig.model_validate({})
        properties = settings.get_catalog_properties()

        assert properties["uri"] == "glue://my-catalog"
        assert properties["warehouse"] == "s3://bucket/warehouse"

    def test_hive_catalog_properties(self, monkeypatch):
        """Test Hive catalog configuration and properties."""
        monkeypatch.setenv("ICESTAC_CATALOG_URI", "thrift://hive-metastore:9083")
        monkeypatch.setenv("ICESTAC_WAREHOUSE_PATH", "s3://bucket/warehouse")

        settings = HiveCatalogConfig.model_validate({})

        assert settings.catalog_type == "hive"
        assert settings.catalog_uri == "thrift://hive-metastore:9083"
        assert settings.warehouse_path == "s3://bucket/warehouse"

        properties = settings.get_catalog_properties()
        assert properties["type"] == "hive"
        assert properties["uri"] == "thrift://hive-metastore:9083"
        assert properties["warehouse"] == "s3://bucket/warehouse"
