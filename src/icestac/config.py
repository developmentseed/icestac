"""Settings module for icestac Iceberg catalog configuration."""

from typing import Annotated, Literal

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from icestac.constants import DEFAULT_NAMESPACE


class RestConfig(BaseModel):
    """Authentication settings for REST catalogs."""

    token: str | None = Field(
        default=None, description="Bearer token for REST catalog authentication"
    )
    credential: str | None = Field(
        default=None, description="Credential for REST catalog authentication"
    )


class S3Config(BaseModel):
    """S3/MinIO storage settings."""

    endpoint: str | None = Field(
        default=None,
        description="S3 endpoint URL (required for MinIO or custom S3-compatible storage)",
    )
    access_key_id: str | None = Field(default=None, description="S3 access key ID")
    secret_access_key: str | None = Field(
        default=None, description="S3 secret access key"
    )
    path_style_access: bool = Field(
        default=True, description="Use path-style access for S3 (required for MinIO)"
    )


class SqlConfig(BaseModel):
    """Settings specific to SQL catalogs."""

    echo: bool = Field(default=False, description="Enable SQL query logging")


class GlueConfig(BaseModel):
    """Settings specific to AWS Glue catalogs."""

    region: str | None = Field(default=None, description="AWS region for Glue catalog")


_SETTINGS_CONFIG = SettingsConfigDict(
    env_prefix="ICESTAC_",
    env_nested_delimiter="__",
    case_sensitive=False,
    env_file=".env",
    env_file_encoding="utf-8",
    extra="allow",
)


class _BaseCatalogConfig(BaseSettings):
    """Base settings shared by all catalog types."""

    model_config = _SETTINGS_CONFIG

    catalog_type: str  # Narrowed to Literal in each subclass

    catalog_name: str = Field(
        default="default", description="Name of the Iceberg catalog"
    )
    namespace: str = Field(
        default=DEFAULT_NAMESPACE, description="Namespace within Iceberg catalog"
    )
    s3: S3Config = Field(default_factory=S3Config)

    def _get_base_properties(self) -> dict[str, str]:
        """Build properties common to all catalog types."""
        properties: dict[str, str] = {"type": self.catalog_type}

        if self.s3.endpoint:
            properties["s3.endpoint"] = self.s3.endpoint
            properties["s3.path-style-access"] = str(self.s3.path_style_access).lower()
        if self.s3.access_key_id:
            properties["s3.access-key-id"] = self.s3.access_key_id
        if self.s3.secret_access_key:
            properties["s3.secret-access-key"] = self.s3.secret_access_key

        for key, value in (self.model_extra or {}).items():
            if value is not None:
                properties[key] = str(value)

        return properties


class SqlCatalogConfig(_BaseCatalogConfig):
    """
    Configuration for SQL-based Iceberg catalogs (SQLite, PostgreSQL, etc.).

    Examples:
        ICESTAC_CATALOG_TYPE=sql
        ICESTAC_CATALOG_URI=sqlite:///path/to/catalog.db
        ICESTAC_WAREHOUSE_PATH=/path/to/warehouse
        ICESTAC_SQL__ECHO=true
    """

    catalog_type: Literal["sql"] = "sql"
    catalog_uri: str = Field(
        description="URI for the SQL catalog (e.g. sqlite:///path/to/catalog.db)"
    )
    warehouse_path: str = Field(description="Base path for the data warehouse")
    sql: SqlConfig = Field(default_factory=SqlConfig)

    def get_catalog_properties(self) -> dict[str, str]:
        props = self._get_base_properties()
        props.update(
            {
                "uri": self.catalog_uri,
                "warehouse": self.warehouse_path,
                "echo": str(self.sql.echo).lower(),
            }
        )
        return props


class RestCatalogConfig(_BaseCatalogConfig):
    """
    Configuration for REST-based Iceberg catalogs.

    Examples:
        ICESTAC_CATALOG_TYPE=rest
        ICESTAC_CATALOG_URI=https://iceberg-rest.example.com
        ICESTAC_REST__TOKEN=my-token
    """

    catalog_type: Literal["rest"] = "rest"
    catalog_uri: str = Field(description="URI for the REST catalog")
    warehouse_path: str | None = Field(
        default=None, description="Optional warehouse path"
    )
    rest: RestConfig = Field(default_factory=RestConfig)

    def get_catalog_properties(self) -> dict[str, str]:
        props = self._get_base_properties()
        props["uri"] = self.catalog_uri
        if self.warehouse_path:
            props["warehouse"] = self.warehouse_path
        if self.rest.token:
            props["token"] = self.rest.token
        if self.rest.credential:
            props["credential"] = self.rest.credential
        return props


class GlueCatalogConfig(_BaseCatalogConfig):
    """
    Configuration for AWS Glue Iceberg catalogs.

    Examples:
        ICESTAC_CATALOG_TYPE=glue
        ICESTAC_WAREHOUSE_PATH=s3://my-bucket/warehouse/
        ICESTAC_GLUE__REGION=us-east-1
    """

    catalog_type: Literal["glue"] = "glue"
    catalog_uri: str | None = Field(
        default=None, description="Optional URI for the Glue catalog"
    )
    warehouse_path: str | None = Field(
        default=None, description="S3 path for the data warehouse"
    )
    glue: GlueConfig = Field(default_factory=GlueConfig)

    def get_catalog_properties(self) -> dict[str, str]:
        props = self._get_base_properties()
        if self.catalog_uri:
            props["uri"] = self.catalog_uri
        if self.warehouse_path:
            props["warehouse"] = self.warehouse_path
        if self.glue.region:
            props["region"] = self.glue.region
        return props


class HiveCatalogConfig(_BaseCatalogConfig):
    """
    Configuration for Hive-based Iceberg catalogs.

    Examples:
        ICESTAC_CATALOG_TYPE=hive
        ICESTAC_CATALOG_URI=thrift://hive-metastore:9083
        ICESTAC_WAREHOUSE_PATH=s3://my-bucket/warehouse/
    """

    catalog_type: Literal["hive"] = "hive"
    catalog_uri: str = Field(description="URI for the Hive metastore")
    warehouse_path: str = Field(description="Base path for the data warehouse")

    def get_catalog_properties(self) -> dict[str, str]:
        props = self._get_base_properties()
        props["uri"] = self.catalog_uri
        props["warehouse"] = self.warehouse_path
        return props


IcestacCatalogConfig = Annotated[
    SqlCatalogConfig | RestCatalogConfig | GlueCatalogConfig | HiveCatalogConfig,
    Field(discriminator="catalog_type"),
]
