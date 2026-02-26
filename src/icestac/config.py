"""Settings module for icestac Iceberg catalog configuration."""

from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyiceberg.catalog import Catalog, load_catalog


class IcebergCatalogConfig(BaseSettings):
    """
    Pydantic settings for configuring an Apache Iceberg catalog.

    Settings are loaded from environment variables with the ICESTAC_ prefix.
    Supports multiple catalog types: rest, glue, hive, and sql.

    Examples:
        REST Catalog:
            ICESTAC_CATALOG_NAME=my_catalog
            ICESTAC_CATALOG_TYPE=rest
            ICESTAC_CATALOG_URI=https://iceberg-rest.example.com

        AWS Glue Catalog:
            ICESTAC_CATALOG_NAME=glue_catalog
            ICESTAC_CATALOG_TYPE=glue
            ICESTAC_WAREHOUSE_PATH=s3://my-bucket/warehouse/

        SQL Catalog (SQLite):
            ICESTAC_CATALOG_NAME=local_catalog
            ICESTAC_CATALOG_TYPE=sql
            ICESTAC_CATALOG_URI=sqlite:///path/to/catalog.db
            ICESTAC_WAREHOUSE_PATH=/path/to/warehouse
    """

    model_config = SettingsConfigDict(
        env_prefix="ICESTAC_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="allow",  # Allow extra fields for catalog-specific properties
    )

    # Core catalog settings
    catalog_name: str = Field(
        default="default", description="Name of the Iceberg catalog"
    )

    catalog_type: Literal["rest", "glue", "hive", "sql"] = Field(
        default="sql", description="Type of Iceberg catalog backend"
    )

    catalog_uri: str | None = Field(
        default=None,
        description="URI for the catalog (required for rest, hive, and sql catalogs)",
    )

    warehouse_path: str | None = Field(
        default=None,
        description="Base path for the data warehouse (required for most catalog types)",
    )

    # AWS-specific settings
    aws_region: str | None = Field(
        default=None, description="AWS region for Glue catalog"
    )

    # REST catalog authentication
    rest_token: str | None = Field(
        default=None, description="Bearer token for REST catalog authentication"
    )

    rest_credential: str | None = Field(
        default=None, description="Credential for REST catalog authentication"
    )

    # SQL catalog settings
    sql_echo: bool = Field(
        default=False, description="Enable SQL query logging (for sql catalog type)"
    )

    # S3/MinIO storage settings
    s3_endpoint: str | None = Field(
        default=None,
        description="S3 endpoint URL (required for MinIO or custom S3-compatible storage)",
    )

    s3_access_key_id: str | None = Field(default=None, description="S3 access key ID")

    s3_secret_access_key: str | None = Field(
        default=None, description="S3 secret access key"
    )

    s3_path_style_access: bool = Field(
        default=True, description="Use path-style access for S3 (required for MinIO)"
    )

    @model_validator(mode="after")
    def validate_required_fields(self):
        """Ensure required fields are provided based on catalog type."""
        # Validate catalog_uri requirement
        if self.catalog_type in ["rest", "hive", "sql"] and not self.catalog_uri:
            raise ValueError(
                f"catalog_uri is required for catalog_type='{self.catalog_type}'"
            )

        # Validate warehouse_path requirement
        if self.catalog_type in ["sql", "hive"] and not self.warehouse_path:
            raise ValueError(
                f"warehouse_path is required for catalog_type='{self.catalog_type}'"
            )

        return self

    def get_catalog_properties(self) -> dict[str, str]:
        """
        Generate the properties dict for PyIceberg catalog initialization.

        Returns:
            Dictionary of catalog properties suitable for pyiceberg.catalog.load_catalog()
        """
        properties: dict[str, str] = {
            "type": self.catalog_type,
        }

        # Add URI if provided
        if self.catalog_uri:
            properties["uri"] = self.catalog_uri

        # Add warehouse path if provided
        if self.warehouse_path:
            properties["warehouse"] = self.warehouse_path

        # Add AWS region for Glue
        if self.catalog_type == "glue" and self.aws_region:
            properties["region"] = self.aws_region

        # Add REST authentication
        if self.catalog_type == "rest":
            if self.rest_token:
                properties["token"] = self.rest_token
            if self.rest_credential:
                properties["credential"] = self.rest_credential

        # Add SQL-specific settings
        if self.catalog_type == "sql":
            properties["echo"] = str(self.sql_echo).lower()

        # Add S3/MinIO storage settings
        if self.s3_endpoint:
            properties["s3.endpoint"] = self.s3_endpoint
        if self.s3_access_key_id:
            properties["s3.access-key-id"] = self.s3_access_key_id
        if self.s3_secret_access_key:
            properties["s3.secret-access-key"] = self.s3_secret_access_key
        # Always set path-style-access when S3 endpoint is configured
        if self.s3_endpoint:
            properties["s3.path-style-access"] = str(self.s3_path_style_access).lower()

        # Include any extra fields from environment (for catalog-specific properties)
        for key, value in self.model_extra.items() if self.model_extra else []:
            if value is not None:
                properties[key] = str(value)

        return properties

    def load_catalog(self) -> Catalog:
        """
        Create and return a PyIceberg Catalog instance using the configured settings.

        Returns:
            PyIceberg Catalog instance configured with the specified properties

        Example:
            >>> settings = IcebergCatalogSettings()
            >>> catalog = settings.load_catalog()
            >>> catalog.list_namespaces()
        """
        properties = self.get_catalog_properties()
        return load_catalog(self.catalog_name, **properties)
