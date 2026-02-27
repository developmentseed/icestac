from typing import Any

import aws_cdk as cdk
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3 as s3
from config import Config
from constructs import Construct


class IcestacStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        config: Config,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, tags=config.tags, **kwargs)

        database_name = config.glue_database_name

        warehouse_bucket = s3.Bucket(
            self,
            "WarehouseBucket",
            bucket_name=config.bucket_name,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        glue.CfnDatabase(
            self,
            "IcestacDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                location_uri=f"s3://{warehouse_bucket.bucket_name}/warehouse/",
            ),
        )

        catalog_role = iam.Role(
            self,
            "CatalogRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),  # ty: ignore[invalid-argument-type]
        )

        warehouse_bucket.grant_read_write(catalog_role)

        catalog_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:BatchDeleteTable",
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{database_name}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{database_name}/*",
                ],
            )
        )

        cdk.CfnOutput(self, "WarehouseBucketName", value=warehouse_bucket.bucket_name)
        cdk.CfnOutput(self, "GlueDatabaseName", value=database_name)
        cdk.CfnOutput(self, "CatalogRoleArn", value=catalog_role.role_arn)
