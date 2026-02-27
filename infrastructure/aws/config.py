from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Application settings"""

    name: str = "icestac-demo"
    stage: str = "dev"
    owner: str = "hrodmn"
    project: str = "icestac"

    bucket_name: str | None = None
    glue_database_name: str = "icestac"

    def stack_name(self, name: str) -> str:
        """Generate consistent resource prefix."""
        return f"{self.name}-{self.stage}-{name}"

    @property
    def tags(self) -> dict[str, str]:
        """Generate consistent tags for resources."""
        return {
            "Project": self.project,
            "Owner": self.owner,
            "Stage": self.stage,
            "Name": self.name,
        }
