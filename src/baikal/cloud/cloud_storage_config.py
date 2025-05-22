from pathlib import Path

from pydantic import BaseModel, Field, HttpUrl, SecretStr


class CloudStorageConfig(BaseModel):
    local_data_dir: Path = Field(alias="local-data-dir")
    local_worker_count: int = Field(alias="local-worker-count", ge=1)
    cloud_url: HttpUrl = Field(alias="cloud-url")
    cloud_bucket: str = Field(alias="cloud-bucket")
    cloud_key_id: str = Field(alias="cloud-key-id")
    cloud_key_secret: SecretStr = Field(alias="cloud-key-secret")
