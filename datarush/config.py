from dotenv import load_dotenv
from envarify import AnyHttpUrl, BaseConfig, EnvVar, SecretString

load_dotenv()


class S3Config(BaseConfig):
    """S3 Configuration"""

    endpoint: AnyHttpUrl = EnvVar("S3_ENDPOINT")
    access_key: str = EnvVar("S3_ACCESS_KEY")
    secret_key: SecretString = EnvVar("S3_SECRET_KEY")
    default_bucket: str | None = EnvVar("S3_DEFAULT_BUCKET", default=None)


class TemplateStoreConfig(BaseConfig):
    """Template Manager Configuration"""

    s3_bucket: str = EnvVar("TEMPLATE_STORE_S3_BUCKET")
    s3_prefix: str = EnvVar(
        "TEMPLATE_STORE_S3_PREFIX", default="datarush", parse=lambda x: x.strip("/")
    )


class AppConfig(BaseConfig):
    """Application Configuration"""

    s3: S3Config
    template_store: TemplateStoreConfig
