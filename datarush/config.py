from dotenv import load_dotenv
from envarify import AnyHttpUrl, BaseConfig, EnvVar, SecretString

load_dotenv()


class S3Config(BaseConfig):
    endpoint: AnyHttpUrl = EnvVar("S3_ENDPOINT")
    access_key: str = EnvVar("S3_ACCESS_KEY")
    secret_key: SecretString = EnvVar("S3_SECRET_KEY")
    bucket: str | None = EnvVar("S3_BUCKET", default=None)
