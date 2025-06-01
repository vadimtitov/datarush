"""Application config."""

from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum
from functools import cached_property
from typing import Callable

from dotenv import load_dotenv
from envarify import AnyHttpUrl, BaseConfig, EnvVar, SecretString

from datarush.core.dataflow import Operation

load_dotenv(override=False)


################################
########## S3 CONFIG ###########
################################


@dataclass(frozen=True)
class S3Config(BaseConfig):
    """S3 Configuration."""

    endpoint: AnyHttpUrl = EnvVar("S3_ENDPOINT")
    access_key: str = EnvVar("S3_ACCESS_KEY")
    secret_key: SecretString = EnvVar("S3_SECRET_KEY")
    default_bucket: str | None = EnvVar("S3_DEFAULT_BUCKET", default=None)


################################
#### TEMPLATE STORE CONFIG #####
################################


class TemplateStoreType(StrEnum):
    """Template Store Type."""

    S3 = "S3"
    FILESYSTEM = "FILESYSTEM"


class S3TemplateStoreConfig(BaseConfig):
    """S3 based template manager config."""

    bucket: str = EnvVar("TEMPLATE_STORE_S3_BUCKET")
    prefix: str = EnvVar("TEMPLATE_STORE_S3_PREFIX", default="datarush")


class FilesystemTemplateStoreConfig(BaseConfig):
    """File system based template manager config."""

    path: str = EnvVar("TEMPLATE_STORE_FILESYSTEM_PATH", default=".")


class TemplateStoreConfig(BaseConfig):
    """Template Manager Configuration."""

    store_type: TemplateStoreType = EnvVar("TEMPLATE_STORE_TYPE")

    @property
    def s3(self) -> S3TemplateStoreConfig:
        """S3 template store config."""
        if self.store_type != TemplateStoreType.S3:
            raise ValueError("Template store type is not S3")
        return S3TemplateStoreConfig.fromenv()

    @property
    def filesystem(self) -> FilesystemTemplateStoreConfig:
        """File system template store config."""
        if self.store_type != TemplateStoreType.FILESYSTEM:
            raise ValueError("Template store type is not FILESYSTEM")
        return FilesystemTemplateStoreConfig.fromenv()


################################
###### APPLICATION CONFIG ######
################################


class DatarushConfig:
    """Datarush application configuration."""

    def __init__(
        self,
        custom_operations: list[type[Operation]] | None = None,
        s3_config_factory: Callable[[], S3Config] | None = None,
    ) -> None:
        """
        Initialize the Datarush configuration.

        Args:
            custom_operations: List of custom operations to be registered.
            s3_config_factory: Optional factory function to create S3Config.
        """
        self._custom_operations = custom_operations or []
        self._s3_config_factory = s3_config_factory

    @property
    def custom_operations(self) -> list[type[Operation]]:
        """Custom operations."""
        return self._custom_operations

    @cached_property
    def s3(self) -> S3Config:
        """Get S3 configuration."""
        if self._s3_config_factory is not None:
            return self._s3_config_factory()
        return S3Config.fromenv()

    @cached_property
    def template_store(self) -> TemplateStoreConfig:
        """Get template store configuration."""
        return TemplateStoreConfig.fromenv()


_config_var = ContextVar[DatarushConfig]("config")


def set_datarush_config(config: DatarushConfig | None = None) -> None:
    """Set the Datarush configuration."""
    config = config or DatarushConfig()

    _config_var.set(config)

    from datarush.core.operations import register_operation_type

    for operation in config.custom_operations:
        register_operation_type(operation)


def get_datarush_config() -> DatarushConfig:
    """Get the current Datarush configuration."""
    try:
        return _config_var.get()
    except LookupError:
        set_datarush_config()
        return _config_var.get()
