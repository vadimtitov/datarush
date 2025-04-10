import json
from io import BytesIO

from datarush.config import TemplateStoreConfig
from datarush.core.dataflow import Dataflow, Operation
from datarush.core.operations import get_operation_type_by_name
from datarush.core.types import ParameterSpec
from datarush.utils.s3_client import S3Client

_S3_FOLDER = "templates"
_TEMPLATE_FILE = "template.json"


class TemplateManager:

    def __init__(self, config: TemplateStoreConfig | None = None):
        config = config or TemplateStoreConfig.fromenv()
        self._s3 = S3Client()
        self._bucket = config.s3_bucket
        self._prefix = config.s3_prefix

    def list_templates(self) -> list[str]:
        return self._s3.list_folders(self._bucket, f"{self._prefix}/{_S3_FOLDER}")

    def list_template_versions(self, template_name: str) -> list[str]:
        raw_versions = self._s3.list_folders(
            self._bucket, f"{self._prefix}/{_S3_FOLDER}/{template_name}"
        )
        return [v.replace("version=", "") for v in raw_versions if v.startswith("version=")]

    def read_template(self, template_name: str, version: str) -> dict:
        key = f"{self._prefix}/{_S3_FOLDER}/{template_name}/version={version}/{_TEMPLATE_FILE}"
        obj = self._s3.get_object(self._bucket, key)
        return json.load(obj)

    def write_template(self, template: dict, template_name: str, version: str) -> None:
        key = f"{self._prefix}/{_S3_FOLDER}/{template_name}/version={version}/{_TEMPLATE_FILE}"
        if self._s3.list_object_keys(self._bucket, key):
            raise ValueError(f"Template version {version} already exists")

        buffer = BytesIO(json.dumps(template).encode("utf-8"))
        self._s3.put_object(self._bucket, key, buffer)


def template_to_dataflow(template: dict) -> Dataflow:
    parameters = [ParameterSpec.model_validate(param) for param in template.get("parameters", [])]

    operations: list[Operation] = []
    for operation in template["operations"]:
        operation_type = get_operation_type_by_name(operation["name"])
        operations.append(
            operation_type(
                model_dict=operation["data"],
                advanced_mode=operation.get("advanced_mode", False),
            )
        )

    return Dataflow(parameters=parameters, operations=operations)


def dataflow_to_template(dataflow: Dataflow) -> dict:
    paramaters = [param.model_dump() for param in dataflow.parameters]
    operations = [
        {
            "name": operation.name,
            "data": operation.model_dict,
            "advanced_mode": operation.advanced_mode,
        }
        for operation in dataflow.operations
    ]

    return {"parameters": paramaters, "operations": operations}
