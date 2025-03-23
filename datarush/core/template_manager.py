from datarush.config import S3Config
from datarush.core.dataflow import Dataflow, Operation
from datarush.core.operations import get_operation_type_by_name


def dataflow_from_json(data: dict) -> Dataflow:
    operations: list[Operation] = []

    for operation in data["operations"]:
        operation_type = get_operation_type_by_name(operation["name"])
        model = operation_type.model.model_validate(operation["data"])
        operations.append(operation_type(model))

    return Dataflow(operations=operations)


def dataflow_to_json(dataflow: Dataflow) -> dict:
    operations = [
        {"name": operation.name, "data": operation.model.model_dump()}
        for operation in dataflow.operations
    ]
    return {"operations": operations}


def save_dataflow_to3(dataflow: Dataflow, name: str, version: str) -> None:
    config = S3Config.fromenv()
