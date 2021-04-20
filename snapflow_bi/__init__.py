from typing import TypeVar

from snapflow.core.module import SnapflowModule

Transaction = TypeVar("Transaction")

module = SnapflowModule(
    "bi",
    py_module_path=__file__,
    py_module_name=__name__,
)
