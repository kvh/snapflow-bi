from typing import TypeVar

from snapflow.core.module import SnapflowModule

from .pipes.ltv import transaction_ltv_model
from .pipes.tests import test_ltv

Transaction = TypeVar("Transaction")

module = SnapflowModule(
    "bi",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=["schemas/transaction.yml"],
    pipes=[transaction_ltv_model],
    tests=[test_ltv.test_ltv],
)
module.export()
