from typing import TypeVar

from snapflow.core.module import SnapflowModule

from .snaps.ltv import transaction_ltv_model
from .snaps.tests import test_ltv

Transaction = TypeVar("Transaction")

module = SnapflowModule(
    "bi",
    py_module_path=__file__,
    py_module_name=__name__,
    schemas=["schemas/transaction.yml"],
    snaps=[transaction_ltv_model],
    tests=[test_ltv.test_ltv],
)
module.export()
