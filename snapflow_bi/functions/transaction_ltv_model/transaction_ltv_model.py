from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Optional

from lifetimes import BetaGeoFitter, GammaGammaFitter
from lifetimes.utils import summary_data_from_transaction_data
from pandas import DataFrame
from snapflow import DataBlock, Param, Function, FunctionContext

if TYPE_CHECKING:
    from snapflow_bi import Transaction


class LTVModel:
    def __init__(self, penalizer_coef: float = None):
        if penalizer_coef is None:
            penalizer_coef = 0.01
        self.penalizer_coef = penalizer_coef

    def get_spend_model(self):
        return GammaGammaFitter(penalizer_coef=self.penalizer_coef)

    def get_recurrence_model(self):
        return BetaGeoFitter(penalizer_coef=self.penalizer_coef)

    def fit_spend_model(self, summary):
        returning_txs = summary[summary["frequency"] > 0]
        sm = self.get_spend_model()
        sm.fit(returning_txs["frequency"], returning_txs["monetary_value"])
        return sm

    def fit_recurrence_model(self, summary):
        rm = self.get_recurrence_model()
        rm.fit(summary["frequency"], summary["recency"], summary["T"])
        return rm

    def compute_ltvs_from_transactions(
        self,
        transactions,
        customer_id_col="customer_id",
        transaction_date_col="transacted_at",
        transaction_amount_col="amount",
        future_months_to_project=None,
        annual_discount=None,
        observation_period_end=None,
    ):
        if observation_period_end is None:
            observation_period_end = datetime.today()
        if future_months_to_project is None:
            future_months_to_project = 24
        if annual_discount is None:
            annual_discount = 0.2
        summary = summary_data_from_transaction_data(
            transactions,
            customer_id_col=customer_id_col,
            datetime_col=transaction_date_col,
            monetary_value_col=transaction_amount_col,
            observation_period_end=observation_period_end,
        )
        sm = self.fit_spend_model(summary)
        rm = self.fit_recurrence_model(summary)
        ltvs = sm.customer_lifetime_value(
            rm,
            summary["frequency"],
            summary["recency"],
            summary["T"],
            summary["monetary_value"],
            time=future_months_to_project,
            discount_rate=(1 + annual_discount) ** (1 / 12) - 1,  # Convert to monthly
        )
        return ltvs.reset_index()


@Function("transaction_ltv_model", namespace="bi", display_name="Transaction LTV model")
def transaction_ltv_model(
    ctx: FunctionContext,
    transactions: DataBlock[Transaction],
    annual_discount_rate: float = 0.2,
    future_months_to_project: int = 12 * 5,
    observation_period_end: Optional[datetime] = None,
    penalizer_coef: float = 0.01,
) -> DataFrame:
    tx_df = transactions.as_dataframe()
    model = LTVModel(penalizer_coef=penalizer_coef)
    return model.compute_ltvs_from_transactions(
        tx_df,
        annual_discount=annual_discount_rate,
        future_months_to_project=future_months_to_project,
        observation_period_end=observation_period_end,
    )
