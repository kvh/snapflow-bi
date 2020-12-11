from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Optional

from lifetimes import BetaGeoFitter, GammaGammaFitter
from lifetimes.utils import summary_data_from_transaction_data
from snapflow import DataBlock, PipeContext, pipe

if TYPE_CHECKING:
    from snapflow_bi import Transaction


@dataclass
class LTVConfig:
    annual_discount_rate: float = 0.2
    future_months_to_project: int = 5 * 12
    observation_period_end: Optional[datetime] = None
    penalizer_coef: float = 0.01


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
            customer_id_col,
            transaction_date_col,
            transaction_amount_col,
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


@pipe(
    "transaction_ltv_model",
    module="bi",
    config_class=LTVConfig,
)
def transaction_ltv_model(
    ctx: PipeContext, transactions: DataBlock[Transaction]
) -> DataBlock:
    tx_df = transactions.as_dataframe()
    penalizer_coef = ctx.get_config_value("penalizer_coef")
    discount_rate = ctx.get_config_value("annual_discount_rate")
    future_months_to_project = ctx.get_config_value("future_months_to_project")
    observation_period_end = ctx.get_config_value("observation_period_end")
    model = LTVModel(penalizer_coef=penalizer_coef)
    return model.compute_ltvs_from_transactions(
        tx_df,
        annual_discount=discount_rate,
        future_months_to_project=future_months_to_project,
        observation_period_end=observation_period_end,
    )
