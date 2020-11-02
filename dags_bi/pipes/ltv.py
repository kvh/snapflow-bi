from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from dags import DataBlock, DataSet, pipe, PipeContext
from lifetimes import BetaGeoFitter, GammaGammaFitter
from lifetimes.utils import summary_data_from_transaction_data


@dataclass
class LTVConfig:
    annual_discount_rate: float = .2
    future_months_to_project: int = 5 * 12
    observation_period_end: Optional[datetime] = None


class LTVModel:

    def get_spend_model(self):
        return GammaGammaFitter(penalizer_coef=0.01)

    def get_recurrence_model(self):
        return BetaGeoFitter(penalizer_coef=0.01)

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
        customer_id_col="master_customer_id",
        order_date_col="order_date",
        order_amount_col="total_price",
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
            "customer_id",
            "transacted_at",
            "amount",
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
def transaction_ltv_model(ctx: PipeContext, transactions: DataSet[bi.Transaction]) -> DataSet:
    tx_df = transactions.as_dataframe()
    discount_rate = ctx.get_config_value("annual_discount_rate")
    future_months_to_project = ctx.get_config_value("future_months_to_project")
    observation_period_end = ctx.get_config_value("observation_period_end")
    model = LTVModel()
    return model.compute_ltvs_from_transactions(tx_df,
        annual_discount=discount_rate,
        future_months_to_project=future_months_to_project,
        observation_period_end=observation_period_end)



