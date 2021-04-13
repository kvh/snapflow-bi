from __future__ import annotations

from snapflow import Environment, graph, produce
from snapflow.testing.utils import str_as_dataframe


def test_ltv():
    import snapflow_bi as bi

    input_data = """
        customer_id,transacted_at,amount
        1,2020-01-01 00:00:00,100
        2,2020-02-01 00:00:00,100
        2,2020-03-01 00:00:00,100
        3,2020-01-01 00:00:00,300
        3,2020-04-01 00:00:00,400
        4,2020-01-01 00:00:00,100
        4,2020-02-01 00:00:00,100
        4,2020-03-01 00:00:00,50
        5,2020-01-01 00:00:00,1000
    """
    txs = str_as_dataframe(input_data, nominal_schema=bi.schemas.Transaction)

    env = Environment(metadata_storage="sqlite://")
    g = graph()
    df = g.create_node(
        "core.import_dataframe", params={"dataframe": txs, "schema": "bi.Transaction"}
    )
    ltv = g.create_node(bi.snaps.transaction_ltv_model, upstream=df)

    output = produce(ltv, env=env, modules=[bi])
    output_df = output.as_dataframe()
    assert len(output_df) == 5
    assert set(output_df["customer_id"]) == set(i for i in range(1, 6))
