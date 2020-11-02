from __future__ import annotations

from dags import Environment
from dags.testing.utils import (
    DataInput,
    get_tmp_sqlite_db_url,
    produce_pipe_output_for_static_input,
)


def test_ltv():
    import dags_bi as bi
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
    data_input = DataInput(input_data, schema="bi.Transaction")
    s = get_tmp_sqlite_db_url()
    md = get_tmp_sqlite_db_url()
    env = Environment(metadata_storage=md)
    env.add_module(bi)
    db = produce_pipe_output_for_static_input(
        bi.pipes.transaction_ltv_model, input=data_input, env=env, target_storage=s, 
    )
    df = db.as_dataframe()
    assert len(df) == 5
    assert set(df["customer_id"]) == set(str(i) for i in range(1,6))

