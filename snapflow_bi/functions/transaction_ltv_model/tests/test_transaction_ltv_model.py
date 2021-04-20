inputs = {
    "transactions": dict(
        data="""
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
        """,
        schema="bi.Transaction",
    )
}

outputs = {
    "default": """
    customer_id,ltv
    1,100
    2,100
    3,400
    4,50
    5,1000
    """
}


# from __future__ import annotations

# from dcp.storage.database.utils import get_tmp_sqlite_db_url
# from snapflow import Environment, graph, produce
# from snapflow.testing.utils import str_as_dataframe


# def test_ltv():
#     from snapflow_bi import module as bi

#     input_data = """
#         customer_id,transacted_at,amount
#         1,2020-01-01 00:00:00,100
#         2,2020-02-01 00:00:00,100
#         2,2020-03-01 00:00:00,100
#         3,2020-01-01 00:00:00,300
#         3,2020-04-01 00:00:00,400
#         4,2020-01-01 00:00:00,100
#         4,2020-02-01 00:00:00,100
#         4,2020-03-01 00:00:00,50
#         5,2020-01-01 00:00:00,1000
#     """
#     env = Environment(metadata_storage=get_tmp_sqlite_db_url())
#     txs = str_as_dataframe(env, input_data, nominal_schema=bi.schemas.Transaction)

#     g = graph()
#     df = g.create_node(
#         "core.import_dataframe", params={"dataframe": txs, "schema": "bi.Transaction"}
#     )
#     ltv = g.create_node(bi.functions.transaction_ltv_model, upstream=df)

#     blocks = produce(ltv, env=env, modules=[bi])
#     output_df = blocks[0].as_dataframe()
#     assert len(output_df) == 5
#     assert set(output_df["customer_id"]) == set(i for i in range(1, 6))
