Business Intelligence module for the [snapflow](https://github.com/kvh/snapflow) framework.

#### Install

`pip install snapflow-bi` or `poetry add snapflow-bi`

#### Example

```python
from snapflow_bi import module as bi
from snapflow import graph, produce
from snapflow.testing.utils import str_as_dataframe

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

g = graph()
df = g.create_node(
    "core.import_dataframe", params={"dataframe": txs, "schema": "bi.Transaction"}
)
ltv = g.create_node(bi.functions.transaction_ltv_model, upstream=df)

blocks = produce(ltv, modules=[bi])
print(blocks[0].as_dataframe())
```
