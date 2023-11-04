import pandas as pd
from pandera import DataFrameSchema, Column
from sqlalchemy.engine import create_engine

SCHEMA = DataFrameSchema({
    'id': Column(int),
    'name': Column(str)
})

data = {'id': [1, 2], 'name': ['aa', 'bb']}
df = pd.DataFrame(data=data)


def test(df: pd.DataFrame) -> pd.DataFrame:
    df_tf = df.rename({'id': 'id', 'name': 'name'})
    return SCHEMA.validate(df_tf)


df_test = test(df)

engine = create_engine("mysql+mysqldb://root:nhuan@10.5.0.3/data_mart")

df_test.to_sql(con=engine, name='test', if_exists='append', index=False)
