from snowflake_ai.common import DataFrameFactory
from snowflake_ai.connect import SnowConnect

def test_init_default():
    conn = SnowConnect()
    q = "select current_role()"
    sdf = DataFrameFactory.create_df(q, conn)
    pd = sdf.to_pandas()
    num_result = pd.shape[0]
    assert num_result == 1
     
def test_is_active():
    conn = SnowConnect()
    conn.get_current_connection()
    assert conn.is_current_active() == True

def test_not_active():
    conn = SnowConnect()
    conn.close_connection()
    assert conn.is_current_active() == False

if __name__ == '__main__':
    test_init_default()