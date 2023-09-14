
from snowflake_ai.connect import AuthCodeConnect
from snowflake_ai.common import AppConfig

def test_init_default():
    a = AppConfig("group_def.app1")
    acc = AuthCodeConnect(app_config=a)
    assert acc.app_config is not None


if __name__ == '__main__':
    test_init_default()