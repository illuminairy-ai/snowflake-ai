from snowflake_ai.common import AppConfig, DataConnect

def test_init():
    AppConfig("app_1")
    b = AppConfig("group_1.app_1")
    # two apps are initialized
    assert len(b.apps) == 2


if __name__ == '__main__':
    conn = DataConnect("snowflake_default")
    print(f"DEBUG: all connects => {conn.app_connects}")