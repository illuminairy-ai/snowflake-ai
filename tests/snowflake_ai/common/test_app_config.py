from snowflake_ai.common import AppConfig, ConfigType


def test_init_app():
    a = AppConfig("group_def.app1")
    assert a.app_key == "group_def.app1"

def test_base_app():
    a = AppConfig("group_def.app1")
    ba = a.get_all_configs().get(ConfigType.BaseApps.value)
    assert len(ba) > 0


if __name__ == '__main__':
    test_init_app()