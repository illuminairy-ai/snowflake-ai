from snowflake_ai.mlops.flow_test import FlowTest
    

def test_flow():
    t = FlowTest("default")
    t.task_1().task_2().task_3()
    assert t.flow_context.outputs["sum"] == 3