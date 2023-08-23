# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains FlowTest class for testing ML flow targeting
test/stage domain/environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


from snowflake_ai.mlops import Pipeline


class FlowTest(Pipeline):
    """
    This module contains FlowTest class for testing ML flow targeting
    test/stage domain/environment
    """

    def __init__(self) -> None:
        super().__init__()


    @Pipeline.flow
    def task_1(self):
        self.flow_context.direct_inputs = {"a": 1}
        return self
    

    @Pipeline.flow
    def task_2(self):
        self.flow_context.direct_inputs = {"a": 2}
        return self
    

    @Pipeline.flow
    def task_3(self):
        ctx = self.flow_context
        ns = [x["a"] for x in ctx.context_inputs if "a" in x]
        ctx.output = {"sum" : sum(ns)}
        return self
    

    def test_main(self):
        self.task_1().task_2().task_3()
        assert self.flow_context.outputs["sum"] == 3
    

if __name__ == '__main__':
    t = FlowTest()
    t.test_main()
