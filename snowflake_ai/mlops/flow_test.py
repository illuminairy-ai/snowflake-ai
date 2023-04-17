#!/usr/bin/env python3
# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains FlowTest class for testing ML flow targeting
test/stage domain/environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


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
        self.flow_context.direct_input = {"a": 1}
        return self
    

    @Pipeline.flow
    def task_2(self):
        self.flow_context.direct_input = {"a": 2}
        return self
    

    @Pipeline.flow
    def task_3(self):
        ctx = self.flow_context
        ns = [x["a"] for x in ctx.context_input if "a" in x]
        ctx.output = {"sum" : sum(ns)}
        return self
    

    def test_main(self):
        self.task_1().task_2().task_3()
        assert self.flow_context.output["sum"] == 3
    

if __name__ == '__main__':
    t = FlowTest()
    t.test_main()
