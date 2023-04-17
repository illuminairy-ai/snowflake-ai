# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains Pipeline class for ML flow focusing on model
building, training, validation and testing in DEV domain.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


import logging

from snowflake_ai.mlops import FlowContext


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]  %(message)s'
)



class Pipeline:
    """
    This class provides general setup for MLops flow in
    model building, training, validation and testing. 
    (TO-DO).
    """
    
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.flow_context = FlowContext()

    @classmethod
    def flow(cls, func):
        def wrapper(
            obj
        ):
            _logger = logging.getLogger(__name__)
            ctx = FlowContext()
            if obj.flow_context is None:
                obj.flow_context = ctx
            else:
                ctx = obj.flow_context

            if ctx.direct_input is None:
                _logger.warn("FlowContext direct input is None")
                ctx.direct_input = {}
                return obj
            elif ctx.context_input is None:
                _logger.warn("FlowContext context input is None")
                ctx.context_input = []
                return obj
            else:
                if ctx.direct_input and ctx.context_input:
                    ctx.context_input.append(ctx.direct_input)
                    func(obj)
                    ctx.context_input.append(ctx.output)
                    return obj
                elif ctx.context_input:
                    func(obj)
                    ctx.context_input.append(ctx.output)
                    return obj
                else:
                    ctx.context_input = []
                    func(obj)
                    ctx.context_input.append(ctx.output)
                    return obj
        return wrapper
    

    def run(self):
        return 0
