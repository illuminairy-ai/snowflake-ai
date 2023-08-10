# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains Pipeline class for ML flow focusing on model
building, training, validation and testing in DEV domain.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


import logging
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from snowflake_ai.mlops import FlowContext


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]  %(message)s'
)



class TaskType(Enum):
    Setup = "setup"
    Ingest = "ingest"
    PreProcess = "preprocess"
    PreModel = "premodel"
    PreTrain = "pretrain"
    PrepData = "prepdata"
    PrepModel = "prepmodel"
    Learn = "learn"
    Train = "train"
    Validate = "validate"
    FineTune = "finetune"
    Test = "test"
    Predict = "predict"
    PostProcess = "postprocess"
    Evaluate = "evaluate"
    Deploy = "deploy"
    Serve = "serve"
    Feedback = "feedback"
    Monitor = "monitor"
    Retrain = "retrain"
    Cleanup = "cleanup"
    Custom = "custom"



class Step:

    def __init__(
            self,
            fn: Callable, 
            ctx: FlowContext,
            conditions: List[Callable] = None, 
            task_type: str = None, 
            predecessors: List['Step'] = None, 
            successors: List['Step'] = None
        ):
        self.fn = fn
        self.context = ctx
        self.conditions = conditions if conditions is not None else []
        self.task_type = task_type
        self.predecessors = predecessors if predecessors is not None else []
        self.successors = successors if successors is not None else []
        self.finished = False


    def execute(self, context):
        if all(condition(context) for condition in self.conditions):
            for _ in range(self.iterations):
                context = self.fn(context)
        return context



class Pipeline:
    """
    This class provides general setup for MLops flow in
    model building, training, validation and testing. 
    """

    def __init__(self, ctx: FlowContext = None) -> None:
        self.logger = logging.getLogger(__name__)
        if ctx is None:
            self.flow_context = FlowContext()
        else:
            self.flow_context = ctx
        self.main : Callable = None
    

    def context(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(
                self.flow_context,
                **kwargs
            )
        return wrapper


    @classmethod
    def flow(cls, func):
        def wrapper(
            obj: Pipeline
        ):
            _logger = logging.getLogger(__name__)
            ctx = FlowContext()
            if obj.flow_context is None:
                obj.flow_context = ctx
            else:
                ctx = obj.flow_context

            if ctx.direct_inputs is None:
                _logger.warn("Pipeline.flow(): Context input is None!")
                ctx.direct_inputs = {}
                return obj
            elif ctx.context_inputs is None:
                _logger.warn("Pipeline.flow(): Context input is None!")
                ctx.context_inputs = []
                return obj
            else:
                if ctx.direct_inputs and ctx.context_inputs:
                    ctx.context_inputs.append(ctx.direct_inputs)
                    func(obj)
                    ctx.context_inputs.append(ctx.outputs)
                    return obj
                elif ctx.context_inputs:
                    func(obj)
                    ctx.context_inputs.append(ctx.outputs)
                    return obj
                else:
                    ctx.context_inputs = []
                    func(obj)
                    ctx.context_inputs.append(ctx.outputs)
                    return obj
        return wrapper
    

    def run(self):
        ctx = self.flow_context
        if self.main is None:
            raise ValueError("Pipeline.run(): Misses main function!")
        else:
            self.main(ctx, **ctx.direct_inputs)
        return 0
