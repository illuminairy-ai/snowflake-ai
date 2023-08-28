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
__version__ = "0.5.0"


import logging
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from snowflake_ai.common import AppConfig, ConfigType, ConfigKey
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

    _logger = logging.getLogger(__name__)
    _step_id = 0

    def __init__(
            self,
            func: Callable, 
            ctx: FlowContext,
            task_type: TaskType = None, 
            predecessor: 'Step' = None, 
            successor: 'Step' = None,
            iteration = 1
        ):
        self.func = func
        self.context = ctx
        self.task_type = task_type
        self.predecessor = predecessor
        self.successor = successor
        self.iteration = iteration
        self.error = False
        Step._step_id += 1
        self.step_id = Step._step_id



class Pipeline:
    """
    This class provides general setup for MLops flow in
    model building, training, validation and testing. 
    """
    _logger = logging.getLogger(__name__)

    def __init__(
            self, 
            pipeline_key: str,
            ctx: FlowContext = None,
            app_config: AppConfig = None
    ) -> None:
        self.logger = Pipeline._logger
        self.pipeline_key = pipeline_key
        _, self.pipeline_name = AppConfig.split_group_key(self.pipeline_key)
        if ctx is None:
            self.flow_context = FlowContext()
        else:
            self.flow_context = ctx
        self.flow_context.pipelines[pipeline_key] = {}
        self.pipeline_context = \
                self.flow_context.pipelines[pipeline_key]
        self.app_config = app_config
        self.steps : List[Step] = []
        self.step_tasks : List[TaskType] = []
        self.run: Callable = None
        self._init_pipeline_config()



    def _init_pipeline_config(self):
        conf_d = AppConfig.get_all_configs() if self.app_config is None \
                else self.app_config.get_all_configs()
        _, pp_config = AppConfig.get_group_item_config(
                self.pipeline_key,
                ConfigType.MLPipelines.value,
                conf_d
            )
        self.pipeline_type = pp_config.get(ConfigKey.TYPE.value, "")
        self.type = self.pipeline_type
        self.execution_mode = pp_config.get(ConfigKey.EXE_MODE.value, "")
        self.script = pp_config.get(ConfigKey.SCRIPT.value, "")
        self.class_name = pp_config.get(ConfigKey.CLASS_NAME.value, "")
        self.run_method = pp_config.get(ConfigKey.RUN.value, "")
        self.step_tasks = pp_config.get(ConfigKey.STEP_TASKS.value, [])

        rt = "" if self.app_config is None else self.app_config.root_path
        s = "" if self.app_config is None else self.app_config.script_home
        script_module = AppConfig.load_module(self.script, rt, s)
        if self.class_name:
            class_ = getattr(script_module, self.class_name)
            instance = class_()
            self.run = getattr(instance, self.run_method, "run") 
        else:
            self.run = getattr(script_module, self.run_method, "run")


    def execute(self):
        step_idx = 0
        task_idx = 0
        k_pp = self.pipeline_key
        ctx_pp: Dict[str, Dict] = self.flow_context.pipelines.get(k_pp, {})

        while task_idx < len(self.step_tasks):
        
            task_type = self.step_tasks[task_idx]
            step = self.steps[step_idx]
            step_name = step.func.__name__
            ctx_pp.setdefault(step_name, {FlowContext.T_STEP_ITER, 1})
            n_iter = ctx_pp[step_name][FlowContext.T_STEP_ITER]

            if step.task_type == task_type:
                self.logger.debug("Pipeline.execute(): "\
                        f"Executing Pipeline [{self.pipeline_key}];"\
                        f" Step [{step_name}]; Task index: {task_idx}")

                step.func(self.flow_context)

                if step.error:
                    self.logger.error("Pipeline.execute(): "\
                        f"Error in Step [{step_name}] in Pipeline "\
                        f"[{self.pipeline_key}]!")
                    return

                n_iter -= 1
                ctx_pp[step_name][FlowContext.T_STEP_ITER] = n_iter

                if n_iter > 0:
                    continue 
                elif step.successor:
                    step_idx = self.steps.index(
                        [s for s in self.steps if \
                                s.func.__name__ == step.successor][0]
                    )
                else:
                    step_idx += 1
                    task_idx += 1
            else:
                step_idx += 1


    def step(self, ctx: FlowContext, task_type, 
                predecessor=None, successor=None, iteration=1):
        def decorator(func):
            step_obj = Step(
                    func, self.flow_context, task_type, 
                    predecessor, successor)
            self.steps.append(step_obj)

            step_name = func.__name__
            ctx.pipelines.setdefault(self.pipeline_key, {})
            ctx.pipelines[self.pipeline_key][step_name] = \
                    {FlowContext.T_STEP_ITER: iteration}
            self.steps.append(Step(func, self.flow_context, task_type, 
                    predecessor, successor, iteration))

            return func
        return decorator


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
        if self.run is None:
            raise ValueError("Pipeline.run(): Misses main run function!")
        else:
            self.run(ctx, **ctx.direct_inputs)
        return 0
