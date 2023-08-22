# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains MLOps class for ML flow operationization
targeting production domain/environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.4.0"


import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from snowflake_ai.common import AppConfig
from snowflake_ai.mlops import FlowContext
from snowflake_ai.mlops import Pipeline, TaskType


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]  %(message)s'
)



class MLOps:
    pass



class PipelineFlow:
    """
    This class manages multiple pipelines as an overall flow in MLOps
    operationalizing the model pipeline built, including deloyment
    serving the model, monitor and re-train the model 
    (To-DO).
    """

    def __init__(
            self,
            flow_key: str,
            ctx: FlowContext = None,
            app_config: AppConfig = None            
    ) -> None:
        super().__init__()
        self.logger = logging.getLogger(__name__)


    def execute(self):
        pass
