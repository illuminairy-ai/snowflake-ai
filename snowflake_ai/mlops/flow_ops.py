#!/usr/bin/env python3
# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains FlowOps class for ML flow operationization
targeting production domain/environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


from snowflake_ai.mlops import Pipeline


class FlowOps(Pipeline):
    """
    This class provides general setup for MLops flow in
    operationalizing the model pipeline built, including deloyment
    serving the model, monitor and re-train the model 
    (To-DO).
    """

    def __init__(self) -> None:
        super().__init__()
