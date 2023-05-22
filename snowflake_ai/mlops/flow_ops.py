# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains FlowOps class for ML flow operationization
targeting production domain/environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.2.0"


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
