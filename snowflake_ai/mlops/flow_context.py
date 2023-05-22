# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains FlowContext class representing the context
of MLOps flow
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.2.0"


import logging



class FlowContext:
    """
    This class provides a context for the MLOps flow consisting
    of direct input (prior processing/task), context input (full
    historical context sequence), current output, and related
    metadata.

    Example:

    To use this class, instantiate the initial context:

        >>> from snowflake_ai.mlops import FlowContext
        ... 
        >>> ctx: FlowContext = FlowContext()
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.direct_input = {}
        self.context_input = []
        self.output = {}
        self.metadata = {}

