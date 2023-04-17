# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains FlowContext class representing the context
of MLOps flow
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


import logging



class FlowContext:
    """
    This class provides a context for the MLOps flow consisting
    of direct input (prior processing/task), context input (full
    historical context sequence), current output, and related
    metadata.

    Example:

    To use this class, instantiate the initial context:

        from snowflake_ai.mlops import FlowContext

        ctx: FlowContext = FlowContext()
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.direct_input = {}
        self.context_input = []
        self.output = {}
        self.metadata = {}

