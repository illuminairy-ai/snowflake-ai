# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains SnowEDA and SnowPrep class specific for 

"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


from typing import (
    Dict, Union, Optional
)
from pandas import DataFrame

from snowflake_ai.snowpandas import EDA, DataPrep, SnowSetup



class SnowEDA(EDA):
    """
    This class extends EDA for Snowflake specific data exploration.

    To use this class, instantiate SnowEDA with SnowSetup as follows:

        from snowflake_ai.common import SnowConnect
        from snowflake_ai.snowpandas import SnowSetup

        conn: SnowConnect = SnowConnect()
        setup: SnowSetup = SnowSetup(conn)
        eda: SnowEDA = SnowEDA(setup)
    
    """
    def __init__(
        self, 
        setup: SnowSetup,
        dfs: Optional[Union[DataFrame, Dict[str, DataFrame]]] = None
    ):
        super().__init__(setup, dfs)


    def get_setup(self) -> SnowSetup:
        if isinstance(self.__setup, SnowSetup):
            return self.__setup
        else:
            raise TypeError(
                "SnowEDA.get_setup(): Initialization of setup should be SnowSetup"
            )



class SnowPrep(DataPrep):
    """
    This class extends EDA for Snowflake specific data exploration.

    To use this class, instantiate SnowEDA with SnowSetup as follows:

        from snowflake_ai.common import SnowConnect
        from snowflake_ai.snowpandas import SnowSetup

        conn: SnowConnect = SnowConnect()
        setup: SnowSetup = SnowSetup(conn)
        eda: SnowEDA = SnowEDA(setup)
    
    """

    def __init__(
        self, 
        setup: SnowSetup,
        dfs: Optional[Union[DataFrame, Dict[str, DataFrame]]] = None
    ):
        super().__init__(setup, dfs)

    
    def get_setup(self) -> SnowSetup:
        if isinstance(self.__setup, SnowSetup):
            return self.__setup
        else:
            raise TypeError(
                "SnowEDA.get_setup(): Initialization of setup should be SnowSetup"
            )