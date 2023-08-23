# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains SnowSetup class specific for setting-up, 
initialization, configuration, and tearing-down of Snowflake environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


from typing import Optional, Dict, Optional
from snowflake.snowpark import DataFrame as SDF

from snowflake_ai.common import AppConfig
from snowflake_ai.connect import SnowConnect
from snowflake_ai.snowpandas import DataSetup



class SnowSetup(DataSetup):
    """
    This class extends DataSetup for setting-up, initialization,
    configuration, and tearing-down Snowflake specific Environment.

    To use this class, instantiate SnowSetup with SnowConnect as follows:

        >>> from snowflake_ai.common import SnowConnect
        >>> from snowflake_ai.snowpandas import SnowSetup
        ... 
        >>> conn: SnowConnect = SnowConnect()
        >>> setup: SnowSetup = SnowSetup(conn)
        >>> setup.create_stage()
    """
    def __init__(
            self, datasetup_key: str, 
            connect: SnowConnect,
            data: Optional[Dict[str, SDF]] = {},
            app_config: AppConfig = None
    ) -> None:
        super().__init__(
            datasetup_key, 
            connect, 
            {key: sdf.to_pandas() for key, sdf in data.items()},
            app_config
        )
        self._snow_dfs = data


    def get_connect(self) -> SnowConnect:
        """
        Get snowflake connection for the setup.

        Returns:
            SnowConnect: Snowflake data connection
        """
        if isinstance(self._connect, SnowConnect):
            return self._connect
        else:
            raise TypeError(
                "SnowSetup should be initialized with SnowConnect"
            )


    def create_stage(
        self, 
        stage_name: Optional[str] = None
    ):
        """
        Create a snowflake stage in the database referenced by
        SnowConnect configuration.

        Args:
            stage_name (str): snowflake stage name. if none or empty,
                use SnowConnect config parameter     

        Returns:
            None: succesful creation of snowflake stage
        """
        if (stage_name is not None) and (stage_name):
            ddl = f"CREATE STAGE IF NOT EXISTS {stage_name}"
            self.get_connect().ddl(ddl)
        
