# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains SnowSetup class specific for setting-up, 
initialization, configuration, and tearing-down of Snowflake environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


from typing import Optional
from snowflake_ai.common import SnowConnect
from snowflake_ai.snowpandas import DataSetup



class SnowSetup(DataSetup):
    """
    This class extends DataSetup for setting-up, initialization,
    configuration, and tearing-down Snowflake specific Environment.

    To use this class, instantiate SnowSetup with SnowConnect as follows:

        from snowflake_ai.common import SnowConnect
        from snowflake_ai.snowpandas import SnowSetup

        conn: SnowConnect = SnowConnect()
        setup: SnowSetup = SnowSetup(conn)
        setup.create_stage()
    """

    def __init__(self, connect: SnowConnect) -> None:
        super().__init__(connect)

    
    def get_connection(self) -> SnowConnect:
        """
        Get snowflake connection for the setup.

        Returns:
            SnowConnect: Snowflake data connection
        """
        if isinstance(self.__connect, SnowConnect):
            return self.__connect
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
        if (stage_name is None) or (not stage_name):
            snow_conn = self.get_connection()
            stage_keys = [
                k for k in snow_conn.configs["data"]["connect"]\
                    [snow_conn.curr_conn_name]["data_setup"].keys() \
                    if str(k).lower().startswith("stage")
            ]
            for sk in stage_keys:
                stage_name = snow_conn.configs["data"]["connect"]\
                    [snow_conn.curr_conn_name]["data_setup"][sk]
                ddl = f"CREATE STAGE IF NOT EXISTS {stage_name}"
                self.get_connection().ddl(ddl)
        else:
            ddl = f"CREATE STAGE IF NOT EXISTS {stage_name}"
            self.get_connection().ddl(ddl)
        

