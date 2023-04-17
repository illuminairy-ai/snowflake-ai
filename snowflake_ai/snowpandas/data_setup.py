# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains DataSetup class for setting-up, initialization,
configuration, and tearing-down of data Environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


from typing import Optional
from pandas import DataFrame as DF

from snowflake_ai.common import DataConnect, FileConnect, DataFrameFactory



class DataSetup:
    """
    This class sets up, initializes, configures or tears down
    the data Environment

    To use this class, instantiate DataSetup with appropriate DataConnect
    as follows:

        from snowflake_ai.common import FileConnect
        from snowflake_ai.snowpandas import DataSetup

        conn: FileConnect = FileConnect()
        setup: DataSetup = DataSetup(conn)
        df = setup.load_data()
    """

    def __init__(self, connect: DataConnect) -> None:
        self.__connect = connect

    

    def get_connection(self) -> DataConnect:
        """
        Get the data connection for the setup.

        Returns:
            DataConnect: data connection object for the setup
        """        
        return self.__connect
    

    def load_data(self, file_name: Optional[str] = None) -> DF:
        """
        Load pandas dataframe from local-csv file connection.

        Returns:
            DataFrame: Pandas dataframe
        """
        if isinstance(self.__connect, FileConnect):
            if (file_name is None) or (file_name.strip() == ""):
                return DataFrameFactory.create_df(
                    "", self.__connect
                )  # type: ignore
            else:
                df: DF = DataFrameFactory.create_df(
                    file_name, self.__connect
                )  # type: ignore
                df.columns = df.columns.str.strip().str.lower()\
                    .str.replace(' ', '_')
                return df
        else:
            raise ValueError(
                "DataSetup Error. Loading File requires FileConnect"
            )


    def clean_data(self, file_name: Optional[str] = None):
        # TO-DO: clean up file
        pass