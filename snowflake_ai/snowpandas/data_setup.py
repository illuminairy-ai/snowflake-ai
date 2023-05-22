# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains DataSetup class for setting-up, initialization,
configuration, and tearing-down of data Environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.2.0"


from typing import Optional, Dict
from pandas import DataFrame as DF

from snowflake_ai.common import DataConnect
from snowflake_ai.connect import FileConnect, \
    SnowConnect, DataFrameFactory



class DataSetup:
    """
    This class sets up, initializes, configures or tears down
    the data Environment

    To use this class, instantiate DataSetup with appropriate DataConnect
    as follows:

        >>> from snowflake_ai.common import FileConnect
        >>> from snowflake_ai.snowpandas import DataSetup
        ... 
        >>> conn: FileConnect = FileConnect()
        >>> setup: DataSetup = DataSetup(conn)
        >>> df = setup.load_data()
    """
    def __init__(
        self, 
        datasetup_key: str,
        connect: DataConnect, 
        data: Optional[Dict[str, DF]] = {}
    ) -> None:
        self.datasetup_key = datasetup_key
        self._connect = connect
        self._data = data
        if data is not None:
            self._data = data


    def get_connect(self) -> DataConnect:
        """
        Get the data connection for the setup.

        Returns:
            DataConnect: data connection object for the setup
        """        
        return self._connect
    

    def load_data(self, file_name: Optional[str] = None) -> DF:
        """
        Load pandas dataframe from local-csv file connection.

        Returns:
            DataFrame: Pandas dataframe
        """
        if isinstance(self._connect, FileConnect):
            if (file_name is None) or (file_name.strip() == ""):
                return DataFrameFactory.create_pdf(
                    "", self._connect
                )
            else:
                df: DF = DataFrameFactory.create_pdf(
                    file_name, self._connect
                )
                
                df.columns = df.columns.str.strip().str.lower()\
                    .str.replace(' ', '_')
                return df
        elif isinstance(self._connect, SnowConnect):
            pass
        else:
            raise ValueError(
                "DataSetup Error. Loading File requires FileConnect"
            )


    def _load_file_data(self):
        pass


    def _load_snowflake_data(self):
        pass


    def get_data(self) -> Dict[str, DF]:
        """
        Get all dataframes for exploration and preparation.

        Returns:
            Dict: dictionary of dataframes
        """
        return self._data


    def clean_data(self, file_name: Optional[str] = None):
        self._data = {}
        self._connect.close_connection()