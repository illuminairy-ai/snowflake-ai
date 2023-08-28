# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains SnowEDA and SnowPrep class specific for 

"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


from typing import (
    Dict, Union, Optional, List
)
import logging

from pandas import DataFrame as DF
import json
from sklearn.preprocessing import LabelEncoder
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType

from snowflake.snowpark import DataFrame as SDF
from snowflake.snowpark.functions import col, object_construct, CaseExpr
from snowflake.snowpark.functions import lit, array_agg, to_varchar, when
from snowflake.snowpark.functions import sql_expr, iff

from snowflake_ai.snowpandas import EDA, DataPrep, SnowSetup



class SnowEDA(EDA):
    """
    This class extends EDA for Snowflake specific data exploration.

    In general, the framework can bootstrap and use this class implicitly,
    but if the client want to use this class directly, instantiate SnowEDA
    with SnowEDA as follows:

        >>> from snowflake_ai.common import SnowConnect
        >>> from snowflake_ai.snowpandas import SnowSetup
        ...
        >>> conn: SnowConnect = SnowConnect()
        >>> setup: SnowSetup = SnowSetup(conn)
        >>> eda: SnowEDA = SnowEDA(setup)
    
    """
    _logger = logging.getLogger(__name__)

    def __init__(
        self, 
        setup: SnowSetup,
        dfs: Optional[Dict[str, SDF]] = None
    ):
        super().__init__(setup)
        self.logger = SnowEDA._logger
        self._sdf = SDF()
        if dfs is not None and isinstance(dfs, Dict):
            for _, value in dfs.items():
                if not isinstance(value, SDF):
                    raise TypeError(
                        f"Dictionary values must be DataFrames, "
                        f"but got {type(value)} instead!"
                    )
            self._data = dfs


    def all_dfs(self) -> Dict[str, SDF]:
        """
        Get all dataframes for exploration.

        Returns:
            Dict: dictionary of dataframes
        """
        return self._data
    

    def active_df(
        self, 
        df_name: Optional[str] = None,
        df: Optional[SDF] = None
    ) -> SDF:
        """
        Get active Snowflake dataframe for exploration.

        Args:
            df_name (str): Snowflake Dataframe name.
            df (DataFrame): set the Snowflake dataframe as active

        Returns:
            DataFrame: active snowflake dataframe for exploration
        """
        if df_name is not None :
            try:
                if df_name in self._data.keys():
                    self._sdf = self._data[df_name]
            except:
                raise ValueError(
                    f"SnowPrep.active_df (): Dictionary of DataFrame doesn't "\
                    f"have the key [{df_name}]!"
                )
            if df is not None:
                self._sdf = df
                self._data[df_name] = df
        elif df is not None:
            self._sdf = df
            self._data[EDA.T_DEFAULT_DF] = df            
        return self._sdf
    

    @staticmethod
    def top_df(df: SDF, n: int = 10, by_col: str = None,
        is_asc: bool = False
    ) -> DF:
        """
        Return pandas dataframe of top n rows from snowflake dataframe
        ordered by one column

        Args:
            df (DataFrame): Snowflake dataframe
            n (int): top n rows
            by_col (str): name of the column ordered by
            is_asc (bool): True if it is ascending order; default False
         
        Returns:
            DataFrame: Pandas Dataframe.
        """
        if by_col is None:
            return df.limit(n).to_pandas()
        else:
            sorted_sdf = df.sort(col(by_col), ascending=is_asc)
            return sorted_sdf.limit(n).to_pandas()
        

    def desc(self, df_name: Optional[str] = None):
        """
        Describe the dataframe by dataframe name.

        Args:
            df_name (str): dataframe name. if emptry, the current
                active dataframe desc is called.
         
        Returns:
            Series or DataFrame: Summary statistics of the Series or
                Dataframe provided.
        """
        if df_name is None:
            return self._df.describe()
        elif df_name is not None and isinstance(self._data, Dict):
            return self._data[df_name].describe()
        else:
            raise TypeError(
                f"SnowPrep.desc(): df_name or initialization of dataframe"\
                f" is required!"
            )



class SnowPrep(DataPrep):
    """
    This class extends DataPrep for Snowflake specific data preparation.

    To use this class directly, instantiate SnowPrep as follows:

        >>> from snowflake_ai.common import SnowConnect
        >>> from snowflake_ai.snowpandas import SnowSetup
        ... 
        >>> conn: SnowConnect = SnowConnect()
        >>> setup: SnowSetup = SnowSetup(conn)
        >>> sp: SnowPrep = SnowPrep(setup)
    
    """
    _logger = logging.getLogger(__name__)

    def __init__(
        self, 
        setup: SnowSetup,
        dfs: Optional[Union[SDF, Dict[str, SDF]]] = None
    ):
        super().__init__(setup)
        self.logger = SnowPrep._logger
        
    
    @property
    def data_setup(self) -> SnowSetup:
        if isinstance(self.data_setup, SnowSetup):
            return self.data_setup
        else:
            raise TypeError(
                "SnowPrep.data_setup(): The datasetup should be SnowSetup!"
            )    


    @staticmethod
    def check_input_columns(df:SDF, input_columns):
        """
        Checks and formats the input_columns parameter.

        If input_columns is None, it defaults to using all columns 
        in the DataFrame. If input_columns is not a list (i.e., it's a
        single column name), it wraps it in a list.

        Parameters:
            df (DataFrame): The DataFrame whose columns are to be checked.
            input_columns (None, str, list): The columns to check. This 
            could be None, a single column name, or a list of column names.

        Returns:
            list: A list of column names from the DataFrame.
        """
        if not input_columns:
            input_columns = df.columns
        else:
            if not isinstance(input_columns, list):
                input_columns = [input_columns]

        return input_columns


    @staticmethod
    def get_columns_unique_categories(
        df: SDF, 
        categories : Dict = {}, 
        cat_cols: List = []
    ):
        """
        Generate a dictionary containing unique categories for each specified
        column in the DataFrame.
        
        Parameters:
        df (DataFrame): The DataFrame to extract categories from.
        categories (str, dict): Parameter to determine whether to automatically 
            generate categories. If empty, unique categories are computed for each
            column. If it's a dictionary, it's assumed that the unique categories 
            for each column are already defined.
        cat_cols (list): The list of column names to extract categories from.
        
        Returns:
            dict: A dictionary where keys are column names and values are lists of
            unique categories.
        """
        if categories == "":
            obj_const_args = []
            
            for c in cat_cols:
                # Get a sorted list of unique string values in the column
                unique_values_agg = array_agg(to_varchar(c), is_distinct=True)\
                    .within_group(to_varchar(col(c)).asc())
                # Add column name and unique values to the argument list
                obj_const_args.extend([lit(c), unique_values_agg])
            
            # Create a DataFrame with one row and one column named "cats",
            # containing a dictionary of unique categories for each column
            df_cats = df.select(
                    object_construct(*obj_const_args).alias("cats")
                )

            cats_dict = json.loads(df_cats.collect()[0]["cats"])
        else: 
            cats_dict = categories

        return cats_dict

