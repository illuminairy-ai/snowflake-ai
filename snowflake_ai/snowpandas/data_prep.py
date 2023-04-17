# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains EDA and DataPrep class for general or pandas-like 
data exploration, splits, and preprocessing.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


from typing import (
    Dict, Union, Optional, List, Iterable
)
from pandas import DataFrame, Series, to_numeric

import matplotlib.pyplot as plt

from snowflake_ai.common import DataConnect
from snowflake_ai.snowpandas import DataSetup



class EDA:
    """
    This class provides pandas dataframe exploration.

    To use this class, initialize it with DataSetup as follows:

        from snowflake_ai.snowpandas import DataSetup

        eda: EDA = EDA(setup)
        
    """    
    DEFAULT_DF = "default"


    def __init__(
        self, 
        setup: DataSetup,
        dfs: Optional[Union[DataFrame, Dict[str, DataFrame]]] = None
    ):
        self.__setup = setup
        self.__df = DataFrame()
        self.__data = {}
        if dfs is not None and isinstance(dfs, DataFrame):
            self.__df = dfs
        elif dfs is not None and isinstance(dfs, Dict):
            for key, value in dfs.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"Dictionary keys must be strings, got {type(key)}"
                    )
                if not isinstance(value, DataFrame):
                    raise TypeError(
                        f"Dictionary values must be DataFrames, got {type(value)}"
                    )
            self.__data = dfs
        

    def get_setup(self):
        return self.__setup


    def all_dfs(self) -> Dict[str, DataFrame]:
        """
        Get all dataframes for exploration.

        Returns:
            Dict: dictionary of dataframes
        """
        return self.__data


    def active_df(
        self, 
        df_name: Optional[str] = None,
        df: Optional[DataFrame] = None
    ) -> DataFrame:
        """
        Get active dataframe for exploration.

        Args:
            df_name (str): Dataframe name.
            df (DataFrame): set the dataframe as active

        Returns:
            DataFrame: active dataframe for exploration
        """
        if df_name is not None :
            try:
                if df_name in self.__data.keys():
                    self.__df = self.__data[df_name]
            except:
                raise ValueError(
                    f"Dictionary of DataFrame doesn't have the key: {df_name}"
                )
            if df is not None:
                self.__df = df
                self.__data[df_name] = df
        elif df is not None:
            self.__df = df
            self.__data[EDA.DEFAULT_DF] = df            
        return self.__df


    @staticmethod
    def top_df(df: DataFrame):
        """
        Return top 10 rows of input data frame
        """        
        return df.head(10)


    def top(self, df_name: Optional[str] = None):
        """
        Return top 10 rows of the data frame by name
        """           
        if df_name is None:
            return self.__df.head(10)
        elif df_name is not None and isinstance(self.__data, Dict):
            return self.__data[df_name].head(10)
        else:
            raise TypeError(
                f"df_name or initialization of dataframe is required"
            )


    @staticmethod
    def desc_df(
        df: DataFrame, 
        includes: Optional[List] = None, 
        excludes: Optional[List] = None
    ):
        """
        Describe the dataframe.

        Args:
            df (DataFrame): dataframe to be described
            includes (List): Includes list of data types in the result
            excludes (List) : Excludes the provided data types from 
                the result. 
        Returns:
            Series or DataFrame: Summary statistics of the Series or
                Dataframe provided.
        """
        if includes is None and excludes is None:
            return df.describe()
        elif includes is not None and excludes is None:
            return df.describe(include = includes)
        elif includes is None and excludes is not None:
            return df.describe(exclude = excludes)
        else:
            return df.describe(include = includes, exclude= excludes)


    def desc(self, df_name: Optional[str] = None):
        if df_name is None:
            return self.__df.describe()
        elif df_name is not None and isinstance(self.__data, Dict):
            return self.__data[df_name].describe()
        else:
            raise TypeError(
                f"df_name or initialization of dataframe is required"
            )


    @staticmethod
    def corr_df(df: DataFrame, label_name: str):
        corr_mat = df.corr()
        return corr_mat[label_name].sort_values(ascending=False)


    def corr(self, label_name: str, feat_name: Optional[str] = None) \
            -> Union[float, Series, None]:
        if label_name in self.__df.columns and feat_name is None:
            corr_mat: DataFrame = self.__df.corr()
            return corr_mat[label_name].sort_values(ascending=False)
        elif label_name in self.__df.colums and \
                feat_name in self.__df.columns:
            self.__df[feat_name] = to_numeric(
                self.__df[feat_name], errors='coerce'
            )
            self.__df[label_name] = to_numeric(
                self.__df[label_name], errors='coerce')
            return Series(self.__df[feat_name]).corr(Series(self.__df[label_name]))
        else:
            raise ValueError(
                f"Check label name {label_name} or feature name {feat_name} "\
                    "is not part of active dataframe's columns"
            )


    def plot(
        self, feat_name, show_hist: bool = True, \
        label_name: Optional[str] = None
    ):
        if show_hist:
            self.__df[feat_name].hist()
            plt.show()
        if label_name is not None:
            plt.scatter(self.__data[feat_name], self.__data[label_name])
            plt.show()


    def explore(self):
        pass



class DataPrep:
    """
    This class provides general data preparation

    To use this class, initialize it with DataSetup as follows:

        from snowflake_ai.snowpandas import DataSetup

        prep: DataPrep = DataPrep(setup)
        
    """
    def __init__(
        self, 
        setup: DataSetup,
        dfs: Optional[Union[DataFrame, Dict[str, DataFrame]]] = None
    ):
        self.__setup = setup
        if dfs is None:
            self.__dfs = DataFrame()
        else:
            self.__dfs = dfs 


    def process(self, *args) -> Iterable[DataFrame]:
        df: DataFrame = DataFrame()
        # subclass specific processing logic
        yield df


    def end_partition(self) -> Iterable[Union[DataFrame, Dict[str, DataFrame]]]:
        yield self.__dfs


    def get_setup(self) -> DataSetup:
        return self.__setup
