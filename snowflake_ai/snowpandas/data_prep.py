# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains EDA and DataPrep class for general or pandas-like 
data exploration, splits, and preprocessing.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


from typing import (
    Dict, Union, Optional, List, Iterable
)
import logging

from pandas import DataFrame, Series, to_numeric

import matplotlib.pyplot as plt

from snowflake_ai.common import DataConnect
from snowflake_ai.snowpandas import DataSetup



class EDA:
    """
    This class provides pandas dataframe exploration.

    To use this class, initialize it with DataSetup as follows:

        >>> from snowflake_ai.snowpandas import DataSetup
        ... 
        >>> eda: EDA = EDA(setup)
        
    """    
    T_DEFAULT_DF = "default"
    _logger = logging.getLogger(__name__)

    def __init__(
        self, 
        setup: DataSetup,
        data: Optional[Dict[str, DataFrame]] = {}
    ):
        self.logger = EDA._logger
        self._setup = setup
        self._active_df = DataFrame()
        self._data = data


    @property
    def data_setup(self):
        """
        Get DataPrep's Setup.

        Returns:
            DataSetup: data setup for the data preparation
        """
        return self._setup


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
                if df_name in self._data.keys():
                    self._active_df = self._data[df_name]
            except:
                raise ValueError(
                    f"Dictionary of DataFrame doesn't have the key: {df_name}"
                )
            if df is not None:
                self._active_df = df
                self._data[df_name] = df
        elif df is not None:
            self._active_df = df
            self._data[EDA.T_DEFAULT_DF] = df            
        return self._active_df


    @staticmethod
    def top_df(df: DataFrame, n: int = 10, by_col: str = None,
        is_asc: bool = False
    ):
        """
        Return top n rows from Pandas dataframe ordered by one column

        Args:
            df (DataFrame): Pandas dataframe
            n (int): top n rows
            by_col (str): name of the column ordered by
            is_asc (bool): True if it is ascending order; default False
         
        Returns:
            Series or DataFrame: Pandas Series or Dataframe.
        """
        if by_col is None:
            return df.head(n)
        else:
            sorted_df = df.sort_values(by=by_col, ascending=is_asc)
            return sorted_df.head(n)


    def top(self, df_name: Optional[str] = None, n: int = 10, 
        by_col: str = None, is_asc: bool = False
    ):
        """
        Return top n rows from current active Pandas dataframe or input 
        dataframe ordered by one column

        Args:
            df_name (str): dataframe name

        Retruns:
            Series or DataFrame: Pandas Series or DataFrame
        """
        return EDA.top_df(self.active_df(df_name), n, by_col, is_asc)    


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
        """
        Describe the dataframe by dataframe name.

        Args:
            df_name (str): dataframe name. if emptry, the current
                active dataframe desc is called.
         
        Returns:
            Series or DataFrame: Summary statistics of the Series or
                Dataframe provided.
        """
        return EDA.desc(self.active_df(df_name))
    

    @staticmethod
    def corr_df(df: DataFrame, label_name: str):
        """
        Compute correlation of columns against target label.

        Args:
            df (dataframe): input dataframe
            label_name (str): target label name.
         
        Returns:
            Correlation matrix.
        """
        corr_mat = df.corr()
        return corr_mat[label_name].sort_values(ascending=False)


    def corr(self, label_name: str, feat_name: Optional[str] = None) \
            -> Union[float, Series, None]:
        """
        Compute correlation between target label and feature column.

        Args:
            label_name (str): target label.
            feat_name (str): feature column.
         
        Returns:
            Correlation matrix.
        """
        if label_name in self._active_df.columns and feat_name is None:
            corr_mat: DataFrame = self._active_df.corr()
            return corr_mat[label_name].sort_values(ascending=False)
        elif label_name in self._active_df.colums and \
                feat_name in self._active_df.columns:
            self._active_df[feat_name] = to_numeric(
                self._active_df[feat_name], errors='coerce'
            )
            self._active_df[label_name] = to_numeric(
                self._active_df[label_name], errors='coerce')
            return Series(self._active_df[feat_name]).corr(Series(
                self._active_df[label_name])
            )
        else:
            raise ValueError(
                f"Check label name {label_name} or feature name {feat_name} "\
                    "is not part of active dataframe's columns"
            )


    def plot(
        self, feat_name, show_hist: bool = True, \
        label_name: Optional[str] = None
    ):
        """
        Show feature column's histgram and scatter plot if target label
        is specified.

        Args:
            show_hist (bool): show histgram if true.
            label_name (str): target label is supplied show scatter plot.
        """        
        if show_hist:
            self._active_df[feat_name].hist()
            plt.show()
        if label_name is not None:
            plt.scatter(self._data[feat_name], self._data[label_name])
            plt.show()



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
        data: Optional[Dict[str, DataFrame]] = {}
    ):
        self._setup = setup
        self._data = data 


    def process(self, *args) -> Iterable[DataFrame]:
        df: DataFrame = DataFrame()
        # subclass specific processing logic
        yield df


    def end_partition(self) -> Iterable[Dict[str, DataFrame]]:
        yield self._data


    def get_setup(self) -> DataSetup:
        return self._setup


    @property
    def data_setup(self) -> DataSetup:
        return self._setup
    
