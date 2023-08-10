# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains DataFrameFactory class to create Pandas'
DataFrame or Snowflake DataFrame depending on context.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


import os
import logging
from typing import Optional, List, Tuple, Union, Any, Dict

import numpy as np
import pandas as pd
from pandas._typing import (
    Dtype,
    Axes,
)
from pandas import DataFrame as DF

from snowflake.snowpark.dataframe import DataFrame as SDF
from snowflake.snowpark._internal.analyzer.snowflake_plan_node \
    import LogicalPlan
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType

from snowflake_ai.common import DataConnect
from snowflake_ai.connect import SnowConnect, FileConnect



class DataFrameFactory:
    """
    This class provides an uniform dataframe creation interface to create
    a Pandas DataFrame or Snowflake DataFrame instance depending on the
    input context.

    To create a dataframe, construct an appropriate DataConnect object and
    pass it to this factory class together with the data content, optionally
    supplied with the corresponding schema. Note, you can configure Datasets
    to directly get snowflake or pandas dataframe without using this factory
    class.

    Example 1:
        >>> from snowflake_ai.common import DataFrameFactory
        ...
        ... # create snowflake dataframe with SnowConnect
        >>> sdf = DataFrameFactory.create_df(tbl_name, connect)

    Example 2:
        ... # create snowflake dataframe with SnowConnect
        >>> sdf = DataFrameFactory.create_df('select col from tbl', connect)
        
    Example 3:
        ... # create pandas dataframe with FileConnect
        >>> df = DataFrameFactory.create_df(csv_name, connect)

    Example 4:
        ... # create pandas dataframe
        >>> df = DataFrameFactory.create_df([0, 1, 2], columns=['number'])
    """

    @classmethod
    def create_df(
        cls,
        data: Any, 
        connect : Optional[Union[DataConnect, Session]] = None,
        columns: Optional[
            Union[StructType, Tuple, List[str], Axes, None] 
        ] = None,
        index: Optional[Axes] = None,
        dtype: Optional[Dtype] = None, # type: ignore 
    ) -> Union[SDF, DF]:
        """
        Create a Snowflake specific dataframe or Pandas dataframe depending
        on DataConnect types.

        Args:
            data (Any): input Snowflake table/view name, or sql statement,
                or list, tuple, Pandas dataframe, or LogicalPath
            connect (DataConnect): SnowConnect or FileConnect object
            columns (StructType | List | Tuple | Axes): dataframe schema
            index (Axes) : pandas dataframe index
            dtype (Dtype) : pandas dataframe data type

        Returns:
            DataFrame: Snowflake or Pandas Dataframe
        """
        session = None
        if isinstance(connect, SnowConnect):
            session = connect.get_connection()
            col = columns
            if isinstance(columns, tuple) or isinstance(columns, np.ndarray):
                col = list(columns)
            return cls.create_sdf(data, session, col)  # type: ignore
        
        elif isinstance(connect, FileConnect):
            conn: FileConnect = connect
            if str(data).strip() == "":
                return pd.read_csv(conn.current_connection)
            else:
                f = os.path.join(
                    os.path.dirname(
                        os.path.abspath(str(conn.current_connection))
                    ), 
                    str(data)
                )
                return pd.read_csv(f)
        
        elif isinstance(connect, Session):
            col = columns
            if isinstance(columns, tuple) or isinstance(columns, np.ndarray):
                col = list(columns)
            return cls.create_sdf(data, connect, col)  # type: ignore
        
        else:
            return cls.create_pdf(data, columns, index, dtype) # type: ignore


    @classmethod
    def create_sdf(
        cls,
        data: Any, 
        session: Optional[Session] = None,
        columns: Optional[
            Union[StructType, List[str], None] 
        ] = None
    ) -> SDF:
        """
        Create a snowflake dataframe.

        Args:
            data (Any): input Snowflake table/view name, or sql statement,
                or list, tuple, Pandas dataframe, or LogicalPath
            session (Session): snowflake session
            columns (StructType | List ): dataframe schema

        Returns:
            DataFrame: Snowflake Dataframe
        """

        if session is None and isinstance(data, SDF):
            return data.__copy__()
        
        elif session is None and not isinstance(data, SDF):
            raise ValueError(
                "DataframeFactory.create_sdf(): Creation of Snowflake "\
                "DataFrame requires Snowflake connection session!"
            )

        if session is not None and (
            isinstance(data, List) or isinstance(data, tuple) or
            isinstance(data, DF)
        ):
            if columns is None \
                    or isinstance(columns, StructType) \
                    or isinstance(columns, List):
                return session.create_dataframe(data, columns)
            else:
                raise ValueError(
                    "DataframeFactory.create_sdf(): Creation of Snowflake "\
                    "DataFrame requires related schema or Pandas schema!"
                )
        
        elif session is not None and isinstance(data, str):
            q = data.strip().split()
            if len(q) > 1:
                return session.sql(data)
            else:
                return session.table(data)
        
        elif session is not None and isinstance(data, LogicalPlan):
            return SDF(session, data, False)
        
        elif session is not None and (
                isinstance(data, pd.Series) or isinstance(data, Dict)):
            if columns is None \
                    or isinstance(columns, StructType) \
                    or isinstance(columns, List):
                return session.create_dataframe(DF(data), columns)
            else:
                raise ValueError(
                    "DataframeFactory.create_sdf(): Creation of Snowflake "\
                    "DataFrame failed using Dict data or Pandas Series!"
                )
            
        else:
            raise ValueError(
                "DataframeFactory.create_sdf(): Creation of Snowflake "\
                "DataFrame failed, please check input value and type!"
            )
    

    @classmethod
    def create_pdf(
        cls,
        data: Any = None, 
        columns: Optional[
            Union[Tuple, List[str], Axes, None] 
        ] = None, 
        index: Optional[Axes] = None,
        dtype: Optional[Dtype] = None, # type: ignore 
    ) -> DF:
        """
        Create a Pandas dataframe.

        Args:
            data (Any): input Snowflake dataframe,
                or list, tuple, Dict, Pandas Series or dataframe
            columns (Tuple | List | Axes): dataframe schema
            index (Axes) : dataframe index
            dtype (Dtype) : dataframe data type

        Returns:
            DataFrame: Pandas Dataframe
        """

        logger = logging.getLogger(cls.__name__)
        df = DF(data={}, columns=[])
        
        if isinstance(data, SDF):
            df = data.to_pandas()

        elif isinstance(data, DF):
            df = data.copy()
            
        elif isinstance(data, List) or isinstance(data, tuple) \
                or isinstance(data, Dict) or isinstance(data, pd.Series):
            
            if columns is None or isinstance(columns, np.ndarray) or \
                    isinstance(columns, List) or isinstance(columns, tuple):
                df = cls._create_df(data, index, columns)

        else:
            logger.warn(
                f"DataframeFactory.create_pdf(): Initialization with empty "\
                f"Pandas Dataframe."
            )    
            
        return df
    

    @classmethod
    def _create_df(
        cls,
        data = None,
        index: Union[Axes, None] = None,
        columns: Union[Axes, None] = None,
        dtype: Union[Dtype, None] = None, # type: ignore 
        copy: bool = False,
    ) -> pd.DataFrame:
        rdf = pd.DataFrame()
        logger = logging.getLogger(cls.__name__)
        try:
            rdf = pd.DataFrame(data, index, columns, dtype, copy)
        except Exception as e:
            logger.exception(
                "DataFrameFactory._create_df(): Exception occured when "\
                f"creating local pandas dataframe - {e}!"
            )
        return rdf
