# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains an utility class of SnowTransform
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


from typing import Optional, Union, Tuple, List
from datetime import *
from dateutil.relativedelta import *
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
from snowflake.snowpark import Window as W
from snowflake.snowpark import DataFrame as SDF



class SnowTransform:
    """
    This class contains a set of static methods for transforming Snowflake
    dataframe including creating columns and tables for ease of time series
    data analysis.
    """

    @staticmethod
    def create_year_column(
        sdf: SDF, 
        dt_col_nm : str,
        yr_col_nm: Optional[str] = 'YEAR'
    ) -> SDF:
        """
        Create a new Snowflake dataframe column (named as yr_col_nm) for the 
        calendar year number (e.g., 2023, etc.) from the existing Snowflake 
        dataframe based on input date column (named as dt_col_nm)

        Args:
            sdf (Snowflake Dataframe): input Dataframe
            dt_col_nm (str): existing date column name, e.g. 'TIME_IDX_DT'
            yr_col_nm (str): year column name, default to 'YEAR'; its value type 
                is int.

        Returns:
            Snowflake Dataframe: Dataframe with new year column (int) added.
        """
        if str.strip(dt_col_nm) == '':
            dt_col_nm = "TIME_IDX_DT"
        if str.strip(yr_col_nm) == '':
            yr_col_nm = "YEAR"

        return sdf.withColumn(
            yr_col_nm, 
            F.year(dt_col_nm)
            )
    

    @staticmethod
    def create_week_column(
        sdf: SDF, 
        dt_col_nm : str,
        wk_col_nm: Optional[str] = 'WEEK'
    ) -> SDF:
        """
        Create a new Snowflake dataframe column (named as wk_col_nm) for the
        week of year string formatted as '00' padded with '0' (e.g., '01', etc.)
        from the existing Snowflake dataframe based on input date column 
        (named as dt_col_nm).
        NOTE: Not applying additional filters on the resulted dataframe as 
        the additional filtering may change week partition behavior.

        Args:
            sdf (Snowflake Dataframe): input Dataframe
            dt_col_nm (str): existing date column name, e.g. 'TIME_IDX_DT'
            wk_col_nm (str): week column name, default to 'WEEK'; its value 
                is formatted as '00' string

        Returns:
            Snowflake Dataframe: Dataframe with a new week column (str) added.
        """
        if str.strip(dt_col_nm) == '':
            dt_col_nm = "TIME_IDX_DT"
        if str.strip(wk_col_nm) == '':
            wk_col_nm= "WEEK"

        sdf = sdf.withColumn(
            wk_col_nm, 
            F.when(
                (F.month(dt_col_nm) == 1) & (F.weekofyear(dt_col_nm) >= 52),
                F.trim(F.to_char(F.lit(1), "00"))
            )
            .otherwise(
                F.trim(F.lpad(F.weekofyear(dt_col_nm), 2, F.lit('0')))
            )
        )

        window = W.partition_by(wk_col_nm).order_by(dt_col_nm)
        sdf = sdf.withColumn(
            wk_col_nm, 
            F.when(
                F.dayofweek(dt_col_nm) == 1,
                sdf[wk_col_nm]
            )
            .otherwise(
                F.last_value(sdf[wk_col_nm], ignore_nulls=True).over(window)
            )
        )

        return sdf
    

    @staticmethod
    def create_year_month_column(
        sdf: SDF, 
        dt_col_nm : str,
        yr_col_nm: Optional[str] = '',
        mo_col_nm: Optional[str] = '',
        mo_str_col_nm: Optional[str] = ''
    ) -> SDF:
        """
        Create two types Snowflake dataframe columns if the input column 
        names are not empty - Column (yr_col_nm) for the year number, and 
        Column (mo_col_nm) for the month number, and Column (mo_str_col_nm)
        for the month string formatted as 'MM padded with '0' (e.g., '01' 
        for Jan, etc.) from the existing Snowflake dataframe based on input 
        date column (named as dt_col_nm).

        Args:
            sdf (Snowflake Dataframe): input Dataframe
            dt_col_nm (str): existing date column name, e.g. 'TIME_IDX_DT'
            yr_col_nm (str): new year column name if not empty, e.g. 'YEAR'; 
                its value type is int.
            mo_col_nm (str): new month column name if not empty, e.g. 'MONTH';
                its value type is int (1 - Jan, 2 - Feb, etc.)
            mo_str_col_nm (str): new month column name, its value type 
                is formatted as 'MM' string ('01' - Jan, '02' - Feb, etc.)

        Returns:
            Snowflake Dataframe: Dataframe with new year column (int) and 
                month column (int) and month string column (int) added.
        """
        if str.strip(dt_col_nm) == '':
            dt_col_nm = "TIME_IDX_DT"

        if str.strip(yr_col_nm) != '':
            sdf = sdf.withColumn(
                yr_col_nm,
                F.year(dt_col_nm)
            )

        if str.strip(mo_col_nm) != '':
            sdf = sdf.withColumn(
                mo_col_nm, 
                F.month(dt_col_nm)                
            )

        if str.strip(mo_str_col_nm) != '':
            sdf = sdf.withColumn(
                mo_str_col_nm,
                F.trim(
                    F.lpad(F.month(dt_col_nm), 2, F.lit('0'))
                )               
            )
        return sdf
    

    @staticmethod
    def create_weekly_time_index_column(
        sdf: SDF, 
        dt_col_nm : str,
        yr_col_nm: Optional[str] = '',
        wk_col_nm: Optional[str] = '',
        time_idx_col_nm: Optional[str] = 'TIME_IDX_WK'
    ) -> SDF:
        """
        Create a new Snowflake dataframe time index column (named as 
        time_idx_col_nm) for week of year string formatted as 'YYYYMM' from
        input snowflake dataframe based on the existing date column 
        (dt_col_nm). Also, if yr_col_nm and wk_col_nm are not
        empty, new year number column and week string column are created.

        Args:
            sdf (Snowflake Dataframe): input Dataframe
            dt_col_nm (str): existing date column name, e.g. 'TIME_IDX_DT'
            yr_col_nm (str): new year column name, e.g. 'YEAR'; its value type
                is int
            wk_col_nm (str): new week column name, e.g. 'WEEK'; its value
                type is string, e.g., '01' is 1st week of the year.
            time_idx_col_nm (str): new weekly time index column name; default
                value to 'TIME_IDX_WK'; its value formatted as 'YYYYWW' string,
                e.g. '202301' for the 1st week of year 2023.

        Returns:
            Snowflake Dataframe: Dataframe with new weekly time index column
                (int), optionally new year column (int), week column (str)
                added.
        """
        if str.strip(dt_col_nm) == '':
            dt_col_nm = "TIME_IDX_DT"
        
        if str.strip(yr_col_nm) != '':
            sdf = SnowTransform.create_year_column(
                sdf, dt_col_nm, yr_col_nm
            )

        if str.strip(wk_col_nm) != '':
            sdf = SnowTransform.create_week_column(
                sdf, dt_col_nm, wk_col_nm
            )        

        return sdf.withColumn(time_idx_col_nm, F.concat(
                F.trim(F.to_char(F.col(yr_col_nm), "0000")), 
                F.trim(F.col(wk_col_nm)))
            )
    

    @staticmethod
    def create_year_week_month_column(
        sdf: SDF, 
        dt_col_nm : str,
        yr_col_nm: Optional[str] = 'YEAR',
        wk_col_nm: Optional[str] = 'WEEK',    
        mon_col_nm: Optional[str] = 'MONTH',
        time_idx_col_nm: Optional[str] = 'TIME_IDX_WK'
    ) -> SDF:
        """
        Create a new Snowflake dataframe time index column (named as 
        time_idx_col_nm) for week of year string formatted as 'YYYYMM' from
        input snowflake dataframe. NOTE: the week and month are calculation
        based on traditional western system and if the week spans two months,
        it is counted for the month that has Thursday of the week. Also, 
        if yr_col_nm, wk_col_nm, mon_col_nm are not empty, new year, month 
        number columns and week string column are created.

        Args:
            sdf (Snowflake Dataframe): input Dataframe.
            dt_col_nm (str): existing date column name, e.g. 'TIME_IDX_DT'
            yr_col_nm (str): new year column name, e.g. 'YEAR'; its value type
                is int.
            wk_col_nm (str): new week column name, e.g. 'WEEK'; its value
                type is string, e.g., '01' is 1st week of the year.
            mon_col_nm (str): new month column name, e.g. 'MONTH'; its value
                type is int.
            time_idx_col_nm (str): new weekly time index column name; default
                value to 'TIME_IDX_WK'; its value formatted as 'YYYYWW' string,
                e.g. '202301' for the 1st week of year 2023.

        Returns:
            Snowflake Dataframe: Dataframe with new weekly time index column
                (int), optionally new year column (int), week column (str),
                month column (int) added.
        """
        sdf = SnowTransform.create_weekly_time_index_column(
            sdf, 
            dt_col_nm=dt_col_nm,
            yr_col_nm=yr_col_nm,
            wk_col_nm=wk_col_nm, 
            time_idx_col_nm=time_idx_col_nm
        )

        if str.strip(mon_col_nm) != '':
            sdf = sdf.withColumn(mon_col_nm, F.month(dt_col_nm))
        
        sdf = sdf.withColumn("THURSDAY_MONTH", F.month(F.date_add(
                dt_col_nm, 4 - F.dayofweek(dt_col_nm))))

        window_wk = W.partitionBy(time_idx_col_nm).orderBy(dt_col_nm)

        sdf = sdf.withColumn("MAJORITY_MONTH", 
                             F.first_value("THURSDAY_MONTH").over(window_wk))

        sdf = sdf.withColumn(
            mon_col_nm,
            F.when(
                (F.col(mon_col_nm) == 1) & (F.col("MAJORITY_MONTH") == 1),
                F.lit(1)
            ).when (
                ((F.col(mon_col_nm) == 12) & (F.col("MAJORITY_MONTH") == 1)),
                F.col(mon_col_nm)
            ).when(
                F.col(mon_col_nm) != F.col("MAJORITY_MONTH"),
                F.col("MAJORITY_MONTH")
            ).otherwise(
                F.col(mon_col_nm)
            )
        )
        sdf = sdf.drop("THURSDAY_MONTH", "MAJORITY_MONTH")
        return sdf


    @staticmethod
    def create_quarter_column(
        sdf: SDF, 
        mon_col_nm: str = '',
        qtr_col_nm: Optional[str] = 'QUARTER'
    ) -> SDF:
        """
        Create a new Snowflake dataframe column for the calendar quarter number 
        from the input Snowflake dataframe based on the existing month
        column (named as mon_col_nm)

        Args:
            sdf (Snowflake Dataframe): input Dataframe
            mon_col_nm (str): existing month column name, e.g. 'MONTH'
            qtr_col_nm (str): quarter column name, default to 'QUARTER'; 
                its value type is int.

        Returns:
            Snowflake Dataframe: Dataframe with a new quarter column (int)
                added based on existing month column.
        """
        if str.strip(mon_col_nm) == '':
            mon_col_nm = "MONTH"
        
        sdf = sdf.withColumn(
            qtr_col_nm, 
            F.when(
                F.col(mon_col_nm).between(1, 3), F.lit(1)
            ).when(
                F.col(mon_col_nm).between(4, 6), F.lit(2)
            ).when(
                F.col(mon_col_nm).between(7, 9), F.lit(3)
            ).otherwise(
                F.lit(4)
            )
        )
        return sdf
    

    @staticmethod
    def create_month_quarter_column(
        sdf: SDF, 
        dt_col_nm : str,
        mon_col_nm: Optional[str] = "MONTH",
        qtr_col_nm: Optional[str] = "QUARTER"
    ) -> SDF:
        """
        Create new Snowflake dataframe columns for the month and quater
        based on the input Snowflake dataframe with the existing date column.

        Args:
            sdf (Snowflake Dataframe): input Dataframe
            mon_col_nm (str): existing month column name, e.g. 'MONTH'
            qtr_col_nm (str): quarter column name, default to 'QUARTER'; 
                its value type is int.

        Returns:
            Snowflake Dataframe: Dataframe with new month column (int)
                using standard calendar system and new quarter column 
                (int) added.
        """
        if str.strip(dt_col_nm) == '':
            dt_col_nm = "TIME_IDX_DT"

        if str.strip(mon_col_nm) != '':
            sdf = sdf.withColumn(mon_col_nm, F.month(dt_col_nm))

        if str.strip(qtr_col_nm) != '':
            sdf = sdf.withColumn(qtr_col_nm, F.quarter(dt_col_nm))

        return sdf
    

    @staticmethod
    def generate_daily_time_index(
        session: Session, 
        start_dt: Union[str, datetime, date],
        end_dt: Union[str, datetime, date],
        time_idx_col_nm: Optional[str] = 'TIME_IDX_DT',
        time_idx_tbl_nm : Optional[str] = ''
    ) -> SDF:
        """
        Generate a daily time index column (named as time_idx_col_nm) with a 
        series of dates formatted as 'YYYY-MM-DD' date objects for a new 
        Snowflake dataframe based on dates from the start_dt to end_dt 
        (inclusive).

        Args:
            session (Session): a Snowflake session
            start_dt (str, datetime, date): starting date object can be parsed
                as 'YYYY-MM-DD' date object.
            end_dt (str, datetime, date): ending date object can be parsed
                as 'YYYY-MM-DD' date object.
            time_idx_col_nm (str): new time index column name; default
                value to 'TIME_IDX_DT', its value is formatted as 'YYYY-MM-DD'.
            time_idx_tbl_nm (str): new dataframe persisted table name; default
                value to '' which is without persisting the dataframe.

        Returns:
            Snowflake Dataframe and Table: Dataframe with a single daily time
                index column (date) typed date object.
        """
        if isinstance(start_dt, datetime):
            start_dt :str = start_dt.strftime("%Y-%m-%d")
        elif isinstance(start_dt, date):
            dt = datetime(
                year=start_dt.year, 
                month=start_dt.month,
                day=start_dt.day,
            )
            start_dt :str = dt.strftime("%Y-%m-%d")
        
        if isinstance(end_dt, datetime):
            end_dt :str = end_dt.strftime("%Y-%m-%d")
        elif isinstance(end_dt, date):
            dt = datetime(
                year=end_dt.year, 
                month=end_dt.month,
                day=end_dt.day,
            )
            end_dt :str = dt.strftime("%Y-%m-%d")

        session.sql("ALTER SESSION SET WEEK_OF_YEAR_POLICY=1, WEEK_START=7")\
            .collect()
        sdf = session.sql(
            f"SELECT '{start_dt}'::DATE + VALUE::INT AS " \
            f"{time_idx_col_nm} FROM TABLE(FLATTEN(ARRAY_GENERATE_RANGE("\
            f"0, DATEDIFF('DAY', '{start_dt}'::DATE, '{end_dt}'::DATE)+1)))"
        )

        if time_idx_tbl_nm:
            sdf = sdf.sort(F.col(time_idx_col_nm).asc())
            sdf.write.mode("overwrite").save_as_table(time_idx_tbl_nm)
            sdf = session.table(time_idx_tbl_nm).to_df(time_idx_col_nm)
        return sdf


    @staticmethod
    def generate_weekly_time_index(
        session: Session, 
        start_dt: Union[str, datetime, date],
        end_dt: Union[str, datetime, date],
        time_idx_col_nm: Optional[str] = 'TIME_IDX_WK',
        time_idx_int_col : Optional[str] = 'TIME_IDX',
        time_idx_tbl_nm : Optional[str] = ''
    ) -> SDF:
        """
        Generate a weekly time index column (named as time_idx_col_nm, 
        default to 'TIME_IDX_WK') with a series of 'YYYYWW' weeks of
        year sequence strings, and its corresponding integer based weekly
        time index column (named as time_idx_int_col, default to 'TIME_IDX') 
        using traditional western system for a new Snowflake dataframe based
        on dates from the start_dt to end_dt (inclusive).
        NOTE: Use this method to create or show week time index dataframe or
        table. It is NOT advised to apply any additional filters which may 
        result in wrong time index.

        Args:
            session (Session): a Snowflake session
            start_dt (str, datetime, date): starting date object can be parsed
                as 'YYYY-MM-DD' date object.
            end_dt (str, datetime, date): ending date object can be parsed
                as 'YYYY-MM-DD' date object.
            time_idx_col_nm (str): new weekly time index column name; default
                value to 'TIME_IDX_WK', its value is type of string and formatted
                as 'YYYYWW', e.g. '202301' - 1st week of year 2023.
            time_idx_int_col (str): new weekly time index column name; default
                value to 'TIME_IDX', its value type is int, e.g., 1, 2, .., etc.
            time_idx_tbl_nm (str): new dataframe persisted table name; default
                value to '' which is without persisting the dataframe.

        Returns:
            Snowflake Dataframe and Table: Dataframe with a weekly time index
                column (str) formatted as 'YYYYWW', and a integer time index
                column (int) named as time_idx_int_col default to 'TIME_IDX', 
                and other dates related columns - year (int), quarter (int),
                month (int), week (str) formatted as '00', week_start_dt 
                column (date), week_end_dt column (date).
        """
        session.sql("ALTER SESSION SET WEEK_OF_YEAR_POLICY=1, WEEK_START=7")\
            .collect()
        
        sdf : SDF = SnowTransform.generate_daily_time_index(
            session, start_dt, end_dt, "TIME_IDX_DT"
        )

        sdf = SnowTransform.create_year_week_month_column(
            sdf, "TIME_IDX_DT", "YEAR", "WEEK", "MONTH", time_idx_col_nm
        )

        sdf = SnowTransform.create_quarter_column(
            sdf, "MONTH", "QUARTER"
        )

        sdf = sdf.with_column(
                "WEEK_DAY", F.dayofweek("TIME_IDX_DT")
            ).filter((F.col("WEEK_DAY") == 1)).select(
                F.col(time_idx_col_nm),
                F.col("TIME_IDX_DT").alias("WEEK_START_DT"),
                F.col("YEAR"), 
                F.col("QUARTER"), 
                F.col("MONTH"), 
                F.col("WEEK")
            )

        sdf = sdf.with_column(
                "WEEK_END_DT", F.date_add("WEEK_START_DT", 6)
            ).distinct().sort(time_idx_col_nm)

        window = W.orderBy(time_idx_col_nm)
        sdf = sdf.withColumn(
            time_idx_int_col, F.row_number().over(window) - 1
        )
        
        sdf = sdf.select(
                F.col(time_idx_int_col),
                F.col(time_idx_col_nm),
                F.col("WEEK_START_DT"),
                F.col("WEEK_END_DT"),
                F.col("YEAR"),
                F.col("QUARTER"), 
                F.col("MONTH"), 
                F.col("WEEK")
            ).sort(time_idx_int_col)
        
        if time_idx_tbl_nm:
            sdf.write.mode("overwrite").save_as_table(time_idx_tbl_nm)
            sdf = session.table(time_idx_tbl_nm).to_df(
                time_idx_int_col, time_idx_col_nm, 
                "WEEK_START_DT", "WEEK_END_DT", 
                "YEAR", "QUARTER", "MONTH", "WEEK"
            )

        return sdf


    @staticmethod
    def generate_week_start_date(
        session: Session,
        start_dt: Union[str, datetime, date],
        end_dt: Union[str, datetime, date],
        day_of_week: int = 1,
        yr_col_nm: Optional[str] = "YEAR",
        wk_col_nm: Optional[str] = "WEEK",
        time_idx_col_nm: Optional[str] = 'TIME_IDX_DT',
        time_idx_tbl_nm : Optional[str] = ''
    ) -> SDF:
        """
        Generate a weekly time index column (named as time_idx_col_nm) with
        its values as the week start date formatted as 'YYYY-MM-DD'
        calculated from day_of_week using traditional western system for a
        new Snowflake dataframe based on dates from the start_dt to end_dt 
        (inclusive).

        Args:
            session (Session): a Snowflake session
            start_dt (str, datetime, date): starting date object can be parsed
                as 'YYYY-MM-DD' date object.
            end_dt (str, datetime, date): ending date object can be parsed
                as 'YYYY-MM-DD' date object.
            day_of_week (int): number indicating which day to start the week;
                in traditional western system, it is default to 1 to indicate Sunday
                as the start of the week.
            yr_col_nm (str): new column name if not empty to get year number.
            wk_col_nm (str): new column name if not empty to get week string.
            time_idx_col_nm (str): new weekly time index column name; default
                value to 'TIME_IDX_WK', its value is type of string and formatted
                as 'YYYYWW', e.g. '202301'.
            time_idx_int_col (str): new weekly time index column name; default
                value to 'TIME_IDX', its value type is int.
            time_idx_tbl_nm (str): new dataframe persisted table name; default
                value to '' which is without persisting the dataframe.

        Returns:
            Snowflake Dataframe and Table: Dataframe with a weekly time index
                using week start date as column (date), year column (int), and 
                week column (str).    
        """
        sdf : SDF = SnowTransform.generate_daily_time_index(
            session, 
            start_dt,
            end_dt,
            time_idx_col_nm,
            '' 
        )
        sdf = SnowTransform.create_year_column(
            sdf, time_idx_col_nm, yr_col_nm
        )
        sdf = SnowTransform.create_week_column(
            sdf, time_idx_col_nm, wk_col_nm
        )

        sdf = sdf.with_column(
                "WEEK_DAY", F.dayofweek(time_idx_col_nm)
            )

        sdf = sdf.filter((F.col("WEEK_DAY") == day_of_week))\
            .select(
                time_idx_col_nm, 
                yr_col_nm, 
                wk_col_nm,
            )

        if time_idx_tbl_nm:
            sdf = sdf.sort(F.col(time_idx_col_nm).asc())
            sdf.write.mode("overwrite").save_as_table(time_idx_tbl_nm)
            sdf = session.table(time_idx_tbl_nm).to_df(time_idx_col_nm)

        return sdf
    

    @staticmethod
    def generate_grouped_time_index(
        sdf: SDF, 
        time_idx_col_nm: str, 
        incl_cols = [], 
        time_idx_int_col : Optional[str] = 'TIME_IDX'
    ) -> SDF:
        """
        Generate a time integer index column (named as time_idx_int_col, 
        default to 'TIME_IDX') based on another ordered time index column
        associated with a group of selected columns.
        NOTE: Use this method to create integer based time index columns with
        selection of a group of columns without additional fitering applied.

        Args:
            sdf (Snowflake Dataframe): input Snowflake dataframe.
            time_idx_col_nm (str): existing time index columns, e.g, 
                TIME_IDX_WK, or TIME_IDX_DT.
            incl_cols (list of string): a list of column names to be selected.
            time_idx_int_col (str): new integer based time index column name; 
                default value to 'TIME_IDX', its value type is int, 
                e.g., 1, 2, .., etc. sequential time index numbers.

        Returns:
            Snowflake Dataframe: Dataframe with added integer based time index
                column (int) together with included columns.
        """   
        if time_idx_col_nm is None or not time_idx_col_nm:
            time_idx_col_nm = "TIME_IDX_NM"

        cols: List[str] = [str(c) for c in incl_cols]
        cols.append(time_idx_col_nm)
        cols_lst = list(set(cols))

        # Convert generator expression to a list of columns
        cols_lst = [str(c) for c in cols_lst]

        sdf = sdf.select(*cols_lst).distinct().sort(time_idx_col_nm)

        window = W.orderBy(time_idx_col_nm)
        sdf = sdf.withColumn(
            time_idx_int_col, F.row_number().over(window) - 1
        )
        return sdf


    @staticmethod
    def generate_monthly_time_index(
        session: Session, 
        start_dt: Union[str, datetime, date],
        end_dt: Union[str, datetime, date],
        yr_col_nm: Optional[str] = '',
        mo_col_nm: Optional[str] = '',
        time_idx_col_nm: Optional[str] = 'TIME_IDX',
        time_idx_tbl_nm : Optional[str] = ''
    ) -> SDF:
        """
        Generate a monthly time index column (named as time_idx_col_nm) 
        based on traditional western system for a new Snowflake dataframe 
        from the start_dt to end_dt (inclusive).
        NOTE: Use this method to create a monthly index dataframe or table.
        It is NOT advised to apply any additional filters which may result in
        wrong time index.

        Args:
            session (Session): a Snowflake session
            start_dt (str, datetime, date): starting date object can be parsed
                as 'YYYY-MM-DD' date object.
            end_dt (str, datetime, date): ending date object can be parsed
                as 'YYYY-MM-DD' date object.
            yr_col_nm (str): new year column name if not empty, e.g. 'YEAR'; 
                its value type is int.
            mo_col_nm (str): new month column name if not empty, e.g. 'MONTH';
                its value type is int (1 - Jan, 2 - Feb, etc.)
            time_idx_col_nm (str): new monthly time index column name; default
                value to 'TIME_IDX', its value type is int.
            time_idx_tbl_nm (str): new dataframe persisted table name; default
                value to '' which is without persisting the dataframe.

        Returns:
            Snowflake Dataframe and Table: Dataframe with a monthly time index
                column (int), year column (int), month column (int).
        """
        sdf : SDF = SnowTransform.generate_daily_time_index(
            session, start_dt, end_dt, "TIME_IDX_DT"
        )

        sdf = SnowTransform.create_year_month_column(
            sdf, "TIME_IDX_DT", yr_col_nm, mo_col_nm
        )

        sdf = sdf.select(yr_col_nm, mo_col_nm)\
                .distinct().sort(yr_col_nm, mo_col_nm)

        window = W.orderBy(yr_col_nm, mo_col_nm)
        sdf = sdf.withColumn(
                time_idx_col_nm, F.row_number().over(window) - 1
            ).select(time_idx_col_nm, yr_col_nm, mo_col_nm)\
            .sort(F.col(time_idx_col_nm).asc(),
                  F.col(yr_col_nm).asc(),
                  F.col(mo_col_nm).asc()
            )
            
        if time_idx_tbl_nm:
            sdf.write.mode("overwrite").save_as_table(time_idx_tbl_nm)
            sdf = session.table(time_idx_tbl_nm).to_df(
                    time_idx_col_nm, yr_col_nm, mo_col_nm)

        return sdf


    @staticmethod
    def get_year_week(
        session: Session,
        dt: Union[str, datetime, date] = None
    ) -> Tuple[str, int, int]:
        """
        Get input date's year and week based on traditional western system.
        
        Return:
            Tuple: a week index string of 'YYYYWW' (str), year number (int)
                and week number (int)
        """
        if dt is None:
            dt : datetime = date.today()
        if isinstance(dt, date):
            dt : datetime = datetime(
                year=dt.year, 
                month=dt.month,
                day=dt.day,
            )
        elif isinstance(dt, str):
            dt: datetime = datetime.strptime(dt, "%Y-%m-%d")

        session.sql("ALTER SESSION SET WEEK_OF_YEAR_POLICY=1, WEEK_START=7")\
            .collect()
        year = dt.year
        sdf : SDF = SnowTransform.generate_daily_time_index(
            session, datetime(year, 1, 1), datetime(year, 12, 31), "DT"
        )
        sdf = sdf.withColumn(
            "WK", 
            F.when(
                (F.month("DT") == 1) & (F.weekofyear("DT") >= 52),
                F.trim(F.to_char(F.lit(1), "00"))
            )
            .otherwise(
                F.trim(F.lpad(F.weekofyear("DT"), 2, F.lit('0')))
            )
        )

        sdf = sdf.withColumn("YR", F.year("DT"))
    
        window = W.partition_by("WK").order_by("DT")
        sdf = sdf.withColumn(
            "WK", 
            F.when(
                F.dayofweek("DT") == 1,
                sdf["WK"]
            )
            .otherwise(
                F.last_value(sdf["WK"], ignore_nulls=True).over(window)
            )
        )
        sdf = sdf.withColumn("YW", F.concat(
            F.trim(F.to_char(F.col("YR"), "0000")), 
            F.trim(F.col("WK")))
        )
        yw_lst = sdf.filter(F.col("DT") == dt).select("YW", "YR", "WK")\
            .collect()
        yw = {}
        if yw_lst:
            yw = yw_lst[0]
        return (str(yw["YW"]), int(yw["YR"]), int(yw["WK"]))