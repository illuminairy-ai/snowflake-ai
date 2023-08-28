# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains FlowContext class representing the context
of MLOps flow
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


import logging
from typing import Any, Callable, Dict, List, Optional, Tuple
from snowflake.snowpark import DataFrame as SDF
from snowflake.snowpark import Session


class FlowContext:
    """
    This class provides a context for the MLOps flow consisting
    of direct input (prior processing/task), context input (full
    historical context sequence), current output, and related
    metadata.

    Example:

    To use this class, instantiate the initial context:

        >>> from snowflake_ai.mlops import FlowContext
        ... 
        >>> ctx: FlowContext = FlowContext()
    """

    FLG_DEF = 0
    T_STEP_ITER = "iteration"

    _logger = logging.getLogger(__name__)

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.primary_session: Session = None
        self.secondary_session: Session = None
        self.session: Session = self.primary_session
        self.pipelines: Dict[str, Dict] = {}
        self.data: Dict[str, Any] = {}
        self.indexes: Dict[str, Any] = {}
        self.models: Dict[str, Any] = {}
        self.params: Dict[str, Any] = {}
        self.metrics: Dict[str, Any] = {}
        self.metadata = {}
        self.outputs : Dict[str, Any] = {}
        self.debug_info: Dict[str, Any] = {}
        self.debug = True

        self.pipeline: Dict[str, Callable] = {}
        self.step_types: List[str] = []
        self.loggings = {}
        self.process_flag: int = FlowContext.FLG_DEF
        self.direct_inputs = {}
        self.context_inputs = []
    


    @staticmethod
    def exists_in_snowflake(
            session: Session, 
            db_nm: str,
            sch_nm: str,
            tbl_nm: str
        ) -> bool:
        s, db_name, sch_name = "", "", ""
        if db_nm is not None and db_nm.strip():
            db_name = db_nm.strip()        
        if sch_nm is not None and sch_nm.strip():
            sch_name = sch_nm.strip()
        
        if db_name:
            s = f"""
                SELECT COUNT(*) as CNT
                FROM information_schema.tables
                WHERE table_catalog = '{db_name}' and
                table_schema = '{sch_nm}' and 
                table_name = '{tbl_nm}'
            """
        elif sch_name:
            s = f"""
                SELECT COUNT(*) as CNT
                FROM information_schema.tables
                WHERE table_schema = '{sch_nm}' and 
                table_name = '{tbl_nm}'
            """
        else:
            s = f"""
                SELECT COUNT(*) as CNT
                FROM information_schema.tables
                WHERE table_name = '{tbl_nm}'
            """
        t:SDF = session.sql(s)
        n:int = t.collect()[0]["CNT"]
        return False if n == 0 else True


    def exist(self, table_name:str) -> bool:
        """
        Check whether a fully qualified table exist in Snowflake session.
        Note: table_name would be converted all uppercase.
        """
        nl = table_name.rsplit('.', 2)
        if self.session is None or len(nl) == 0:
            return False
        
        if len(nl) == 1:
            return self.exists_in_snowflake(
                self.session, None, None, nl[0].upper()
            )
        elif len(nl) == 2:
            return self.exists_in_snowflake(
                self.session, None, nl[0].upper(), nl[1].upper()
            )
        else:
            return self.exists_in_snowflake(
                self.session, nl[0].upper(), 
                nl[1].upper(), nl[2].upper()
            )