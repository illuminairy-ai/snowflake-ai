# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains SnowConnect class representing a specific Snowflake
connection.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.2.0"


import os
import sys
import logging
import types
from typing import List, Dict, Iterator, Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from pandas import DataFrame as DF

from snowflake.snowpark import Session
from snowflake.snowpark import Row
from snowflake.snowpark import DataFrame as SDF

from snowflake_ai.common import AppConfig, AppConnect, DataConnect



class SnowConnect(DataConnect):
    """
    This class provides specific Snowflake connection.

    Example:

    To use this class, just instantiate SnowConnect as follows:

        >>> from snowflake_ai.common import SnowConnect
        ... 
        >>> connect: SnowConnect = SnowConnect()
        >>> session = connect.get_session()
    """

    T_SNOWFLAKE_CONN = AppConnect.T_SNOWFLAKE_CONN
    T_AUTH_SNOWFLAKE = AppConnect.T_AUTH_SNOWFLAKE
    T_AUTH_KEYPAIR = AppConnect.T_AUTH_KEYPAIR
    T_AUTH_EXT_BROWSER = AppConnect.T_AUTH_EXT_BROWSER
    T_AUTH_OAUTH = AppConnect.T_AUTH_OAUTH

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))


    def __init__(self, connect_key : Optional[str] = None):
        super().__init__(connect_key)
        self.logger = SnowConnect._logger
        
        if not (self.connect_group == DataConnect.K_DATA_CONN and \
                self.connect_params):
            self.logger.error(
                f"SnowConnect.init(): ConnectType => {self.connect_type};"\
                f"ConnectGroup => {self.connect_group}; ConnectName => "\
                f"{self.connect_name}"
            )
            raise ValueError(
                "SnowConnect.init(): SnowConnect configuration Error!"
            )
        if (not self.connect_type) or (
            self.connect_type != AppConfig.T_SNOWFLAKE_CONN
        ):
            raise ValueError(
                f"SnowConnect.init(): Type [{self.connect_type}] Error"
            )


    def create_connection(self, params: Dict):
        """
        Create a snowflake connection based on the specific configuration.
        A set of authentication types are supported:
            snowflake, keypair, oauth and externalbrowser
        Note, user specific connection would be session.

        Args:
            params (Dict): Configuration dictionary as input parameters.

        Returns:
            object: snowflake connection session
        """
        if params is None:
            raise ValueError(
                f"SnowConnect.create_connection params cannot be None"
            )
        session = None
        try:
            auth = params[AppConfig.K_AUTH_TYPE]
            if auth == AppConnect.T_AUTH_SNOWFLAKE:
                session = self._do_snowflake_auth(params)
            elif auth == AppConnect.T_AUTH_KEYPAIR:
                session = self._do_keypair_auth(params)
            elif auth == AppConnect.T_AUTH_EXT_BROWSER: 
                # cannot create shared connection
                self.logger.warning(
                    "SnowConnect.create_connection(): use create_session "\
                    "to create externalbrowser enabled connection."
                )
            elif auth == AppConnect.T_AUTH_OAUTH:
                # cannot create shared connection
                self.logger.warning(
                    "SnowConnect.create_connection(): use create_session "\
                    "to create oauth enabled connection."
                )
        except Exception as e:
            self.logger.exception(
                f"SnowConnect.create_connection cannot use "\
                f"auth_type={auth} to creaet snowflake session: {e}"
            )
        return session


    def get_connection(self, connect_key: Optional[str] = None):
        """
        Get lazy created snowflake shared connection.

        Args:
            connect_key (str): data connect key.

        Returns:
            object: snowflake connection object, i.e., snowflake session
        """
        conn = super().get_connection(connect_key)
        if conn is not None and (
            (self.connect_params["type"] != SnowConnect.T_SNOWFLAKE_CONN) \
            or (not isinstance(conn, Session))
        ):
            raise TypeError(
                f"SnowConnect.get_connection(): Type Error {connect_key}"
            )
        return conn


    def is_current_active(self) -> bool:
        """
        Check whether current Snowflake connection (shared session) is
        still active or not. 
        
        Returns:
            bool: True if the connection is active, otherwise False
        """
        rb = super().is_current_active()
        try:
            r = self.dql("select current_role()")           
            try:
                pd = next(r)
                self.logger.info(
                    "SnowConnect.is_current_active(): Active "
                    f"connection using role [{pd}]"
                )
                if DF(pd).shape[0] > 0:
                    rb = True
            except Exception as ee:
                self.logger.warning(
                    "SnowConnect.is_current_active(): No Result "\
                    f"returned [{ee}]"
                )            
        except Exception as e:
            self.logger.warning(
                f"SnowConnect.is_current_active(): Warning {e}"
            )
        return rb
    

    def create_session(self, ctx: Optional[Dict] = {}) -> Session:
        """
        Create snowflake user connection session, or get the existing
        shared data connection.

        Returns:
            Session: Snowflake connection's session
        """
        conn = self.get_connection()
        if (conn is not None) and isinstance(conn, Session):
            return conn
        else:
            conn = self.create_connection(self.connect_params)
        session = conn
        if session is None:
            if self.auth_type == self.T_AUTH_EXT_BROWSER:
                session = self._do_externalbrowser_auth(self.connect_params)
            elif self.auth_type == self.T_AUTH_OAUTH:
                session = self._do_oauth(self.connect_params, ctx)
        return session
    

    def close_session(self):
        """
        Close current snowflake connection and session.
        """
        self.close_connection()


    def _create_session(self, auth_type: str, conn_params) -> Session:
        try:
            session = Session.builder.configs(conn_params).create() 
            c = session.sql("select current_warehouse(), current_role()")\
                .collect()
            self.logger.info(
                "SnowConnect._create_session() Creation of connection [OK]"\
                f"Warehouse, Role => {c}"
            )
        except Exception as ex:
            self.logger.exception(
                "SnowConnect._create_session() Cannot connect to snowflake"\
                f" Error: {ex} with Auth_Type: {auth_type}"
            )
        return session


    def _do_snowflake_auth(self, params):
        pass_env: str = params["password_env"]
        password = os.environ[pass_env]
        conn_params = {
            "account": params["account"],
            "user": params["user"],
            "authenticator": self.T_AUTH_SNOWFLAKE,
            "password": password,
            "role": params["role"], 
            "warehouse": params["warehouse"],
            "database": params["database"],
            "schema": params["schema"]
        }
        return self._create_session(
            SnowConnect.T_AUTH_SNOWFLAKE, conn_params
        )


    def _do_keypair_auth(self, params):
        session, pkey, pkb, conn_params = None, None, bytes(), None
        kphrase_env = params["private_key_phrase_env"]
        kphrase = os.environ[kphrase_env]
        kpath_env = params["private_key_path_env"]
        kpath = os.environ[kpath_env]
        try:
            with open(kpath, "rb") as key:
                pkey = serialization.load_pem_private_key(
                    key.read(),
                    password = kphrase.encode(),
                    backend = default_backend()
                )
        except Exception as e:
            self.logger.exception(
                f"Cannot read snowflake user private key file: {e}"
            )
        if pkey:
            try:
                pkb = pkey.private_bytes(
                    encoding = serialization.Encoding.DER,
                    format = serialization.PrivateFormat.PKCS8,
                    encryption_algorithm = serialization.NoEncryption()
                )
            except Exception as e:
                self.logger.exception(
                    f"Cannot read snowflake user private key as bytes: {e}"
                )
        else: 
            raise ValueError(
                "Cannot read snowflake user private key file; "\
                f"check exception and key file path {kpath}."
            )
        if pkb:
            conn_params = {
                "account": params["account"],
                "user": params["user"],
                "private_key": pkb,
                "role": params["role"], 
                "warehouse": params["warehouse"],
                "database": params["database"],
                "schema": params["schema"],
                "client_session_keep_alive": True,
                "max_connection_pool" : 20
            }
            session = self._create_session(
                SnowConnect.T_AUTH_KEYPAIR, conn_params
            )
        else: 
            raise ValueError(
                "Cannot read snowflake user private key bytes in "\
                "SnowConnect._do_keypair_auth()"
            )
        return session


    def _do_externalbrowser_auth(self, params):
        conn_params = {
            "account": params["account"],
            "user": params["user"],
            "authenticator": self.T_AUTH_EXT_BROWSER,
            "role": params["role"], 
            "warehouse": params["warehouse"],
            "database": params["database"],
            "schema": params["schema"]
        }
        return self._create_session(
            SnowConnect.T_AUTH_EXT_BROWSER, conn_params
        )


    def _do_oauth(self, params, ctx: Dict):
        token = ctx.get("access_token")
        dtok = ctx.get("decoded_access_token")

        if dtok is not None and dtok:
            upn:str = dict(dtok).get("upn")
            upn = upn.upper()
        
            conn_params = {
                "account": params["account"],
                "user": upn,
                "authenticator": AppConnect.T_AUTH_OAUTH,
                "token" : token,
                "role": params["role"], 
                "warehouse": params["warehouse"],
                "database": params["database"],
                "schema": params["schema"]
            }
            return self._create_session(
                self.T_AUTH_OAUTH, conn_params
            )
    

    @staticmethod
    def ddl(session: Session, sql: str) -> List[Row]:
        """
        Excute Data Definition Language statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            List[Row]: list of Snowflake Snowpark Rows
        """
        cmds =  {
            'create', 'drop', 'alter', 'truncate', 'delete', 'replace',
            'rename', 'copy', 'clone', 'show', 'desc', 'undrop',
            'set', 'unset'
        }
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.ddl(): input sql doesn't seem to be DDL [{sql}]"
            )             
        return session.sql(sql).collect()


    @staticmethod
    def dml(session: Session, sql: str) -> List[Row]:
        """
        Excute Data Manupilation Language statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            List[Row]: list of Snowflake Snowpark Rows
        """
        cmds =  {'insert', 'update', 'delete', 'call', 'explain'}
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.dml(): input sql doesn't seem to be DML [{sql}]"
            )             
        return session.sql(sql).collect()


    @staticmethod    
    def tcl(session: Session, sql: str) -> List[Row]:
        """
        Excute Tranction Control Language statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            List[Row]: list of Snowflake Snowpark Rows
        """
        cmds =  {'begin', 'commit', 'rollback', 'savepoint', 'release'}
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.tcl(): input sql doesn't seem to be TCL [{sql}]"
            )             
        return session.sql(sql).collect()


    @staticmethod    
    def dcl(session: Session, sql: str) -> List[Row]:
        """
        Excute Data Control Language statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            List[Row]: list of Snowflake Snowpark Rows
        """
        cmds =  {
            'grant', 'revoke', 'create', 'drop', 'alter', 'use', 'show',
            'desc', 'set', 'unset'
        }
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.dcl(): input sql doesn't seem to be DCL [{sql}]"
            )             
        return session.sql(sql).collect()
    

    @staticmethod
    def dql(session: Session, sql: str) -> Iterator[DF]:
        """
        Excute Data Query Language (Select) statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            Iterator[DF]: Iterator of Pandas DataFrame list
        """
        cmds =  {'select'}
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.dql(): input sql doesn't seem to be DQL [{sql}]"
            )             
        return session.sql(sql).to_pandas_batches()
    

    @staticmethod
    def sdf(session: Session, sql: str) -> SDF:
        """
        Excute Data Query Language (Select) statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            Dataframe: Snowflake Dataframe as result of the query
        """
        cmds =  {'select'}
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.sdf(): input sql doesn't seem to be DQL [{sql}]"
            )
        return session.sql(sql).to_df()