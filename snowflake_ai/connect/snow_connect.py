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
__version__ = "0.5.0"


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

from snowflake_ai.common import AppConfig, ConfigKey
from snowflake_ai.common import AppConnect, DataConnect
from snowflake_ai.connect import AuthCodeConnect, DeviceCodeConnect
from snowflake_ai.connect import ClientCredsConnect



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


    def __init__(
            self, 
            connect_key : Optional[str] = None,
            app_config: AppConfig = None
        ):
        super().__init__(connect_key, app_config)
        self.logger = SnowConnect._logger

        if not (self.connect_group == DataConnect.K_DATA_CONN and \
                self.connect_params):
            self.logger.error(
                f"SnowConnect.init(): ConnectType [{self.connect_type}];"\
                f"ConnectGroup [{self.connect_group}]; ConnectName ["\
                f"{self.connect_name}]"
            )
            raise ValueError(
                "SnowConnect.init(): SnowConnect configuration Error!"
            )
        if (not self.connect_type) or (
                self.connect_type != AppConfig.T_CONN_SNFLK):
            raise ValueError(
                f"SnowConnect.init(): Error - Type [{self.connect_type}]!"
            )
        
        # setup referenced oauth connect
        self.logger.debug(
            f"SnowConnect.init(): Oauth_connect_ref[{self.oauth_connect_ref}]"
        )
        if self.oauth_connect_ref:
            ot = self.oauth_flow_type if self.oauth_flow_type else \
                    AppConfig.T_OAUTH_DEVICE
            self.oauth_connect = None
            conn = AppConnect._connects.get(self.oauth_connect_ref)
            if conn is None:
                if ot == AppConfig.T_OAUTH_DEVICE:
                    self.oauth_connect = DeviceCodeConnect(
                            self.oauth_connect_ref,
                            self.app_config
                        )

                elif ot == AppConfig.T_OAUTH_CODE:
                    self.oauth_connect = AuthCodeConnect(
                            self.oauth_connect_ref,
                            self.app_config
                        ) 
                 
                elif ot == AppConfig.T_OAUTH_CREDS:
                    self.oauth_connect = ClientCredsConnect(
                            self.oauth_connect_ref,
                            self.app_config
                        )               

                AppConnect._connects[self.oauth_connect_ref] = \
                        self.oauth_connect
            else:
                self.oauth_connect = conn

        # create data service connection and store at DataConnect class
        if (self.connect_key is not None) and self.connect_key:
            if ((self.data_connections.get(self.connect_key) is None) \
                    or (not self.is_current_active())) \
                    and (self.is_service_connect()):
                self.data_connections[self.connect_key] = \
                    self.create_connection(self.connect_params)
                self.set_current_connection(
                    self.data_connections[self.connect_key]
                )

        self.logger.debug(f"SnowConnect.init(): Connect_key"\
                f" [{self.connect_key}]; DataConnections => "\
                f"{self.data_connections}; Is_service_connect ["\
                f"{self.is_service_connect()}]")

        # setup initialization
        self.init_connects()



    def get_oauth_connect(self):
        return self.oauth_connect
    

    def create_connection(self, params: Dict):
        """
        Create a snowflake service connection based on the specific 
        configuration. A set of authentication types are supported:
            snowflake, keypair, oauth and externalbrowser
        Note: user specific connection would be in form of session which
        is typically created through oauth connect.

        Args:
            params (Dict): Configuration dictionary as input parameters.

        Returns:
            object: snowflake connection session
        """
        if params is None:
            raise ValueError(
                "SnowConnect.create_connection(): Input params cannot be None!"
            )
        session = None
        try:
            auth = params[ConfigKey.AUTH_TYPE.value]
            self.logger.debug(
                "SnowConnect.create_connection(): Try to "\
                f"connect to snowflake using [{auth}] auth."
            )
            if auth == AppConnect.T_AUTH_SNOWFLAKE:
                session = self._do_snowflake_auth(params)

            elif auth == AppConnect.T_AUTH_KEYPAIR:
                session = self._do_keypair_auth(params)

            elif auth == AppConnect.T_AUTH_EXT_BROWSER: 
                # should not be used to create svc connection
                self.logger.warning(
                    "SnowConnect.create_connection(): Use create_session "\
                    "to create externalbrowser enabled connection."
                )

            elif auth == AppConnect.T_AUTH_OAUTH:
                if self.oauth_flow_type == AppConfig.T_OAUTH_CREDS:
                    if self.oauth_connect is None:
                        self.logger.error(
                            "SnowConnect.create_connection(): OAuth "\
                            f"[{self.connect_key}] connect is None!"
                        )                        
                        return None
                    cc: ClientCredsConnect = self.oauth_connect
                    d_rs: Dict = cc.grant_request(self.oauth_connect_config)
                    ctx: Dict = cc.decode_token(d_rs, ["access_token"])
                    self.logger.debug(
                        "SnowConnect.create_connection(): OAuth response "\
                        f"decoded_token => {ctx}."
                    )
                    session = self._do_oauth(self.connect_params, ctx)
                else:
                    self.logger.warning(
                        "SnowConnect.create_connection(): No action in "\
                        "create_session to create oauth enabled connection!"
                    )

        except Exception as e:
            self.logger.exception(
                f"SnowConnect.create_connection(): Cannot use "\
                f"auth_type [{auth}] to creaet snowflake session - {e}!"
            )
        return session


    def get_connection(self, connect_key: Optional[str] = None):
        """
        Get lazy created snowflake shared connection.

        Args:
            connect_key (str): data connect key in form of
                "data_connects".<connect_name>

        Returns:
            object: snowflake connection object, i.e., snowflake session
        """
        conn = super().get_connection(connect_key)
        if conn is not None and (
            (self.connect_params[ConfigKey.TYPE.value] != \
                    SnowConnect.T_SNOWFLAKE_CONN) or 
                    (not isinstance(conn, Session))
        ):
            raise TypeError(
                f"SnowConnect.get_connection(): Type Error - {connect_key}!"
            )
        elif conn is None:
            self.logger.warning(
                "SnowConnect.get_connection():  Shared/Service connection "\
                f"is None; Current_connect_key [{connect_key}]; "
                f"Current_connect_type [{self.connect_params['type']}]."
            )

        return conn


    @classmethod
    def is_session_active(cls, session: Session) -> bool:
        rb = False
        try:
            r = cls.dql(session, "select current_role()")           
            try:
                pd = next(r)
                cls._logger.debug(
                    "SnowConnect.is_session_active(): Active "
                    f"session using role [{pd}]"
                )
                if DF(pd).shape[0] > 0:
                    rb = True
            except Exception as ee:
                cls._logger.warning(
                    "SnowConnect.is_session_active(): No Result "\
                    f"returned [{ee}]"
                )            
        except Exception as e:
            cls._logger.warning(
                f"SnowConnect.is_session_active(): Warning {e}!"
            )
        return rb


    def is_current_active(self, session: Session = None) -> bool:
        """
        Check whether current Snowflake connection (shared session) is
        still active or not. 
        
        Returns:
            bool: True if the connection is active, otherwise False
        """
        if session is None:
            session = self._current_connection
        if session is None:
            self.logger.warn(
                f"SnowConnect.is_current_active(): Session is None!"
            )
            return False
        
        rb = SnowConnect.is_session_active(session)
        return rb
    

    def create_user_session(self, ctx: Dict = {}) -> Session:
        """
        Create snowflake user specific session, typically through
        OAuth Auth code/device code flow.

        Arg:
            ctx (Dict): OAuth context dictionary including access
                token, etc.
        Returns:
            Session: Snowflake connection's session
        """
        usr: str = ctx.get(ConfigKey.USER.value)

        session: Session = None
        if self.auth_type == self.T_AUTH_EXT_BROWSER:
            if usr is not None and usr:
                self.connect_params[ConfigKey.USER.value] = usr
            session = self._do_externalbrowser_auth(self.connect_params)

        elif self.auth_type == self.T_AUTH_OAUTH:
            session = self._do_oauth(self.connect_params, ctx)

        elif self.auth_type == self.T_AUTH_SNOWFLAKE:
            session = self._do_snowflake_auth(self.connect_params)

        return session
    

    def get_service_session(self) -> Session:
        """
        Get existing snowflake data service connection.

        Returns:
            Session: Snowflake connection's session
        """
        if (self.connect_type == SnowConnect.T_SNOWFLAKE_CONN) and\
                ((self.auth_type == self.T_AUTH_KEYPAIR) or
                 (self.auth_type == self.T_AUTH_SNOWFLAKE)):
            return self.get_connection(self.connect_key)
        
        if (self.connect_type == SnowConnect.T_SNOWFLAKE_CONN) and\
                (self.auth_type == self.T_AUTH_OAUTH) and \
                (self.oauth_flow_type == AppConfig.T_OAUTH_CREDS):
            return self.get_connection(self.connect_key)
        
        else:
            self.logger.error(
                    "SnowConnect.create_service_session(): Error -"\
                    f"OAuth Client Credentials not yet implemented!")
            return None


    def create_service_session(self) -> Session:
        """
        Create snowflake data service connection.
        
        Returns:
            Session: Snowflake connection's session
        """
        if (self.connect_type == SnowConnect.T_SNOWFLAKE_CONN) and\
                (self.auth_type == self.T_AUTH_KEYPAIR):
            return self.create_connection(self.connect_params)
        
        if (self.connect_type == SnowConnect.T_SNOWFLAKE_CONN) and\
                (self.auth_type == self.T_AUTH_OAUTH) and\
                (self.oauth_flow_type == AppConfig.T_OAUTH_CREDS):
            return self.create_connection(self.connect_params)
        
        elif (self.connect_type == SnowConnect.T_SNOWFLAKE_CONN) and\
                (self.auth_type == self.T_AUTH_OAUTH):
            raise ValueError(
                    "SnowConnect.create_service_session(): Error -"\
                    f"OAuth Client Credentials not yet implemented!")

        elif self.auth_type == self.T_AUTH_SNOWFLAKE:
            return self.create_connection(self.connect_params)


    def create_session(self, ctx: Optional[Dict] = {}) -> Session:
        """
        Create snowflake user specific session, or get the existing
        service data connection. Note, use create_user_session() for
        auth code/device code based oauth based user session, and 
        create_service_session() for key based or client credentials
        flow of oauth service session.

        Arg:
            ctx (Dict): OAuth context dictionary including access
                token, etc.
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
                if not ctx:
                    cc: ClientCredsConnect = self.oauth_connect
                    d_rs: Dict = cc.grant_request(self.oauth_connect_config)
                    ctx = cc.decode_token(d_rs, ["access_token"])
                
                self.logger.debug(
                        "SnowConnect.create_session()): OAuth "\
                        f"context => {ctx}."
                    )
                session = self._do_oauth(self.connect_params, ctx)
        return session
    

    def close_session(self):
        """
        Close current snowflake connection and session.
        """
        self.close_connection()


    def _create_session(self, auth_type: str, conn_params) -> Session:
        session = None
        try:
            self.logger.debug(
                "SnowConnect._create_session(): Session creation "\
                f"- Auth_type [{auth_type}]; Connect_params => {conn_params}."
            )

            session = Session.builder.configs(conn_params).create() 
            c = session.sql("select current_warehouse(), current_role()")\
                .collect()
            
            self.logger.debug(
                "SnowConnect._create_session(): Session creation "\
                f"[OK]; Warehouse, Role: [{c}]"
            )
        except Exception as ex:
            self.logger.exception(
                "SnowConnect._create_session(): Cannot connect to "\
                f"Snowflake! Trying auth_type [{auth_type}]; Error [{ex}]!"
            )
        return session


    def _do_snowflake_auth(self, params: Dict):
        pass_env: str = params["password_env"]
        password = os.environ[pass_env]
        conn_params = {
            "account": params["account"],
            "user": params.get("user", ""),
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


    def _do_keypair_auth(self, params: Dict):
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
                f"SnowConnect._do_keypair_auth(): Cannot read snowflake "\
                f"user private key file: {e}!"
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
                    f"SnowConnect._do_keypair_auth(): Cannot read snowflake "\
                    f"user private key as bytes: {e}!"
                )
        else: 
            raise ValueError(
                "SnowConnect._do_keypair_auth(): Cannot read snowflake user "\
                "private key file; Check exception and key file path "\
                f"[{kpath}]!"
            )
        if pkb:
            conn_params = {
                "account": params["account"],
                "user": params.get("user", ""),
                "private_key": pkb,
                "role": params["role"], 
                "warehouse": params.get("warehouse", ""),
                "database": params.get("database", ""),
                "schema": params.get("schema", ""),
                "client_session_keep_alive": True,
                "max_connection_pool" : 20
            }
            session = self._create_session(
                SnowConnect.T_AUTH_KEYPAIR, conn_params
            )
        else: 
            raise ValueError(
                "SnowConnect._do_keypair_auth(): Cannot read snowflake user "\
                "private key; Keypair Auth Failed!"
            )
        return session


    def _do_externalbrowser_auth(self, params: Dict):
        conn_params = {
            "account": params["account"],
            "user": params.get("user", ""),
            "authenticator": self.T_AUTH_EXT_BROWSER,
            "role": params["role"], 
            "warehouse": params.get("warehouse", ""),
            "database": params.get("database", ""),
            "schema": params.get("schema", "")
        }
        return self._create_session(
            SnowConnect.T_AUTH_EXT_BROWSER, conn_params
        )


    def _do_oauth(self, params: Dict, ctx: Dict):
        token = ctx.get("access_token")
        dtok = ctx.get("decoded_access_token")

        if dtok is None:
            dtok = params.get("decoded_access_token")
            token = params.get("access_token")
        
        if dtok is not None and dtok:
            upn:str = dict(dtok).get("upn")
            if upn is None:
                upn = params.get(ConfigKey.USER.value, "")
            
            if (upn is None) or (not upn):
                upn = dict(dtok).get("sub")

            upn = upn.upper()
            self.logger.debug(
                f"SnowConnect._do_oauth(): Access_token =>\n"\
                f"{token}\nDecoded_token =>\n{dtok}\n"
            )

            conn_params = {
                "account": params["account"],
                "user": upn,
                "authenticator": AppConnect.T_AUTH_OAUTH,
                "token" : token,
                "role": params["role"], 
                "warehouse": params.get("warehouse", ""),
                "database": params.get("database", ""),
                "schema": params.get("schema", "")
            }
            return self._create_session(
                self.T_AUTH_OAUTH, conn_params
            )
    

    def is_service_connect(self) -> bool:
        """
        Return whether this application connect is service type
        """
        auth_t = self.auth_type
        rs = False

        if auth_t == AppConnect.T_AUTH_EXT_BROWSER:
            rs = False
        elif auth_t == AppConnect.T_AUTH_KEYPAIR:
            rs = True
        elif (auth_t == AppConfig.T_OAUTH) and \
                (self.oauth_flow_type == AppConfig.T_OAUTH_CREDS):
            rs = True
        elif (auth_t == AppConfig.T_OAUTH):
            rs = False
        elif (auth_t == AppConfig.T_AUTH_SNFLK):
            rs = True

        self.logger.debug(
                f"SnowConnect.is_service_connect(): AuthType "\
                f"[{self.auth_type}]; Is_service_connect [{rs}]."
            )
        return rs
    

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
                f"SnowConnect.ddl(): Input sql doesn't seem to be DDL [{sql}]"
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
                f"SnowConnect.dml(): Input sql doesn't seem to be DML [{sql}]"
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
                f"SnowConnect.tcl(): Input sql doesn't seem to be TCL [{sql}]"
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
                f"SnowConnect.dcl(): Input sql doesn't seem to be DCL [{sql}]"
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
                f"SnowConnect.dql(): Input sql doesn't seem to be DQL [{sql}]"
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
                f"SnowConnect.sdf(): Input sql doesn't seem to be DQL [{sql}]"
            )
        return session.sql(sql).to_df()