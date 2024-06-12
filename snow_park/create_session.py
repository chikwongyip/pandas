# coding: utf-8
from snowflake.snowpark import Session

connection_parameters = {
    "user": "ETL_USER",
    "password": "WD6gvmPY",
    "account": "ul51368.ap-southeast-1",
    "database": "ODS",
    "schema": "WECOM",
}


def create_session():
    session = Session.builder.configs(connection_parameters).create()
    return session