from typing import Type
from ..mariadb_client import MariaDBClient

from ..mariadb_server import MariaDBServer
from ..client_config import ClientConfig

from ..sql_fetch import SqlFetch
from unittest.mock import Mock

import unittest


def test_mariadb_sql_fetch_get_database_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    assert set(["information_schema", "mysql", "test", "performance_schema"]).issubset(
        sql_fetch.databases()
    )


def test_mariadb_sql_fetch_get_table_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("create database t1;")
    client.run_statement("use t1;")
    client.run_statement("create table table1(a int);")
    client.run_statement("create table table2(a int);")
    sql_fetch = SqlFetch(client, mocklog)

    unittest.TestCase().assertListEqual(
        sql_fetch.tables(), ["table1", "table2"],
    )

    client.run_statement("drop database t1;")


def test_mariadb_sql_fetch_get_show_candiates(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    unittest.TestCase().assertListEqual(
        sql_fetch.show_candidates(),
        [
            "SHOW AUTHORS",
            "SHOW BINARY LOGS",
            "SHOW BINLOG EVENTS",
            "SHOW CHARACTER SET",
            "SHOW COLLATION",
            "SHOW COLUMNS",
            "SHOW CONTRIBUTORS",
            "SHOW CREATE DATABASE",
            "SHOW CREATE EVENT",
            "SHOW CREATE FUNCTION",
            "SHOW CREATE PROCEDURE",
            "SHOW CREATE TABLE",
            "SHOW CREATE TRIGGER",
            "SHOW CREATE VIEW",
            "SHOW DATABASES",
            "SHOW ENGINE",
            "SHOW ENGINES",
            "SHOW ERRORS",
            "SHOW EVENTS",
            "SHOW FUNCTION CODE",
            "SHOW FUNCTION STATUS",
            "SHOW GRANTS",
            "SHOW INDEX",
            "SHOW MASTER STATUS",
            "SHOW OPEN TABLES",
            "SHOW PLUGINS",
            "SHOW PRIVILEGES",
            "SHOW PROCEDURE CODE",
            "SHOW PROCEDURE STATUS",
            "SHOW PROCESSLIST",
            "SHOW PROFILE",
            "SHOW PROFILES",
            "SHOW RELAYLOG EVENTS",
            "SHOW SLAVE HOSTS",
            "SHOW SLAVE STATUS",
            "SHOW STATUS",
            "SHOW TABLE STATUS",
            "SHOW TABLES",
            "SHOW TRIGGERS",
            "SHOW VARIABLES",
            "SHOW WARNINGS",
        ],
    )


def test_mariadb_sql_fetch_get_user_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    assert set(["'root'@'127.0.0.1'", "''@'localhost'", "'root'@'localhost'"]).issubset(
        sql_fetch.users()
    )


def test_mariadb_sql_fetch_get_function_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    db_name = "test"
    sql_fetch.change_db_name(db_name)
    client.run_statement(f"use {db_name};")

    client.run_statement(
        """CREATE FUNCTION hello (s CHAR(20)) 
        RETURNS CHAR(50) DETERMINISTIC 
        RETURN CONCAT('Hello, ',s,'!');"""
    )

    assert set(["hello"]).issubset(sql_fetch.functions())

    client.run_statement("drop function hello if exists;")


def test_mariadb_sql_fetch_get_table_column_list(mariadb_server: Type[MariaDBServer]):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)
    db_name = "test"
    sql_fetch.change_db_name(db_name)
    client.run_statement(f"use {db_name};")
    client.run_statement("create table t1(a int);")
    client.run_statement("create table t2(b int);")

    unittest.TestCase().assertListEqual(
        sql_fetch.table_columns(), [("t1", "a"), ("t2", "b")]
    )

    client.run_statement("drop table t1;")
    client.run_statement("drop table t2;")


def test_mariadb_sql_fetch_get_connected_clients_num(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    sql_fetch = SqlFetch(client, mocklog)

    assert sql_fetch.num_connected_clients() == 1


def test_mariadb_sql_fetch_get_current_used_database_name(
    mariadb_server: Type[MariaDBServer],
):
    mocklog = Mock()
    cfg = ClientConfig(mocklog, name="nonexistentcfg.json")  # default config

    mariadb_server(mocklog, cfg)

    client = MariaDBClient(mocklog, cfg)
    client.start()
    client.run_statement("use test;")
    sql_fetch = SqlFetch(client, mocklog)

    assert sql_fetch.get_db_name() == "test"
