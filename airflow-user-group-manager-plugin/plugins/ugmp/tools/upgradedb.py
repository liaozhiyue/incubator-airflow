# encoding: utf-8

import os

from airflow import settings
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.base_hook import CONN_ENV_PREFIX


MYSQL_CONN_ID = "dag_creation_manager_plugin_sql_alchemy_conn"


def get_mysql_hook():
    os.environ[CONN_ENV_PREFIX + MYSQL_CONN_ID.upper()] = settings.SQL_ALCHEMY_CONN
    return MySqlHook(mysql_conn_id=MYSQL_CONN_ID)


def run_sql(sql, ignore_error=False):
    hook = get_mysql_hook()
    print "sql:\n%s" % sql
    try:
        res = hook.get_records(sql)
    except Exception as e:
        if not ignore_error:
            raise e
        res = None
    print res
    return res


def run_version_0_0_1():
    run_sql("""
        CREATE TABLE IF NOT EXISTS `ugmp_user_group` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `username` varchar(250) NOT NULL,
          `group` varchar(250) NOT NULL,
          `updated_at` datetime(6) NOT NULL,
          `created_at` datetime(6) NOT NULL,
          PRIMARY KEY (`id`),
          KEY `username` (`username`),
          KEY `group` (`group`),
          UNIQUE KEY `user_group` (`username`, `group`)
        ) DEFAULT CHARSET=utf8mb4;
    """)


def run_version_0_0_2():
    run_sql("""
        ALTER TABLE `ugmp_user_group` CHANGE COLUMN `username` `user_name` VARCHAR(250) NOT NULL;

    """)


def main():
    run_version_0_0_1()
    run_version_0_0_2()


if __name__ == "__main__":
    main()
