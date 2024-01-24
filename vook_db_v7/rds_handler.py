import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder

from vook_db_v7.local_config import (
    get_ec2_config,
    get_rds_config,
    get_rds_config_for_put,
    put_ec2_config,
)


def read_sql_file(file_path):
    """
    指定されたファイルパスからSQLファイルを読み込み、その内容を文字列として返す。

    :param file_path: 読み込む.sqlファイルのパス
    :return: ファイルの内容を含む文字列
    """
    try:
        with open(file_path, "r") as file:
            return file.read()
    except IOError as e:
        # ファイルが開けない、見つからない、などのエラー処理
        return f"Error reading file: {e}"


def get_knowledges():
    config_ec2 = get_ec2_config()
    query = read_sql_file("./vook_db_v7/sql/knowledges.sql")
    df_from_db = pd.DataFrame()
    with SSHTunnelForwarder(
        (config_ec2["host_name"], config_ec2["ec2_port"]),
        ssh_username=config_ec2["ssh_username"],
        ssh_pkey=config_ec2["ssh_pkey"],
        remote_bind_address=(
            config_ec2["rds_end_point"],
            config_ec2["rds_port"],
        ),
    ) as server:
        print(f"Local bind port: {server.local_bind_port}")
        conn = None
        try:
            conn = pymysql.connect(
                **get_rds_config(server.local_bind_port), connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute(query)
            for row in cursor:  # column1, column2, ...は取得したいカラム名に合わせて変更してください
                df_from_db = pd.concat(
                    [df_from_db, pd.DataFrame([row])], ignore_index=True
                )
            return df_from_db
        except pymysql.MySQLError as e:
            print(f"Error connecting to MySQL: {e}")
        finally:
            if conn is not None:
                conn.close()


def put_products(df_bulk):
    config_ec2 = put_ec2_config()
    create_table_query = read_sql_file("./vook_db_v7/sql/create_products.sql")
    insert_query = read_sql_file("./vook_db_v7/sql/insert_into_products.sql")
    with SSHTunnelForwarder(
        (config_ec2["host_name"], config_ec2["ec2_port"]),
        ssh_username=config_ec2["ssh_username"],
        ssh_pkey=config_ec2["ssh_pkey"],
        remote_bind_address=(
            config_ec2["rds_end_point"],
            config_ec2["rds_port"],
        ),
    ) as server:
        print(f"Local bind port: {server.local_bind_port}")
        conn = None
        try:
            conn = pymysql.connect(
                **get_rds_config_for_put(server.local_bind_port),
                connect_timeout=10,
            )
            cursor = conn.cursor()
            # 既存DBの中身を削除する処理を記載
            cursor.execute("TRUNCATE TABLE products")
            cursor.execute(create_table_query)
            # DataFrameをRDSのテーブルに挿入
            for _, row in df_bulk.iterrows():
                print(row)
                cursor.execute(
                    insert_query,
                    (
                        row["id"],
                        row["name"],
                        row["url"],
                        row["price"],
                        row["knowledge_id"],
                        row["platform_id"],
                        row["size_id"],
                        row["created_at"],
                        row["updated_at"],
                    ),
                )
            conn.commit()
        except pymysql.MySQLError as e:
            print(f"Error connecting to MySQL: {e}")
        finally:
            if conn is not None:
                conn.close()


def get_products():
    config_ec2 = put_ec2_config()
    query = read_sql_file("./vook_db_v7/sql/products.sql")
    df_from_db = pd.DataFrame()
    with SSHTunnelForwarder(
        (config_ec2["host_name"], config_ec2["ec2_port"]),
        ssh_username=config_ec2["ssh_username"],
        ssh_pkey=config_ec2["ssh_pkey"],
        remote_bind_address=(
            config_ec2["rds_end_point"],
            config_ec2["rds_port"],
        ),
    ) as server:
        print(f"Local bind port: {server.local_bind_port}")
        conn = None
        try:
            conn = pymysql.connect(
                **get_rds_config_for_put(server.local_bind_port),
                connect_timeout=10,
            )
            cursor = conn.cursor()
            cursor.execute(query)
            for row in cursor:  # column1, column2, ...は取得したいカラム名に合わせて変更してください
                df_from_db = pd.concat(
                    [df_from_db, pd.DataFrame([row])], ignore_index=True
                )
            return df_from_db
        except pymysql.MySQLError as e:
            print(f"Error connecting to MySQL: {e}")
        finally:
            if conn is not None:
                conn.close()
