#!/usr/bin/env python
# coding: utf-8
import base64
import hashlib
from io import StringIO
from time import sleep

import boto3
import numpy as np
import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder

from vook_db_v7.local_config import (
    get_ec2_config,
    get_rds_config_for_put,
    put_ec2_config,
)
from vook_db_v7.tests import (
    columns_checker,
    created_at_checker,
    id_checker,
    knowledge_id_checker,
    name_checker,
    platform_id_checker,
    price_checker,
    size_id_checker,
    updated_at_checker,
    url_checker,
)
from vook_db_v7.utils import (
    create_df_no_ng_keyword,
    create_wort_list,
    get_knowledges,
    read_sql_file,
    repeat_dataframe_maker,
)


def main(event, context):
    config_ec2 = get_ec2_config()
    query = read_sql_file("./vook_db_v7/sql/knowledges.sql")
    df_from_db = pd.DataFrame()
    df_from_db = get_knowledges(config_ec2, query, df_from_db)
    # 対象のワードリスト作成
    words_brand_name = create_wort_list(df_from_db, "brand")
    words_line_name = create_wort_list(df_from_db, "line")
    words_knowledge_name = create_wort_list(df_from_db, "knowledge")
    # 修正版のテーブルを作成
    df_no_ng_keyword = create_df_no_ng_keyword(
        df_from_db, words_knowledge_name, words_brand_name, words_line_name
    )

    df_bulk = repeat_dataframe_maker(df_no_ng_keyword)
    df_prev = pd.read_csv("./data/output/products_raw_prev.csv")
    PREV_ID_MAX = df_prev["id"].max()
    df_bulk["id"] = np.arange(PREV_ID_MAX, PREV_ID_MAX + len(df_bulk)) + 1

    columns_checker(df_bulk)
    id_checker(df_bulk)
    name_checker(df_bulk)
    knowledge_id_checker(df_bulk)
    platform_id_checker(df_bulk)
    url_checker(df_bulk)
    price_checker(df_bulk)
    size_id_checker(df_bulk)
    updated_at_checker(df_bulk)
    created_at_checker(df_bulk)
    print(df_bulk.dtypes)

    print("finish")

    """作成したdataframをcsvとして　s３にも保存しておく"""

    df = df_bulk
    # S3のバケット名とオブジェクトキーを指定
    s3_bucket = "vook-vook"
    s3_key = "lambda_output/test2.csv"

    # S3にアップロードするためのBoto3クライアントを作成
    s3_client = boto3.client("s3")
    # Pandas DataFrameをCSV形式の文字列に変換
    csv_data = df.to_csv(index=False)
    # 文字列IOを使ってCSVデータを書き込む
    csv_buffer = StringIO()
    csv_buffer.write(csv_data)
    # 文字列IOのカーソルを先頭に戻す
    csv_buffer.seek(0)

    # バイナリデータとしてエンコード
    csv_binary = csv_buffer.getvalue().encode("utf-8")
    # ファイルのハッシュを計算
    file_hash = hashlib.md5(csv_binary).digest()
    # Base64エンコード
    content_md5 = base64.b64encode(file_hash).decode("utf-8")
    # S3にCSVファイルをアップロード
    s3_client.put_object(
        Body=csv_binary, Bucket=s3_bucket, Key=s3_key, ContentMD5=content_md5
    )

    print(f"CSV file uploaded to s3://{s3_bucket}/{s3_key}")

    config_ec2 = put_ec2_config()

    # SSHトンネルの設定
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
            # SQLクエリの実行
            print("ここに処理を書く")

            create_table_query = read_sql_file("./vook_db_v7/sql/create_products.sql")
            # 既存DBの中身を削除する処理を記載
            cursor.execute("TRUNCATE TABLE products")

            cursor.execute(create_table_query)
            # DataFrameをRDSのテーブルに挿入
            insert_query = read_sql_file("./vook_db_v7/sql/insert_into_products.sql")

            for index, row in df_bulk.iterrows():
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

    """DBからテーブル取得"""

    config_ec2 = put_ec2_config()
    query = read_sql_file("./vook_db_v7/sql/products.sql")
    df_from_db = pd.DataFrame()

    # SSHトンネルの設定
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
            # SQLクエリの実行
            cursor.execute(query)
            for row in cursor:  # column1, column2, ...は取得したいカラム名に合わせて変更してください
                df_from_db = pd.concat(
                    [df_from_db, pd.DataFrame([row])], ignore_index=True
                )

        except pymysql.MySQLError as e:
            print(f"Error connecting to MySQL: {e}")
        finally:
            if conn is not None:
                conn.close()

    print(df_from_db.head(), df_from_db.shape)
