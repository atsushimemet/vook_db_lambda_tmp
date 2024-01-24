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
    get_rds_config,
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
from vook_db_v7.utils import DataFrame_maker, read_sql_file, validate_input


def main(event, context):
    """DBからテーブル取得"""

    config_ec2 = get_ec2_config()
    query = read_sql_file("./vook_db_v7/sql/knowledges.sql")
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
                **get_rds_config(server.local_bind_port), connect_timeout=10
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

    # 対象のワードリスト作成
    words_brand_name = df_from_db["brand_name"].values
    words_knowledge_name = df_from_db["knowledge_name"].values
    words_line_name = df_from_db["line_name"].values

    for row in np.arange(len(words_brand_name)):
        word = words_brand_name[row]
        words_brand_name[row] = validate_input(word)

    for row in np.arange(len(words_knowledge_name)):
        word = words_knowledge_name[row]
        words_knowledge_name[row] = validate_input(word)

    for row in np.arange(len(words_line_name)):
        word = words_line_name[row]
        words_line_name[row] = validate_input(word)

    # 修正版のテーブルを作成
    df_from_db_corrected = pd.DataFrame(columns=df_from_db.columns)
    df_from_db_corrected["knowledge_id"] = df_from_db["knowledge_id"].values
    df_from_db_corrected["knowledge_name"] = words_knowledge_name
    df_from_db_corrected["brand_name"] = words_brand_name
    df_from_db_corrected["line_name"] = words_line_name

    platform_id = 1  # 楽天
    size_id = 999
    sleep_second = 1

    data = df_from_db_corrected

    n_bulk = len(data)
    df_bulk = pd.DataFrame()

    for n in np.arange(n_bulk):
        brand_name = data.brand_name[n]
        line_name = data.line_name[n]
        knowledge_name = data.knowledge_name[n]
        query = f"{brand_name} {line_name} {knowledge_name} 中古"
        # query validatorが欲しい　半角1文字をなくす

        knowledge_id = data.knowledge_id[n]
        print("検索キーワード:[" + query + "]", "knowledge_id:", knowledge_id)
        output = DataFrame_maker(query, platform_id, knowledge_id, size_id)
        df_bulk = pd.concat([df_bulk, output], ignore_index=True)
        sleep(sleep_second)
        break
        # 429エラー防止のためのタイムストップ

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

    # df = pd.DataFrame(data)
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
    query = """
    SELECT
        *
    FROM
        products
    """
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
