#!/usr/bin/env python
# coding: utf-8
import base64
import hashlib
import re
from io import StringIO
from time import sleep

import boto3
import numpy as np
import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder

from vook_db_v7.local_config import get_rds_config  # noqa
from vook_db_v7.local_config import get_ec2_config, put_ec2_config
from vook_db_v7.utils import DataFrame_maker, convertor


def main(event, context):
    """DBからテーブル取得"""

    config_ec2 = get_ec2_config()
    query = """
    SELECT
        a.id as knowledge_id,
        a.name as knowledge_name,
        b.name as brand_name,
        c.name as line_name
    FROM
        knowledges a
    LEFT JOIN
        brands b
    ON
        a.brand_id = b.id
    LEFT JOIN
        `lines` c
    ON
        a.line_id = c.id
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

    # 対応表を読み出し
    errata_table = pd.read_csv("./data/input/query_ng_ok.csv")

    def validate_input(input_string):
        """
        連続する2文字以上で構成されたワードのみをOKとし、単体1文字またはスペースの前後に単体1文字が含まれるワードをNGとするバリデータ関数
        """
        # 正規表現パターン: 単体1文字またはスペースの前後に単体1文字が含まれるワードを検出
        pattern_ng = re.compile(r"^[!-~]$|\s[!-~]$|^[!-~]\s")

        # 入力文字列がOKパターンに一致するか確認
        # 入力文字列がNGパターンに一致するか確認
        if not pattern_ng.search(input_string):
            return input_string

        else:
            # エラーワードがあればメッセージを吐き、convertor関数によって対応する
            print(f"エラーワード　{input_string}が存在しました:")
            return convertor(input_string, errata_table)

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

    columns_correct = [
        "id",
        "name",
        "url",
        "price",
        "knowledge_id",
        "platform_id",
        "size_id",
        "created_at",
        "updated_at",
    ]

    def columns_checker(file):
        if all(file.columns == columns_correct):
            print("columns ok!")
        else:
            print("incorrect columns!")

    def id_checker(file):
        if file[columns_correct[0]].notnull().all():
            if file[columns_correct[0]].dtypes == "int64":
                print("id ok!")
            else:
                print("incorrect id values")
        else:
            print("there are null ids")

    def name_checker(file):
        if file[columns_correct[1]].notnull().all():
            if file[columns_correct[1]].dtypes == "O":
                print("name ok!")
            else:
                print("incorrect name values")
        else:
            print("there are null names")

    def url_checker(file):
        if file[columns_correct[2]].notnull().all():
            if file[columns_correct[2]].dtypes == "O":
                print("url ok!")
            else:
                print("incorrect url values")
        else:
            print("there are null urls")

    def price_checker(file):
        if file[columns_correct[3]].notnull().all():
            if file[columns_correct[3]].dtypes == "int64":
                print("price ok!")
            else:
                print("incorrect price value")
        else:
            print("there are null prices")

    def knowledge_id_checker(file):
        if file[columns_correct[4]].notnull().all():
            if file[columns_correct[4]].dtypes == "int64":
                print("knowledge_id ok!")
            else:
                print("incorrect knowledge_id value")
        else:
            print("there are null knowledge_ids")

    def platform_id_checker(file):
        if file[columns_correct[5]].notnull().all():
            if file[columns_correct[5]].dtypes == "int64":
                print("pltaform_id ok!")
            else:
                print("incorrect pltaform_id value")
        else:
            print("there are null pltaform_ids")

    def size_id_checker(file):
        if file[columns_correct[6]].notnull().all():
            if file[columns_correct[6]].dtypes == "int64":
                print("size_id ok!")
            else:
                print("incorrect size_id value")
        else:
            print("there are null size_ids")

    def created_at_checker(file):
        if file[columns_correct[7]].notnull().all():
            if file[columns_correct[7]].dtypes == "O":
                print("created_at ok!")
            else:
                print("incorrect created_at values")
        else:
            print("there are null created_at")

    def updated_at_checker(file):
        if file[columns_correct[8]].notnull().all():
            if file[columns_correct[8]].dtypes == "O":
                print("updated_at ok!")
            else:
                print("incorrect updated_at values")
        else:
            print("there are null updated_at")

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

    # 検証dvに格納
    def get_rds_config_for_put(port):
        return {
            "user": "root",
            "password": "rds-vook",
            "port": port,
            "host": "localhost",
            "database": "vook_web_v3_development",
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
        }

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
                connect_timeout=10,  # noqa
            )
            cursor = conn.cursor()
            # SQLクエリの実行
            print("ここに処理を書く")

            create_table_query = """
                    CREATE TABLE IF NOT EXISTS products (
                        id bigint PRIMARY KEY AUTO_INCREMENT,
                        name varchar(255) NOT NULL,
                        url varchar(255) NOT NULL UNIQUE,
                        price int NOT NULL,
                        knowledge_id bigint NOT NULL,
                        platform_id bigint NOT NULL,
                        size_id bigint NOT NULL,
                        created_at datetime(6) NOT NULL,
                        updated_at datetime(6) NOT NULL
                    )
                """
            # 既存DBの中身を削除する処理を記載
            cursor.execute("TRUNCATE TABLE products")

            cursor.execute(create_table_query)
            # DataFrameをRDSのテーブルに挿入
            insert_query = "INSERT INTO products (id,name,url,price,knowledge_id,platform_id,size_id,created_at,updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"  # noqa

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
                connect_timeout=10,  # noqa
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
