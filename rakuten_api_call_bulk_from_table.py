#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import pymysql
from sshtunnel import SSHTunnelForwarder

from vook_db_v7.local_config import (
    get_ec2_config,
    get_rds_config_for_put,
    put_ec2_config,
)
from vook_db_v7.tests import run_all_if_checker
from vook_db_v7.utils import (
    create_df_no_ng_keyword,
    create_wort_list,
    get_knowledges,
    put_products,
    read_sql_file,
    repeat_dataframe_maker,
    upload_s3,
)


def main(event, context):
    # 知識情報の取得
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
    # df_bulkの作成
    df_bulk = repeat_dataframe_maker(df_no_ng_keyword)
    df_prev = pd.read_csv("./data/output/products_raw_prev.csv")
    PREV_ID_MAX = df_prev["id"].max()
    df_bulk["id"] = np.arange(PREV_ID_MAX, PREV_ID_MAX + len(df_bulk)) + 1
    run_all_if_checker(df_bulk)
    # df_bulkをs３に保存
    df = df_bulk
    upload_s3(df)
    # df_bulkをRDSに保存
    put_products(df_bulk)
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
