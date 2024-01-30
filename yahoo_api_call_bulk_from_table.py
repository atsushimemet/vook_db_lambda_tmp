#!/usr/bin/env python
# coding: utf-8
import datetime
import json
from time import sleep

import numpy as np
import pandas as pd
import requests

from vook_db_v7.config import (
    REQ_URL_CATE,
    platform_id,
    s3_bucket,
    s3_file_name_products_raw_prev,
    size_id,
    sleep_second,
)
from vook_db_v7.local_config import ClientId, aff_id
from vook_db_v7.rds_handler import get_knowledges, get_products, put_products
from vook_db_v7.tests import run_all_if_checker
from vook_db_v7.utils import (
    create_df_no_ng_keyword,
    create_wort_list,
    read_csv_from_s3,
    time_decorator,
    upload_s3,
)

WANT_ITEMS = [
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


def DataFrame_maker_yahoo(keyword, platform_id, knowledge_id, size_id):
    start_num = 1
    step = 100
    max_products = 1000

    params = {
        "appid": ClientId,
        "output": "json",
        "query": keyword,
        "sort": "-price",
        "affiliate_id": aff_id,
        "affiliate_type": "vc",
        "results": 100,  # NOTE: 100個ずつしか取得できない。
    }

    l_df = []
    for inc in range(0, max_products, step):
        params["start"] = start_num + inc
        df = pd.DataFrame(columns=WANT_ITEMS)
        res = requests.get(url=REQ_URL_CATE, params=params)
        res_cd = res.status_code
        if res_cd != 200:
            print("Bad request")
            break
        else:
            res = json.loads(res.text)
            if len(res["hits"]) == 0:
                print("If the number of returned items is 0, the loop ends.")
            print("Get Data")
            l_hit = []
            for h in res["hits"]:
                l_hit.append(
                    (
                        h["index"],
                        h["name"],
                        h["url"],
                        h["price"],
                        knowledge_id,
                        platform_id,
                        size_id,
                        # 現在の日付と時刻を取得 & フォーマットを指定して文字列に変換
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        # 現在の日付と時刻を取得 & フォーマットを指定して文字列に変換
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    )
                )
            df = pd.DataFrame(l_hit, columns=WANT_ITEMS)
            l_df.append(df)
    return pd.concat(l_df, ignore_index=True)


@time_decorator
def repeat_dataframe_maker_yahoo(
    df_no_ng_keyword,
    platform_id,
    size_id=size_id,
    sleep_second=sleep_second,
):
    n_bulk = len(df_no_ng_keyword)
    df_bulk = pd.DataFrame()
    for i, n in enumerate(np.arange(n_bulk)):
        brand_name = df_no_ng_keyword.brand_name[n]
        line_name = df_no_ng_keyword.line_name[n]
        knowledge_name = df_no_ng_keyword.knowledge_name[n]
        query = f"{brand_name} {line_name} {knowledge_name} 中古"
        # query validatorが欲しい　半角1文字をなくす
        knowledge_id = df_no_ng_keyword.knowledge_id[n]
        print("検索キーワード:[" + query + "]", "knowledge_id:", knowledge_id)
        output = DataFrame_maker_yahoo(query, platform_id, knowledge_id, size_id)
        df_bulk = pd.concat([df_bulk, output], ignore_index=True)
        sleep(sleep_second)
        break
    return df_bulk


def main(event, context):
    # 知識情報の取得
    df_from_db = get_knowledges()
    # 対象のワードリスト作成
    words_brand_name = create_wort_list(df_from_db, "brand")
    words_line_name = create_wort_list(df_from_db, "line")
    words_knowledge_name = create_wort_list(df_from_db, "knowledge")
    # 修正版のテーブルを作成
    df_no_ng_keyword = create_df_no_ng_keyword(
        df_from_db, words_knowledge_name, words_brand_name, words_line_name
    )
    # df_bulkの作成
    df_bulk = repeat_dataframe_maker_yahoo(df_no_ng_keyword, platform_id)
    # IDの設定
    df_prev = read_csv_from_s3(s3_bucket, s3_file_name_products_raw_prev)
    nan_arr = np.isnan(df_prev["id"])
    if all(nan_arr):
        df_bulk["id"] = np.arange(1, len(df_bulk) + 1)
    elif any(nan_arr):
        Exception("一部に欠損が生じているという想定外の事象です。")
    else:
        PREV_ID_MAX = df_prev["id"].max()
        df_bulk["id"] = np.arange(PREV_ID_MAX, PREV_ID_MAX + len(df_bulk)) + 1
    run_all_if_checker(df_bulk)
    # df_bulkをs３に保存
    upload_s3(df_bulk)
    # # df_bulkをRDSに保存
    # put_products(df_bulk)
    # print("fin put products")
    # # RDSに保存したデータを確認
    # df_from_db = get_products()
    # print("shape:", df_from_db.shape)
    # print("id min:", df_from_db["id"].min())
    # print("id max:", df_from_db["id"].max())


if __name__ == "__main__":
    main(1, 1)
