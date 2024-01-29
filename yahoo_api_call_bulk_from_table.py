#!/usr/bin/env python
# coding: utf-8
import datetime
import json

import pandas as pd
import requests

from vook_db_v7.config import KNOWLEDGE_ID, PLATFORM_ID, REQ_URL_CATE, query
from vook_db_v7.local_config import ClientId, aff_id
from vook_db_v7.utils import time_decorator

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

params = {
    "appid": ClientId,
    "output": "json",
    "query": query,
    "sort": "-price",
    "affiliate_id": aff_id,
    "affiliate_type": "vc",
    "results": 100,  # NOTE: 100個ずつしか取得できない。
}


@time_decorator
def DataFrame_maker_yahoo():
    start_num = 1
    step = 100
    max_products = 1000
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
                        KNOWLEDGE_ID,
                        PLATFORM_ID,
                        "",
                        # 現在の日付と時刻を取得 & フォーマットを指定して文字列に変換
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                        # 現在の日付と時刻を取得 & フォーマットを指定して文字列に変換
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    )
                )
            df = pd.DataFrame(l_hit, columns=WANT_ITEMS)
            l_df.append(df)
    return l_df


l_df = DataFrame_maker_yahoo()
products_raw = pd.concat(l_df, axis=0, ignore_index=True)
print(products_raw.shape)
print(products_raw.head())
# products_raw.to_csv(f"./data/output/products_raw.csv", index=False)
