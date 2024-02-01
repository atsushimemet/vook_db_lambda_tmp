#!/usr/bin/env python
# coding: utf-8

import numpy as np

from vook_db_v7.config import platform_id, s3_bucket, s3_file_name_products_raw_prev
from vook_db_v7.rds_handler import get_products, put_products
from vook_db_v7.tests import run_all_if_checker
from vook_db_v7.utils import (
    DataFrame_maker_yahoo,
    create_api_input,
    read_csv_from_s3,
    repeat_dataframe_maker,
    upload_s3,
)


def main(event, context):
    # APIのインプットデータ作成
    df_api_input = create_api_input()
    # df_bulkの作成
    df_bulk = repeat_dataframe_maker(df_api_input, platform_id, DataFrame_maker_yahoo)
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
