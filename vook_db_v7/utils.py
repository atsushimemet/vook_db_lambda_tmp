import datetime
import json
import re
from time import sleep

import numpy as np
import pandas as pd
import pymysql
import requests
from sshtunnel import SSHTunnelForwarder

from vook_db_v7.config import MAX_PAGE, REQ_URL, WANT_ITEMS, req_params
from vook_db_v7.local_config import get_rds_config


def DataFrame_maker(keyword, platform_id, knowledge_id, size_id):
    """apiコールした結果からdataframeを出力する関数を定義"""
    cnt = 1
    df = pd.DataFrame(columns=WANT_ITEMS)
    req_params["page"] = cnt
    req_params["keyword"] = keyword
    while True:
        req_params["page"] = cnt
        res = requests.get(REQ_URL, req_params)
        res_code = res.status_code
        res = json.loads(res.text)
        if res_code != 200:
            print(
                f"""
            ErrorCode -> {res_code}\n
            Error -> {res['error']}\n
            Page -> {cnt}"""
            )
        else:
            if res["hits"] == 0:
                print("返ってきた商品数の数が0なので、ループ終了")
                break
            tmp_df = pd.DataFrame(res["Items"])[WANT_ITEMS]
            df = pd.concat([df, tmp_df], ignore_index=True)
        if cnt == MAX_PAGE:
            print("MAX PAGEに到達したので、ループ終了")
            break
        # logger.info(f"{cnt} end!")
        cnt += 1
        # リクエスト制限回避
        sleep(1)
        print("Finished!!")

    df["platform_id"] = platform_id
    df["knowledge_id"] = knowledge_id
    df["size_id"] = size_id
    df_main = df.rename(
        columns={"itemName": "name", "itemPrice": "price", "itemUrl": "url"}
    )
    df_main = df_main.reindex(
        columns=[
            "id",
            "name",
            "url",
            "price",
            "knowledge_id",
            "platform_id",
            "size_id",
        ]
    )
    print("price type before:", df_main["price"].dtype)
    df_main["price"] = df_main["price"].astype(int)
    print("price type after:", df_main["price"].dtype)
    run_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    df_main["created_at"] = run_time
    df_main["updated_at"] = run_time
    return df_main


# エラーワードに対して対応表をもとにレスポンスする関数
def convertor(input_string, errata_table):
    # 特定のワードが DataFrame に含まれているかどうかを確認し、行番号を表示
    row_indices = errata_table.index[
        errata_table.apply(lambda row: input_string in row.values, axis=1)
    ].tolist()
    if row_indices:
        output = errata_table["corrected"][row_indices[0]]
        print(f"{input_string}を{output}に変換します")
        return output

    else:
        print(f"{input_string}は対応表に存在しません。")
        return input_string


# 対応表を読み出し
errata_table = pd.read_csv("./data/input/query_ng_ok.csv")


def validate_input(input_string):
    """
    連続する2文字以上で構成されたワードのみをOKとし、単体1文字またはスペースの前後に単体1文字が含まれるワードをNGとするバリデータ関数
    """
    print("対応表", errata_table)
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


def get_knowledges(config_ec2, query, df_from_db):
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
            return df_from_db
        except pymysql.MySQLError as e:
            print(f"Error connecting to MySQL: {e}")
        finally:
            if conn is not None:
                conn.close()


def create_wort_list(df_from_db, unit):
    # 対象のワードリスト作成
    words = df_from_db[f"{unit}_name"].values
    for row in np.arange(len(words)):
        word = words[row]
        words[row] = validate_input(word)
    return words


def create_df_no_ng_keyword(
    df_from_db, words_knowledge_name, words_brand_name, words_line_name
):
    df_no_ng_keyword = pd.DataFrame(columns=df_from_db.columns)
    df_no_ng_keyword["knowledge_id"] = df_from_db["knowledge_id"].values
    df_no_ng_keyword["knowledge_name"] = words_knowledge_name
    df_no_ng_keyword["brand_name"] = words_brand_name
    df_no_ng_keyword["line_name"] = words_line_name
    return df_no_ng_keyword
