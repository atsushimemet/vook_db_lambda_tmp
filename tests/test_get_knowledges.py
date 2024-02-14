import pandas as pd
import pymysql
import pytest

from vook_db_v7.local_config import SSH_PKEY_PATH, get_ec2_config
from vook_db_v7.rds_handler import get_knowledges
from vook_db_v7.utils import convertor, create_wort_list, validate_input


class TestGetKnowledgesInvalid:
    def test_not_get_knowledges_wrong_end_point(self):
        wrong_config = {
            "host_name": "3.114.154.242",
            "ec2_port": 22,
            "ssh_username": "ec2-user",
            "ssh_pkey": SSH_PKEY_PATH,
            "rds_end_point": "wrong_end_point",
            "rds_port": 3306,
        }

        msg = "Lost connection to MySQL server during query"
        with pytest.raises(pymysql.MySQLError, match=rf".*{msg}.*"):
            get_knowledges(wrong_config)


@pytest.fixture(scope="class")
def df_knowledge_brand_line(request):
    right_config = get_ec2_config()
    df = get_knowledges(right_config)
    request.cls.df = df  # クラス変数としてデータフレームを設定


@pytest.mark.usefixtures("df_knowledge_brand_line")
class TestGetKnowledgesValid:
    def test_columns_name(self):
        actual = self.df.columns.tolist()
        expected = ["knowledge_id", "knowledge_name", "brand_name", "line_name"]
        assert actual == expected

    def test_columns_type(self):
        actual = [dtype.name for dtype in self.df.dtypes.values]
        expected = ["int64", "object", "object", "object"]
        assert actual == expected

    def test_columns_notnull(self):
        actual = all([not boolian for boolian in self.df.isnull().any()])
        # NOTE:全カラム一つでも欠損あるか-No-False-全部FalseでOK
        expected = True
        assert actual == expected

    def test_columns_pk_unique(self):
        actual = self.df["knowledge_id"].nunique()
        expected = self.df.shape[0]
        assert actual == expected

    def test_len_df(self):
        assert len(self.df)


class TestValidateInput:
    def test_valid(self):
        word = "66前期"
        actual = validate_input(word)
        expected = word
        assert actual == expected

    def test_invalid(self):
        word = "BIG E"
        actual = validate_input(word)
        expected = "BIGE"
        assert actual == expected


class TestConvertorValid:
    def test_converted(self):
        word_bfr = "BIG E"
        word_aft = "BIGE"
        ng_ok_table = pd.read_csv("./data/input/query_ng_ok.csv")
        actual = convertor(word_bfr, ng_ok_table)
        expected = word_aft
        assert actual == expected

    def test_not_converted(self):
        word_bfr = "66前期"
        word_aft = "66前期"
        ng_ok_table = pd.read_csv("./data/input/query_ng_ok.csv")
        actual = convertor(word_bfr, ng_ok_table)
        expected = word_aft
        assert actual == expected


@pytest.fixture(scope="class")
def df_no_ng_keyword(request):
    config_ec2 = get_ec2_config()
    df_from_db = get_knowledges(config_ec2)
    # 対象のワードリスト作成
    words_brand_name = create_wort_list(df_from_db, "brand")
    words_line_name = create_wort_list(df_from_db, "line")
    words_knowledge_name = create_wort_list(df_from_db, "knowledge")
    df_no_ng_keyword = pd.DataFrame(columns=df_from_db.columns)
    df_no_ng_keyword["knowledge_id"] = df_from_db["knowledge_id"].values
    df_no_ng_keyword["knowledge_name"] = words_knowledge_name
    df_no_ng_keyword["brand_name"] = words_brand_name
    df_no_ng_keyword["line_name"] = words_line_name
    request.cls.df = df_no_ng_keyword


@pytest.mark.usefixtures("df_no_ng_keyword")
class TestDataframeNoNgKeyword:
    def test_columns_name(self):
        actual = self.df.columns.tolist()
        expected = ["knowledge_id", "knowledge_name", "brand_name", "line_name"]
        assert actual == expected

    def test_columns_type(self):
        actual = [dtype.name for dtype in self.df.dtypes.values]
        expected = ["int64", "object", "object", "object"]
        assert actual == expected

    def test_columns_notnull(self):
        actual = all([not boolian for boolian in self.df.isnull().any()])
        # NOTE:全カラム一つでも欠損あるか-No-False-全部FalseでOK
        expected = True
        assert actual == expected

    def test_columns_pk_unique(self):
        actual = self.df["knowledge_id"].nunique()
        expected = self.df.shape[0]
        assert actual == expected

    def test_len_df(self):
        assert len(self.df)
