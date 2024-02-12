import pymysql
import pytest

from vook_db_v7.local_config import SSH_PKEY_PATH, get_ec2_config
from vook_db_v7.rds_handler import get_knowledges


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
