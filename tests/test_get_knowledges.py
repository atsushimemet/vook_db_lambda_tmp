import pymysql
import pytest

from vook_db_v7.local_config import SSH_PKEY_PATH, get_ec2_config
from vook_db_v7.rds_handler import get_knowledges


def test_not_get_knowledges_wrong_end_point():
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


def test_get_knowledges_right_IF_columns_name():
    right_config = get_ec2_config()
    df_from_db = get_knowledges(right_config)
    actual = df_from_db.columns.tolist()
    expected = ["knowledge_id", "knowledge_name", "brand_name", "line_name"]
    assert actual == expected


def test_get_knowledges_right_IF_columns_type():
    right_config = get_ec2_config()
    df_from_db = get_knowledges(right_config)
    actual = [v.name for v in df_from_db.dtypes.values]
    expected = ["int64", "object", "object", "object"]
    assert actual == expected
