import pymysql
import pytest

from vook_db_v7.local_config import SSH_PKEY_PATH
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
