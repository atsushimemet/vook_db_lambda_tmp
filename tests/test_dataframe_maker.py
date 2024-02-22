import pandas as pd
from pandas.testing import assert_frame_equal

from vook_db_v7.config import WANT_ITEMS_RAKUTEN, req_params
from vook_db_v7.utils import (
    DataFrame_maker_rakuten_setup,
    DataFrame_maker_rakuten_update_params,
)


def test_dataFrame_maker_rakuten_setup():
    brand_name = "Levi's"
    line_name = "501"
    knowledge_name = "66前期"
    keyword = f"{brand_name} {line_name} {knowledge_name} 中古"
    actual_cnt, actual_df, actual_req_params = DataFrame_maker_rakuten_setup(keyword)
    expected_cnt, expected_df, expected_req_params = (
        1,
        pd.DataFrame(columns=WANT_ITEMS_RAKUTEN),
        req_params,
    )
    assert actual_cnt == expected_cnt
    assert_frame_equal(actual_df, expected_df)
    assert actual_req_params == expected_req_params


def test_dataFrame_maker_rakuten_update_params_init():
    """関数を通じてcntを代入した場合と直接代入した場合(変更前)の結果の等価性"""
    cnt = 1
    tmp = DataFrame_maker_rakuten_update_params(req_params, cnt)
    actual = tmp["page"]
    req_params["page"] = cnt
    expected = req_params["page"]
    assert actual == expected


def test_dataFrame_maker_rakuten_update_params_not_init():
    """関数を通じてcntを代入した場合と直接代入した場合(変更前)の結果の等価性"""
    cnt = 2
    tmp = DataFrame_maker_rakuten_update_params(req_params, cnt)
    actual = tmp["page"]
    req_params["page"] = cnt
    expected = req_params["page"]
    assert actual == expected


# def test_dataFrame_maker_rakuten_get_response():
#     assert 1 == 0
