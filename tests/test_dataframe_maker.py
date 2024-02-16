import pandas as pd
from pandas.testing import assert_frame_equal

from vook_db_v7.config import WANT_ITEMS_RAKUTEN, req_params
from vook_db_v7.utils import DataFrame_maker_rakuten_setup


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
