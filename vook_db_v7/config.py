import datetime

from vook_db_v7.local_config import CLIENT_ME, ClientId, pid, sid

# Yahoo
REQ_URL_CATE = "https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch"
PLATFORM_ID = 2
BRAND = "リーバイス"
ITEM = "デニム"
LINE = "501"
KNOWLEDGE = "66前期"
KNOWLEDGE_ID = 1
COL_NAMES = [
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

aff_id = f"//ck.jp.ap.valuecommerce.com/servlet/referral?vs={sid}&vp={pid}&vc_url="
run_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

start_num = 1
step = 100
max_products = 1000

# Rakuten
REQ_URL = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20170706"
MAX_PAGE = 10  # NOTE: auto_postが300ほどしかできない。
HITS_PER_PAGE = 30


req_params = {
    "applicationId": CLIENT_ME["APPLICATION_ID"],
    "affiliateId": CLIENT_ME["AFF_ID"],
    "format": "json",
    "formatVersion": "2",
    "keyword": "",
    "hits": HITS_PER_PAGE,
    "sort": "-itemPrice",
    "page": 0,
    "minPrice": 100,
}
WANT_ITEMS = [
    "itemName",
    "itemPrice",
    "itemUrl",
]

platform_id = 1  # 楽天
size_id = 999
sleep_second = 1
