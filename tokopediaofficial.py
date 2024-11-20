import pandas as pd
import httpx
from datetime import datetime
import requests
import numpy as np
from google.cloud import bigquery
 

# Initialize Google Cloud client
client = bigquery.Client(project="astro-prd-323705")

# Load multiple tables using the provided query and logger
from query import loadmultitable
from query import logger

sidlist = ['2511885', '2288160', '2923486', '2589683', '3824444', '3823913', '12986932', '11896962', '3326823', '6016352', '5437119', '3418893', '1854168', '2589222', '208144','2832970']
sourcelist = ['lottemart-indo', 'enfagrow official store', 'nestle indonesia', 'S-26 Procal GOLD', 'sgm official store', 'nutrilon shop', 'bebelac official store', 'makuku official store', 'unicharm official store', 'merries official store', 'sweety indonesia', 'Dettol, Vanish, & Harpic indonesia', 'unilever official store', 'kao official store', 'p&g official store','rajasusu']

def scrape_data(sid, source):
    print(f'starting {source}')
    sourcefile = "scraper/tokopedia.py/" + source
    logger(sourcefile, 'start', datetime.now(), "")
    
    try:
        payload = {
            "operationName": "ShopShowcase",
            "variables": {
                "sid": sid,
                "isManageShop": False,
                "hideEmpty": True
            },
            'query': "query ShopShowcase($sid: String!, $isManageShop: Boolean, $hideEmpty: Boolean, $hideGroup: Boolean) {\n  shopShowcasesByShopID(shopId: $sid, hideNoCount: $hideEmpty, hideShowcaseGroup: $hideGroup, isOwner: $isManageShop) {\n    result {\n      id\n      title: name\n      count\n      type\n      highlighted\n      alias\n      link: uri\n      useAce\n      badge\n      __typename\n    }\n    error {\n      message\n      __typename\n    }\n    __typename\n  }\n}\n"
        }

        rsp = httpx.post("https://gql.tokopedia.com/graphql/ShopShowcase", json=payload)
        catalog = pd.DataFrame(rsp.json()['data']['shopShowcasesByShopID']['result'])

        df = pd.DataFrame()
        if 1 in catalog['type'].unique():
            cat = catalog[catalog['type'] == 1]
            for cid, ctitle in zip(cat['id'], cat['title']):
                n = 1
                payload = {
                    "operationName": "ShopProducts",
                    "variables": {
                        "sid": sid,
                        "page": n,
                        "perPage": 80,
                        "etalaseId": f"{cid}",
                        "sort": 1,
                        "user_districtId": "2274",
                        "user_cityId": "176",
                        "user_lat": "",
                        "user_long": ""
                    },
                    "query": "query ShopProducts($sid: String!, $page: Int, $perPage: Int, $keyword: String, $etalaseId: String, $sort: Int, $user_districtId: String, $user_cityId: String, $user_lat: String, $user_long: String) {\n  GetShopProduct(shopID: $sid, filter: {page: $page, perPage: $perPage, fkeyword: $keyword, fmenu: $etalaseId, sort: $sort, user_districtId: $user_districtId, user_cityId: $user_cityId, user_lat: $user_lat, user_long: $user_long}) {\n    status\n    errors\n    links {\n      prev\n      next\n      __typename\n    }\n    data {\n      name\n      product_url\n      product_id\n      price {\n        text_idr\n        __typename\n      }\n      primary_image {\n        original\n        thumbnail\n        resize300\n        __typename\n      }\n      flags {\n        isSold\n        isPreorder\n        isWholesale\n        isWishlist\n        __typename\n      }\n      campaign {\n        discounted_percentage\n        original_price_fmt\n        start_date\n        end_date\n        __typename\n      }\n      label {\n        color_hex\n        content\n        __typename\n      }\n      label_groups {\n        position\n        title\n        type\n        url\n        __typename\n      }\n      badge {\n        title\n        image_url\n        __typename\n      }\n      stats {\n        reviewCount\n        rating\n        averageRating\n        __typename\n      }\n      category {\n        id\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
                }
                resp = httpx.post("https://gql.tokopedia.com/graphql/ShopProducts", json=payload)
                tempdf = pd.DataFrame(resp.json()['data']['GetShopProduct']['data'])
                tempdf['category'] = ctitle
                df = pd.concat([df, tempdf])

                paging = True
                while paging == True:
                    if resp.json()['data']['GetShopProduct']['links']['next'] == "":
                        paging = False
                    n = n + 1
                    payload = {
                        "operationName": "ShopProducts",
                        "variables": {
                            "sid": sid,
                            "page": n,
                            "perPage": 80,
                            "etalaseId": f"{cid}",
                            "sort": 1,
                            "user_districtId": "2274",
                            "user_cityId": "176",
                            "user_lat": "",
                            "user_long": ""
                        },
                        "query": "query ShopProducts($sid: String!, $page: Int, $perPage: Int, $keyword: String, $etalaseId: String, $sort: Int, $user_districtId: String, $user_cityId: String, $user_lat: String, $user_long: String) {\n  GetShopProduct(shopID: $sid, filter: {page: $page, perPage: $perPage, fkeyword: $keyword, fmenu: $etalaseId, sort: $sort, user_districtId: $user_districtId, user_cityId: $user_cityId, user_lat: $user_lat, user_long: $user_long}) {\n    status\n    errors\n    links {\n      prev\n      next\n      __typename\n    }\n    data {\n      name\n      product_url\n      product_id\n      price {\n        text_idr\n        __typename\n      }\n      primary_image {\n        original\n        thumbnail\n        resize300\n        __typename\n      }\n      flags {\n        isSold\n        isPreorder\n        isWholesale\n        isWishlist\n        __typename\n      }\n      campaign {\n        discounted_percentage\n        original_price_fmt\n        start_date\n        end_date\n        __typename\n      }\n      label {\n        color_hex\n        content\n        __typename\n      }\n      label_groups {\n        position\n        title\n        type\n        url\n        __typename\n      }\n      badge {\n        title\n        image_url\n        __typename\n      }\n      stats {\n        reviewCount\n        rating\n        averageRating\n        __typename\n      }\n      category {\n        id\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
                    }
                    resp = httpx.post("https://gql.tokopedia.com/graphql/ShopProducts", json=payload)
                    tempdfP = pd.DataFrame(resp.json()['data']['GetShopProduct']['data'])
                    tempdfP['category'] = ctitle
                    df = pd.concat([df, tempdfP])
        else:
            n = 1
            payload = {
                "operationName": "ShopProducts",
                "variables": {
                    "sid": sid,
                    "page": n,
                    "perPage": 80,
                    "etalaseId": "etalase",
                    "sort": 1,
                    "user_districtId": "2274",
                    "user_cityId": "176",
                    "user_lat": "",
                    "user_long": ""
                },
                "query": "query ShopProducts($sid: String!, $page: Int, $perPage: Int, $keyword: String, $etalaseId: String, $sort: Int, $user_districtId: String, $user_cityId: String, $user_lat: String, $user_long: String) {\n  GetShopProduct(shopID: $sid, filter: {page: $page, perPage: $perPage, fkeyword: $keyword, fmenu: $etalaseId, sort: $sort, user_districtId: $user_districtId, user_cityId: $user_cityId, user_lat: $user_lat, user_long: $user_long}) {\n    status\n    errors\n    links {\n      prev\n      next\n      __typename\n    }\n    data {\n      name\n      product_url\n      product_id\n      price {\n        text_idr\n        __typename\n      }\n      primary_image {\n        original\n        thumbnail\n        resize300\n        __typename\n      }\n      flags {\n        isSold\n        isPreorder\n        isWholesale\n        isWishlist\n        __typename\n      }\n      campaign {\n        discounted_percentage\n        original_price_fmt\n        start_date\n        end_date\n        __typename\n      }\n      label {\n        color_hex\n        content\n        __typename\n      }\n      label_groups {\n        position\n        title\n        type\n        url\n        __typename\n      }\n      badge {\n        title\n        image_url\n        __typename\n      }\n      stats {\n        reviewCount\n        rating\n        averageRating\n        __typename\n      }\n      category {\n        id\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
            }
            resp = httpx.post("https://gql.tokopedia.com/graphql/ShopProducts", json=payload)
            tempdf = pd.DataFrame(resp.json()['data']['GetShopProduct']['data'])
            tempdf['category'] = ctitle
            df = pd.concat([df, tempdf])

            paging = True
            while paging == True:
                if resp.json()['data']['GetShopProduct']['links']['next'] == "":
                    paging = False
                n = n + 1
                payload = {
                    "operationName": "ShopProducts",
                    "variables": {
                        "sid": sid,
                        "page": n,
                        "perPage": 80,
                        "etalaseId": "etalase",
                        "sort": 1,
                        "user_districtId": "2274",
                        "user_cityId": "176",
                        "user_lat": "",
                        "user_long": ""
                    },
                    "query": "query ShopProducts($sid: String!, $page: Int, $perPage: Int, $keyword: String, $etalaseId: String, $sort: Int, $user_districtId: String, $user_cityId: String, $user_lat: String, $user_long: String) {\n  GetShopProduct(shopID: $sid, filter: {page: $page, perPage: $perPage, fkeyword: $keyword, fmenu: $etalaseId, sort: $sort, user_districtId: $user_districtId, user_cityId: $user_cityId, user_lat: $user_lat, user_long: $user_long}) {\n    status\n    errors\n    links {\n      prev\n      next\n      __typename\n    }\n    data {\n      name\n      product_url\n      product_id\n      price {\n        text_idr\n        __typename\n      }\n      primary_image {\n        original\n        thumbnail\n        resize300\n        __typename\n      }\n      flags {\n        isSold\n        isPreorder\n        isWholesale\n        isWishlist\n        __typename\n      }\n      campaign {\n        discounted_percentage\n        original_price_fmt\n        start_date\n        end_date\n        __typename\n      }\n      label {\n        color_hex\n        content\n        __typename\n      }\n      label_groups {\n        position\n        title\n        type\n        url\n        __typename\n      }\n      badge {\n        title\n        image_url\n        __typename\n      }\n      stats {\n        reviewCount\n        rating\n        averageRating\n        __typename\n      }\n      category {\n        id\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
                }
                resp = httpx.post("https://gql.tokopedia.com/graphql/ShopProducts", json=payload)
                tempdfP = pd.DataFrame(resp.json()['data']['GetShopProduct']['data'])
                tempdfP['category'] = ctitle
                df = pd.concat([df, tempdfP])

        # Data cleaning
        catlist = ['price', 'campaign', 'stats']
        for cl in catlist:
            df = pd.concat([df.drop(cl, axis=1), df[cl].apply(pd.Series)], axis=1)
        df = df.rename(columns={'text_idr': 'sale_price', 'discounted_percentage': 'discount', 'product_url': 'url', 'product_id': 'competitor_id', 'name': 'product'})

        listpr = ['original_price_fmt', 'sale_price']
        for lpr in listpr:
            df[lpr] = df[lpr].str.replace("Rp", "").str.replace(".", "").str.strip()

        df['normal_price'] = np.select([df['original_price_fmt'] == None, df['original_price_fmt'] == ""], [df['sale_price'], df['sale_price']], default=df['original_price_fmt'])
        df = df.astype(str)
        df['sold'] = df['label_groups'].str.extract(r"(\d+)(?:\+)? terjual", expand=False)
        df['sold']=df['sold'].str.replace(' terjual',"").str.replace('+',"").str.replace("rb",'000')
        df['competitor_skuid'] = df['competitor_id']
        df['promo'] = df['start_date'] + " _ " + df['end_date']
        df[['unit', 'unitPrice', 'quantity', 'samedayQuantity', 'stockType', 'astroId']] = ""
        df['out_of_stock'] = ""
        df['web'] = "tokopedia"
        df['source'] = source
        df['date'] = datetime.now()
        df['discount'] = df['discount'].fillna("0")

        dfs = df[['competitor_id', 'competitor_skuid', 'product', 'category', 'unit', 'normal_price', 'sale_price', 'unitPrice', 'promo', 'discount', 'quantity', 'samedayQuantity', 'sold', 'stockType', 'out_of_stock', 'web', 'source', 'reviewCount', 'rating', 'date', 'astroId', 'url']]
        dfs = dfs.astype(str)
        dfs[['normal_price', 'sale_price', 'discount']] = dfs[['normal_price', 'sale_price', 'discount']].astype(int)

        dfs = dfs.drop_duplicates()
        client.load_table_from_dataframe(dfs, 'astro-prd-323705.sandbox_marketing.scraped_pricing_raw')
        #----------------
        dfs['date_key'] = datetime.now().date()
        dfs['date_key'] = pd.to_datetime(dfs['date_key'], errors='coerce')
        loadmultitable(dfs)

        logger(sourcefile, 'finish', datetime.now(), str(len(dfs)))
        print(f'finish {source}')
    except Exception as e:
        logger(sourcefile, 'error', datetime.now(), str(e))

#run main program
for sid, source in zip(sidlist, sourcelist):
    scrape_data(sid,source)

 
