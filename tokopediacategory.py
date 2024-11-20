import httpx
import pandas as pd
from google.cloud import bigquery
clnt = bigquery.Client(project="astro-data-prd")

#getallcategories
url="https://gql.tokopedia.com/graphql/categoryAllList"
payload={
    "operationName":"categoryAllList",
      "variables":{},
      "query":"query categoryAllList($categoryID: Int, $type: String) {\n  CategoryAllList: categoryAllList(categoryID: $categoryID, type: $type) {\n    categories {\n      identifier\n      name\n      id\n      child {\n        id\n        template\n        name\n        url\n        iconImageUrl\n        child {\n          name\n          url\n          id\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
}
resp=httpx.post(url,json=payload)
df=pd.DataFrame(resp.json()['data']['CategoryAllList']['categories'])

#empty dataframe
tf=pd.DataFrame() #child upper
dfchildmaster=pd.DataFrame() #child of child upper
dfitems=pd.DataFrame()

#get child from get all categories
for chl,catutama in zip(df['child'],df['identifier']):
  tempdf=pd.DataFrame(chl)
  tempdf['main_category']=catutama
  tf=pd.concat([tf,tempdf])
  try:
    for ch,maincat,subcat in zip(tempdf['child'],tempdf['main_category'],tempdf['name']):
      tempdfchild=pd.DataFrame(ch)
      tempdfchild['main_category']=maincat
      tempdfchild['sub_category']=subcat
      dfchildmaster=pd.concat([dfchildmaster,tempdfchild])
  except:pass

if not dfchildmaster.empty:
    #get merek all child from child upper 
    for id,childname,urlchild,maincat,subcat in zip(dfchildmaster['id'],dfchildmaster['name'],dfchildmaster['url'],dfchildmaster['main_category'],dfchildmaster['sub_category']): #--------------------------------------change back
 
        url='https://gql.tokopedia.com/graphql/DynamicAttributes'
        payload={"operationName":"DynamicAttributes",
            "variables":{"source":"directory",
                            "q":"",
                            "filter":{"sc":f"{id}"}},
            "query":"query DynamicAttributes($source: String, $q: String, $filter: DAFilterQueryType) {\n  dynamicAttribute(source: $source, q: $q, filter: $filter) {\n    data {\n      filter {\n        title\n        search {\n          searchable\n          placeholder\n          __typename\n        }\n        options {\n          name\n          key\n          icon\n          value\n          inputType\n          totalData\n          valMax\n          valMin\n          hexColor\n          child {\n            key\n            value\n            name\n            icon\n            inputType\n            totalData\n            child {\n              key\n              value\n              name\n              icon\n              inputType\n              totalData\n              child {\n                key\n                value\n                name\n                icon\n                inputType\n                totalData\n                __typename\n              }\n              __typename\n            }\n            __typename\n          }\n          isPopular\n          __typename\n        }\n        __typename\n      }\n      sort {\n        name\n        key\n        value\n        inputType\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"
            }
        resp=httpx.post(url,json=payload)

        identifier=urlchild.split(".com/p/")[-1].replace("/","_")

        try:
            dfmerek=pd.DataFrame(resp.json()['data']['dynamicAttribute']['data']['filter'])
            dfm=dfmerek[dfmerek['title']=="Merek"]
            if not dfm.empty:
                merek=pd.DataFrame(dfm['options'].item())

                for namamerek, keymerek,valuemerek in zip(merek['name'],merek['key'],merek['value']): 
                    urlspq="https://gql.tokopedia.com/graphql/SearchProductQuery"
                    payloadspq={
                            "operationName":"SearchProductQuery",
                            "variables":
                                {"params":f"page=1&anno_id_merek={valuemerek}&ob=5&identifier={identifier}&sc={id}&user_id=0&rows=60&start=1&source=directory&device=desktop&page=1&related=true&st=product&safe_search=false",
                                        "adParams":f"page=1&anno_id_merek={valuemerek}&page=1&dep_id={id}&ob=5&ep=product&item=15&src=directory&device=desktop&user_id=0&minimum_item=15&start=1&no_autofill_range=5-14"},
                                        "query":"query SearchProductQuery($params: String, $adParams: String) {\n  CategoryProducts: searchProduct(params: $params) {\n    count\n    data: products {\n      id\n      url\n      imageUrl: image_url\n      imageUrlLarge: image_url_700\n      catId: category_id\n      gaKey: ga_key\n      countReview: count_review\n      discountPercentage: discount_percentage\n      preorder: is_preorder\n      name\n      price\n      priceInt: price_int\n      original_price\n      rating\n      wishlist\n      labels {\n        title\n        color\n        __typename\n      }\n      badges {\n        imageUrl: image_url\n        show\n        __typename\n      }\n      shop {\n        id\n        url\n        name\n        goldmerchant: is_power_badge\n        official: is_official\n        reputation\n        clover\n        location\n        __typename\n      }\n      labelGroups: label_groups {\n        position\n        title\n        type\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  displayAdsV3(displayParams: $adParams) {\n    data {\n      id\n      ad_ref_key\n      redirect\n      sticker_id\n      sticker_image\n      productWishListUrl: product_wishlist_url\n      clickTrackUrl: product_click_url\n      shop_click_url\n      product {\n        id\n        name\n        wishlist\n        image {\n          imageUrl: s_ecs\n          trackerImageUrl: s_url\n          __typename\n        }\n        url: uri\n        relative_uri\n        price: price_format\n        campaign {\n          original_price\n          discountPercentage: discount_percentage\n          __typename\n        }\n        wholeSalePrice: wholesale_price {\n          quantityMin: quantity_min_format\n          quantityMax: quantity_max_format\n          price: price_format\n          __typename\n        }\n        count_talk_format\n        countReview: count_review_format\n        category {\n          id\n          __typename\n        }\n        preorder: product_preorder\n        product_wholesale\n        free_return\n        isNewProduct: product_new_label\n        cashback: product_cashback_rate\n        rating: product_rating\n        top_label\n        bottomLabel: bottom_label\n        __typename\n      }\n      shop {\n        image_product {\n          image_url\n          __typename\n        }\n        id\n        name\n        domain\n        location\n        city\n        tagline\n        goldmerchant: gold_shop\n        gold_shop_badge\n        official: shop_is_official\n        lucky_shop\n        uri\n        owner_id\n        is_owner\n        badges {\n          title\n          image_url\n          show\n          __typename\n        }\n        __typename\n      }\n      applinks\n      __typename\n    }\n    template {\n      isAd: is_ad\n      __typename\n    }\n    __typename\n  }\n}\n"}
                    responseitems=httpx.post(urlspq,json=payloadspq)
                    tempdfitems=pd.DataFrame(responseitems.json()['data']['CategoryProducts']['data'])
                    tempdfitems['main_category']=maincat
                    tempdfitems['sub_category']=subcat
                    tempdfitems['child_category']=childname
                    tempdfitems['merek']=namamerek
                    tempdfitems=tempdfitems.astype(str)
                    tempdfitems[['countReview','discountPercentage','priceInt']]=tempdfitems[['countReview','discountPercentage','priceInt']].astype(int)
                    dfitems=pd.concat([dfitems,tempdfitems])
            else:pass
        except:pass

    dfitems['date_created']=pd.Timestamp.now()
    # Define the time and categorical partitioning columns
    time_partitioning_field = 'date_created'

    # Specify partitioning by ingestion time (optional)
    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=time_partitioning_field
    )

    # Load the data into the table with partitioning and clustering
    job_config = bigquery.LoadJobConfig(
        time_partitioning=time_partitioning,
    )

    clnt.load_table_from_dataframe(dfitems,'astro-data-prd.astro_raw_data.beyond_groceries_data_tokopedia', job_config=job_config)