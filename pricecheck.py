import requests 
import numpy as np
import re
import pandas as pd
import urllib.parse
from datetime import date, timedelta,datetime
from google.cloud import bigquery
from time import sleep
from random import randint
client = bigquery.Client(project="astro-data-prd")
import time
import httpx
import random

part1=0
part2=0
#------------------------
def logger(file,status,time,desc):
    # Specify time partitioning by date_key column
  time_partitioning = bigquery.TimePartitioning(
      type_=bigquery.TimePartitioningType.DAY,
      field='date_key'
  )
  # Load the data into the table with time partitioning
  job_config = bigquery.LoadJobConfig(time_partitioning=time_partitioning)
  
  logf={
    'file':file,
    'status':status,
    'date_key':time,
    'desc':desc
  }
  logf=pd.DataFrame([logf])
  client.load_table_from_dataframe(logf,'astro-data-prd.astro_raw_data.scraped_log',job_config=job_config)

#-----------------------
loggerfile='price_checker/pricecheck.py'
#--------------------------start
logger(loggerfile,'start',datetime.now(),"") 
try:
  #----------------load items to scrape
  items=client.query(
      '''
      SELECT  
        product_id, trim(regexp_replace(lower(product_name),r'[^a-zA-Z0-9 ]',"")) product_name
      FROM 
        `astro-data-prd.astro_dataset.dim_products_x_categories_x_attributes`
      WHERE 
        product_id in(SELECT distinct product_id FROM `astro-data-prd.astro_dataset.fact_monthly_pareto_astro_level` WHERE month_year = date_trunc(current_date(),month))
        AND source_status="SOURCE"
        AND not regexp_contains(lower(product_name),'astro|1nterstellar|test')
        AND not regexp_contains(lower(product_name),r'\btest\b')
        AND l1_category_name not in ('Special Deals [OFF]','Official Merchandise','[OFF} Kesehatan','Astro Kitchen ( Raw Material )','Internal Warehouse','Diskon s.d 40%','Tiket','Voucher Langganan')
      '''
  ).to_dataframe()

  #-----------------------------------------------main function:scraper

  url='https://gql.tokopedia.com/graphql/SearchProductQueryV4'
  import warnings
  warnings.filterwarnings("ignore", category=FutureWarning)
  df=pd.DataFrame()
  for pid,keyword in zip(items['product_id'],items['product_name']):
      try:
        payload={"operationName":"SearchProductQueryV4",
              "variables":{"params":f"device=desktop&navsource=&ob=23&page=1&q={keyword}&related=true&rows=60&safe_search=false&scheme=https&shipping=&show_adult=false&source=search&srp_component_id=02.01.00.00&srp_page_id=&srp_page_title=&st=product&start=0&topads_bucket=true&unique_id=&user_addressId=&user_cityId=174&user_districtId=2256&user_id=&user_lat=&user_long=&user_postCode=11520&user_warehouseId=12210375&variants=&warehouses=12210375%232h%2C0%2315m"},
              "query":"query SearchProductQueryV4($params: String!) {\n  ace_search_product_v4(params: $params) {\n    header {\n      totalData\n      totalDataText\n      processTime\n      responseCode\n      errorMessage\n      additionalParams\n      keywordProcess\n      componentId\n      __typename\n    }\n    data {\n      banner {\n        position\n        text\n        imageUrl\n        url\n        componentId\n        trackingOption\n        __typename\n      }\n      backendFilters\n      isQuerySafe\n      ticker {\n        text\n        query\n        typeId\n        componentId\n        trackingOption\n        __typename\n      }\n      redirection {\n        redirectUrl\n        departmentId\n        __typename\n      }\n      related {\n        position\n        trackingOption\n        relatedKeyword\n        otherRelated {\n          keyword\n          url\n          product {\n            id\n            name\n            price\n            imageUrl\n            rating\n            countReview\n            url\n            priceStr\n            wishlist\n            shop {\n              city\n              isOfficial\n              isPowerBadge\n              __typename\n            }\n            ads {\n              adsId: id\n              productClickUrl\n              productWishlistUrl\n              shopClickUrl\n              productViewUrl\n              __typename\n            }\n            badges {\n              title\n              imageUrl\n              show\n              __typename\n            }\n            ratingAverage\n            labelGroups {\n              position\n              type\n              title\n              url\n              __typename\n            }\n            componentId\n            __typename\n          }\n          componentId\n          __typename\n        }\n        __typename\n      }\n      suggestion {\n        currentKeyword\n        suggestion\n        suggestionCount\n        instead\n        insteadCount\n        query\n        text\n        componentId\n        trackingOption\n        __typename\n      }\n      products {\n        id\n        name\n        ads {\n          adsId: id\n          productClickUrl\n          productWishlistUrl\n          productViewUrl\n          __typename\n        }\n        badges {\n          title\n          imageUrl\n          show\n          __typename\n        }\n        category: departmentId\n        categoryBreadcrumb\n        categoryId\n        categoryName\n        countReview\n        customVideoURL\n        discountPercentage\n        gaKey\n        imageUrl\n        labelGroups {\n          position\n          title\n          type\n          url\n          __typename\n        }\n        originalPrice\n        price\n        priceRange\n        rating\n        ratingAverage\n        shop {\n          shopId: id\n          name\n          url\n          city\n          isOfficial\n          isPowerBadge\n          __typename\n        }\n        url\n        wishlist\n        sourceEngine: source_engine\n        __typename\n      }\n      violation {\n        headerText\n        descriptionText\n        imageURL\n        ctaURL\n        ctaApplink\n        buttonText\n        buttonType\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"}
        resp=requests.post(url, headers = {'User-Agent':'Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36'},json=payload)
        tempdf=pd.DataFrame(resp.json()['data']['ace_search_product_v4']['data']['products'])
        if len(tempdf)!=0:
          tempdf['searchtype']="found"
        else:
          len(keyword)
          tempdfRel=pd.DataFrame(resp.json()['data']['ace_search_product_v4']['data']['related']['otherRelated'])
          tempdf=pd.DataFrame()
          for titems,kywrd in zip(tempdfRel['product'],tempdfRel['keyword']):
            foundjson=pd.DataFrame(titems)
            foundjson[['ads','category','categoryBreadcrumb','categoryId','categoryName','customVideoURL','discountPercentage','gaKey','imageUrl','originalPrice','priceRange','rating','wishlist','sourceEngine']]=""
            tdf=foundjson[['id', 'name', 'ads', 'badges', 'category', 'categoryBreadcrumb',
                          'categoryId', 'categoryName', 'countReview', 'customVideoURL',
                          'discountPercentage', 'gaKey', 'imageUrl', 'labelGroups',
                          'originalPrice', 'price', 'priceRange', 'rating', 'ratingAverage',
                          'shop', 'url', 'wishlist', 'sourceEngine', '__typename']]
            tdf['searchtype']="not found using new keyword: "+kywrd
            tempdf=pd.concat([tempdf,tdf])
        tempdf['keyword_astro']=keyword
        tempdf['product_id_astro']=pid
        df=pd.concat([df,tempdf])
      except Exception as e:
        pass
        # time.sleep(random.randint(600, 630))
        # print(keyword,e)

  #load to bq
  df=df.astype(str)
  df['date_scraped']=datetime.now()
  df['date_scraped'] = pd.to_datetime(df['date_scraped'], errors='coerce')

  time_partitioning_field = 'date_scraped'

  # Specify partitioning by ingestion time (optional)
  time_partitioning = bigquery.TimePartitioning(
      type_=bigquery.TimePartitioningType.DAY,
      field=time_partitioning_field
  )

  # Load the data into the table with partitioning and clustering
  job_config = bigquery.LoadJobConfig(
      time_partitioning=time_partitioning,
  )

  client.load_table_from_dataframe(df,'astro-data-prd.astro_price_scraping.tokopedia_price_checking',job_config=job_config )
  time.sleep(60)
  #---------------------
  logger(loggerfile,'finish',datetime.now(),str(len(df)))
  part1=1
except Exception as e:
  #----------------------error
  logger(loggerfile,'error',datetime.now(),str(e))

if part1==1:
  #-----------------------------------------------logger part 2
  #-----------------------
  loggerfile='price_checker/pricecheck/sql_query.py'
  #--------------------------start
  logger(loggerfile,'start',datetime.now(),"") 

  #-----------------query to create table for further query
  query_job=client.query(
    '''
      CREATE TEMPORARY FUNCTION match(str1 STRING, str2 STRING)
        RETURNS FLOAT64
        LANGUAGE js AS \"""
          var stopWords = /\\\\b(g|kg|ml|L|gr|astro|goods|daun|murah|promo|diskon|termurah)\\\\b/ig
          var stopChars = /[-_ \\\\.,'"'/()]/g
          var tokens = /\\\\b(segari|sayurbox|happyfresh|alfamart|indomaret|astro)\\\\b/i
          var grams = 3
          var tokenValue = 2
          var substrs1 = extract(str1)
          var substrs2 = extract(str2)
          var maxL = max(substrs1.length, substrs2.length)
          return (parseInt(100 * (
            substrs1.filter(s => ~substrs2.indexOf(s)).length +
            substrs2.filter(s => ~substrs1.indexOf(s)).length
          ) / (0.01 + maxL) / 2))

          function max(a, b) {
            return a > b ? a : b
          }

          function extract(str) {
            return str.toLowerCase()
              .replace(stopWords, "")
              .replace(tokens, m => m.slice(0, tokenValue).toUpperCase())
              .replace(stopChars, "")
              .split('').map((char, c, arr) => arr.slice(max(0, 1 + c - grams), 1 + c).join('')).filter(s => s.length >= 3)
          }
        \""";
      Create or replace table `astro-data-prd.astro_price_scraping.fact_cogs_competitor_checking` as
      WITH 
      cogs_ as 
      (
        SELECT  
          distinct
          product_id, cogs, 
        FROM 
          `astro-data-prd.astro_dataset.fact_cogs_per_product_per_date` 
        WHERE date_key = current_date()
      ),
      date_dict as 
      (
        select max(date(date_scraped))date_scraped from  `astro-data-prd.astro_price_scraping.tokopedia_price_checking` 
      ),
      PARETO as 
      (
      SELECT 
        product_id
        ,pareto_tag
      FROM 
        `astro-data-prd.astro_dataset.fact_monthly_pareto_astro_level` 
      WHERE 
        month_year = date_trunc(current_date(),month)
      ),
      MATCHPRICES as 
      (
        select 
        name,keyword_astro,product_id_astro,cast(replace(replace(price,"Rp",""),".","") as int) price,cogs,url ,JSON_EXTRACT(shop, '$.city') city
        FROM 
          `astro-data-prd.astro_price_scraping.tokopedia_price_checking` 
        LEFT JOIN 
          cogs_ on product_id=cast(product_id_astro as int)
        where
          date(date_scraped) in (select date_scraped from date_dict) 
          AND cast(cogs as int)>cast(replace(replace(price,"Rp",""),".","") as int)
          AND JSON_EXTRACT(shop, '$.city') in ('"Jakarta Selatan"','"Bogor"','"Kab. Tangerang"','"Jakarta Pusat"','"Jakarta Barat"','"Jakarta Timur"','"Kab. Bekasi"','"Tangerang"','"Jakarta Utara"','"Tangerang Selatan"','"Depok"','"Bekasi"','"Kab. Bogor"','""')
      ),
      UNITSextract as 
      (
        SELECT  
          name comp_name,
          regexp_replace(regexp_replace(lower(name), r'[^a-zA-Z ]', '') 
                        , r'\\\\s+', ' ') comp_name_clean,
          regexp_replace(regexp_replace(lower(keyword_astro), r'[^a-zA-Z ]', '') 
                        , r'\\\\s+', ' ') astro_name_clean,   
          keyword_astro,
          price,cogs,
          product_id_astro,
          url,
          city
        FROM MATCHPRICES
      
      ),
      SIMILARITY_SCORING as 
      (
      SELECT
        lower(comp_name)comp_name,
        keyword_astro, 
        match(comp_name_clean,astro_name_clean)similarity,
        price comp_price,
        cogs,
        cast(product_id_astro as int) product_id,
        url,
        city
      FROM 
        UNITSextract
      ),
      FINALSIMILARITY as 
      (
      SELECT 
        *
      FROM 
        similarity_scoring
      WHERE 
        similarity>30   #---------------------------------------------> similarity scoring
      ),
      unitsANDKeyword as 
      (
        SELECT 
        *
        ,CASE
            -- Condition 1: Number x Number with/without String
            WHEN REGEXP_CONTAINS(isi, r'(\\d+)x\\s*\\d+') THEN REGEXP_EXTRACT(isi, r'(\\d+)x\\s*\\d+')
            WHEN REGEXP_CONTAINS(isi, r'\\d+x\\s*(\\d+)') THEN REGEXP_EXTRACT(isi, r'\\d+x\\s*(\\d+)')
            -- Condition 2: Number with 'pc' or 'pcs'
            WHEN REGEXP_CONTAINS(isi, r'(\\d+)(pc|pcs|bks|sachet)') THEN REGEXP_EXTRACT(isi, r'(\\d+)pc|pcs|bks|sachet')
            -- Condition 3: Number with ml, l, litre, gram, kilogram, g, kg (return null)
            WHEN REGEXP_CONTAINS(isi, r'\\d+(ml|l|litre|gram|kilogram|g|kg)') THEN "1"
            -- Condition 4: Sum of Numbers in the Format Number + Number
            WHEN REGEXP_CONTAINS(isi, r'(\\d+)\\+(\\d+)') THEN
              CAST(CAST(REGEXP_EXTRACT(isi, r'(\\d+)\\+') AS INT64) + CAST(REGEXP_EXTRACT(isi, r'\\+(\\d+)') AS INT64)as STRING)
            WHEN REGEXP_CONTAINS(isi, r'^\\d+$') THEN isi
            ELSE "1"
        END AS isiDus 
        FROM 
        (
        SELECT 
          *,
      
        REGEXP_EXTRACT(
                      LOWER(keyword_astro), 
                      r'\\b(\\d+(?:[.,]\\d+)?)\\s?(?:gram|g|ml|litre|liter|l|kilogram|kg|gr|mg|lembar)\\b'
                      ) astro_unit,
        
        REGEXP_EXTRACT(
                      LOWER(comp_name), 
                      r'\\b(\\d+(?:[.,]\\d+)?)\\s?(?:gram|g|ml|litre|liter|l|kilogram|kg|gr|mg|lembar)\\b'
                      ) comp_unit,
        REGEXP_REPLACE(
                        REGEXP_EXTRACT(
                                    LOWER(comp_name),
                                    r'\\bisi\\b\\s*([^\\s]+)'
                                    ), r'[^a-zA-Z0-9]', ' '
                      )isi
        FROM 
          (
          SELECT 
            finalsimilarity.*,
            case 
              when regexp_contains(lower(keyword_astro),lower(brand_name)) then lower(brand_name)
              when length(SPLIT(keyword_astro, ' ')[OFFSET(0)])<3 then SPLIT(keyword_astro, ' ')[OFFSET(1)]
              when length(SPLIT(keyword_astro, ' ')[OFFSET(0)])>=3 then SPLIT(keyword_astro, ' ')[OFFSET(0)] 
            end brand_name 
          FROM 
            finalsimilarity
          LEFT JOIN
          `astro-data-prd.astro_dataset.dim_products_x_categories_x_attributes`
          using(product_id)
          )
        where 
          regexp_contains(lower(comp_name),lower(brand_name))
      )    
      )

      #--------------final
      SELECT *except(unitfilter,pareto_tag,isi) FROM 
      (
      SELECT 
        *except(astro_unit,comp_unit)
        ,case 
          when astro_unit=comp_unit  then 'units matched'
        end matchUnits  
        ,case 
          when 
            astro_unit is not null and comp_unit is not null and astro_unit=comp_unit then 'ada'
          when 
            astro_unit is null or comp_unit is null then 'ada'
        end unitfilter,
        (cogs-(comp_price/cast(ifnull(isiDus,'1')as int)))/cogs perc_cheaper
        , pareto_tag pareto_all_astro
        ,(select distinct date_scraped from date_dict) date_scraped
      FROM
        unitsANDKeyword
      LEFT JOIN 
        (select product_id,pareto_tag from `astro-data-prd.astro_dataset.fact_monthly_pareto_astro_level` where month_year=date_trunc(current_date(),month))
        using(product_id)
      )
      WHERE matchUnits is not null

        '''
  )
  query_job.result() 
  if query_job.state == 'DONE':
    time.sleep(30)
  #-----------
  logger(loggerfile,'finish',datetime.now(),'')

#-----------------scrape by stock type
  #-----------------------
  loggerfile='price_checker/pricecheck/scrapebystocktype.py'
  #--------------------------start
  logger(loggerfile,'start',datetime.now(),"") 
  topeditems=client.query(
    '''
    select
        * 
    from 
        `astro-data-prd.astro_price_scraping.fact_cogs_competitor_checking`
    where perc_cheaper>0.10 and similarity>50
    '''
  ).to_dataframe()
  #scrape part 3
  dfFinal=pd.DataFrame()
  urlutama="https://gql.tokopedia.com/graphql/PDPGetLayoutQuery"
  for prodid, url,astro,city,pareto,cogs in zip(topeditems['product_id'],topeditems['url'],topeditems['keyword_astro'],topeditems['city'],topeditems['pareto_all_astro'],topeditems['cogs']):
    try:
      extParam=url.split('/')[-1].split('?')[1]
      productkey=url.split('/')[-1].split('?')[0]
      shopdom=url.split('/')[-2]
      headers={'Accept':'*/*',
              'Content-Type':'application/json',
              'Referer':url,
              'Sec-Ch-Ua':'"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
              'Sec-Ch-Ua-Mobile':'?0',
              'Sec-Ch-Ua-Platform':'"Windows"',
              'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
              'X-Device':'desktop',
              'X-Source':'tokopedia-lite',
              'X-Tkpd-Akamai':'pdpGetLayout',
              'X-Tkpd-Lite-Service':'zeus'
      }
    
      payload={"operationName":"PDPGetLayoutQuery",
            "variables":
              {"shopDomain":shopdom,
              "productKey":productkey,
              "layoutID":"",
              "apiVersion":1,
              "tokonow":{"shopID":"11530573","whID":"12210375","serviceType":"2h"},
              "userLocation":{"cityID":"176","addressID":"0","districtID":"2274","postalCode":"","latlon":f""},
              "extParam":extParam},
              "query":"fragment ProductVariant on pdpDataProductVariant {\n errorCode\n parentID\n defaultChild\n sizeChart\n totalStockFmt\n variants {\n productVariantID\n variantID\n name\n identifier\n option {\n picture {\n urlOriginal: url\n urlThumbnail: url100\n __typename\n }\n productVariantOptionID\n variantUnitValueID\n value\n hex\n stock\n __typename\n }\n __typename\n }\n children {\n productID\n price\n priceFmt\n slashPriceFmt\n discPercentage\n optionID\n optionName\n productName\n productURL\n picture {\n urlOriginal: url\n urlThumbnail: url100\n __typename\n }\n stock {\n stock\n isBuyable\n stockWordingHTML\n minimumOrder\n maximumOrder\n __typename\n }\n isCOD\n isWishlist\n campaignInfo {\n campaignID\n campaignType\n campaignTypeName\n campaignIdentifier\n background\n discountPercentage\n originalPrice\n discountPrice\n stock\n stockSoldPercentage\n startDate\n endDate\n endDateUnix\n appLinks\n isAppsOnly\n isActive\n hideGimmick\n isCheckImei\n minOrder\n __typename\n }\n thematicCampaign {\n additionalInfo\n background\n campaignName\n icon\n __typename\n }\n __typename\n }\n __typename\n}\n\nfragment ProductMedia on pdpDataProductMedia {\n media {\n type\n urlOriginal: URLOriginal\n urlThumbnail: URLThumbnail\n urlMaxRes: URLMaxRes\n videoUrl: videoURLAndroid\n prefix\n suffix\n description\n variantOptionID\n __typename\n }\n videos {\n source\n url\n __typename\n }\n __typename\n}\n\nfragment ProductCategoryCarousel on pdpDataCategoryCarousel {\n linkText\n titleCarousel\n applink\n list {\n categoryID\n icon\n title\n isApplink\n applink\n __typename\n }\n __typename\n}\n\nfragment ProductHighlight on pdpDataProductContent {\n name\n price {\n value\n currency\n priceFmt\n slashPriceFmt\n discPercentage\n __typename\n }\n campaign {\n campaignID\n campaignType\n campaignTypeName\n campaignIdentifier\n background\n percentageAmount\n originalPrice\n discountedPrice\n originalStock\n stock\n stockSoldPercentage\n threshold\n startDate\n endDate\n endDateUnix\n appLinks\n isAppsOnly\n isActive\n hideGimmick\n __typename\n }\n thematicCampaign {\n additionalInfo\n background\n campaignName\n icon\n __typename\n }\n stock {\n useStock\n value\n stockWording\n __typename\n }\n variant {\n isVariant\n parentID\n __typename\n }\n wholesale {\n minQty\n price {\n value\n currency\n __typename\n }\n __typename\n }\n isCashback {\n percentage\n __typename\n }\n isTradeIn\n isOS\n isPowerMerchant\n isWishlist\n isCOD\n preorder {\n duration\n timeUnit\n isActive\n preorderInDays\n __typename\n }\n __typename\n}\n\nfragment ProductCustomInfo on pdpDataCustomInfo {\n icon\n title\n isApplink\n applink\n separator\n description\n __typename\n}\n\nfragment ProductInfo on pdpDataProductInfo {\n row\n content {\n title\n subtitle\n applink\n __typename\n }\n __typename\n}\n\nfragment ProductDetail on pdpDataProductDetail {\n content {\n title\n subtitle\n applink\n showAtFront\n isAnnotation\n __typename\n }\n __typename\n}\n\nfragment ProductDataInfo on pdpDataInfo {\n icon\n title\n isApplink\n applink\n content {\n icon\n text\n __typename\n }\n __typename\n}\n\nfragment ProductSocial on pdpDataSocialProof {\n row\n content {\n icon\n title\n subtitle\n applink\n type\n rating\n __typename\n }\n __typename\n}\n\nfragment ProductDetailMediaComponent on pdpDataProductDetailMediaComponent {\n title\n description\n contentMedia {\n url\n ratio\n type\n __typename\n }\n show\n ctaText\n __typename\n}\n\nquery PDPGetLayoutQuery($shopDomain: String, $productKey: String, $layoutID: String, $apiVersion: Float, $userLocation: pdpUserLocation, $extParam: String, $tokonow: pdpTokoNow) {\n pdpGetLayout(shopDomain: $shopDomain, productKey: $productKey, layoutID: $layoutID, apiVersion: $apiVersion, userLocation: $userLocation, extParam: $extParam, tokonow: $tokonow) {\n requestID\n name\n pdpSession\n basicInfo {\n alias\n createdAt\n isQA\n id: productID\n shopID\n shopName\n minOrder\n maxOrder\n weight\n weightUnit\n condition\n status\n url\n needPrescription\n catalogID\n isLeasing\n isBlacklisted\n isTokoNow\n menu {\n id\n name\n url\n __typename\n }\n category {\n id\n name\n title\n breadcrumbURL\n isAdult\n isKyc\n minAge\n detail {\n id\n name\n breadcrumbURL\n isAdult\n __typename\n }\n __typename\n }\n txStats {\n transactionSuccess\n transactionReject\n countSold\n paymentVerified\n itemSoldFmt\n __typename\n }\n stats {\n countView\n countReview\n countTalk\n rating\n __typename\n }\n __typename\n }\n components {\n name\n type\n position\n data {\n ...ProductMedia\n ...ProductHighlight\n ...ProductInfo\n ...ProductDetail\n ...ProductSocial\n ...ProductDataInfo\n ...ProductCustomInfo\n ...ProductVariant\n ...ProductCategoryCarousel\n ...ProductDetailMediaComponent\n __typename\n }\n __typename\n }\n __typename\n }\n}\n"}
      #get response
      resp=httpx.post(urlutama,headers=headers,json=payload)
  
      checker=0
      for comp in resp.json()['data']['pdpGetLayout']['components']:
        tempdf=pd.DataFrame()
        if comp['name']=='new_variant_options':
          items=pd.DataFrame(comp['data'])
          if len(pd.DataFrame(items['children'].item()))>1:
              tempdf=pd.DataFrame(items['children'].item())
              if len(tempdf)!=0:
                tempdf=tempdf[['productName','price','stock']].copy()
                tempdf=tempdf.rename(columns={
                'productName':'name'
                })
                tempdf['stock'] = tempdf['stock'].apply(lambda x: x['stock'])
                tempdf['product_id']=prodid
                tempdf['city']=city
                tempdf['pareto']=pareto
                tempdf['cogs']=cogs
                tempdf['url']=url
                dfFinal=pd.concat([dfFinal,tempdf])
                checker=1

      if checker==0:
        for comp in resp.json()['data']['pdpGetLayout']['components']:
          if comp['name']=='product_content':
            tempdf=pd.DataFrame(comp['data'])
            tempdf=tempdf[['name','price','stock']].copy()
            tempdf['price'] = tempdf['price'].apply(lambda x: x['value'])
            tempdf['stock'] = tempdf['stock'].apply(lambda x: x['value'])
            tempdf['product_id']=prodid
            tempdf['city']=city
            tempdf['pareto']=pareto
            tempdf['cogs']=cogs
            tempdf['url']=url
            dfFinal=pd.concat([dfFinal,tempdf])
    except Exception as E:
      pass

  #writing to bigquery      
  dfFinal['date_scraped']=datetime.now()

  time_partitioning_field = 'date_scraped'

  # Specify partitioning by ingestion time (optional)
  time_partitioning = bigquery.TimePartitioning(
      type_=bigquery.TimePartitioningType.DAY,
      field=time_partitioning_field
  )

  # Load the data into the table with partitioning and clustering
  job_config = bigquery.LoadJobConfig(
      time_partitioning=time_partitioning,
  )

  dfFinal=dfFinal.drop_duplicates()
  client.load_table_from_dataframe(dfFinal,'astro-data-prd.astro_price_scraping.tokopedia_price_checking_with_stock',job_config=job_config )
 
  logger(loggerfile,'finish',datetime.now(),len(dfFinal))
  time.sleep(60)
  #query in for load
  
 
  client.query(
    '''
    CREATE TEMPORARY FUNCTION match(str1 STRING, str2 STRING)
            RETURNS FLOAT64
            LANGUAGE js AS \"""
              var stopWords = /\\\\b(g|kg|ml|L|gr|astro|goods|daun|murah|promo|diskon|termurah)\\\\b/ig
              var stopChars = /[-_ \\\\.,'"'/()]/g
              var tokens = /\\\\b(segari|sayurbox|happyfresh|alfamart|indomaret|astro)\\\\b/i
              var grams = 3
              var tokenValue = 2
              var substrs1 = extract(str1)
              var substrs2 = extract(str2)
              var maxL = max(substrs1.length, substrs2.length)
              return (parseInt(100 * (
                substrs1.filter(s => ~substrs2.indexOf(s)).length +
                substrs2.filter(s => ~substrs1.indexOf(s)).length
              ) / (0.01 + maxL) / 2))

              function max(a, b) {
                return a > b ? a : b
              }

              function extract(str) {
                return str.toLowerCase()
                  .replace(stopWords, "")
                  .replace(tokens, m => m.slice(0, tokenValue).toUpperCase())
                  .replace(stopChars, "")
                  .split('').map((char, c, arr) => arr.slice(max(0, 1 + c - grams), 1 + c).join('')).filter(s => s.length >= 3)
              }
            \""";
    CREATE OR REPLACE TABLE `astro-data-prd.astro_price_scraping.rpt_tokopedia_price_checking_final` as
    WITH 
    unitsExtract as 
    (
      SELECT 
        pc.*
        ,product_name
        , regexp_replace(regexp_replace(lower(product_name), r'[^a-zA-Z ]', '') 
                              , r'\\\\s+', ' ') astro_name_clean
        , regexp_replace(regexp_replace(lower(name), r'[^a-zA-Z ]', '') 
                              , r'\\\\s+', ' ') comp_name_clean
        -- ,REGEXP_EXTRACT(
        --                     LOWER(product_name), 
        --                     r'\\b(\\d+(?:[.,]\\d+)?)\\s?(?:gram|g|ml|litre|liter|l|kilogram|kg|gr|mg|lembar)\\b'
        --                     ) astro_unit
        -- ,REGEXP_EXTRACT(
        --                     LOWER(name), 
        --                     r'\\b(\\d+(?:[.,]\\d+)?)\\s?(?:gram|g|ml|litre|liter|l|kilogram|kg|gr|mg|lembar)\\b'
        --                     ) comp_unit
        -- ,REGEXP_REPLACE(
        --                       REGEXP_EXTRACT(
        --                                   LOWER(name),
        --                                   r'\\bisi\\b\\s*([^\\s]+)'
        --                                   ), r'[^a-zA-Z0-9]', ' '
        --                     )isi
      FROM 
        `astro-data-prd.astro_price_scraping.tokopedia_price_checking_with_stock` pc
      LEFT JOIN
      `astro-data-prd.astro_dataset.dim_products_x_categories_x_attributes`atr
      using(product_id) 
      WHERE date(pc.date_scraped)=(select date(max(date_scraped)) from `astro-data-prd.astro_price_scraping.tokopedia_price_checking_with_stock`)
      AND safe_cast(stock as int)>50
    )
    Select 
      *except(astro_name_clean,comp_name_clean)
      ,round((cogs-(price))/cogs,2) percsCheaper
      ,match(astro_name_clean,comp_name_clean)similarity
    FROM 
      unitsExtract  
    WHERE
      round((cogs-(price))/cogs,2)>0.03

          '''
        )
      
#end




