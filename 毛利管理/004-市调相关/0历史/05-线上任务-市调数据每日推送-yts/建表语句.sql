
create table csx_analyse.csx_analyse_fr_price_market_research_price_detail_df(
`performance_province_name`             string              COMMENT    '省份',
`performance_city_name`                 string              COMMENT    '城市',
`location_code`                         string              COMMENT    'DC编码',
`market_source_type_name`               string              COMMENT    '市调来源类型',
`shop_code`                             string              COMMENT    '对标门店编码',
`shop_name`                             string              COMMENT    '对标门店名称',
`product_code`                          string              COMMENT    '商品编码',
`product_name`                          string              COMMENT    '商品名称',
`market_research_price`                 decimal(12,2)       COMMENT    '市调价格',
`min_price`                             decimal(12,2)       COMMENT    '最低价',
`max_price`                             decimal(12,2)       COMMENT    '最高价',
`price_begin_time`                      string              COMMENT    '生效开始时间',
`price_end_time`                        string              COMMENT    '生效结束时间',
`create_date`                           string              COMMENT    '创建时间'
) COMMENT '市调数据'
STORED AS PARQUET;
