-- 客户报价配置表20250909
with tmp_csx_price_prod_customer_config_df as 
(select
  id,
  customer_code ,
--   regexp_replace(customer_name, '\n|\t|\r|\,|\"|\\\\n|\\s', '') as `客户名称`,
  (case when customer_price_scale_status=0 then '不控制' 
        when customer_price_scale_status=1 then '1位小数' end) as `客户报价小数精度`,
  create_time as `创建时间`,
  create_by as `创建人`,
  update_time as `更新时间`,
  update_by as `更新人`,
  get_json_object(item_json, '$.customerCode') as `关联客户编码`,
  regexp_replace(get_json_object(item_json, '$.customerName'), '\n|\t|\r|\,|\"|\\\\n|\\s', '') as `关联客户名称`,
  (case when selling_price_status=0 then '关闭' 
        when selling_price_status=1 then '开启' end) as `售价报价开关`,
  (case when quotation_selling_price_status='false' or quotation_selling_price_status is null then '关闭' 
        when quotation_selling_price_status='true' then '开启' end) as `导入客户报价和售价开关`,
  (case when sub_customer_price_status='false' or quotation_selling_price_status is null then '关闭' 
        when sub_customer_price_status='true' then '开启' end) as `支持子客户报价开关`,
  (case when customer_price_kg_status='false' or quotation_selling_price_status is null then '不控制' 
        when customer_price_kg_status='true' then '控制' end) as `客户报价仅kg单位保留一位`,
  (case when customer_price_kg_jin_status='false' or quotation_selling_price_status is null then '不控制' 
        when customer_price_kg_jin_status='true' then '控制' end) as `客户报价仅kg单位先按斤计算再转kg`,
  (case when customer_price_cut_status='false' or quotation_selling_price_status is null then '不控制' 
        when customer_price_cut_status='true' then '控制' end) as `截断处理，偶数结尾` 
FROM csx_ods.csx_ods_csx_price_prod_customer_config_df
LATERAL VIEW explode(split(regexp_replace(substr(customer_link, 2, length(customer_link) - 2), '\\}\\,', '\\}\\|\\|'), '\\|\\|')) r1 AS item_json
where sdt='20250909' -- 日期每次换成昨日的日期就行了
)
select b.performance_region_name as `大区`,
    b.performance_province_name as `省区`,
    b.performance_city_name as `城市`,
    a.customer_code  as `客户编码`,
    b.customer_name  as `客户名称`,
    a.* from tmp_csx_price_prod_customer_config_df a 
left join 
(select performance_region_name,performance_province_name,performance_city_name,customer_code,customer_name
from    csx_dim.csx_dim_crm_customer_info where sdt='current') b on a.customer_code=b.customer_code

