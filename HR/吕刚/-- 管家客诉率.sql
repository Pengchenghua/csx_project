-- 管家客诉率
-- 客诉省区汇总  SKU 更改为省区整体SKU，DC更改为城市整体SKU，任静   不含 客诉20240715
with sale_detail  as (

select
  smonth,
  -- performance_region_code,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  -- IF(performance_province_name='浙江省',performance_city_name,performance_province_name) performance_province_name,
  customer_code,
  customer_name,
  sum(sku) sku,
  sum(sum(sku))over(partition by smonth,performance_province_name,performance_city_name) all_months_sku,
--   sum(sum(sku))over(partition by smonth,performance_province_name) all_months_dc_sku,
  sum(sum(sku))over(partition by performance_province_name,performance_city_name) all_sku
from (
select 
  substr(sdt,1,6) smonth,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  customer_code,
  customer_name,
  a.order_code,
  count(distinct a.goods_code) sku
from csx_dws.csx_dws_sale_detail_di a
left  join
  (
    select
      goods_code,
      classify_large_code,
      classify_large_name,
      classify_middle_code,
      classify_middle_name,
      classify_small_code,
      classify_small_name
    from csx_dim.csx_dim_basic_goods
    where sdt = 'current'
   ) c on c.goods_code = a.goods_code
where  sdt>='20240101' and sdt<='20241231'
    and channel_code in('1','7','9')
    -- and performance_province_name='上海'
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and performance_province_name !='平台-B'
group by substr(sdt,1,6) ,
  performance_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  performance_city_code,
  performance_city_name,
  customer_code,
  customer_name,
  a.order_code
  )a 
  group by smonth,
  -- performance_region_code,
  performance_region_name,
  performance_province_name,
  performance_city_name,
  -- IF(performance_province_name='浙江省',performance_city_name,performance_province_name) performance_province_name,
  customer_code,
  customer_name
	) 
	,
sales_info as (
  select substr(sdt, 1, 6) as smt,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    customer_code ,
    customer_name,
    service_manager_user_number,
    service_manager_user_name,
    business_attribute_code attribute_code,
    business_attribute_name attribute_name,
    case
      when business_attribute_code = 1 then 1
      when business_attribute_code = 2 then 2
      when business_attribute_code = 5 then 6
    end business_type_code,
    service_manager_user_position,
    sales_user_name,
    sales_user_number,
    sales_user_position,
    -- count() over(partition by customer_code, business_attribute_code ) as cnt,
    -- row_number() over(partition by customer_code,business_attribute_code order by service_manager_user_number asc  ) as ranks,
    current_timestamp() as update_time
  from csx_dim.csx_dim_crm_customer_business_ownership
  where 
  sdt = '20241231' 
  
      and service_manager_user_position = 'CUSTOMER_SERVICE_MANAGER'
    )
,
  
complaint as 
(
  select 
    substr(sdt,1,6) smonth,        -- 客诉日期
	performance_province_name,
    performance_city_name,
    customer_code,
    count(complaint_code)  kesu --- 客诉单量
  from csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di a
  where  sdt>='20240101' and sdt<='20241231'
    and complaint_status_code in (20,30)  -- 客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
		and complaint_deal_status in (10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and second_level_department_name !=''
		and first_level_department_name='采购'
        -- and complaint_level !='3'
		and complaint_amt <> 0
  --  and complaint_source_code<>2    --	客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	--  and second_level_department_name not like '%地采%'
  group by   substr(sdt,1,6) ,        -- 客诉日期
	performance_province_name,
    performance_city_name,
    customer_code 
  )
 
 select  a.smonth,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  c.service_manager_user_number,
  c.service_manager_user_name,
--   customer_code,
--   customer_name,
  sum(a.sku) sale_sku,
  sum(kesu) kesu,
  sum(kesu)/sum(a.sku) as ke_lv
 from sale_detail a 
 left join 
 sales_info c on a.customer_code=c.customer_code
 left join 
 complaint b on a.smonth=b.smonth 
       and a.customer_code=b.customer_code
       and a.performance_city_name=b.performance_city_name
       and a.performance_province_name=b.performance_province_name 
group by   a.smonth,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
  c.service_manager_user_number,
  c.service_manager_user_name
 