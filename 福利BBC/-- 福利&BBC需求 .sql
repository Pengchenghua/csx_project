-- 福利 
-- 商品top
with a as (select
  performance_province_name,
  a.channel_name,
  business_type_name,
  goods_code,
  goods_name,
  unit_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  if(sdt>='20230601' and sdt<='20230606','同期','本期') type,
  sum(sale_qty) sale_qty,
  sum(sale_amt)/10000 sale,
  sum(profit)/10000 profit
from
     csx_dws.csx_dws_sale_detail_di a
where
  (
    (
      sdt >= '20230601'
      and sdt <= '20230606'
    )
    or (
      sdt >= '20240601'
      and sdt <= '20240606'
    )
  )
 -- and a.channel_code in ('1', '7', '9')
--  and inventory_dc_code not in ('W0K9','WB26','W0A3','WB62','WC02')
  and business_type_code in ('6', '2')
group by
  performance_province_name,
  a.channel_name,
  business_type_name,
  goods_code,
  goods_name,
  unit_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  if(sdt>='20230601' and sdt<='20230606','同期','本期')
  ) 
  select performance_province_name,
  a.channel_name,
  business_type_name,
  goods_code,
  goods_name,
  unit_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  type,
  sale_qty,
  sale,
  profit,
  sum(sale)over(partition by performance_province_name,type,business_type_name ) as total_amt,
  row_number()over(partition by performance_province_name,type,business_type_name order by sale desc ) as rnk
  from a 
  ;


  -- 客户维度

  with a as (select
  performance_province_name,
  a.channel_name,
  business_type_name,
  customer_code,
  customer_name,
  first_category_name,
  second_category_name,
  if(sdt>='20230601' and sdt<='20230606','同期','本期') type,
  sum(sale_amt)/10000 sale,
  sum(profit)/10000 profit
from
    csx_dws.csx_dws_sale_detail_di a
where
  (
    (
      sdt >= '20230601'
      and sdt <= '20230606'
    )
    or (
      sdt >= '20240601'
      and sdt <= '20240606'
    )
  )
--  and a.channel_code in ('1', '7', '9')
--  and inventory_dc_code not in ('W0K9','WB26','W0A3','WB62','WC02')
  and business_type_code in ('6', '2')
group by
  customer_code,
  customer_name,
  channel_name,
  business_type_name,
  performance_province_name,
  first_category_name,
  second_category_name,
  if(sdt>='20230601' and sdt<='20230606','同期','本期')
  ) select
  performance_province_name,
  a.channel_name,
  business_type_name,
  customer_code,
  customer_name,
  first_category_name,
  second_category_name,
--   type,
  tq_sale,
  tq_profit,
  tq_profit/tq_sale tq_profit_rate,
  sale,
  sale/tq_sale-1 as sale_ratio,
  profit,
  profit/sale profit_rate,
  if(tq_sale>0,1,0) old_cust,
  if(tq_sale>0 and sale>0,1,0) as to_old_cust
  from (select
  performance_province_name,
  a.channel_name,
  business_type_name,
  customer_code,
  customer_name,
  first_category_name,
  second_category_name,
--   type,
  sum(if(type='同期',sale,0)) tq_sale,
  sum(if(type='同期',profit,0)) tq_profit,
  sum(if(type='本期',sale,0)) sale,
  sum(if(type='本期',profit,0)) profit
  from a
  group by performance_province_name,
  a.channel_name,
  business_type_name,
  customer_code,first_category_name,
  second_category_name,
  customer_name
  ) a


  -- 销售员数


  -- 福利销售人员数据 


  -- 福利销售人员数据 
with a as (select
  performance_province_name,
 case when business_type_code in (6,2) then '福利' else '其他' end  business_type_name,
  sales_user_name,
  customer_code,
 -- if(sdt>='20230601' and sdt<='20230606','同期','本期') type,
  sum(sale_qty) sale_qty,
  sum(sale_amt) sale,
  sum(profit) profit
from
       csx_dws.csx_dws_sale_detail_di a
where
      sdt >= '20240601'
      and sdt <= '20240606'
--  and a.channel_code in ('1', '7', '9')
 --  and inventory_dc_code not in ('W0K9','WB26','W0A3','WB62','WC02')
 -- and business_type_code in ('6', '2')
group by
  performance_province_name,
  a.channel_name,
   case when business_type_code in (6,2) then '福利' else '其他' end ,
  sales_user_name,
  customer_code
  ) 
  select 
   performance_province_name,
  count(distinct if(business_type_name='福利',sales_user_name,'')) as cnt,
  sum( if(business_type_name='福利',sale,0))/10000 fl_sale_amt,
  count(distinct sales_user_name) all_cnt,
  sum(sale)/10000 as all_sale,
  count(distinct if(business_type_name='福利',customer_code,'')) as cust_cn
  from a 
  group by performance_province_name



  -- 新签商机客户数
with a as (select owner_user_name,
			if(to_date(business_sign_time)=to_date(first_business_sign_time),'新商机','老商机') cust_flag,
			business_sign_time,
			regexp_replace(substr(to_date(business_sign_time),1,7),'-','') business_sign_month,
			business_number,
			customer_id,
			customer_code,
			customer_name,
            contract_number,			
			performance_region_name,performance_province_name,performance_city_name,
            business_type_code,
			business_type_name,
			contract_cycle_desc,estimate_contract_amount,
			regexp_replace(to_date(business_sign_time),'-','') business_sign_date,
			to_date(business_sign_time) as business_sign_date_2,
			regexp_replace(to_date(first_business_sign_time),'-','') first_business_sign_date,business_stage
		from 
		  	csx_dim.csx_dim_crm_business_info
		where 
			sdt='current'
			and business_attribute_code in (1,2,5) --  商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
			and status=1  --  是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
			and 
			(to_date(business_sign_time) >= '2024-01-01'  and to_date(business_sign_time) < '2024-01-16' )
			and performance_province_name !='平台-B'
			) select 
   performance_province_name,
  count(distinct if(business_type_name='福利业务',owner_user_name,'')) as cnt,
 -- sum( if(business_type_name='福利业务',sale,0)) fl_sale_amt,
  count(distinct owner_user_name) all_cnt
 -- sum(sale) as all_sale
  from a 
  group by performance_province_name 

  -- BBC渠道

   with a as (   select 
                substr(sdt,1,4) as syear,
        substr(sdt,1,6) as smonth,
                -- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
        performance_region_name,
        performance_province_name,
        performance_city_name,
                inventory_dc_code,
                6 as business_type_code,
        'BBC' as business_type_name,
        (case when (credit_pay_type_name='餐卡' or credit_pay_type_code='F11') then '餐卡'
                     when (credit_pay_type_name='福利' or credit_pay_type_code='F10') then '福利' 
            else '福利' end) type_flag,
            operation_mode_name,
        customer_code,
        goods_code,
        sum(sale_qty)sale_qty,
        sum(sale_amt)/10000 as sale_amt, 
        sum(profit)/10000 as profit 
    from csx_dws.csx_dws_bbc_sale_detail_di  
    where (sdt >='20240601' and sdt <='20240606')
     --    and inventory_dc_code not in ('W0K9','WB26','W0A3','WB62','WC02') -- W0K9是监狱日采，WB26是小店过机，W0A3是日配仓库，WB62是京东仓库
    group by 
                substr(sdt,1,4),
        substr(sdt,1,6),
                -- weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)),
        performance_region_name,
        performance_province_name,
        performance_city_name,
                inventory_dc_code,
        (case when (credit_pay_type_name='餐卡' or credit_pay_type_code='F11') then '餐卡'
                     when (credit_pay_type_name='福利' or credit_pay_type_code='F10') then '福利' 
            else '福利' end),
            operation_mode_name,
        customer_code,
        goods_code
        ) 
    select performance_province_name,
        operation_mode_name,
        a.goods_code,
       	goods_name,unit_name,
        brand_name,classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,	classify_small_code	,classify_small_name,
        sum(sale_qty) sale_qty,
        sum(sale_amt)sale_amt ,
        sum(profit)profit,
        sum(profit)/sum(sale_amt) as profit_rate
        from a 
        join 
        (select goods_code,	goods_name,unit_name,
        brand_name,classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,	classify_small_code	,classify_small_name
        from csx_dim.csx_dim_basic_goods where sdt='current')
        b on a.goods_code=b.goods_code
        group by performance_province_name,
        operation_mode_name,
        a.goods_code,
        goods_name,unit_name,
        brand_name,classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code	,
        classify_small_name