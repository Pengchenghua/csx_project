-- 每客户每商品-华南
-- drop  table csx_analyse_tmp.csx_analyse_tmp_sx_sale ;

create table csx_analyse_tmp.csx_analyse_tmp_sx_sale as
select a.*,
	d.price_period_name,
	d.price_date_name,
	d.price_type as price_type_0, -- 定价类型
	case d.price_type
	when '客户定价->合同固定价' then '长周期'
	when '客户定价->单独议价' then '自主定价'
	when '对标定价->全对标' then '强对标'
	when '' then '自主定价'
	when null then '自主定价'
	when '空' then '自主定价'
	when '自主定价->采购或车间定价' then '自主定价'
	when '临时报价->单品项' then '自主定价'
	when '临时报价->下单时' then '自主定价'
	when '客户定价->多方比价' then '多方比价'
	when '对标定价->半对标' then '半对标'
	when '自主定价->建议售价' then '自主定价'
	end as price_type,
	e.second_category_name,     --  二级客户分类名称
	e.sales_user_number,
	e.sales_user_name,	
	f.customer_large_level
from 
(
select
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.business_type_name,
	a.customer_code,
	d.customer_name,
	b.business_division_name,
	b.purchase_group_code,b.purchase_group_name,
	b.classify_large_code,b.classify_large_name,
	b.classify_middle_code,b.classify_middle_name,
	b.classify_small_code,b.classify_small_name,
	a.goods_code,b.goods_name,
	sales_type,
	fanli_type,
	delivery_type_name,
	direct_delivery_type,
	inventory_dc_code,
	types,
	is_tuihuo,
	if(c.first_sales_date >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and c.first_sales_date<= regexp_replace(add_months('${i_sdate}',0),'-',''),'新客','老客') as xinlaok,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end) by_sale_amt,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_qty end) by_sale_qty,
	sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end) sy_sale_amt,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_qty end) sy_sale_qty,
	sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end) sy_profit,	
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_amt end) bz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_qty end) bz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end) bz_profit,	
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_amt end) sz_sale_amt,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_qty end) sz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.profit end) sz_profit		
from 
  (
	select 
		performance_region_name,
		performance_province_name,
        performance_city_name,
	   sdt,substr(sdt,1,6) smonth,
	 	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week, 
		business_type_name,a.business_type_code,
		customer_code,
		if(order_channel_code=6 ,'是','否') sales_type,
		if(order_channel_code=4 ,'是','否') fanli_type,
		delivery_type_name,
		case 
			when delivery_type_name='配送' then ''
			when direct_delivery_type=1 then 'R直送1'
			when direct_delivery_type=2 then 'Z直送2'
			when direct_delivery_type=11 then '临时加单'
			when direct_delivery_type=12 then '紧急补货'
			when direct_delivery_type=0 then '普通' else '普通' end direct_delivery_type,			
		a.goods_code,
		a.inventory_dc_code,
		if( c.shop_code is null,'否','是') types,
		if( refund_order_flag=1,'是','否') is_tuihuo,
		sum(sale_amt)as sale_amt,
		sum(profit)as profit,		
		sum(if(order_channel_detail_code=26,0,sale_qty)) as sale_qty
	from (
	       select 
		     * 
	       from csx_dws.csx_dws_sale_detail_di 
	       where 
	          sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') 
			  and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	          and channel_code in('1','9')
	          and business_type_code in ('1') 
			  and performance_province_name in ('陕西省')
			-- and performance_region_name='华南大区'
		  ) a
    join ( 
	            select
                   distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=0  
			  )c on a.inventory_dc_code = c.shop_code
    group by 
		performance_region_name,
		performance_province_name,
        performance_city_name,
	   sdt,substr(sdt,1,6),
	 	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)), 
		business_type_name,a.business_type_code,
		customer_code,
		if(order_channel_code=6 ,'是','否'),
		if(order_channel_code=4 ,'是','否'),
		delivery_type_name,
		case 
			when delivery_type_name='配送' then ''
			when direct_delivery_type=1 then 'R直送1'
			when direct_delivery_type=2 then 'Z直送2'
			when direct_delivery_type=11 then '临时加单'
			when direct_delivery_type=12 then '紧急补货'
			when direct_delivery_type=0 then '普通' else '普通' end,
		a.goods_code,
		a.inventory_dc_code,
		if( c.shop_code is null,'否','是'),
		if( refund_order_flag=1,'是','否')
 )a  
left join 
 (
   select 
     *  
   from  csx_dim.csx_dim_basic_goods 
   where sdt = 'current'
 ) b on b.goods_code = a.goods_code 
left join  -- 首单日期
(
  select 
    customer_code,
	business_type_code,
	min(first_business_sale_date) first_sales_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt ='current' and 	business_type_code in (1,2)
  group by customer_code,
           business_type_code
)c on c.customer_code=a.customer_code and c.business_type_code=a.business_type_code
left join  
   (
	 select
		customer_code,
		customer_name
	 from  csx_dim.csx_dim_crm_customer_info 
	 where sdt='current'	       
	)d on d.customer_code=a.customer_code
group by a.performance_region_name,
	   a.performance_province_name,
	   a.performance_city_name,
	   a.business_type_name,
	   a.customer_code,
	   d.customer_name,
	   b.business_division_name,
	   b.purchase_group_code,
	   b.purchase_group_name,
	   b.classify_large_code,
	   b.classify_large_name,
	   b.classify_middle_code,
	   b.classify_middle_name,
	   b.classify_small_code,
	   b.classify_small_name,
	   a.goods_code,
	   b.goods_name,
	   sales_type,
	   fanli_type,
	   delivery_type_name,
	   direct_delivery_type,
	   inventory_dc_code,
	   types,
	   is_tuihuo,
	   if(c.first_sales_date >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and c.first_sales_date<= regexp_replace(add_months('${i_sdate}',0),'-',''),'新客','老客')
having by_sale_amt is not null or sy_sale_amt is not null or bz_sale_amt is not null or sz_sale_amt is not null
)a 
left join 
( -- 客户报价周期、商机表最后一次商机
select 
    customer_code,
    (case when a.price_period_code=1 then '每日' 
          when a.price_period_code=2 then '每周' 
          when a.price_period_code=3 then '每半月' 
          when a.price_period_code=4 then '每月' end) as price_period_name,-- 报价周期 
    price_date_name,
	concat(a.price_set_type_first,'->',a.price_set_type_sec) as price_type -- 定价类型 
from 
    (select 
        *,
        (case when split(price_set_type,',')[0]='1' then '对标定价' 
              when split(price_set_type,',')[0]='4' then '客户定价' 
              when split(price_set_type,',')[0]='8' then '自主定价' 
              when split(price_set_type,',')[0]='11' then '临时报价' 
              when split(price_set_type,',')[0]='2' then '全对标' 
              when split(price_set_type,',')[0]='3' then '半对标' 
              when split(price_set_type,',')[0]='5' then '合同固定价' 
              when split(price_set_type,',')[0]='6' then '多方比价' 
              when split(price_set_type,',')[0]='7' then '单独议价' 
              when split(price_set_type,',')[0]='9' then '采购或车间定价' 
              when split(price_set_type,',')[0]='10' then '建议售价' 
              when split(price_set_type,',')[0]='12' then '下单时' 
              when split(price_set_type,',')[0]='13' then '单品项' 
        end) as price_set_type_first,
        (case when split(price_set_type,',')[1]='1' then '对标定价' 
              when split(price_set_type,',')[1]='4' then '客户定价' 
              when split(price_set_type,',')[1]='8' then '自主定价' 
              when split(price_set_type,',')[1]='11' then '临时报价' 
              when split(price_set_type,',')[1]='2' then '全对标' 
              when split(price_set_type,',')[1]='3' then '半对标' 
              when split(price_set_type,',')[1]='5' then '合同固定价' 
              when split(price_set_type,',')[1]='6' then '多方比价' 
              when split(price_set_type,',')[1]='7' then '单独议价' 
              when split(price_set_type,',')[1]='9' then '采购或车间定价' 
              when split(price_set_type,',')[1]='10' then '建议售价' 
              when split(price_set_type,',')[1]='12' then '下单时' 
              when split(price_set_type,',')[1]='13' then '单品项' 
        end) as price_set_type_sec, 
        row_number()over(partition by customer_code order by business_number desc) as ranks 
    from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
    and business_attribute_code=1 
    and status=1 
    -- and sign_type_code=1 
    )a 
where a.ranks=1
)d on a.customer_code=d.customer_code
left join -- 客户信息
(
select 
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	channel_code,
	channel_name,
	bloc_code,     --  集团编码
	bloc_name,     --  集团名称
	customer_id,
	customer_code,
	customer_name,     --  客户名称
	first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	third_category_code,     --  三级客户分类编码
	third_category_name,     --  三级客户分类名称

	sales_user_number,
	sales_user_name	
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and customer_type_code=4
)e on a.customer_code=e.customer_code
left join 
  (
   select customer_no,customer_large_level
   from csx_analyse.csx_analyse_report_sale_customer_level_mf 
   where month=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6)
   and tag=1
   )f on f.customer_no=a.customer_code
;

select * from csx_analyse_tmp.csx_analyse_tmp_sx_sale