select
	a.province_name,
	a.city_group_name,
	a.business_type_name,
	a.customer_code,
	d.customer_name,
	case when b.division_code in ('10','11') then '生鲜'
		when b.division_code in ('12','13','15','14') then '食百'
		else '' end as division,
	b.purchase_group_code,b.purchase_group_name,
	b.classify_large_code,b.classify_large_name,
	b.classify_middle_code,b.classify_middle_name,
	b.classify_small_code,b.classify_small_name,
	a.goods_code,b.goods_name,
	sales_type,
	fanli_type,
	delivery_type_name,
	inventory_dc_code,
	types,
	if(c.first_sales_date >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and c.first_sales_date<= regexp_replace(add_months(date_sub(current_date,1),0),'-',''),'新客','老客') as xinlaok,
	sum(case when a.sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') then a.sales_value end) by_sales_value,
	sum(case when a.sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') then a.sale_qty end) by_sale_qty,
	sum(case when a.sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') then a.profit end) by_profit,	
	sum(case when a.sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),-1),'-','') then a.sales_value end) sy_sales_value,
	sum(case when a.sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),-1),'-','') then a.sale_qty end) sy_sale_qty,
	sum(case when a.sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),-1),'-','') then a.profit end) sy_profit,	
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sales_value end) bz_sales_value,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.sale_qty end) bz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 3)) then a.profit end) bz_profit,	
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sales_value end) sz_sales_value,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.sale_qty end) sz_sale_qty,
	sum(case when a.week=weekofyear(date_sub(current_date, 10)) then a.profit end) sz_profit,
    a.direct_delivery_type,
    d.second_category_name,
    d.sales_user_name,
    e.price_period,-- 报价周期 
    e.price_type, -- 定价类型 
	f.customer_large_level
from 
  (
	select 
		performance_province_name province_name,
        performance_city_name city_group_name
	   ,sdt,substr(sdt,1,6) smonth,
	 	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week, 
		business_type_name,a.business_type_code,
		customer_code,
		if(order_channel_code=6 ,'是','否') sales_type
		,if(order_channel_code=4 ,'是','否') fanli_type
		,delivery_type_name
		,goods_code,
		a.inventory_dc_code,
		if( c.shop_code is null,'否','是') types,
		case when direct_delivery_type=0 then '普通'
		     when direct_delivery_type=1 then 'RD'
			 when direct_delivery_type=2 then 'ZZ'
			 when direct_delivery_type=11 then '临时加单'
			 when direct_delivery_type=12 then '紧急补货'
			else '否' end as direct_delivery_type
			 ,  -- 直送类型: 0-p(普通) 1-r(融单)、2-z(过账)、11-(临时加单)、12-(紧急补货)
		sum(sale_amt)as sales_value,
		sum(profit)as profit,		
		sum(if(order_channel_detail_code=26,0,sale_qty)) as sale_qty
	from (
	       select 
		     * 
	       from csx_dws.csx_dws_sale_detail_di 
	       where 
	          sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	          and channel_code in('1','9')
	          and business_type_code in ('1','2') 
			  and performance_province_name in ('河北省','河南省','湖北省','贵州省')
		  ) a
    left join ( 
	            select
                   distinct shop_code 
				from csx_dim.csx_dim_shop 
				where sdt='current' and shop_low_profit_flag=1  
			  )c
               on a.inventory_dc_code = c.shop_code
    group by performance_province_name,
             performance_city_name,
		     sdt,substr(sdt,1,6),
		     weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)),
			 business_type_name,business_type_code,
		     customer_code,
		     a.inventory_dc_code,
		     if( c.shop_code is null,'否','是')
		     ,if(order_channel_code=6 ,'是','否')
			 ,if(order_channel_code=4 ,'是','否')
		     ,delivery_type_name,
		     goods_code,
			 case when direct_delivery_type=0 then '普通'
		     when direct_delivery_type=1 then 'RD'
			 when direct_delivery_type=2 then 'ZZ'
			 when direct_delivery_type=11 then '临时加单'
			 when direct_delivery_type=12 then '紧急补货'
			else '否' end
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
join  
   (
	 select
    	substr(sdt,1,6) smonth,
		customer_code,
		customer_name,second_category_name,sales_user_name
	 from  csx_dim.csx_dim_crm_customer_info 
	 where sdt>='20200101'
	       and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-',''))  
	)d on d.customer_code=a.customer_code and d.smonth=a.smonth	

left join 
(
 select 
    customer_code,
    (case when a.price_period_code=1 then '每日' 
          when a.price_period_code=2 then '每周' 
          when a.price_period_code=3 then '每半月' 
          when a.price_period_code=4 then '每月' end) as price_period,-- 报价周期 
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
        row_number()over(partition by customer_code order by business_number desc) as pm 
    from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
    and business_attribute_code=1 
    and status=1 
    -- and sign_type_code=1 
    ) a 
 where a.pm=1
)e on e.customer_code=a.customer_code
left join 
  (
   select customer_no,customer_large_level
   from csx_analyse.csx_analyse_report_sale_customer_level_mf 
   where month=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6)
   and tag=1
   )f on f.customer_no=a.customer_code
group by a.province_name,
	   a.city_group_name,
	   a.business_type_name,
	   a.customer_code,
	   d.customer_name,
	   case when b.division_code in ('10','11') then '生鲜'
	   	    when b.division_code in ('12','13','15','14') then '食百'
	   	else '' end,
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
	   sales_type,fanli_type,
	   delivery_type_name,
	   inventory_dc_code,
	   types,
	   a.direct_delivery_type,
       d.second_category_name,
       d.sales_user_name,
       e.price_period,-- 报价周期 
       e.price_type,
	   f.customer_large_level,
	   if(c.first_sales_date >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and c.first_sales_date<= regexp_replace(add_months(date_sub(current_date,1),0),'-',''),'新客','老客')
having by_sales_value is not null or sy_sales_value is not null or bz_sales_value is not null or sz_sales_value is not null
;


