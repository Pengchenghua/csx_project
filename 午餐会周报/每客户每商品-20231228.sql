select
	a.province_name as `省区`,
	a.city_group_name as `城市`,
	a.business_type_name as `业务类型`,
	a.customer_code as `客户编号`,
	d.customer_name as `客户名称`,
	nvl(f.fir_price_type,e.price_type1) as `定价类型1`, -- 定价类型1
	nvl(f.sec_price_type,e.price_type2) as `定价类型2`, -- 定价类型2
	e.price_period_name as `报价周期`, -- 报价周期
	e.price_date_name as `报价日`, -- 报价日
	d.first_category_name as `一级客户分类`,
	d.second_category_name as `二级客户分类`,
	d.third_category_name as `三级客户分类`,
	c.first_sales_date as `客户此业务类型首次下单日期`,
	b.business_division_name as `部类`,
	b.purchase_group_code as `课组编号`,b.purchase_group_name as `课组名称`,
	b.classify_large_code as `管理大类编号`,b.classify_large_name as `管理大类名称`,
	b.classify_middle_code as `管理中类编号`,b.classify_middle_name as `管理中类名称`,
	b.classify_small_code as `管理小类编号`,b.classify_small_name as `管理小类名称`,
	a.goods_code as `商品编号`,b.goods_name as `商品名称`,
	(case when g.goods_code is not null then '是' else '否' end) as `是否是客户市调商品`,
	sales_type as `是否调价`,
	fanli_type as `是否返利`,
	tuihuo_type as `是否退货`,
	delivery_type_name as `物流模式`,
	direct_delivery_name as `物流模式细分`,
	inventory_dc_code as `DC编码`,
	types as `是否直送仓`,
	if(c.first_sales_date >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and c.first_sales_date<= regexp_replace(add_months(date_sub(current_date,1),0),'-',''),'新客','老客') as `新老客`,
	nvl(sum(case when a.sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') then a.sales_value end),0) as `本月_月至今销售额`,
	nvl(sum(case when a.sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') then a.sale_qty end),0) as `本月_月至今销售数量`,
	nvl(sum(case when a.sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and a.sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') then a.profit end),0) as `本月_月至今毛利额`,	
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and a.sdt <regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') then a.sales_value end),0) as `上月_月至今销售额`,
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and a.sdt <regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') then a.sale_qty end),0) as `上月_月至今销售数量`,
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and a.sdt <regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') then a.profit end),0) as `上月_月至今毛利额`  
from 
  (
	select 
		performance_province_name province_name,
    performance_city_name city_group_name,
	  sdt,substr(sdt,1,6) smonth,
	 	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week, 
		business_type_name,
		business_type_code,
		customer_code,
		if(order_channel_code=6 ,'是','否') sales_type,
		if(order_channel_code=4 ,'是','否') fanli_type,
		if(refund_order_flag=1,'是','否') as tuihuo_type,
		delivery_type_name,
		(case when delivery_type_name<>'直送' then delivery_type_name 
		      when delivery_type_name='直送' and direct_delivery_type=0 then '普通直送' 
          when delivery_type_name='直送' and direct_delivery_type=1 then '融单' 
          when delivery_type_name='直送' and direct_delivery_type=2 then '过账' 
          when delivery_type_name='直送' and direct_delivery_type=11 then '临时加单' 
          when delivery_type_name='直送' and direct_delivery_type=12 then '紧急补货' end) as direct_delivery_name,
		goods_code,
		a.inventory_dc_code,
		if( c.shop_code is null,'否','是') types,
		sum(sale_amt)as sales_value,
		sum(profit)as profit,		
		sum(if(order_channel_detail_code=26,0,sale_qty)) as sale_qty
	from 
			(select * 
	     from csx_dws.csx_dws_sale_detail_di 
	     where 
	        sdt >= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','') and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	        and channel_code <> '2' and substr(customer_code, 1, 1) <> 'S' -- 剔除商超数据 
	        and business_type_code in ('1') 
			 	and performance_city_name in ('西安市','东北')
			-- and performance_region_name in ('华南大区')
		  ) a
      left join 
      (select  distinct shop_code 
			from csx_dim.csx_dim_shop 
			where sdt='current' and shop_low_profit_flag=1  
			)c
      on a.inventory_dc_code = c.shop_code
    	group by 
    				 performance_province_name,
             performance_city_name,
			       sdt,
			       substr(sdt,1,6),
			       weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)),
				     business_type_name,
				     business_type_code,
			       customer_code,
			       a.inventory_dc_code,
			       if( c.shop_code is null,'否','是'),
			       if(order_channel_code=6 ,'是','否'),
				     if(order_channel_code=4 ,'是','否'),
				     if(refund_order_flag=1,'是','否'),
			       delivery_type_name,
			       goods_code,
			       (case when delivery_type_name<>'直送' then delivery_type_name 
							      when delivery_type_name='直送' and direct_delivery_type=0 then '普通直送' 
					          when delivery_type_name='直送' and direct_delivery_type=1 then '融单' 
					          when delivery_type_name='直送' and direct_delivery_type=2 then '过账' 
					          when delivery_type_name='直送' and direct_delivery_type=11 then '临时加单' 
					          when delivery_type_name='直送' and direct_delivery_type=12 then '紧急补货' end)
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
  where sdt ='current' and 	business_type_code in (1)
  group by customer_code,
           business_type_code
)c on c.customer_code=a.customer_code and c.business_type_code=a.business_type_code
left join  
   (
	 select
		customer_code,
		customer_name,
		first_category_name,
		second_category_name,
		third_category_name 
	 from  csx_dim.csx_dim_crm_customer_info 
	 where sdt='current'	       
	)d on d.customer_code=a.customer_code 
  left join  -- 线上客户定价类型
	csx_analyse_tmp.csx_analyse_tmp_customer_price_type_ky e 
	on a.customer_code=e.customer_code 
	left join  -- 线下客户定价类型
	dev.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
	on a.customer_code=f.customer_code 
	left join -- 客户市调表商品
	(select 
	    g1.customer_code,
	    g2.product_code as goods_code
	from 
	(select *  
	from csx_dwd.csx_dwd_price_market_customer_research_price_di 
	where status='1' 
	) g1 
	left join 
	(select * 
	from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	) g2 
	on g1.product_id=g2.id 
	group by 
	    g1.customer_code,
	    g2.product_code
	) g 
	on a.customer_code=g.customer_code and a.goods_code=g.goods_code 
group by a.province_name,
	    a.province_name,
			a.city_group_name,
			a.business_type_name,
			a.customer_code,
			d.customer_name,
			nvl(f.fir_price_type,e.price_type1), -- 定价类型1
			nvl(f.sec_price_type,e.price_type2), -- 定价类型2
			e.price_period_name, -- 报价周期
			e.price_date_name, -- 报价日
			d.first_category_name,
			d.second_category_name,
			d.third_category_name,
			c.first_sales_date,
			b.business_division_name,
			b.purchase_group_code,b.purchase_group_name,
			b.classify_large_code,b.classify_large_name,
			b.classify_middle_code,b.classify_middle_name,
			b.classify_small_code,b.classify_small_name,
			a.goods_code,b.goods_name,
			sales_type,
			fanli_type,
			tuihuo_type,
			delivery_type_name,
			direct_delivery_name,
			inventory_dc_code,
			types,
			if(c.first_sales_date >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','') and c.first_sales_date<= regexp_replace(add_months(date_sub(current_date,1),0),'-',''),'新客','老客'),
			(case when g.goods_code is not null then '是' else '否' end)
;
