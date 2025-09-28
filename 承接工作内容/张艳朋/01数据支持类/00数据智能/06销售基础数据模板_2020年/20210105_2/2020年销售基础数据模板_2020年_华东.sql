--===============================================================================================
-- B端

insert overwrite directory '/tmp/zhangyanpeng/sales_base_b_huadong_20210105' row format delimited fields terminated by '\t' 

select
	c.region_name,a.dc_province_name,a.dc_city_name,a.smonth,
	case when a.channel='1' then '大客户' when a.channel='7' then '企业购' when a.channel='9' then '业务代理' else '其他' end as cus_channel,
	e.sale_group,a.dc_code,a.dc_name,a.customer_no,b.customer_name,b.sign_time,b.attribute,b.first_category,b.second_category,b.third_category,
	a.work_no,a.sales_name,a.department_code,a.department_name,d.classify_large_name,d.classify_middle_name,goods_code,goods_name,unit,
	sum(a.sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
from
	(
	select
		dc_province_code,dc_province_name,dc_city_name,province_code,substr(sdt,1,6) as smonth,channel,order_kind,work_no,sales_name,dc_code,dc_name,customer_no,customer_name,
		department_code,department_name,order_no,goods_code,goods_name,unit,sales_qty,sales_value,profit,excluding_tax_sales,excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20200101' and '20201231'
		and channel in( '1','7','9') --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and province_name not like '平台%'
	) as a 
	left join
		(
		select 
			customer_no,customer_name,attribute,regexp_replace(to_date(sign_time),'-','') as sign_time,first_category,second_category,third_category
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20201231'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,regexp_replace(to_date(sign_time),'-',''),first_category,second_category,third_category
		) b on a.customer_no = b.customer_no
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) c on a.province_code=c.province_code
	left join
		(
		select 
			goods_id,classify_large_name,classify_middle_name,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='20201231'
		group by
			goods_id,classify_large_name,classify_middle_name,classify_small_name
		) d on a.goods_code=d.goods_id
	left join
		(
		select
			a.order_no,
			case when a.channel ='7' then 'BBC'	
				when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
				when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
				when a.channel in ('1','9') and b.attribute='贸易客户' and a.order_profit_rate<=0.015 then '批发内购' 
				when a.channel in ('1','9') and b.attribute='贸易客户' and a.order_profit_rate>0.015 then '省区大宗'
				when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
				else '日配单' end sale_group
		from 
			(
			select 
				channel,province_name,substr(sdt,1,6) smonth,order_no,customer_no,order_kind,
				sum(profit)/sum(sales_value) order_profit_rate
			from 
				csx_dw.dws_sale_r_d_customer_sale 
			where 
				sdt between '20200101' and '20201231' 
				and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
				and channel in('1','7','9')	
				and province_name not like '平台%'
			group by 
				channel,province_name,substr(sdt,1,6),order_no,customer_no,order_kind
			) a  
			left join   
				(
				select 
					customer_no,attribute,customer_name
				from 
					csx_dw.dws_crm_w_a_customer_m_v1 
				where 
					sdt='20201231'
				) b on b.customer_no=a.customer_no 	
		group by 
			a.order_no,
			case when a.channel ='7' then 'BBC'	
				when a.channel in ('1','9') and b.attribute='合伙人客户' then '城市服务商' 
				when a.channel in ('1','9') and (b.customer_name like '%内%购%' or b.customer_name like '%临保%') then '批发内购'		
				when a.channel in ('1','9') and b.attribute='贸易客户' and a.order_profit_rate<=0.015 then '批发内购' 
				when a.channel in ('1','9') and b.attribute='贸易客户' and a.order_profit_rate>0.015 then '省区大宗'
				when a.channel in ('1','9') and a.order_kind='WELFARE' then '福利单'   
				else '日配单' end
		) e on a.order_no=e.order_no
where
	c.region_name='华东大区'
group by 
	c.region_name,a.dc_province_name,a.dc_city_name,a.smonth,
	case when a.channel='1' then '大客户' when a.channel='7' then '企业购' when a.channel='9' then '业务代理' else '其他' end,
	e.sale_group,a.dc_code,a.dc_name,a.customer_no,b.customer_name,b.sign_time,b.attribute,b.first_category,b.second_category,b.third_category,
	a.work_no,a.sales_name,a.department_code,a.department_name,d.classify_large_name,d.classify_middle_name,goods_code,goods_name,unit





--===============================================================================================
-- M端

insert overwrite directory '/tmp/zhangyanpeng/sales_base_m_huadong_20210105' row format delimited fields terminated by '\t' 

select
	c.region_name,a.province_name,a.smonth,a.dc_code,a.dc_name,e.sales_belong_flag,a.customer_no,b.customer_name,a.goods_code,a.goods_name,
	a.department_code,a.department_name,d.classify_large_name,d.classify_middle_name,a.unit,
	sum(a.sales_qty) as sales_qty,
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
from
	(
	select
		province_code,province_name,substr(sdt,1,6) as smonth,dc_code,dc_name,customer_no,customer_name,
		department_code,department_name,goods_code,goods_name,unit,sales_qty,sales_value,profit,excluding_tax_sales,excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20200101' and '20201231'
		and channel in('2') --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and province_name not like '平台%'
	) as a
	left join   
		(
		select 
			customer_no,attribute,customer_name
		from 
			csx_dw.dws_crm_w_a_customer_m_v1 
		where 
			sdt='20201231'
		) b on b.customer_no=a.customer_no 		
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) c on a.province_code=c.province_code
	left join
		(
		select 
			goods_id,classify_large_name,classify_middle_name,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='20201231'
		group by
			goods_id,classify_large_name,classify_middle_name,classify_small_name
		) d on a.goods_code=d.goods_id
	left join
		(
		select 
			shop_id,shop_name,company_code,sales_belong_flag
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt = '20201231'
		group by 
			shop_id,shop_name,company_code,sales_belong_flag
		) e on a.customer_no = concat('S', e.shop_id)
where
	c.region_name='华东大区'
group by 
	c.region_name,a.province_name,a.smonth,a.dc_code,a.dc_name,e.sales_belong_flag,a.customer_no,b.customer_name,a.goods_code,a.goods_name,
	a.department_code,a.department_name,d.classify_large_name,d.classify_middle_name,a.unit
	
	
	
	
--===============================================================================================
-- 新签客户

insert overwrite directory '/tmp/zhangyanpeng/sales_base_new_sign_huadong_20210105' row format delimited fields terminated by '\t' 

select
	c.region_name,a.province_name,a.province_code,a.city_group_name,b.channel,b.channel_code,a.customer_no,b.customer_name,b.create_by,b.create_time,
	b.sign_time,b.estimate_contract_amount,b.attribute_code,b.attribute,
	b.first_category,b.first_category_code,b.second_category,b.second_category_code,b.third_category,b.third_category_code,
	b.work_no,b.sales_name,b.third_supervisor_name,b.third_supervisor_work_no,b.fourth_supervisor_name,b.fourth_supervisor_work_no,
	a.smonth,d.first_order_date,a.days_cnt,a.sales_value,a.customer_price,a.profit
from
	(
	select
		province_name,province_code,city_group_name,substr(sdt,1,6) as smonth,customer_no,
		count(distinct sdt) as days_cnt,
		sum(sales_value)/10000 as sales_value,
		sum(sales_value)/count(distinct sdt)/10000 as customer_price,
		sum(profit)/10000 as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20200101' and '20201231'
		--and channel in('2') --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and province_name not like '平台%'
	group by 
		province_name,province_code,city_group_name,substr(sdt,1,6),customer_no
	) as a
	left join   
		(
		select 
			customer_no,customer_name,channel,channel_code,create_by,regexp_replace(to_date(create_time),'-','') as create_time,regexp_replace(to_date(sign_time),'-','') as sign_time,estimate_contract_amount,attribute_code,attribute,
			first_category,first_category_code,second_category,second_category_code,third_category,third_category_code,work_no,sales_name,
			third_supervisor_name,third_supervisor_work_no,fourth_supervisor_name,fourth_supervisor_work_no
		from 
			csx_dw.dws_crm_w_a_customer_m_v1 
		where 
			sdt='20201231'
		) b on b.customer_no=a.customer_no 		
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) c on a.province_code=c.province_code
	left join
		(
		select 
			customer_no, 
			first_order_date
		from 
			csx_dw.ads_crm_w_a_customer_active_info
		where 
			sdt = '20201231' --可取最新分区
		group by
			customer_no,first_order_date	
		) d on a.customer_no=d.customer_no
where
	c.region_name='华东大区'
	and b.sign_time between '20200101' and '20201231'



--===============================================================================================
-- 新履约客户

insert overwrite directory '/tmp/zhangyanpeng/sales_base_new_deal_huadong_20210105' row format delimited fields terminated by '\t' 

select
	c.region_name,a.province_name,a.province_code,a.city_group_name,b.channel,b.channel_code,a.customer_no,b.customer_name,b.create_by,b.create_time,
	b.sign_time,b.estimate_contract_amount,a.smonth,d.first_order_date,a.days_cnt,a.sales_value,a.customer_price,a.profit,b.attribute_code,b.attribute,
	b.first_category,b.first_category_code,b.second_category,b.second_category_code,b.third_category,concat("'",b.third_category_code) as third_category_code
from
	(
	select
		province_name,province_code,city_group_name,substr(sdt,1,6) as smonth,customer_no,
		count(distinct sdt) as days_cnt,
		sum(sales_value)/10000 as sales_value,
		sum(sales_value)/count(distinct sdt)/10000 as customer_price,
		sum(profit)/10000 as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20200101' and '20201231'
		--and channel in('2') --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and province_name not like '平台%'
	group by 
		province_name,province_code,city_group_name,substr(sdt,1,6),customer_no
	) as a
	left join   
		(
		select 
			customer_no,customer_name,channel,channel_code,create_by,regexp_replace(to_date(create_time),'-','') as create_time,regexp_replace(to_date(sign_time),'-','') as sign_time,estimate_contract_amount,attribute_code,attribute,
			first_category,first_category_code,second_category,second_category_code,third_category,third_category_code,work_no,sales_name,
			third_supervisor_name,third_supervisor_work_no,fourth_supervisor_name,fourth_supervisor_work_no
		from 
			csx_dw.dws_crm_w_a_customer_m_v1 
		where 
			sdt='20201231'
		) b on b.customer_no=a.customer_no 		
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) c on a.province_code=c.province_code
	left join
		(
		select 
			customer_no, 
			first_order_date
		from 
			csx_dw.ads_crm_w_a_customer_active_info
		where 
			sdt = '20201231' --可取最新分区
			and first_order_date between '20200101' and '20201231'
		group by
			customer_no,first_order_date	
		) d on a.customer_no=d.customer_no
where
	c.region_name='华东大区'
	and d.customer_no is not null