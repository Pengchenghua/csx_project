insert overwrite directory '/tmp/zhangyanpeng/linshi_1104_3' row format delimited fields terminated by '\t' 


select 
	first_category,
	customer_no,
	customer_name,
	case when channel ='7' then 'BBC'	
		when channel in ('1') and attribute='合伙人客户' then '城市服务商' 
		when channel in ('1') and attribute='贸易客户'  then '贸易客户' 
		when channel in ('1') and order_kind='WELFARE' then '福利单'  
		when channel in ('1') and attribute not in('合伙人客户','贸易客户') and (order_kind<>'WELFARE' or order_kind is null) then '日配单'	 
		else '其他' 
	end as customer_type,
	merchant_type,
	work_no,
	sales_name,
	count(distinct goods_code) as sku_cnt,
	count(distinct order_no) as order_cnt,
	'' as customer_complaint,
	sum(sales_value) as sales_value
from 
	csx_dw.dws_sale_r_d_customer_sale 
where 
	sdt between '20201001' and '20201031'
	and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
	and channel in ('1','7')
	and province_name not like '平台%'
	and province_name='四川省'
	and customer_no>'0'
group by
	first_category,
	customer_no,
	customer_name,
	case when channel ='7' then 'BBC'	
		when channel in ('1') and attribute='合伙人客户' then '城市服务商' 
		when channel in ('1') and attribute='贸易客户'  then '贸易客户' 
		when channel in ('1') and order_kind='WELFARE' then '福利单'  
		when channel in ('1') and attribute not in('合伙人客户','贸易客户') and (order_kind<>'WELFARE' or order_kind is null) then '日配单'	 
		else '其他' 
	end,
	merchant_type,
	work_no,
	sales_name
	
	
	
	

--===========================================================================================	
select
	first_category,
	second_category,
	third_category,
	customer_no,
	customer_name,
	concat_ws('/',collect_set(customer_type)) as customer_type,
	concat_ws('/',collect_set(source_type)) as source_type,
	concat_ws('/',collect_set(work_no)) as work_no,
	concat_ws('/',collect_set(sales_name)) as sales_name,
	count(distinct goods_code) as sku_cnt,
	count(distinct order_no) as order_cnt,
	'' as customer_complaint,
	sum(sales_value) as sales_value
from
	(
	select 
		first_category,
		second_category,
		third_category,
		customer_no,
		customer_name,
		case when channel ='7' then 'BBC'	
			when channel in ('1') and attribute='合伙人客户' then '城市服务商' 
			when channel in ('1') and attribute='贸易客户'  then '贸易客户' 
			when channel in ('1') and order_kind='WELFARE' then '福利单'  
			when channel in ('1') and attribute not in('合伙人客户','贸易客户') and (order_kind<>'WELFARE' or order_kind is null) then '日配单'	 
			else '其他' 
		end as customer_type,
		case when merchant_type='MANUAL_BULK' then '手工单'
			when merchant_type='MAPP_BULK' then '小程序'
			else merchant_type
		end as source_type,
		work_no,
		sales_name,
		goods_code,
		order_no,
		'' as customer_complaint,
		sales_value
	from 
		csx_dw.dws_sale_r_d_customer_sale 
	where 
		sdt between '20201001' and '20201031'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in ('1','7')
		and province_name not like '平台%'
		and province_name='四川省'
		and customer_no>'0'
	) t1
group by
	first_category,
	second_category,
	third_category,
	customer_no,
	customer_name

		
		