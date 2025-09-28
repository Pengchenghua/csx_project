--====================================================================================================================================
--三级分类维度
select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sum(a.sales_value) sales_value,
	--sum(a.profit) profit,
	sum(a.profit)/sum(a.sales_value) as profit_rate,
	count(distinct a.customer_no) by_cust_count,
	count(distinct a.goods_code) by_goods_count

from 
	(
	select 
		substr(sdt,1,6) smonth,customer_no,channel,goods_code,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale 
	where 
		sdt between '20200501' and '20201031'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in('1')
		and province_name not like '平台%'
		and dc_code ='W0A8' --仓库
	group by 
		substr(sdt,1,6),customer_no,channel,goods_code
	)a  
	left join   --CRM客户信息取每月最后一天 剔除合伙人
		(
		select 
			substr(sdt,1,6) smonth,customer_no,customer_name,attribute,attribute_code
		from 
			csx_dw.dws_crm_w_a_customer_m_v1 
		where 
			sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
			--and (attribute_code!='5' or attribute_code is null)
			and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
				)  --sdt为每月最后一天
		) b on b.customer_no=a.customer_no and b.smonth=a.smonth
	left join   
		(
		select 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='current'
		) c on a.goods_code=c.goods_id
where 
	a.channel in ('1')
	and b.attribute not in('合伙人客户','贸易客户') --日配+福利
group by 
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name
;







--====================================================================================================================================
--二级分类维度
select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	sum(a.sales_value) sales_value,
	--sum(a.profit) profit,
	sum(a.profit)/sum(a.sales_value) as profit_rate,
	count(distinct a.customer_no) by_cust_count,
	count(distinct a.goods_code) by_goods_count

from 
	(
	select 
		substr(sdt,1,6) smonth,customer_no,channel,goods_code,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale 
	where 
		sdt between '20200501' and '20201031'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and channel in('1')
		and province_name not like '平台%'
		and dc_code ='W0A8' --仓库
	group by 
		substr(sdt,1,6),customer_no,channel,goods_code
	)a  
	left join   --CRM客户信息取每月最后一天 剔除合伙人
		(
		select 
			substr(sdt,1,6) smonth,customer_no,customer_name,attribute,attribute_code
		from 
			csx_dw.dws_crm_w_a_customer_m_v1 
		where 
			sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
			--and (attribute_code!='5' or attribute_code is null)
			and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
				)  --sdt为每月最后一天
		) b on b.customer_no=a.customer_no and b.smonth=a.smonth
	left join   
		(
		select 
			goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt ='current'
		) c on a.goods_code=c.goods_id
where 
	a.channel in ('1')
	and b.attribute not in('合伙人客户','贸易客户') --日配+福利
group by 
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name