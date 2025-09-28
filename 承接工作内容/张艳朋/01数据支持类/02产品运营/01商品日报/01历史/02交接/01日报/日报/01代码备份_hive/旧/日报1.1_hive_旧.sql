insert overwrite directory '/tmp/zhangyanpeng/linshi_01/daily-s' row format delimited fields terminated by '\t' 
select 
	a.province_name AS `销售省区`,
	a.sales_city as `城市`,
	a.goods_code as`商品编码`,
	a.name as`商品名称`,
	sum(a.sales_value)as`销售额`,
	sum(a.profit)as`毛利额`,
	count(distinct AAA)as`客户数`
from 
	(
	select
		substr(sdt,1,6)smonth,
		a.customer_no AAA,
		province_name,
		sales_city,
		a.goods_code,
		regexp_replace(goods_name, '\n|\t|\r', '') AS `name`,
		sales_value,
		profit
	from
		(
		select 
			* 
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','')
			and sdt <= regexp_replace(date_sub(current_date,1),'-','')
			and attribute_code != 5
			and channel_name LIKE '大客户%'
			and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		) a 
		left join
			(
			select
				customer_no,
				first_sale_day
			from 
				csx_dw.ads_sale_w_d_ads_customer_sales_q
			where 
				sdt = regexp_replace(date_sub(current_date,1),'-','')
			) b on a.customer_no = b.customer_no
	) a
	left join 
		(
		select 
			distinct customer_no,
			substr(sdt,1,6)smonth 
		from 
			csx_dw.csx_partner_list 
		) d on d.customer_no=a.AAA and d.smonth=a.smonth
where 
	d.customer_no is null
group by 
	a.province_name,
	a.sales_city,
	a.goods_code,
	a.name 