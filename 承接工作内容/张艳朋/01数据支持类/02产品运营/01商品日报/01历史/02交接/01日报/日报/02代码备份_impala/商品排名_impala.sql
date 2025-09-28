select
    t1.r as `辅助列`, 
	t1.province_name AS `销售省区`,
	t1.sales_city as `城市`,
	t1.goods_code as`商品编码`,
	t1.name as`商品名称`,
	t1.sales_value as`销售额`,
	t1.profit as`毛利额`,
	t1.AAA as `客户数`,
	rn as `排名`
from
	(
	select
		concat(t1.sales_city,cast(row_number() over(partition by t1.sales_city order by t1.sales_value desc) as string)) as r, 
		t1.province_name,
		t1.sales_city,
		t1.goods_code,
		t1.name,
		t1.sales_value,
		t1.profit,
		t1.AAA,
		row_number() over(partition by t1.sales_city order by t1.sales_value desc) as rn
	from 
		(
		select 
			a.province_name,
			a.sales_city,
			a.goods_code,
			a.name,
			sum(a.sales_value)as sales_value,
			sum(a.profit)as profit,
			count(distinct AAA)as AAA
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
					sdt >= regexp_replace(to_date(trunc(date_sub(now(),1),'MM')),'-','')
					and sdt <= regexp_replace(to_date(date_sub(now(),1)),'-','')
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
						sdt = regexp_replace(to_date(date_sub(now(),1)),'-','')
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
		) t1
	)t1
where
	rn<=10