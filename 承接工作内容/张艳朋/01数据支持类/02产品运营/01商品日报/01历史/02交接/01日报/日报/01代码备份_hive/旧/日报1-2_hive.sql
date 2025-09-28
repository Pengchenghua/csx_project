select 
  sales_city,
  count(distinct b.customer_no)
from
	(
	select  
		substr(sdt,1,6)smonth,
		customer_no,
		province_name,
		sales_city,
		case when division_name in ('生鲜部','加工部')then '生鲜'
		when division_name in ('食品类')then '食品'
		when division_name in ('用品类','易耗品','服装')then '非食品'
		end as department_name
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt >= regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','')
		and sdt <= regexp_replace(date_sub(current_date,1),'-','')
		and attribute_code != '5'
		and channel_name LIKE '大客户%'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		and sales_city in (
			'成都市',
			'北京市',
			'福州市',
			'贵阳市',
			'杭州市',
			'合肥市',
			'南京市',
			'南平市',
			'宁波市',
			'莆田市',
			'泉州市',
			'厦门市',
			'上海市',
			'石家庄市',
			'苏州市',
			'西安市',
			'重庆市',
			'深圳市')
	) b 
	left join 
		(
		select 
			distinct customer_no,
			substr(sdt,1,6)smonth 
		from 
			csx_dw.csx_partner_list 
		) d on d.customer_no= b.customer_no and b.smonth=d.smonth
where 
	d.customer_no is null
	and department_name in ('食品','非食品')
group by  
	sales_city