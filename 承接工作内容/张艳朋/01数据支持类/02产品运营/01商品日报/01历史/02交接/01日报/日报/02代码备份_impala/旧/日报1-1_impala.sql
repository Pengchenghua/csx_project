select 
	sales_city,
	department_name,
	sum(cc),
	sum(dd),
	count(distinct customer_no),
	sum(aa),
	sum(bb)
from
	(
	select 
		province_name,
		sales_city,
		department_name,
		customer_no,
		count(distinct sales_date)aa,
		sum(a)bb,
		sum(b)cc,
		sum(c)dd
	from
		(
		select 
			province_name,
			sales_city,
			department_name,
			b.customer_no,
			sales_date,
			count(distinct goods_code)a,
			sum(AAA)b,
			sum(BBB)c
		from
			(
			select  
				substr(sdt,1,6)smonth,
				customer_no,
				province_name,
				sales_city,
				sales_date,
				goods_code,
				case when division_name in ('生鲜部','加工部')then '生鲜'
				when division_name in ('食品类')then '食品'
				when division_name in ('用品类','易耗品','服装')then '非食品'
				end as department_name,
				sum(sales_value)AAA,
				sum(profit)BBB 
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= regexp_replace(to_date(trunc(date_sub(now(),1),'MM')),'-','')
				and sdt <= regexp_replace(to_date(date_sub(now(),1)),'-','')
				and attribute_code != 5
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
			group by 
				substr(sdt,1,6),
				customer_no,
				province_name,
				sales_city,
				sales_date,
				goods_code,
				case when division_name in ('生鲜部','加工部') then '生鲜'
					when division_name in ('食品类')then '食品'
					when division_name in ('用品类','易耗品','服装')then '非食品'
				end 
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
		group by   
			province_name,
			sales_city,
			department_name,
			b.customer_no,
			sales_date
		)f
	group by 
		province_name,
		sales_city,
		department_name,
		customer_no
	)e
group by 
	sales_city,department_name
order by 
	sales_city,department_name