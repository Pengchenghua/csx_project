select 
	a.*,
	b.dd 
from
	(
	select 
		a.*,
		row_number() over (partition by sales_city ORDER BY aa) rank -- desc
	from 
		(
		select
			a.province_name,
			a.sales_city,
			case when a.division_name in ('生鲜部','加工部')then '生鲜'
			when a.division_name in ('食品类')then '食品'
			when a.division_name in ('用品类','易耗品','服装')then '非食品'
			end,
			a.department_name,
			a.goods_code,
			a.goods_name,
			sum(c)cc,
			sum(a)aa,
			sum(a)/sum(c) as dd,
			sum(b)bb,
			count(distinct a.customer_no)
		from
			(
			select   
				substr(sdt,1,6)smonth,
				customer_no,
				province_name,
				sales_city,
				goods_code,
				regexp_replace(goods_name, '\n|\t|\r', '') as goods_name,
				department_name,division_name,
				sum(sales_qty)b,
				sum(sales_value)c,
				sum(profit) a 
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= regexp_replace(to_date(date_sub(now(),if(pmod(datediff(to_date(now()), '1920-01-01') - 3, 7)=1,3,1))),'-','')
				and attribute_code != 5
				and channel_name LIKE '大客户%'
				and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
			group by 
				substr(sdt,1,6),
				customer_no,
				province_name,
				sales_city,
				sales_date,
				goods_code,
				goods_name, 
				department_name,
				division_name
			) a 
			left join 
				(
				select 
					distinct customer_no,
					substr(sdt,1,6)smonth 
				from 
					csx_dw.csx_partner_list 
				) d on d.customer_no= a.customer_no and d.smonth=a.smonth
		where 
			d.customer_no is null
		group by
			a.province_name,
			a.sales_city,
			case when a.division_name in ('生鲜部','加工部')then '生鲜'
			when a.division_name in ('食品类')then '食品'
			when a.division_name in ('用品类','易耗品','服装')then '非食品'
			end,
			a.department_name,
			a.goods_code,
			a.goods_name
		having 
			sum(c)>0 and sum(a)<0
		) a
	)a
	left join
		(
		select 
			province_name,
			sales_city,
			goods_code,
			count(distinct b.sales_date) dd
		from
			(
			select  
				substr(sdt,1,6)smonth,
				customer_no,
				province_name,
				sales_city,
				sales_date,
				goods_code,
				department_name,
				sum(profit)BBB 
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= regexp_replace(to_date(trunc(date_sub(now(),1),'MM')),'-','')
				and sdt <= regexp_replace(to_date(date_sub(now(),1)),'-','')
				and attribute_code != 5
				and channel_name LIKE '大客户%'
				and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
			group by 
				substr(sdt,1,6),
				customer_no,
				province_name,
				sales_city,
				sales_date,department_name,
				goods_code
			having 
				BBB<0
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
			goods_code
		) b on b.province_name=a.province_name and a.sales_city=b.sales_city and a.goods_code=b.goods_code
where 
	rank<=10
order by
	a.sales_city,rank