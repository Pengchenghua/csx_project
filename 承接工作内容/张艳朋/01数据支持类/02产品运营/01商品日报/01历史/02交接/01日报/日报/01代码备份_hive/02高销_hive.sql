select 
	a.*
from
	(
	select 
		a.*,
		row_number() over (partition by sales_city ORDER BY cc desc) rank
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
				department_name,
				division_name,
				sum(sales_qty)b,
				sum(sales_value)c,
				sum(profit) a 
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt >= regexp_replace(date_sub(current_date,if(pmod(datediff(current_date, '1920-01-01') - 3, 7)=1,3,1)),'-','')
				and attribute_code != '5'
				and channel_name LIKE '大客户%'
				and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
			group by 
				substr(sdt,1,6),
				customer_no,
				province_name,
				sales_city,
				sales_date,
				goods_code,
				regexp_replace(goods_name, '\n|\t|\r', ''),
				department_name,division_name
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
			sum(c)>0 
		) a
	)a
where 
	rank<=10