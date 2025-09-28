
	select
		a.customer_no,
		b.customer_name,
		a.province_name,
		a.sdt,
		concat("'",a.order_no) as order_no,
		a.return_flag,
		case when a.order_category_name='NORMAL' then '正常单' when a.order_category_name='WELFARE' then '福利单' else '其他' end as order_category_name,
		a.goods_code,
		c.goods_name,
		sum(a.sales_value) sales_value,
		sum(a.profit) as profit,
		sum(a.profit)/abs(sum(a.sales_value)) as profit_rate,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit,
		sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
	from 
		(
		select 
			sdt,province_code,province_name,
			customer_no,order_no,order_category_name,return_flag,goods_code,sales_value,profit,excluding_tax_sales,excluding_tax_profit
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20201101' and '20201130'
			and channel_code in ('1','7','9')
			and customer_no in ('100326','100563','101482','101585','101653','101870','102202','102225','102251','102524','102633','102691','102754','102901','103044','103199','103215','103355','103759','103784','103830','103856','103964','104601','104607','104612','112574','112675','112747','112871','112903','113032','113390','113576','113588','113635','113758','113779','113809','113837','113870','114085','114095','114162')
		)a  
		left join   --CRM客户信息取每月最后一天 剔除合伙人
			(
			select 
				customer_no,customer_name,attribute,attribute_code
			from 
				csx_dw.dws_crm_w_a_customer_m_v1 
			where 
				sdt ='20201130'
			group by
				customer_no,customer_name,attribute,attribute_code
			) b on b.customer_no=a.customer_no
		left join   --商品表
			(
			select 
				goods_id,goods_name,unit,classify_large_name,classify_middle_name,classify_small_name
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt ='20201130'
			group by
				goods_id,goods_name,unit,classify_large_name,classify_middle_name,classify_small_name
			) c on a.goods_code=c.goods_id
	group by 
		a.customer_no,
		b.customer_name,
		a.province_name,
		a.sdt,
		concat("'",a.order_no),
		a.return_flag,
		case when a.order_category_name='NORMAL' then '正常单' when a.order_category_name='WELFARE' then '福利单' else '其他' end,
		a.goods_code,
		c.goods_name