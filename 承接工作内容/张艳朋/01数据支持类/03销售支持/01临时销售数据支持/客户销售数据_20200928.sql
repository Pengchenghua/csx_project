select
	a.dc_province_name,
	a.customer_no,
	b.customer_name,
	a.sales_year,
	a.channel,
	a.sales_value, --含税销售额
	a.excluding_tax_sales, --不含税销售额
	a.profit, --含税毛利
	a.excluding_tax_profit, --不含税毛利
	a.front_profit, --前端含税毛利
	a.mid_profit --中台含税毛利
from
	(
	select
		dc_province_name,
		customer_no,
		channel,
		substr(sales_date,1,4) as sales_year,
		sum(sales_value) as sales_value, --含税销售额
		sum(excluding_tax_sales) as excluding_tax_sales, --不含税销售额
		sum(profit) as profit, --含税毛利
		sum(excluding_tax_profit) as excluding_tax_profit, --不含税毛利
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit-front_profit) as mid_profit --中台含税毛利
	from 
		csx_dw.sale_item_m 
	where
		sdt between '20180101' and '20181231'
		and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
		and customer_no in ('103189','103369','104532','104701','105050','105290','107113','107125')
	group by
		dc_province_name,
		customer_no,
		channel,
		substr(sales_date,1,4)
	) as a
	left join
		(
		select 
			customer_no,customer_name
		from 
			csx_dw.dws_crm_w_a_customer_m_v1 a
		where 
			sdt='20200927'
		group by
			customer_no,customer_name
		) b on b.customer_no=a.customer_no
	