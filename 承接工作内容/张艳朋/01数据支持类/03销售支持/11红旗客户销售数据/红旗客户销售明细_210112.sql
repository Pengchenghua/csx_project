--====================================================================================================================================
--三级分类维度
select
	a.region_name,
	a.province_name,
	a.customer_no,
	a.customer_name,
	a.smonth,
	a.sales_value,
	a.profit,
	a.profit_rate,
	a.excluding_tax_sales,
	a.excluding_tax_profit,
	a.excluding_tax_profit_rate
from 
	(
	select 
		region_name,province_name,customer_no,customer_name,substr(sdt,1,6) as smonth,
		sum(sales_value)as sales_value,
		sum(profit)as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit,
		sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200101' and '20201231'
		and customer_name like '%红%旗%'
		--and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	group by 
		region_name,province_name,customer_no,customer_name,substr(sdt,1,6)
	)a 
	left join   
		(
		select 
			customer_no,customer_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name
		) b on b.customer_no=a.customer_no
