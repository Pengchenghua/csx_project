-- 附件3 B端

INSERT OVERWRITE DIRECTORY '/tmp/zhangyanpeng/20210129_B_08' row FORMAT DELIMITED fields TERMINATED BY '\t'

select
	a.customer_no,
	a.customer_name,
	a.province_name,	
	a.sdt,	
	concat("'",a.order_no) as order_no,
	a.return_flag,
	a.order_category_name,
	a.goods_code,
	a.goods_name,
	a.company_code,
	a.sign_company_code,	
	sum(sales_value) as sales_value,	
	sum(profit) as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,	
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate,
	coalesce(round(0.01*sum( if(b.code is null, null, sales_value) ), 2), 0) as channel_value,
	coalesce(round(0.99*sum( if(b.code is null, null, sales_value) ), 2), 0) as caiwu_sales_value,
	coalesce(round(0.01*sum( if(b.code is null, null, excluding_tax_sales) ), 2), 0) as excluding_tax_channel_value,
	coalesce(round(0.99*sum( if(b.code is null, null, excluding_tax_sales) ), 2), 0) as excluding_tax_caiwu_sales_value
from 
	(
	select 
		* 
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between '20200801' and '20200831' 
		and channel_code in('1','9')
	) a 
	left join
		(
		SELECT 
			code 
		FROM 
			csx_dw.dws_basic_w_a_company_code
		WHERE 
			sdt = 'current' 
			and table_type = 2
		group by 
			code
		) b on a.sign_company_code = b.code
group by 
	a.customer_no,
	a.customer_name,
	a.province_name,	
	a.sdt,	
	concat("'",a.order_no),
	a.return_flag,
	a.order_category_name,
	a.goods_code,
	a.goods_name,
	a.company_code,
	a.sign_company_code