
-- 福利BBC销售剔除茅台
select
	concat('20230501-','20230630') as sdt_s,
	b.performance_region_name,
	b.performance_province_name,
	b.performance_city_name,
	b.sales_user_number,
	b.sales_user_name,
	a.customer_code,
	b.customer_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	sum(sale_amt_no_tax)sale_amt_no_tax,
	sum(profit_no_tax)profit_no_tax,
	sum(profit_no_tax)/sum(sale_amt_no_tax) profit_no_tax_rate,
	row_number() over(order by sum(a.sale_amt) desc) as rn
from 
	(
	select 
		customer_code,
		sum(sale_amt)as sale_amt,
		sum(profit) as profit,
		sum(sale_amt_no_tax)sale_amt_no_tax,
		sum(profit_no_tax)profit_no_tax
	from 
	 	csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230501' and '20230630' -- regexp_replace(to_date(date_sub(current_date(),1)),'-','') -- '20221231'
	--	and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in (2,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and goods_code not in ('8718','8708','8649','840509') --剔除茅台
	group by 
		customer_code

	) a  
	left join   
		(
		select 
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,sales_user_number,sales_user_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt='current' -- 'current'
		) b on b.customer_code=a.customer_code
group by 
	b.performance_region_name,
	b.performance_province_name,
	b.performance_city_name,
	b.sales_user_number,a.customer_code,
	b.customer_name,
	b.sales_user_name