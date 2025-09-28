-- 8-9月 各省区 销售员 工号 福利单 业绩 销售额 毛利额 前端毛利额 

	
select
	a.sdt_s,
	b.sales_province,
	b.work_no,
	b.sales_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	sum(front_profit) as front_profit
from
	(
	select
		substr(sdt,1,6) as sdt_s,
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from
		csx_dw.dws_sale_r_d_customer_sale
	where
		sdt between '20200801' and '20200831'
		and ((channel ='1' and order_kind='WELFARE') or channel ='7')
		-- and order_kind='WELFARE'
		and province_name not like '平台%'
	group by
		substr(sdt,1,6),customer_no
	) as a
	left join
		(
		select 
			substr(sdt,1,6) as sdt_s,customer_no,customer_name,work_no,sales_name,sales_province
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20200831'
			and customer_no<>''
		) as b on a.customer_no=b.customer_no and a.sdt_s=b.sdt_s
group by
	a.sdt_s,
	b.sales_province,
	b.work_no,
	b.sales_name	
	
union all	
	
select
	a.sdt_s,
	b.sales_province,
	b.work_no,
	b.sales_name,
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	sum(front_profit) as front_profit
from
	(
	select
		substr(sdt,1,6) as sdt_s,
		customer_no,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from
		csx_dw.dws_sale_r_d_customer_sale
	where
		sdt between '20200901' and '20200930'
		and ((channel ='1' and order_kind='WELFARE') or channel ='7')
		-- and order_kind='WELFARE'
		and province_name not like '平台%'
	group by
		substr(sdt,1,6),customer_no
	) as a
	left join
		(
		select 
			substr(sdt,1,6) as sdt_s,customer_no,customer_name,work_no,sales_name,sales_province
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = '20200930'
			and customer_no<>''
		) as b on a.customer_no=b.customer_no and a.sdt_s=b.sdt_s
group by
	a.sdt_s,
	b.sales_province,
	b.work_no,
	b.sales_name	
		
