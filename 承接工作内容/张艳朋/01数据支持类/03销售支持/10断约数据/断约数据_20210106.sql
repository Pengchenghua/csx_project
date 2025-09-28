--====================================================================================================================================
--断约
select
	a.province_name,
	a.customer_no,
	c.customer_name,
	a.sales_value,
	a.profit,
	case when b.customer_no is null then '断约' else '未断约' end as is_break_appointment
from 
	(
	select 
		province_name,customer_no,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200401' and '20200630'
		and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	group by 
		province_name,customer_no
	)a 
	left join   
		(
		select 
			province_name,customer_no,
			sum(sales_value)as sales_value,
			sum(profit)as profit
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20200701' and '20200930'
			and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			province_name,customer_no
		) b on b.province_name=a.province_name and b.customer_no=a.customer_no
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute
		) c on c.customer_no = a.customer_no
		
		
--====================================================================================================================================
--断约
select
	a.province_name,
	a.customer_no,
	c.customer_name,
	a.sales_value,
	a.profit
from 
	(
	select 
		province_name,customer_no,
		sum(sales_value)as sales_value,
		sum(profit)as profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200701' and '20200930'
		and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	group by 
		province_name,customer_no
	)a 
	--left join   
	--	(
	--	select 
	--		province_name,customer_no,
	--		sum(sales_value)as sales_value,
	--		sum(profit)as profit
	--	from 
	--		csx_dw.dws_sale_r_d_detail 
	--	where 
	--		sdt between '20200701' and '20200930'
	--		and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	--		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	--	group by 
	--		province_name,customer_no
	--	) b on b.province_name=a.province_name and b.customer_no=a.customer_no
	left join
		(
		select 
			customer_no,customer_name,attribute
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute
		) c on c.customer_no = a.customer_no

