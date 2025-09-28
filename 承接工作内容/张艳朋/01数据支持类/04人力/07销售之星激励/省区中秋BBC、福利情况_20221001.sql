--==================================================================================================================================================================================
set current_start_day ='20220801';

set current_end_day ='20220930';

--set last_month_start_day ='20210801';

--set last_month_end_day ='20210831';

--01_销售员激励案_Q3福利激励案_百万精英奖

insert overwrite directory '/tmp/zhangyanpeng/20220815_01' row format delimited fields terminated by '\t'

select
	concat(${hiveconf:current_start_day},'-',${hiveconf:current_end_day}) as sdt_s,
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	a.business_type_name,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	sum(a.excluding_tax_sales) as excluding_tax_sales,
	sum(a.excluding_tax_profit) as excluding_tax_profit
from 
	(
	select 
		customer_no,business_type_name,
		sales_value,
		if(goods_code not in ('8718','8708','8649'),profit,0) as profit,
		excluding_tax_sales,
		if(goods_code not in ('8718','8708','8649'),excluding_tax_profit,0) as excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between ${hiveconf:current_start_day} and ${hiveconf:current_end_day}
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('2','6') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		-- and goods_code not in ('8718','8708','8649')
	) a  
	left join   
		(
		select 
			customer_no,customer_name,sales_region_name,province_name,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:current_end_day}
		) b on b.customer_no=a.customer_no
group by 
	b.sales_region_name,
	b.province_name,
	b.city_group_name,
	a.business_type_name
;

