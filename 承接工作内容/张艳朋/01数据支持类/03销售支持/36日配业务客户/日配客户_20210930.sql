insert overwrite directory '/tmp/zhangyanpeng/20210930_14' row format delimited fields terminated by '\t'


select
	smonth,sales_region_name,sales_province_name,city_group_name,
	a.customer_no,b.customer_name,b.work_no,sales_name,
	first_category_name,second_category_name,third_category_name,
	sales_value,profit,profit_rate,cnt
from
	(
	select
		substr(sdt,1,6) as smonth,customer_no,
		sum(sales_value) as sales_value, --含税销售额
		sum(profit) as profit, --含税毛利额
		sum(front_profit) as front_profit, --前端含税毛利
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(front_profit)/abs(sum(sales_value)) as front_profit_rate,
		count(distinct sdt) cnt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt between '20210101' and '20210929'
		and channel_code in ('1','7','9') 
		and business_type_code in ('1')
	group by 
		substr(sdt,1,6),customer_no
	having
		sum(profit)/abs(sum(sales_value))<=0.04
	) a
	left join
		(
		select
			customer_no,customer_name,work_no,sales_name,sales_region_name,sales_province_name,
			city_group_name,first_category_name,second_category_name,third_category_name,cooperation_mode_name
		from
			csx_dw.dws_crm_w_a_customer 
		where
			sdt='current'
		) b on b.customer_no=a.customer_no