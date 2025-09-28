--====================================================================================================================================
-- 断约客户
insert overwrite directory '/tmp/zhangyanpeng/20210119_break_contract' row format delimited fields terminated by '\t'

select
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.customer_no,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	regexp_replace(to_date(b.sign_time),'-','') as sign_time,
	a.sales_value,
	a.profit,
	a.profit_rate,
	a.excluding_tax_sales,
	a.excluding_tax_profit,
	a.excluding_tax_profit_rate
from
	(
	select
		region_name,
		province_name,
		city_group_name,
		customer_no,
		sum(sales_value)as sales_value,	
		sum(profit)as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		sum(excluding_tax_sales) as excluding_tax_sales,
		sum(excluding_tax_profit) as excluding_tax_profit,
		sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200101' and '20201031'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	group by 
		region_name,province_name,city_group_name,customer_no
	having
		sum(sales_value)>10000
	) as a
	left join
		(
		select
			customer_no,customer_name,first_category_name,second_category_name,third_category_name,sign_time
		from
			csx_dw.dws_crm_w_a_customer
		where
			sdt='current'
		group by 
			customer_no,customer_name,first_category_name,second_category_name,third_category_name,sign_time
		) as b on b.customer_no=a.customer_no
	left join
		(
		select
			customer_no
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20201101' and '20210118'
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and business_type_code in ('1') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
where
	c.customer_no is null

