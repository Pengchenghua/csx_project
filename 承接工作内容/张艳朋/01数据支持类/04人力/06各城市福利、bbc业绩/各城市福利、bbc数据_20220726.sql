--insert overwrite directory '/tmp/zhangyanpeng/20220704_01' row format delimited fields terminated by '\t'

select
	substr(a.sdt,1,6) smonth,
	b.province_name,
	b.city_group_name,
	a.business_type_name,
	sum(a.sales_value) sales_value,
	sum(a.excluding_tax_profit) excluding_tax_profit
from	
	(
	select 
		sdt,customer_no,business_type_name,sales_value,profit,excluding_tax_sales,excluding_tax_profit
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210801' and '20210930'
		and channel_code in ('1','7','9')
		and business_type_code in ('2','6') -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and goods_code not in ('8718','8708','8649') -- 剔除酒水
	) a 
	left join
		(
		select 
			customer_no,customer_name,attribute,attribute_desc,first_category_name,second_category_name,third_category_name,
			province_code,province_name,city_group_code,city_group_name
		from 
			csx_dw.dws_crm_w_a_customer
		where
			sdt='20220725'
		) b on b.customer_no=a.customer_no	
group by 
	substr(a.sdt,1,6),
	b.province_name,
	b.city_group_name,
	a.business_type_name
;	


select 
	substr(sdt,1,6) smonth,
	province_name,
	--city_name,
	city_group_name,
	business_type_name,
	sum(sales_value) sales_value,
	sum(excluding_tax_profit) excluding_tax_profit
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt between '20210801' and '20210930'
	and channel_code in ('1','7','9')
	and business_type_code in ('2','6') -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	and goods_code not in ('8718','8708','8649') -- 剔除酒水
group by 
	substr(sdt,1,6),
	province_name,
	--city_name,
	city_group_name,
	business_type_name
;