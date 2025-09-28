-- ====================================================================================================================================================

insert overwrite directory '/tmp/zhangyanpeng/20210526_linshi_2' row format delimited fields terminated by '\t' 

select 
	a.province_name,a.city_group_name,a.customer_no,b.customer_name,b.sales_name,a.sales_value,a.profit,
	c.table_type_name,b.sign_time,a.sign_company_code,c.name,b.first_category_name,b.second_category_name,b.third_category_name
from 
	(
	select
		province_name,city_group_name,sign_company_code,customer_no,
		sum(sales_value) sales_value,
		sum(profit) profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20200101' and sdt<='20210525'
		and channel_code in('1','7','9')
		--and sign_company_code in('1933','2121')
	group by 
		province_name,city_group_name,sign_company_code,customer_no
	)a 
	join 
		(
		select 
			customer_no,customer_name,sign_time,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='current'
			-- and second_category_code='303' -- 部队
		)b on a.customer_no=b.customer_no
	left join
		(
		select 
			code,name,table_type,
			case when table_type=1 then '彩食鲜'
				when table_type=2 then '永辉'
				else '其他' 
			end table_type_name
		from 
			csx_dw.dws_basic_w_a_company_code
		where 
			sdt = '20210525'
		)c on a.sign_company_code = c.code
where c.table_type=1;




	