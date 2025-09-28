--==================================================================================================================================================================================

drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_detail;
create table csx_analyse_tmp.csx_analyse_tmp_customer_detail
as
select
	substr(sdt,1,4) as syear,
	a.performance_region_name,
	a.performance_province_name,
	a.customer_code,
	b.customer_name,
	a.business_type_name,
	a.sales_user_number,
	a.sales_user_name,
	sum(a.sale_amt) as sale_amt
from 
	(
	select 
		sdt,performance_region_name,performance_province_name,customer_code,sale_amt,profit,business_type_name,sales_user_number,sales_user_name
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20220101' and '20230530'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code !=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		-- and customer_code not in ()
	) a  
	left join   
		(
		select
			customer_code,customer_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
group by 
	substr(sdt,1,4),
	a.performance_region_name,
	a.performance_province_name,
	a.customer_code,
	b.customer_name,
	a.business_type_name,
	a.sales_user_number,
	a.sales_user_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_customer_detail