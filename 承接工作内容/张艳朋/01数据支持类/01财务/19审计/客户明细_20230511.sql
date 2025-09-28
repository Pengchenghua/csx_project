drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_customer_detail;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_customer_detail
as
select
	a.customer_code,a.customer_name,a.sign_company_code,b.company_name,a.performance_province_name,a.performance_city_name
from
	(
	select
		customer_code,customer_name,sign_company_code,performance_province_name,performance_city_name
	from
		csx_dim.csx_dim_crm_customer_info
	where
		sdt='current'
		and channel_code in('1','7','9')
		and customer_code !=''
	) a 
	left join
		(
		select 
			company_code,company_name,table_type,
			case when table_type=1 then '彩食鲜'
				when table_type=2 then '永辉'
				else '其他' 
			end table_type_name
		from 
			csx_dim.csx_dim_basic_company
		where 
			sdt = 'current'
			-- and table_type=1
		) b on b.company_code=a.sign_company_code
;

select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_customer_detail