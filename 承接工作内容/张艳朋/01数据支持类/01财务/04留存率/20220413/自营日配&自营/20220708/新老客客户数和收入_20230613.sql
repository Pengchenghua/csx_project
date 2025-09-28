-- B+BBC新老客行业分析
drop table if exists csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail;
create table csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail
as	
select
	substr(a.sdt,1,6) as smonth,
	count(distinct a.customer_code) as customer_cnt,
	count(distinct if(substr(a.sdt,1,6)=substr(d.first_sale_date,1,6),a.customer_code,null)) as new_customer_cnt,
	count(distinct if(substr(a.sdt,1,6)!=substr(d.first_sale_date,1,6),a.customer_code,null)) as old_customer_cnt,
	sum(a.sale_amt_no_tax) as sale_amt_no_tax,
	sum(if(substr(a.sdt,1,6)=substr(d.first_sale_date,1,6),a.sale_amt_no_tax,0)) as new_sale_amt_no_tax,
	sum(if(substr(a.sdt,1,6)!=substr(d.first_sale_date,1,6),a.sale_amt_no_tax,0)) as old_sale_amt_no_tax
from 
	(
	select
		sdt,customer_code,business_type_code,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		(sdt between '20220101' and '20220430' or sdt between '20230101' and '20230430')
		and channel_code in ('1', '7', '9')
	) a 
	left join
		(
		select
			customer_code,first_sale_date
		from
			csx_dws.csx_dws_crm_customer_active_di
		where 
			sdt='current'	
		) d on a.customer_code = d.customer_code
group by 
	substr(a.sdt,1,6)
;
select * from csx_analyse_tmp.csx_analyse_tmp_b_category_sale_detail;