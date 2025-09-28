set e_date='${enddate}';


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table  csx_tmp.ads_sss_r_d_customer_accounts_fr partition(sdt)
select     
             x.customer_no,
            customer_name,
            channel_code,
            channel_name,
            attribute_code,
            attribute_name,
            first_category_code,
            first_category_name,
            second_category_code,
            second_category_name,
            third_category_code,
            third_category_name,
            x.sales_id,
            x.work_no,
            x.sales_name,
			g.leader_id as sales_supervisor_id ,
			g.leader_user_number as sales_supervisor_work_no ,
			g.leader_name as sales_supervisor_name ,
            x.province_code,
            x.province_name,
            city_code,
            city_name,
            company_code,
            company_name,
            payment_terms,
            payment_name,
            payment_days,
            customer_level,
            credit_limit,
            temp_credit_limit,
            temp_begin_time,
            temp_end_time,
            overdue_amount,
            overdue_amount1,
            overdue_amount15,
            overdue_amount30,
            overdue_amount60,
            overdue_amount90,
            overdue_amount120,
            overdue_amount180,
            overdue_amount365,
            overdue_amount730,
            overdue_amount1095,
            non_overdue_amount,
            receivable_amount,
            bad_debt_amount,
            max_overdue_day,
            paid_amount,
            overdue_coefficient_numerator,
            overdue_coefficient_denominator,
            overdue_coefficient,
            last_sales_date,
            last_to_now_days,
            customer_active_status_code,
            customer_active_status,
            x.sdt
from    (
        SELECT 
            a.customer_no,
            customer_name,
            channel_code,
            channel_name,
            attribute_code,
            attribute_name,
            sales_id,
            work_no,
            sales_name,
            province_code,
            province_name,
            city_code,
            city_name,
            company_code,
            company_name,
            payment_terms,
            payment_name,
            payment_days,
            customer_level,
            credit_limit,
            temp_credit_limit,
            temp_begin_time,
            temp_end_time,
            overdue_amount,
            overdue_amount1,
            overdue_amount15,
            overdue_amount30,
            overdue_amount60,
            overdue_amount90,
            overdue_amount120,
            overdue_amount180,
            overdue_amount365,
            overdue_amount730,
            overdue_amount1095,
            non_overdue_amount,
            receivable_amount,
            bad_debt_amount,
            max_overdue_day,
            paid_amount,
            overdue_coefficient_numerator,
            overdue_coefficient_denominator,
            overdue_coefficient,
            last_sales_date,
            last_to_now_days,
              customer_active_status_code,
            case when  customer_active_status_code = 1 then '活跃客户'
	        		when customer_active_status_code = 2 then '沉默客户'
	        		when customer_active_status_code = 3 then '预流失客户'
	        		when customer_active_status_code = 4 then '流失客户'
	        		else '其他'
	        		end  as  customer_active_status,
            a.sdt
        FROM csx_dw.dws_sss_r_a_customer_accounts a 
        left OUTER JOIN
        (
          select * from csx_dw.dws_sale_w_a_customer_company_active
          where sdt =regexp_replace(${hiveconf:e_date},'-','')
        ) e on a.customer_no= e.customer_no and a.company_code = e.sign_company_code and a.sdt=e.sdt
        WHERE a.sdt = regexp_replace(${hiveconf:e_date},'-','')
	
	) x
	left outer join 
	(select customer_no,
	    first_category_code,
	    first_category_name,
	    second_category_code,
	    second_category_name,
	    third_category_code,
	    third_category_name
	from csx_dw.dws_crm_w_a_customer where sdt='current' ) d on x.customer_no=d.customer_no
		left join 
		(
		select 
			*
		from
			( 
			select 
				id,leader_id,leader_user_number,leader_name,
				row_number()over(partition by id order by distance) num
			from 
				csx_dw.dwd_uc_w_a_user_adjust
			where 
				sdt='current'
				and leader_user_position='SALES_MANAGER' 
			) a
		where 
			num=1
		) g on x.sales_id=g.id
where
    1 = 1 
;


