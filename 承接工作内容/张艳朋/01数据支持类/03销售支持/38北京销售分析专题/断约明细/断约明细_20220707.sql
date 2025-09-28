
-- 北京客户 断约明细
select
	b.province_name,a.customer_no,b.customer_name,b.work_no,b.sales_name,b.first_supervisor_work_no,b.first_supervisor_name,b.first_category_name,
	b.second_category_name,b.third_category_name,b.attribute_desc,b.sign_date,a.max_sdt,a.after_month
from
	(
	select 
		customer_no,
		substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as after_month,
		max(sdt) as max_sdt
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20190101' and '20220630'
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and sales_type !='fanli'
	group by 
		customer_no
	) as a 
	join 
		(
		select
			customer_no,customer_name,work_no,sales_name,first_supervisor_work_no,first_supervisor_name,first_category_name,second_category_name,third_category_name,attribute_desc,
			sales_region_name,sales_province_name,province_name,regexp_replace(substr(sign_time,1,10),'-','') as sign_date,estimate_contract_amount
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and province_name ='北京市'	
		) b on a.customer_no=b.customer_no	
where
	a.after_month between '202201' and '202206'
;
