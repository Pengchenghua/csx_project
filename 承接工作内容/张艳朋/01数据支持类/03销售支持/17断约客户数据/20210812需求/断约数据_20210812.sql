-- ================================================================================================================
-- 2021年Q1断约客户明细

select
	a.region_name,
	a.province_name,
	a.customer_no,
	b.customer_name,
	a.min_sdt,
	a.max_sdt,
	a.add_day,
	substr(a.add_day,1,6) as add_month,
	months_between(from_unixtime(unix_timestamp(a.max_sdt,'yyyyMMdd')),from_unixtime(unix_timestamp(a.min_sdt,'yyyyMMdd'))) as diff_month,
	if(d.customer_no is not null,'是','否') as type,
	if(e.customer_no is not null,'是','否') as type2,
	b.work_no,
	b.sales_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.cooperation_mode_name,
	coalesce(a.sales_value,0) as sales_value,
	coalesce(a.profit,0) as profit,
	coalesce(a.profit_rate,0) as profit_rate,
	coalesce(a.days_cnt) as days_cnt,
	(case when a.days_cnt=1 then '1次'
		when a.days_cnt>=2 and a.days_cnt<=5 then '2~5次'
		when a.days_cnt>=6 and a.days_cnt<=10 then '6~10次'
		when a.days_cnt>=11 and a.days_cnt<=20 then '11~20次'
		when a.days_cnt>20 then '20次以上'
		else '其他'
	end) as days_cnt_type,
	coalesce(c.receivable_amount,0) as receivable_amount,
	coalesce(c.overdue_amount,0) as overdue_amount,
	coalesce(c.overdue_amount_90,0) as overdue_amount_90
from	
	(
	select 
		region_name,province_name,customer_no,max(sdt) as max_sdt,
		min(sdt) as min_sdt,
		regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90)),'-','') as add_day,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate,
		count(distinct sdt) as days_cnt
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20190101' and '20210810'
		and business_type_code in ('1','4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and sales_type !='fanli'
	group by 
		region_name,province_name,customer_no
	having
		regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90)),'-','') between '20210101' and '20210630'
	) a
	left join
		(
		select 
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,cooperation_mode_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,cooperation_mode_name
		) b on b.customer_no = a.customer_no
	left join
		(
		select --应收逾期
			customer_no,
			sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount, -- 应收金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount-overdue_amount1-overdue_amount15-overdue_amount30-overdue_amount60 else 0 end) overdue_amount_90	-- 逾期90天以上金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt='20210810'
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
	left join
		(	
		select 
			customer_no
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210701' and '20210810'
			and business_type_code not in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			customer_no
		) d on d.customer_no=a.customer_no
	left join
		(	
		select 
			customer_no
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210701' and '20210810'
			and business_type_code in ('4') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			customer_no
		) e on e.customer_no=a.customer_no