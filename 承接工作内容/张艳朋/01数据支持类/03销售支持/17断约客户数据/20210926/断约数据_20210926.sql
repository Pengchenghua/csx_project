-- ================================================================================================================
-- 日配业务客户清单

insert overwrite directory '/tmp/zhangyanpeng/20210926_linshi_1' row format delimited fields terminated by '\t' 

select
	case when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'))))<=30 then '活跃'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'))))<=60 then '沉默'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'))))<=90 then '预流失'
		when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'))))>90 then '流失'
		else '其他'
	end as customer_type,
	b.dev_source_name,
	a.business_type_name,
	a.province_name,
	a.city_group_name,
	a.customer_no,
	b.customer_name,
	b.attribute_desc,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	b.work_no,
	b.sales_name,	
	b.first_supervisor_name,
	b.second_supervisor_name,
	b.sign_date,
	a.max_sdt,
	coalesce(a.sales_value,0) as sales_value,
	-- coalesce(a.profit,0) as profit,
	coalesce(a.profit_rate,0) as profit_rate,
	coalesce(c.receivable_amount,0) as receivable_amount,
	case when d.customer_no is not null then '是' else '否' end as is_other
	--case when datediff(date_sub(current_date,1),to_date(from_unixtime(unix_timestamp(max_sdt,'yyyyMMdd'))))>=90 then '是' else '否' end as is_break_contract
from	
	(
	select 
		business_type_name,province_name,city_group_name,customer_no,max(sdt) as max_sdt,
		sum(sales_value) as sales_value,
		sum(profit) as profit,
		sum(profit)/abs(sum(sales_value)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210401' and '20210630'
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and sales_type !='fanli'
	group by 
		business_type_name,province_name,city_group_name,customer_no
	) a
	left join
		(
		select 
			customer_no
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210701' and '20210925'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli'
		group by 
			customer_no
		) t1 on t1.customer_no=a.customer_no
	join
		(
		select 
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,dev_source_name,attribute_desc,
			regexp_replace(substr(sign_time,1,10),'-','') as sign_date,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,second_supervisor_work_no,second_supervisor_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
			and cooperation_mode_code='01' -- 合作模式编码(01长期客户,02一次性客户)
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
			sdt='20210925'
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
	left join -- 是否下过其他属性订单
		(
		select 
			customer_no
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt between '20190101' and '20210925'
			and business_type_code not in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			-- and sales_type !='fanli'
		group by
			customer_no
		) d on d.customer_no = a.customer_no
where
	t1.customer_no is null