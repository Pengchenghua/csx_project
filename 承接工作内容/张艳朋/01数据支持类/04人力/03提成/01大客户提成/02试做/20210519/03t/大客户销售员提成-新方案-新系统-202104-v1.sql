-- ========================================================================================================================================================
-- 客户销售数据

set i_sdate_11 ='20210430';	
set i_sdate_12 ='20210401';	

insert overwrite directory '/tmp/zhangyanpeng/tc_customer' row format delimited fields terminated by '\t'

-- 客户销售数据
select 
	a.smonth,b.sales_province_name dist,a.customer_no cust_id,b.customer_name cust_name,b.work_no,b.sales_name,c.service_user_work_no,c.service_user_name,
	sum(sales_value)sales_value,
	sum(profit) profit,
	sum(profit)/sum(sales_value) prorate,
	sum(front_profit) front_profit,
	sum(front_profit)/sum(sales_value) fnl_prorate
from 
	(
	select 
		sdt,substr(sdt,1,6) smonth,province_name,customer_no,
		sum(sales_value)sales_value,
		sum(profit) profit,sum(profit)/abs(sum(sales_value)) prorate,
		sum(front_profit) as front_profit,
		sum(front_profit)/abs(sum(sales_value)) as fnl_prorate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11} --昨日月1日
		and channel_code in('1','7','9')
		and business_type_code not in('3','4')
	group by 
		sdt,substr(sdt,1,6),province_name,customer_no	
	)a
	left join 
		(
		select distinct customer_no,customer_name,work_no,sales_name,sales_province_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11}   --上月最后1日
		)b on b.customer_no=a.customer_no
	--关联服务管家
	left join		
		(  
		select 
			customer_no,
			concat_ws(',', collect_list(service_user_work_no)) as service_user_work_no,
			concat_ws(',', collect_list(service_user_name)) as service_user_name
		from 
			(
			select 
				distinct customer_no,service_user_work_no,service_user_name
			from 
				csx_dw.dws_crm_w_a_customer_sales_link 
			where 
				sdt=${hiveconf:i_sdate_11} 
				and is_additional_info = 1 and service_user_id <> 0  
			)a
		group by 
			customer_no
		) c on c.customer_no=a.customer_no		
where 
	b.ywdl_cust is null
	and b.ng_cust is null
group by 
	a.smonth,b.sales_province_name,a.customer_no,b.customer_name,b.work_no,b.sales_name,c.service_user_work_no,c.service_user_name;
	
	

-- ========================================================================================================================================================
-- 客户逾期数据

set i_sdate_1 ='2021-04-30';
set i_sdate_11 ='20210430';
set i_sdate_12 ='20210401';


--订单应收金额、逾期日期、逾期天数
drop table csx_tmp.tmp_tc_cust_order_overdue_dtl;
create table csx_tmp.tmp_tc_cust_order_overdue_dtl
as
select
	c.channel_name,
	c.channel_code,	
	a.order_no,	-- 来源单号
	a.customer_no,	-- 客户编码
	c.customer_name,	-- 客户名称
	a.company_code,	-- 签约公司编码
	b.company_name,	-- 签约公司名称
	a.happen_date,	-- 发生时间		
	a.overdue_date,	-- 逾期时间	
	a.source_statement_amount,	-- 源单据对账金额
	a.money_back_status,	-- 回款状态
	a.unpaid_amount receivable_amount,	-- 应收金额
	a.account_period_code,	--账期编码 
	a.account_period_name,	--账期名称 
	a.account_period_val,	--账期值
	a.beginning_mark,	--是否期初
	a.bad_debt_amount,	
	a.over_days,	-- 逾期天数
	if(a.account_period_code like 'Y%', if(a.account_period_val = 31, 45, a.account_period_val + 15), a.account_period_val) as acc_val_calculation_factor,	-- 标准账期
	${hiveconf:i_sdate_11} sdt
from
	(
	select 
		source_bill_no as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		--customer_name,	-- 客户名称
		company_code,	-- 签约公司编码
		--company_name,	-- 签约公司名称
		happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val,	--账期值
		'否' as beginning_mark,	--是否期初
		bad_debt_amount,
		if((money_back_status<>'ALL' or (datediff(${hiveconf:i_sdate_1}, overdue_date)+1)>=1),datediff(${hiveconf:i_sdate_1}, overdue_date)+1,0) as over_days	-- 逾期天数
	from 
		csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
	where 
		sdt=${hiveconf:i_sdate_11}
		and date(happen_date)<=${hiveconf:i_sdate_1}
		--and beginning_mark='1'  	-- 期初标识 0-是 1-否
		--and money_back_status<>'ALL'
	union all
	select 
		id as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		--customer_name,	-- 客户名称
		company_code,	-- 签约公司编码
		--company_name,	-- 签约公司名称		
		'' happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		beginning_amount source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val,	--账期值
		'是' as beginning_mark,	--是否期初	
		bad_debt_amount,
		if((money_back_status<>'ALL' or (datediff(${hiveconf:i_sdate_1}, overdue_date)+1)>=1),datediff(${hiveconf:i_sdate_1}, overdue_date)+1,0) as over_days	-- 逾期天数
	from 
		csx_dw.dwd_sss_r_a_beginning_receivable_20201116 
	where 
		sdt=${hiveconf:i_sdate_11}
		--and money_back_status<>'ALL'
	)a
left join 
	(
	select 
		code as company_code,name as company_name 
	from 
		csx_dw.dws_basic_w_a_company_code 
	where 
		sdt = 'current'
	)b on a.company_code = b.company_code
left join
	(
	select 
		customer_no,customer_name,channel_name,channel_code 
	from 
		csx_dw.dws_crm_w_a_customer 
	where 
		sdt=${hiveconf:i_sdate_11} 
	) c on a.customer_no=c.customer_no;
	


-- 客户逾期明细	

set i_sdate_11 ='20210430';	
set i_sdate_12 ='20210401';	
	
insert overwrite directory '/tmp/zhangyanpeng/tc_customer_overdue' row format delimited fields terminated by '\t'
	
select
	substr(${hiveconf:i_sdate_11},1,6) as smonth,
	a.channel_name,
	b.sales_province_name,
	a.customer_no,
	a.customer_name,
	b.work_no,
	b.sales_name,
	c.service_user_work_no,
	c.service_user_name,
	a.receivable_amount,
	a.over_amt,
	a.over_amt_s,
	a.receivable_amount_s,
	a.over_rate
from
	(
	select 
		channel_name,	-- 渠道
		customer_no,	-- 客户编码
		customer_name,	-- 客户名称
		sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
		sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
		sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
		sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
		coalesce(round(case  when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
					else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
					/sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) end
			  , 6),0) over_rate	-- 逾期系数
	from
		(
		select
			channel_name,
			customer_no,
			customer_name,
			company_code,
			company_name ,
			sum(receivable_amount) as receivable_amount, -- 应收金额
			sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
			sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
			sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
		from 
			csx_tmp.tmp_cust_order_overdue_dtl a 
		where 
			channel_name = '大客户' 
			and sdt = ${hiveconf:i_sdate_11}
		group by 
			channel_name,customer_no,customer_name,company_code,company_name
		)a	
	group by 
		channel_name,customer_no,customer_name
	) as a
	left join 
		(
		select distinct customer_no,customer_name,work_no,sales_name,sales_province_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11}   --上月最后1日
		)b on b.customer_no=a.customer_no
	--关联服务管家
	left join		
		(  
		select 
			customer_no,
			concat_ws(',', collect_list(service_user_work_no)) as service_user_work_no,
			concat_ws(',', collect_list(service_user_name)) as service_user_name
		from 
			(
			select 
				distinct customer_no,service_user_work_no,service_user_name
			from 
				csx_dw.dws_crm_w_a_customer_sales_link 
			where 
				sdt=${hiveconf:i_sdate_11} 
				and is_additional_info = 1 and service_user_id <> 0  
			)a
		group by 
			customer_no
		) c on c.customer_no=a.customer_no