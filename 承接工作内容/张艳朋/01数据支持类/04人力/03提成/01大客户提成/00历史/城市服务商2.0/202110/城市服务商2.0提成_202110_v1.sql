--查询城市服务商2.0客户,按库存DC


set i_sdate_1 ='2021-10-31';
set i_sdate_11 ='20211031';
set i_sdate_12 ='20211001';

		
-- 处理客户与供应商对应关系		
drop table csx_tmp.tmp_tc_customer_town_service_provider_info;
create table csx_tmp.tmp_tc_customer_town_service_provider_info
as  
select '115352' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '116655' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '117103' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '118411' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '119665' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
    union all   select '119695' as customer_no,'20044273' as town_service_provider_no,'东丰县恒达商贸有限公司(彩食鲜)' as town_service_provider_name
	
;

		
--订单应收金额、逾期日期、逾期天数
drop table csx_tmp.tmp_tc_town_service_provider_order_overdue_dtl;
create table csx_tmp.tmp_tc_town_service_provider_order_overdue_dtl
as
select
	c.channel_name,
	c.channel_code,	
	a.order_no,	-- 来源单号
	a.customer_no,	-- 客户编码
	c.customer_name,	-- 客户名称
	a.appoint_place_code,  --履约地点编码
	a.company_code,	-- 签约公司编码
	b.company_name,	-- 签约公司名称
	regexp_replace(substr(a.happen_date,1,10),'-','') happen_date,	-- 发生时间		
	regexp_replace(substr(a.overdue_date,1,10),'-','') overdue_date,	-- 逾期时间	
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
		appoint_place_code,  --履约地点编码
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
	--from csx_ods.source_sss_r_d_source_bill
	from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
	where sdt=${hiveconf:i_sdate_11}
	and date(happen_date)<=${hiveconf:i_sdate_1}
	--and beginning_mark='1'  	-- 期初标识 0-是 1-否
	--and money_back_status<>'ALL'
	union all
	select 
		id as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		--customer_name,	-- 客户名称
		'' appoint_place_code,  --履约地点编码
		company_code,	-- 签约公司编码
		--company_name,	-- 签约公司名称		
		date_sub(from_unixtime(unix_timestamp(overdue_date,'yyyy-MM-dd hh:mm:ss')),coalesce(account_period_val,0)) as happen_date,	-- 发生时间		
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
	--from csx_ods.source_sss_r_a_beginning_receivable
	from csx_dw.dwd_sss_r_a_beginning_receivable_20201116 
	where sdt=${hiveconf:i_sdate_11}
	--and money_back_status<>'ALL'
	)a
left join 
	(
	select 
		code as company_code,
		name as company_name 
	from csx_dw.dws_basic_w_a_company_code 
	where sdt = 'current'
	)b on a.company_code = b.company_code
left join
	(
	select 
		customer_no,
		customer_name,
		channel_name,
		channel_code 
	from csx_dw.dws_crm_w_a_customer 
	where sdt=${hiveconf:i_sdate_11} 
	--where sdt='20210617'
	)c on a.customer_no=c.customer_no
where
	a.customer_no in ('115352','116655','117103','118411','119665','119695')
;


-- 查询结果集
--计算逾期系数
insert overwrite directory '/tmp/zhangyanpeng/yuqi_town_service_provider' row format delimited fields terminated by '\t'
select 
	a.channel_name,	-- 渠道
	b.sales_province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	c.account_period_code,	-- 账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 帐期天数
	c.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
	case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
	case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
	case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
						else (coalesce(case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end,0)
						/(case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
		    
from
	(
	select
		channel_name,
		customer_no,
		customer_name,
		--account_period_code,
		--COALESCE(account_period_val,0) account_period_val,
		--account_period_name,
		company_code,
		company_name,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from 
		csx_tmp.tmp_tc_town_service_provider_order_overdue_dtl
	where 
		channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11} 
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a
	left join
		(
		select distinct
			customer_no,
			company_code,
			payment_terms account_period_code,
			case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
				else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
			COALESCE(cast(payment_days as int),0) account_period_val
		from 
			csx_dw.dws_crm_w_a_customer_company a
		where 
			sdt='current'
			and customer_no<>''
		)c on a.customer_no=c.customer_no and a.company_code=c.company_code
	--剔除业务代理与内购客户
	join		 ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11} 
			--sdt='20210617'
			and (channel_code in('1','7','8') or customer_no='118689') 
			and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
		)b on b.customer_no=a.customer_no   
;



--客户逾期系数
drop table csx_tmp.temp_tc_tsp_cust_over_rate;
create table csx_tmp.temp_tc_tsp_cust_over_rate
as
select 
	channel_name,	-- 渠道
	customer_no,	-- 客户编码
	customer_name,	-- 客户名称
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	--sum(case when over_amt>=0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	--sum(case when receivable_amount>=0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case  when coalesce(SUM(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) end
		  , 6),0) over_rate 	-- 逾期系数
from
	(
	select
		channel_name,
		customer_no,
		customer_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s				
	from 
		csx_tmp.tmp_tc_town_service_provider_order_overdue_dtl a 
	where 
		sdt = ${hiveconf:i_sdate_11}
		--channel_name = '大客户'		
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a	
group by 
	channel_name,customer_no,customer_name
;

--供应商逾期系数
drop table csx_tmp.temp_tc_tsp_town_service_provider_over_rate;
create table csx_tmp.temp_tc_tsp_town_service_provider_over_rate
as
select 
	a.channel_name,	-- 渠道
	b.town_service_provider_no,	-- 供应商编码
	b.town_service_provider_name,	-- 供应商名称
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
	coalesce(round(case  when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/(sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数		  
from
	(
	select
		channel_name,
		customer_no,
		customer_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from 
		csx_tmp.tmp_tc_town_service_provider_order_overdue_dtl a 
	where 
		sdt = ${hiveconf:i_sdate_11}
		-- channel_name = '大客户'
	group by 
		channel_name,customer_no,customer_name,company_code,company_name
	)a	
	--关联供应商
	left join
		(
		select 
			customer_no,town_service_provider_no,town_service_provider_name
		from 
			csx_tmp.tmp_tc_customer_town_service_provider_info
		group by 
			customer_no,town_service_provider_no,town_service_provider_name
		)b on b.customer_no=a.customer_no  
group by 
	a.channel_name,	-- 渠道
	b.town_service_provider_no,	-- 供应商编码
	b.town_service_provider_name
;



-- 客户提成
insert overwrite directory '/tmp/zhangyanpeng/tc_tsp_customer' row format delimited fields terminated by '\t'
select
	a.s_month,business_type_name,a.region_name,a.province_name,a.customer_no,a.customer_name,c.town_service_provider_no,c.town_service_provider_name,
	a.sales_value,a.profit,a.profit_rate,a.sales_cost,
	coalesce(b.receivable_amount,0) as receivable_amount,
	coalesce(b.over_amt,0) as over_amt,
	coalesce(b.over_amt_s,0) as over_amt_s,
	coalesce(b.receivable_amount_s,0) as receivable_amount_s,
	coalesce(b.over_rate,0) as over_rate
from
	(
	select 
		substr(sdt,1,6) as s_month,business_type_name,region_name,province_name,customer_no,customer_name,
		sum(sales_value) sales_value,   --含税销售额
		sum(profit) profit,  --含税毛利
		sum(sales_cost) as sales_cost,
		sum(profit)/abs(sum(sales_value)) as profit_rate
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt>=${hiveconf:i_sdate_12} 
		and sdt<=${hiveconf:i_sdate_11}
		and channel_code in ('1', '7', '9')
		and business_type_code ='4'
		and sales_type !='fanli'
		and customer_no in ('115352','116655','117103','118411','119665','119695')
	group by 
		substr(sdt,1,6),business_type_name,region_name,province_name,customer_no,customer_name
	) a 
	left join
		(
		select
			customer_no,receivable_amount,over_amt,over_amt_s,receivable_amount_s,over_rate
		from
			csx_tmp.temp_tc_tsp_cust_over_rate
		) b on b.customer_no=a.customer_no	
	--关联供应商
	left join
		(
		select 
			customer_no,town_service_provider_no,town_service_provider_name
		from 
			csx_tmp.tmp_tc_customer_town_service_provider_info
		group by 
			customer_no,town_service_provider_no,town_service_provider_name
		)c on c.customer_no=a.customer_no  		
;



--====================================================================================================================================================================
-- 服务商提成

insert overwrite directory '/tmp/zhangyanpeng/tc_tsp_town_service_provider' row format delimited fields terminated by '\t'

select
	a.s_month,a.business_type_name,a.region_name,a.province_name,b.town_service_provider_no,b.town_service_provider_name,
	a.sales_value,a.profit,a.profit_rate,a.sales_cost,b.over_amt_s,b.receivable_amount_s,b.over_rate
from
	(
	select 
		a.s_month,a.business_type_name,a.region_name,a.province_name,b.town_service_provider_no,b.town_service_provider_name,
		sum(a.sales_value) sales_value,   --含税销售额
		sum(a.profit) profit,  --含税毛利
		sum(sales_cost) as sales_cost,
		sum(a.profit)/abs(sum(a.sales_value)) as profit_rate
	from 
		(
		select 
			region_code,region_name,province_code,province_name,city_group_code,city_group_name,business_type_name,customer_no,
			customer_name,sales_value,sales_cost,profit,front_profit,
			substr(sdt,1,6) as s_month
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:i_sdate_12} 
			and sdt<=${hiveconf:i_sdate_11}
			and channel_code in ('1', '7', '9')
			and business_type_code ='4'
			and sales_type !='fanli'
			and customer_no in ('115352','116655','117103','118411','119665','119695')		
		)a 
		left join
			(
			select
				customer_no,town_service_provider_no,town_service_provider_name
			from
				csx_tmp.tmp_tc_customer_town_service_provider_info
			) b on b.customer_no=a.customer_no	
	group by 
		a.s_month,a.business_type_name,a.region_name,a.province_name,b.town_service_provider_no,b.town_service_provider_name
	) a
	left join
		(
		select
			town_service_provider_no,town_service_provider_name,over_amt_s,receivable_amount_s,over_rate
		from
			csx_tmp.temp_tc_tsp_town_service_provider_over_rate
		) b on b.town_service_provider_no=a.town_service_provider_no
;