-- 切换tez计算引擎
SET hive.execution.engine=mr;
-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;
-- 启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;


--昨天
set last_1day = '20220131';
--昨天所在年第一天
set last_1day_year_first_day='20220101';
--昨天所在月第一天
set last_1day_mon_fisrt_day ='20220101';

set target_table = csx_dw.report_sss_r_m_crm_sales_customer_commission_new;
	
		
-- 销售员年度累计销额提成比例
drop table if exists csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_0;
create table csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_0
as
select 
	sales_id,work_no,sales_name,income_type,salesperson_sales_value_ytd,salesperson_ripei_bbc_sales_value_ytd,salesperson_fuli_sales_value_ytd,
	case when 
			((salesperson_ripei_bbc_sales_value_ytd<=10000000 and income_type in('Q1','Q2','Q3')) 
			or (salesperson_ripei_bbc_sales_value_ytd>10000000 and salesperson_ripei_bbc_sales_value_ytd<=20000000 and income_type in('Q2','Q3'))
			or (salesperson_ripei_bbc_sales_value_ytd>20000000 and salesperson_ripei_bbc_sales_value_ytd<=30000000 and income_type in('Q3'))) then 0.002
		when ((salesperson_ripei_bbc_sales_value_ytd>10000000 and salesperson_ripei_bbc_sales_value_ytd<=20000000 and income_type in('Q1'))
			or (salesperson_ripei_bbc_sales_value_ytd>20000000 and salesperson_ripei_bbc_sales_value_ytd<=30000000 and income_type in('Q2'))
			or (salesperson_ripei_bbc_sales_value_ytd>30000000 and salesperson_ripei_bbc_sales_value_ytd<=40000000 and income_type in('Q3'))) then 0.0025
		when ((salesperson_ripei_bbc_sales_value_ytd>20000000 and salesperson_ripei_bbc_sales_value_ytd<=30000000 and income_type in('Q1'))
			or (salesperson_ripei_bbc_sales_value_ytd>30000000 and salesperson_ripei_bbc_sales_value_ytd<=40000000 and income_type in('Q2'))
			or (salesperson_ripei_bbc_sales_value_ytd>40000000 and income_type in('Q3'))) then 0.003
		when ((salesperson_ripei_bbc_sales_value_ytd>30000000 and salesperson_ripei_bbc_sales_value_ytd<=40000000 and income_type in('Q1'))
			or (salesperson_ripei_bbc_sales_value_ytd>40000000 and income_type in('Q2'))) then 0.0035
		when (salesperson_ripei_bbc_sales_value_ytd>40000000 and income_type in('Q1')) then 0.004			
		else 0.002 end salesperson_ripei_bbc_sales_value_tc_rate,
	0.002 as salesperson_fuli_sales_value_tc_rate
from 
	(
	select 
		b.sales_id,b.work_no,b.sales_name,coalesce(c.income_type,'Q1') as income_type,
		sum(a.sales_value) as salesperson_sales_value_ytd,
		sum(a.ripei_bbc_sales_value) as salesperson_ripei_bbc_sales_value_ytd,
		sum(a.fuli_sales_value) as salesperson_fuli_sales_value_ytd
	from 
		(
		select 
			customer_no,			
			if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
			) as sdt_last,
			sum(case when dc_code !='W0K4' then sales_value else 0 end) as sales_value, --202107月签呈，W0K4仓不计算销售额，仅计算定价毛利额，每月处理
			sum(case when dc_code !='W0K4' and business_type_code in('1','6','4') then sales_value else 0 end) as ripei_bbc_sales_value,
			sum(case when dc_code !='W0K4' and business_type_code in('2') then sales_value else 0 end) as fuli_sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:last_1day_year_first_day} and sdt<=${hiveconf:last_1day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			--安徽省城市服务商2.0，按大客户提成方案计算
			and (business_type_code in('1','2','6') or (business_type_code in ('4') and customer_no in
				('117817','120939','121298','121625','122567','123244','124473','124498','124601')))
		group by 
			customer_no,
			if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
			)
		)a 
		left join   --CRM客户信息取每月最后一天
			(
			select 
				sdt,customer_no,customer_name,work_no,sales_name,sales_id,
				case when channel_code='9' then '业务代理' end as ywdl_cust,
				case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust	
			from 
				csx_dw.dws_crm_w_a_customer 
			where 
				sdt>=${hiveconf:last_1day_year_first_day}
				and sdt<=${hiveconf:last_1day}
				and customer_no !=''
				and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
						regexp_replace(date_sub(current_date,1),'-',''),
						regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
						)
			)b on b.customer_no=a.customer_no and b.sdt=a.sdt_last 	
		left join 
			(
			select 
				distinct work_no,income_type 
			from
				(
				select 
					distinct work_no,income_type,sdt
				from 
					csx_tmp.sales_income_info_new 
				) t1 
				join (select max(sdt) as sdt from csx_tmp.sales_income_info_new) t2 on t2.sdt=t1.sdt
			) c on c.work_no=b.work_no   --上月最后1日
	where 
		b.ywdl_cust is null -- 剔除业务代理和内购 or b.customer_no in ('118689','116957','116629'))
		and b.ng_cust is null
	group by 
		b.sales_id,b.work_no,b.sales_name,coalesce(c.income_type,'Q1')
	)a
;

-- 客户本月销售额、定价毛利额统计
drop table if exists csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_1;
create table csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_1
as
select 
	b.sales_province_name,a.customer_no,b.customer_name,b.sales_id,d.work_no,d.sales_name,d.is_part_time_service_manager,
	coalesce(d.service_user_work_no,'') as service_user_work_no,
	coalesce(d.service_user_name,'') as service_user_name,
	d.salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	d.salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
	d.service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	d.service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
	a.smonth,
	coalesce(c.salesperson_sales_value_ytd,0) as salesperson_sales_value_ytd,-- 销售员年度累计销售额
	coalesce(c.salesperson_ripei_bbc_sales_value_ytd,0) as salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	coalesce(c.salesperson_fuli_sales_value_ytd,0) as salesperson_fuli_sales_value_ytd,-- 销售员年度累计福利销售额
	coalesce(c.salesperson_ripei_bbc_sales_value_tc_rate,0.002) salesperson_ripei_bbc_sales_value_tc_rate, --销售员日配&bbc提成比例
	coalesce(c.salesperson_fuli_sales_value_tc_rate,0.002) salesperson_fuli_sales_value_tc_rate, --销售员福利提成比例
	-- 销售额
	sum(customer_sales_value) as customer_sales_value, -- 客户总销售额
	sum(customer_ripei_bbc_sales_value) as customer_ripei_bbc_sales_value, -- 客户日配&bbc销售额
	sum(customer_fuli_sales_value) as customer_fuli_sales_value, -- 客户福利销售额
	-- 定价毛利额
	sum(customer_profit) as customer_profit,-- 客户总定价毛利额
	sum(customer_ripei_bbc_profit) as customer_ripei_bbc_profit,-- 客户日配&bbc定价毛利额
	sum(customer_fuli_profit) as customer_fuli_profit,-- 客户福利定价毛利额
	--定价毛利率
	coalesce(sum(customer_profit)/abs(sum(customer_sales_value)),0) as customer_prorate, -- 客户总定价毛利率
	coalesce(sum(customer_ripei_bbc_profit)/abs(sum(customer_ripei_bbc_sales_value)),0) as customer_ripei_bbc_prorate, -- 客户日配&bbc定价毛利率
	coalesce(sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)),0) as customer_fuli_prorate, -- 客户福利定价毛利率
	-- 退货金额
	sum(customer_refund_sales_value) as customer_refund_sales_value,
	sum(customer_refund_ripei_bbc_sales_value) as customer_refund_ripei_bbc_sales_value,
	sum(customer_refund_fuli_sales_value) as customer_refund_fuli_sales_value
from 
	(
	select 
		customer_no,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(case when dc_code !='W0K4' then sales_value else 0 end) as customer_sales_value, --202107月签呈，W0K4仓不计算销售额，仅计算定价毛利额，每月处理
		sum(case when dc_code !='W0K4' and business_type_code in('1','6','4') then sales_value else 0 end) as customer_ripei_bbc_sales_value,
		sum(case when dc_code !='W0K4' and business_type_code in('2') then sales_value else 0 end) as customer_fuli_sales_value,
		-- 各类型定价毛利额
		sum(case when dc_code !='W0K4' then profit else 0 end) as customer_profit, 
		sum(case when dc_code !='W0K4' and business_type_code in('1','6','4') then profit else 0 end) as customer_ripei_bbc_profit,
		sum(case when dc_code !='W0K4' and business_type_code in('2') then profit else 0 end) as customer_fuli_profit,
		-- 各类型退货金额
		sum(case when dc_code !='W0K4' and return_flag='X' then sales_value else 0 end) as customer_refund_sales_value, 
		sum(case when dc_code !='W0K4' and business_type_code in('1','6','4') and return_flag='X' then sales_value else 0 end) as customer_refund_ripei_bbc_sales_value,
		sum(case when dc_code !='W0K4' and business_type_code in('2') and return_flag='X' then sales_value else 0 end) as customer_refund_fuli_sales_value		
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:last_1day_mon_fisrt_day} and sdt<=${hiveconf:last_1day}
		and channel_code in('1','7','9')
		and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
		--安徽省城市服务商2.0，按大客户提成方案计算
		and (business_type_code in('1','2','6') or (business_type_code in ('4') and customer_no in
			('117817','120939','121298','121625','122567','123244','124473','124498','124601')))		
	group by 
		sdt,substr(sdt,1,6),province_name,customer_no	
	)a
	left join 
		(
		select 
			distinct customer_no,customer_name,work_no,sales_name,sales_id,
			sales_province_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:last_1day}
			and customer_no !=''
		)b on b.customer_no=a.customer_no
	left join 
		(
		select  
			distinct sales_id,work_no,sales_name,income_type,salesperson_sales_value_ytd,salesperson_ripei_bbc_sales_value_ytd,salesperson_fuli_sales_value_ytd,
			salesperson_ripei_bbc_sales_value_tc_rate,salesperson_fuli_sales_value_tc_rate
		from 
			csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_0 
		)c on c.work_no=b.work_no and c.sales_name=b.sales_name
	--关联服务管家
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,work_no,sales_name,is_part_time_service_manager,
			sales_sale_rate as salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
			sales_profit_rate as salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
			service_user_sale_rate as service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
			service_user_profit_rate as service_user_profit_fp_rate --服务管家_定价毛利额_分配比例
		from 
			csx_dw.report_crm_w_a_customer_service_manager_info_new
		where 
			sdt=${hiveconf:last_1day}
		)d on d.customer_no=a.customer_no
where 
	b.ywdl_cust is null -- or b.customer_no in ('118689','116957','116629'))
	and b.ng_cust is null
group by 
	b.sales_province_name,a.customer_no,b.customer_name,b.sales_id,d.work_no,d.sales_name,d.is_part_time_service_manager,
	coalesce(d.service_user_work_no,''),
	coalesce(d.service_user_name,''),
	d.salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	d.salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
	d.service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	d.service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
	a.smonth,
	coalesce(c.salesperson_sales_value_ytd,0),-- 销售员年度累计销售额
	coalesce(c.salesperson_ripei_bbc_sales_value_ytd,0),-- 销售员年度累计日配&BBC销售额
	coalesce(c.salesperson_fuli_sales_value_ytd,0),-- 销售员年度累计福利销售额
	coalesce(c.salesperson_ripei_bbc_sales_value_tc_rate,0.002), --销售员日配&bbc提成比例
	coalesce(c.salesperson_fuli_sales_value_tc_rate,0.002) --销售员福利提成比例
;

-- 销售员本月定价毛利率，计算销售员定价毛利额提成比例
drop table if exists csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_2;
create table csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_2
as
select
	sales_id,work_no,sales_name,salesperson_sales_value,salesperson_ripei_bbc_sales_value,salesperson_fuli_sales_value,salesperson_profit,salesperson_ripei_bbc_profit,
	salesperson_fuli_profit,salesperson_prorate,
	salesperson_ripei_bbc_prorate,
	salesperson_fuli_prorate,
	-- 日配&bbc定价毛利额提成比例
	case when salesperson_ripei_bbc_prorate<0.08 then 0
		when salesperson_ripei_bbc_prorate>=0.08 and salesperson_ripei_bbc_prorate<0.12 then 0.03
		when salesperson_ripei_bbc_prorate>=0.12 and salesperson_ripei_bbc_prorate<0.15 then 0.033
		when salesperson_ripei_bbc_prorate>=0.15 and salesperson_ripei_bbc_prorate<0.18 then 0.035
		when salesperson_ripei_bbc_prorate>=0.18 and salesperson_ripei_bbc_prorate<0.2 then 0.04
		when salesperson_ripei_bbc_prorate>=0.2 then 0.05
		else 0 
	end as salesperson_ripei_bbc_profit_tc_rate,
	-- 福利定价毛利额提成比例
	case when salesperson_fuli_prorate<0.03 then 0
		when salesperson_fuli_prorate>=0.03 and salesperson_fuli_prorate<0.05 then 0.02
		when salesperson_fuli_prorate>=0.05 and salesperson_fuli_prorate<0.08 then 0.025
		when salesperson_fuli_prorate>=0.08 and salesperson_fuli_prorate<0.1 then 0.03
		when salesperson_fuli_prorate>=0.1 and salesperson_fuli_prorate<0.15 then 0.04
		when salesperson_fuli_prorate>=0.15 then 0.05
		else 0 
	end as salesperson_fuli_profit_tc_rate
from
	(
	select 	
		sales_id,work_no,sales_name,
		-- 销售额
		sum(customer_sales_value) as salesperson_sales_value, -- 总销售额
		sum(customer_ripei_bbc_sales_value) as salesperson_ripei_bbc_sales_value, -- 日配&bbc销售额
		sum(customer_fuli_sales_value) as salesperson_fuli_sales_value, -- 福利销售额
		-- 定价毛利额
		sum(customer_profit) as salesperson_profit,-- 总定价毛利额
		sum(customer_ripei_bbc_profit) as salesperson_ripei_bbc_profit,-- 日配&bbc定价毛利额
		sum(customer_fuli_profit) as salesperson_fuli_profit,-- 福利定价毛利额
		--定价毛利率
		sum(customer_profit)/abs(sum(customer_sales_value)) as salesperson_prorate, -- 总定价毛利率
		sum(customer_ripei_bbc_profit)/abs(sum(customer_ripei_bbc_sales_value)) as salesperson_ripei_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
		sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)) as salesperson_fuli_prorate -- 销售员本月福利定价毛利率
	from
		csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_1
	group by 
		sales_id,work_no,sales_name
	) a 
;

--销售员 粒度 的逾期系数
drop table if exists csx_tmp.customer_commission_over_rate;
create  table csx_tmp.customer_commission_over_rate 
as 
select 
	a.channel_name,	-- 渠道
	b.work_no,	-- 销售员工号
	b.sales_id,
	b.sales_name,	-- 销售员
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数
from 
	(
	select 
		* 
	from 
		csx_dw.dws_sss_r_a_customer_company_accounts
	where 
		sdt = ${hiveconf:last_1day} and channel_name = '大客户' 
	)a
	left join 
		( --剔除合伙人20210609
		select 
			*,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt=${hiveconf:last_1day}
		)b on b.customer_no=a.customer_no 
	left join 
		( --剔除当月有城市服务商与批发内购业绩的客户逾期系数
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:last_1day_mon_fisrt_day} 
			and sdt<regexp_replace(to_date(current_date),'-','')
			and business_type_code in('3','4')
		)e on e.customer_no=a.customer_no
where 
	b.ywdl_cust is null
	and b.ng_cust is null 
	and e.customer_no is null
group by a.channel_name,b.work_no,b.sales_id,b.sales_name;


--客户本月提成，未乘分配比例
drop table if exists csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_3;
create table csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_3
as
select 
	a.smonth,a.sales_province_name,a.customer_no,a.customer_name,a.sales_id,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.customer_sales_value,a.customer_ripei_bbc_sales_value,a.customer_fuli_sales_value,
	a.customer_profit,a.customer_ripei_bbc_profit,a.customer_fuli_profit,
	a.customer_prorate,a.customer_ripei_bbc_prorate,a.customer_fuli_prorate,
	coalesce(e.salesperson_prorate,0) as salesperson_prorate, -- 销售员本月定价毛利率
	coalesce(e.salesperson_ripei_bbc_prorate,0) as salesperson_ripei_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
	coalesce(e.salesperson_fuli_prorate,0) as salesperson_fuli_prorate, -- 销售员本月福利定价毛利率
	coalesce(a.customer_ripei_bbc_sales_value*a.salesperson_ripei_bbc_sales_value_tc_rate,0) as salary_ripei_bbc_sales_value,
	coalesce(a.customer_fuli_sales_value*a.salesperson_fuli_sales_value_tc_rate,0) as salary_fuli_sales_value,
	coalesce(a.customer_ripei_bbc_sales_value*a.salesperson_ripei_bbc_sales_value_tc_rate,0)+coalesce(a.customer_fuli_sales_value*a.salesperson_fuli_sales_value_tc_rate,0) as salary_sales_value, -- 奖金包_销售额
	coalesce(a.customer_ripei_bbc_profit*e.salesperson_ripei_bbc_profit_tc_rate,0) as salary_ripei_bbc_profit,
	coalesce(a.customer_fuli_profit*e.salesperson_fuli_profit_tc_rate,0) as salary_fuli_profit,
	coalesce(a.customer_ripei_bbc_profit*e.salesperson_ripei_bbc_profit_tc_rate,0)+coalesce(a.customer_fuli_profit*e.salesperson_fuli_profit_tc_rate,0) as salary_profit, --奖金包_定价毛利额
	--b.receivable_amount,b.overdue_amount,
	if(a.service_user_work_no<>'','服务管家有提成','服务管家无提成') assigned_type, --分配类别
	a.salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	a.salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
	a.service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	a.service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
	coalesce(c.over_rate,0) as salesperson_over_rate,
	--coalesce(d.over_rate,0) as service_user_over_rate,
	salesperson_sales_value_ytd,-- 销售员年度累计销售额
	salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	salesperson_fuli_sales_value_ytd, -- 销售员年度累计福利销售额
	customer_refund_sales_value, --本月退货金额
	customer_refund_ripei_bbc_sales_value,--本月日配&BBC退货金额
	customer_refund_fuli_sales_value --本月福利退货金额	
from  
	(
	select 
		sales_province_name,customer_no,customer_name,sales_id,work_no,sales_name,is_part_time_service_manager,
		service_user_work_no,service_user_name,
		salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
		salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
		service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
		service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
		smonth,customer_sales_value,customer_ripei_bbc_sales_value,customer_fuli_sales_value,customer_profit,customer_ripei_bbc_profit,customer_fuli_profit,
		customer_prorate,customer_ripei_bbc_prorate,customer_fuli_prorate,
		salesperson_ripei_bbc_sales_value_tc_rate,--销售员日配&bbc提成比例
		salesperson_fuli_sales_value_tc_rate, --销售员福利提成比例
		salesperson_sales_value_ytd,-- 销售员年度累计销售额
		salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
		salesperson_fuli_sales_value_ytd, -- 销售员年度累计福利销售额
		customer_refund_sales_value, --本月退货金额
		customer_refund_ripei_bbc_sales_value,--本月日配&BBC退货金额
		customer_refund_fuli_sales_value --本月福利退货金额
	from 
		csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_1
	)a
	left join csx_tmp.customer_commission_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
	left join csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_2 e on e.work_no=a.work_no and e.sales_name=a.sales_name
;

--客户本月提成，乘分配比例
drop table csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_4; --11
create table csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_4
as
select 
	a.smonth,a.sales_province_name,a.customer_no,a.customer_name,a.sales_id,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.customer_sales_value,a.customer_ripei_bbc_sales_value,a.customer_fuli_sales_value,
	a.customer_profit,a.customer_ripei_bbc_profit,a.customer_fuli_profit,
	a.customer_prorate,a.customer_ripei_bbc_prorate,a.customer_fuli_prorate,
	a.salesperson_prorate,
	a.salesperson_ripei_bbc_prorate,a.salesperson_fuli_prorate,
	a.salary_sales_value, -- 奖金包_销售额
	a.salary_profit, --奖金包_定价毛利额
	--a.receivable_amount,a.overdue_amount,
	a.assigned_type, --分配类别
	a.salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	a.salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
	a.service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	a.service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
	a.salesperson_over_rate,
	--a.service_user_over_rate,
	--提成_销售额_销售员
	a.salary_ripei_bbc_sales_value*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_sales_value_fp_rate,0) as salary_ripei_bbc_sales_value_salesperson, --提成_日配&BBC销售额_销售员
	a.salary_fuli_sales_value*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_sales_value_fp_rate,0) as salary_fuli_sales_value_salesperson, --提成_福利销售额_销售员
	a.salary_sales_value*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_sales_value_fp_rate,0) as salary_sales_value_salesperson, --提成_销售额_销售员
	--提成_销售额_服务管家
	--a.salary_sales_value*(1-coalesce(if(a.service_user_over_rate<=0.5,a.service_user_over_rate,1),0))*coalesce(a.service_user_sales_value_fp_rate,0) salary_sales_value_service,--提成_销售额_服务管家
	--提成_定价毛利额_销售员
	a.salary_ripei_bbc_profit*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_profit_fp_rate,0) salary_ripei_bbc_profit_salesperson,--提成_日配&BBC定价毛利额_销售员
	a.salary_fuli_profit*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_profit_fp_rate,0) salary_fuli_profit_salesperson,--提成_福利定价毛利额_销售员
	a.salary_profit*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_profit_fp_rate,0) salary_profit_salesperson,--提成_定价毛利额_销售员
	--提成_定价毛利额_服务管家
	--a.salary_profit*(1-coalesce(if(a.service_user_over_rate<=0.5,a.service_user_over_rate,1),0))*coalesce(a.service_user_profit_fp_rate,0) salary_profit_service, --提成_定价毛利额_服务管家
	a.salesperson_sales_value_ytd,-- 销售员年度累计销售额
	a.salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	a.salesperson_fuli_sales_value_ytd, -- 销售员年度累计福利销售额
	a.customer_refund_sales_value, --本月退货金额
	a.customer_refund_ripei_bbc_sales_value,--本月日配&BBC退货金额
	a.customer_refund_fuli_sales_value --本月福利退货金额	
from
	csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_3 a 
;



--客户昨天的当月提成
insert overwrite table csx_dw.report_sss_r_m_crm_sales_customer_commission_new partition(smonth)  
select 
	concat_ws('&',customer_no,cast(sales_id as string),smonth) as biz_id,
	customer_no,
	customer_name,
	sales_id,
	work_no,
	sales_name,
	customer_sales_value as sales_value,
	customer_ripei_bbc_sales_value as ripei_bbc_sales_value,
	customer_fuli_sales_value as fuli_sales_value,
	salary_sales_value_salesperson as sales_value_commion,
	salary_ripei_bbc_sales_value_salesperson as ripei_bbc_sales_value_commion,
	salary_fuli_sales_value_salesperson as fuli_sales_value_commion,
	customer_profit as profit,
	customer_ripei_bbc_profit as ripei_bbc_profit,
	customer_fuli_profit as fuli_profit,
	salary_profit_salesperson as profit_commion,
	salary_ripei_bbc_profit_salesperson as ripei_bbc_profit_commion,
	salary_fuli_profit_salesperson as fuli_profit_commion,
	salesperson_prorate as prorate,
	salesperson_ripei_bbc_prorate as ripei_bbc_prorate,
	salesperson_fuli_prorate as fuli_prorate,
	salary_sales_value_salesperson+salary_profit_salesperson as commion_total,
	salary_ripei_bbc_sales_value_salesperson+salary_ripei_bbc_profit_salesperson as commion_ripei_bbc_total,
	salary_fuli_sales_value_salesperson+salary_fuli_profit_salesperson as commion_fuli_total,
	salesperson_over_rate,
	-1*customer_refund_sales_value as refund_sales_value, --本月退货金额
	-1*customer_refund_ripei_bbc_sales_value as ripei_bbc_refund_sales_value,--本月日配&BBC退货金额
	-1*customer_refund_fuli_sales_value as fuli_refund_sales_value,--本月福利退货金额
	${hiveconf:last_1day} as sdt,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as updated_time,
	smonth
from  
	csx_tmp.report_sss_r_m_crm_sales_customer_commission_new_4
;



