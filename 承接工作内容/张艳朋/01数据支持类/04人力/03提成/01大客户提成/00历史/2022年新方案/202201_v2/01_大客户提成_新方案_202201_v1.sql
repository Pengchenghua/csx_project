-- 新建表 销售提成_销售员收入组
--drop table csx_tmp.sales_income_info_new;
--create table if not exists `csx_tmp.sales_income_info_new` (
--  `cust_type` STRING comment '销售员类别',
--  `sales_name` STRING comment '业务员名称',
--  `work_no` STRING comment '业务员工号',
--  `income_type` STRING comment '业务员收入组类'
--) comment '销售提成_销售员收入组'
--partitioned by (sdt string comment '日期分区')
--row format delimited fields terminated by ','
--stored as textfile;

--=============================================================================================================================================================================
-- 确认需对哪些销售员补充收入组
set month_start_day ='20220101';	
set month_end_day ='20220131';
set last_month_end_day='20220131';

select 
	b.work_no,b.sales_name,c.income_type,sum(sales_value) sales_value,sum(profit) profit
from
	(
	select 
		province_code,province_name,customer_no,substr(sdt,1,6) smonth,sum(sales_value) sales_value,sum(profit) profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:month_start_day}
		and sdt<=${hiveconf:month_end_day}
		and channel_code in('1','7')
	group by 
		province_code,province_name,customer_no,substr(sdt,1,6)
	)a	
	left join 
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day}
		) b on b.customer_no=a.customer_no
	left join 
		(
		select 
			distinct work_no,income_type 
		from 
			csx_tmp.sales_income_info_new
		where 
			sdt=${hiveconf:last_month_end_day}
		) c on c.work_no=b.work_no
where 
	c.income_type is null
	and b.sales_name not rlike 'B|C' 
group by 
	b.work_no,b.sales_name,c.income_type;

--=============================================================================================================================================================================
-- 补充收入组并校验
load data inpath '/tmp/zhangyanpeng/sales_income_info_new_202201.csv' overwrite into table csx_tmp.sales_income_info_new partition (sdt=${hiveconf:month_end_day});
select * from csx_tmp.sales_income_info_new where sdt=${hiveconf:month_end_day};

--=============================================================================================================================================================================
-- 设置日期
set month_start_day ='20220101';	
set month_end_day ='20220131';	
set year_start_day ='20220101';		
		
-- 销售员年度累计销额提成比例
drop table csx_tmp.tc_salesperson_rate_ytd;
create table csx_tmp.tc_salesperson_rate_ytd
as
select 
	work_no,sales_name,income_type,salesperson_sales_value_ytd,salesperson_ripei_bbc_sales_value_ytd,salesperson_fuli_sales_value_ytd,
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
		b.work_no,b.sales_name,coalesce(c.income_type,'Q1') as income_type,
		sum(a.sales_value) as salesperson_sales_value_ytd,
		sum(a.ripei_bbc_sales_value) as salesperson_ripei_bbc_sales_value_ytd,
		sum(a.fuli_sales_value) as salesperson_fuli_sales_value_ytd
	from 
		(
		select 
			customer_no,regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') as sdt_last,
			sum(sales_value) as sales_value, --202107月签呈，W0K4仓不计算销售额，仅计算定价毛利额，每月处理
			sum(case when business_type_code in('1','6','4') then sales_value else 0 end) as ripei_bbc_sales_value,
			sum(case when business_type_code in('2') then sales_value else 0 end) as fuli_sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:year_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			--安徽省城市服务商2.0，按大客户提成方案计算
			and (business_type_code in('1','2','6') or (business_type_code in ('4') and customer_no in
				('117817','120939','121298','121625','122567','123244','124473','124498','124601','125284')))
			and province_name='重庆市'
		group by 
			customer_no,regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
		)a 
		left join   --CRM客户信息取每月最后一天
			(
			select 
				sdt,customer_no,customer_name,work_no,sales_name,
				case when channel_code='9' then '业务代理' end as ywdl_cust,
				case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust	
			from 
				csx_dw.dws_crm_w_a_customer 
			where 
				sdt>=${hiveconf:year_start_day}
				and sdt<=${hiveconf:month_end_day}
				and customer_no !=''
				and sdt=regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') --每月最后一天
			)b on b.customer_no=a.customer_no and b.sdt=a.sdt_last 	
		left join 
			(
			select 
				distinct work_no,income_type 
			from 
				csx_tmp.sales_income_info_new 
			where 
				sdt=${hiveconf:month_end_day}
			) c on c.work_no=b.work_no   --上月最后1日
	where 
		b.ywdl_cust is null -- 剔除业务代理和内购 or b.customer_no in ('118689','116957','116629'))
		and b.ng_cust is null
	group by 
		b.work_no,b.sales_name,coalesce(c.income_type,'Q1')
	)a
;

-- 客户本月销售额、定价毛利额统计
drop table csx_tmp.tc_customer_sales_value_profit_00;
create table csx_tmp.tc_customer_sales_value_profit_00
as
select 
	b.sales_province_name,a.customer_no,b.customer_name,d.work_no,d.sales_name,d.is_part_time_service_manager,
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
	coalesce(sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)),0) as customer_fuli_prorate-- 客户福利定价毛利率
	--sum(customer_bbc_sales_value) as customer_bbc_sales_value,
	--sum(customer_bbc_profit) as customer_bbc_profit
from 
	(
	select 
		customer_no,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sales_value) as customer_sales_value, 
		sum(case when business_type_code in('1','6','4') then sales_value else 0 end) as customer_ripei_bbc_sales_value,
		sum(case when business_type_code in('2') then sales_value else 0 end) as customer_fuli_sales_value,
		-- 各类型定价毛利额
		sum(profit) as customer_profit, 
		sum(case when business_type_code in('1','6','4') then profit else 0 end) as customer_ripei_bbc_profit,
		sum(case when business_type_code in('2') then profit else 0 end) as customer_fuli_profit
		-- BBC销售额、定价毛利额
		--sum(case when business_type_code in('6') then sales_value else 0 end) as customer_bbc_sales_value,
		--sum(case when business_type_code in('6') then profit else 0 end) as customer_bbc_profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:month_start_day} and sdt<=${hiveconf:month_end_day}
		and channel_code in('1','7','9')
		and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
		--安徽省城市服务商2.0，按大客户提成方案计算
		and (business_type_code in('1','2','6') or (business_type_code in ('4') and customer_no in
			('117817','120939','121298','121625','122567','123244','124473','124498','124601','125284')))
		and province_name='重庆市'			
	group by 
		sdt,substr(sdt,1,6),province_name,customer_no
		
	--扣减定价毛利额
	--202201月签呈，每月
	--重庆市
	union all select '105569' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
	-10000.00 as customer_profit,-10000.00 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit

	--202201月签呈，当月
	--重庆市
	union all select '107806' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-43426.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -43426.00 as customer_fuli_profit
	union all select '125143' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1548.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -1548.00 as customer_fuli_profit
	union all select '107844' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-640.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -640.00 as customer_fuli_profit
	union all select '114391' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-4350.48 as customer_profit,-4350.48 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '105518' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1200.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -1200.00 as customer_fuli_profit
	union all select '124212' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-7344.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -7344.00 as customer_fuli_profit
	union all select '124218' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-496.00 as customer_profit,-496.00 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '124433' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-2045.00 as customer_profit,-2045.00 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '119659' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-4880.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -4880.00 as customer_fuli_profit
	union all select '120105' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-3251.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -3251.00 as customer_fuli_profit
	union all select '121061' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-31047.76 as customer_profit,-31047.76 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '122129' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-13334.80 as customer_profit,-13334.80 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '124606' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-5000.00 as customer_profit,-5000.00 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '105186' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-16864.39 as customer_profit,-16864.39 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '118206' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-2582.67 as customer_profit,-2582.67 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '104965' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1909.06 as customer_profit,-1909.06 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '112813' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1873.31 as customer_profit,-1873.31 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117753' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1135.29 as customer_profit,-1135.29 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117727' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1569.41 as customer_profit,-1569.41 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117728' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-2146.53 as customer_profit,-2146.53 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117729' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-2867.15 as customer_profit,-2867.15 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117748' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1982.62 as customer_profit,-1982.62 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117773' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1148.49 as customer_profit,-1148.49 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117776' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1728.97 as customer_profit,-1728.97 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117782' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1463.21 as customer_profit,-1463.21 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117790' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1970.92 as customer_profit,-1970.92 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117791' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-982.93 as customer_profit,-982.93 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117795' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1263.36 as customer_profit,-1263.36 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117800' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1239.40 as customer_profit,-1239.40 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117805' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-3629.10 as customer_profit,-3629.10 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117918' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-278.90 as customer_profit,-278.90 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '117920' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-2163.81 as customer_profit,-2163.81 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '121113' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-14584.52 as customer_profit,-14584.52 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '120623' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-18976.98 as customer_profit,-18976.98 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '110866' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-853.74 as customer_profit,-853.74 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '113643' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-9290.84 as customer_profit,-9290.84 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '119517' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-365.87 as customer_profit,-365.87 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '120924' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-12949.68 as customer_profit,-12949.68 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '123084' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-14089.24 as customer_profit,-14089.24 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '115206' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1620.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -1620.00 as customer_fuli_profit
	union all select '118212' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-15401.35 as customer_profit,-15401.35 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '124589' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-17130.00 as customer_profit,0.00 as customer_ripei_bbc_profit, -17130.00 as customer_fuli_profit
	union all select '112177' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-8350.27 as customer_profit,-8350.27 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '113423' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1812.21 as customer_profit,-1812.21 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '115554' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-7194.38 as customer_profit,-7194.38 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '122247' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-2233.89 as customer_profit,-2233.89 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '120976' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-383.89 as customer_profit,-383.89 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '115253' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-676.50 as customer_profit,-676.50 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit
	union all select '112803' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_bbc_sales_value,0 as customer_fuli_sales_value,
		-1562.91 as customer_profit,-1562.91 as customer_ripei_bbc_profit, 0.00 as customer_fuli_profit

	
	)a
	left join 
		(
		select 
			distinct customer_no,customer_name,work_no,sales_name,
			sales_province_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day}
			and customer_no !=''
		)b on b.customer_no=a.customer_no
	left join 
		(
		select  
			distinct work_no,sales_name,income_type,salesperson_sales_value_ytd,salesperson_ripei_bbc_sales_value_ytd,salesperson_fuli_sales_value_ytd,
			salesperson_ripei_bbc_sales_value_tc_rate,salesperson_fuli_sales_value_tc_rate
		from 
			csx_tmp.tc_salesperson_rate_ytd 
		)c on c.work_no=b.work_no and c.sales_name=b.sales_name
	--关联服务管家
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,work_no,sales_name,is_part_time_service_manager,
			salesperson_sales_value_fp_rate,  --销售员_销售额_分配比例
			salesperson_profit_fp_rate,  --销售员_定价毛利额_分配比例
			service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
			service_user_profit_fp_rate	 --服务管家_定价毛利额_分配比例
		from 
			csx_tmp.tc_customer_service_manager_info_new
		)d on d.customer_no=a.customer_no
where 
	b.ywdl_cust is null -- or b.customer_no in ('118689','116957','116629'))
	and b.ng_cust is null
group by 
	b.sales_province_name,a.customer_no,b.customer_name,d.work_no,d.sales_name,d.is_part_time_service_manager,
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

-- 定价毛利额扣点
drop table csx_tmp.tc_customer_sales_value_profit_01;
create table csx_tmp.tc_customer_sales_value_profit_01
as
select 
	sales_province_name,a.customer_no,customer_name,work_no,sales_name,is_part_time_service_manager,service_user_work_no,service_user_name,
	salesperson_sales_value_fp_rate,salesperson_profit_fp_rate,service_user_sales_value_fp_rate,service_user_profit_fp_rate,smonth,salesperson_sales_value_ytd,
	salesperson_ripei_bbc_sales_value_ytd,salesperson_fuli_sales_value_ytd,salesperson_ripei_bbc_sales_value_tc_rate,salesperson_fuli_sales_value_tc_rate,
	-- 销售额
	customer_sales_value, -- 客户总销售额
	customer_ripei_bbc_sales_value, -- 客户日配&bbc销售额
	customer_fuli_sales_value, -- 客户福利销售额
	-- 定价毛利额
	(customer_ripei_bbc_profit-coalesce(customer_ripei_bbc_sales_value*ripei_bbc_kd_rate,0))+
	(customer_fuli_profit-coalesce(customer_fuli_sales_value*fuli_kd_rate,0)) as customer_profit,-- 客户总定价毛利额
	customer_ripei_bbc_profit-coalesce(customer_ripei_bbc_sales_value*ripei_bbc_kd_rate,0) as customer_ripei_bbc_profit,-- 客户日配&bbc定价毛利额
	customer_fuli_profit-coalesce(customer_fuli_sales_value*fuli_kd_rate,0) as customer_fuli_profit,-- 客户福利定价毛利额
	--定价毛利率
	coalesce(((customer_ripei_bbc_profit-coalesce(customer_ripei_bbc_sales_value*ripei_bbc_kd_rate,0))+
	(customer_fuli_profit-coalesce(customer_fuli_sales_value*fuli_kd_rate,0)))/abs(customer_sales_value),0) as customer_prorate, -- 客户总定价毛利率
	coalesce((customer_ripei_bbc_profit-coalesce(customer_ripei_bbc_sales_value*ripei_bbc_kd_rate,0))/abs(customer_ripei_bbc_sales_value),0) as customer_ripei_bbc_prorate, -- 客户日配&bbc定价毛利率
	coalesce((customer_fuli_profit-coalesce(customer_fuli_sales_value*fuli_kd_rate,0))/abs(customer_fuli_sales_value),0) as customer_fuli_prorate-- 客户福利定价毛利率
from
	csx_tmp.tc_customer_sales_value_profit_00 a 
	left join
		(
		select 'X000000' as customer_no, 0 as ripei_bbc_kd_rate, 0 as fuli_kd_rate
		union all select '105186' as customer_no, 0.01 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118206' as customer_no, 0.05 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '104965' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '112813' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117753' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117727' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117728' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117729' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117748' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117773' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117776' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117782' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117790' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117791' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117795' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117800' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117805' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117918' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117920' as customer_no, 0.03 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '119659' as customer_no, 0.03 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120105' as customer_no, 0.03 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '110866' as customer_no, 0.01 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113643' as customer_no, 0.03 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '119517' as customer_no, 0.01 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '121061' as customer_no, 0.01 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120924' as customer_no, 0.06 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '123084' as customer_no, 0.05 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115206' as customer_no, 0.00 as ripei_bbc_kd_rate, 0.03 as fuli_kd_rate
		union all select '118212' as customer_no, 0.10 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '124589' as customer_no, 0.00 as ripei_bbc_kd_rate, 0.10 as fuli_kd_rate
		union all select '112177' as customer_no, 0.05 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115554' as customer_no, 0.05 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '114391' as customer_no, 0.05 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '122247' as customer_no, 0.03 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120976' as customer_no, 0.02 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115253' as customer_no, 0.02 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '112803' as customer_no, 0.02 as ripei_bbc_kd_rate, 0.00 as fuli_kd_rate
		) b on b.customer_no=a.customer_no
;

-- 销售员本月定价毛利率，计算销售员定价毛利额提成比例
drop table csx_tmp.tc_salesperson_profit_tc_rate;
create table csx_tmp.tc_salesperson_profit_tc_rate
as
select
	work_no,sales_name,salesperson_sales_value,salesperson_ripei_bbc_sales_value,salesperson_fuli_sales_value,salesperson_profit,salesperson_ripei_bbc_profit,
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
		work_no,sales_name,
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
		csx_tmp.tc_customer_sales_value_profit_01
	group by 
		work_no,sales_name
	) a 
;

--客户本月提成，未乘分配比例
drop table csx_tmp.tc_customer_salary_00;
create table csx_tmp.tc_customer_salary_00
as
select 
	a.smonth,a.sales_province_name,a.customer_no,a.customer_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.customer_sales_value,a.customer_ripei_bbc_sales_value,a.customer_fuli_sales_value,
	a.customer_profit,a.customer_ripei_bbc_profit,a.customer_fuli_profit,
	a.customer_prorate,a.customer_ripei_bbc_prorate,a.customer_fuli_prorate,
	coalesce(e.salesperson_ripei_bbc_prorate,0) as salesperson_ripei_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
	coalesce(e.salesperson_fuli_prorate,0) as salesperson_fuli_prorate, -- 销售员本月福利定价毛利率
	coalesce(a.customer_ripei_bbc_sales_value*a.salesperson_ripei_bbc_sales_value_tc_rate,0)+coalesce(a.customer_fuli_sales_value*a.salesperson_fuli_sales_value_tc_rate,0) as salary_sales_value, -- 奖金包_销售额
	coalesce(a.customer_ripei_bbc_profit*e.salesperson_ripei_bbc_profit_tc_rate,0)+coalesce(a.customer_fuli_profit*e.salesperson_fuli_profit_tc_rate,0) as salary_profit, --奖金包_定价毛利额
	b.receivable_amount,b.overdue_amount,
	if(a.service_user_work_no<>'','服务管家有提成','服务管家无提成') assigned_type, --分配类别
	a.salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	a.salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
	a.service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	a.service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
	coalesce(c.over_rate,0) as salesperson_over_rate,
	coalesce(d.over_rate,0) as service_user_over_rate,
	salesperson_sales_value_ytd,-- 销售员年度累计销售额
	salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	salesperson_fuli_sales_value_ytd -- 销售员年度累计福利销售额
	--customer_bbc_sales_value,
	--customer_bbc_profit,
	--coalesce(a.customer_bbc_sales_value*a.salesperson_ripei_bbc_sales_value_tc_rate,0)+coalesce(a.customer_fuli_sales_value*a.salesperson_fuli_sales_value_tc_rate,0) as salary_fuli_bbc_sales_value, -- 奖金包_销售额
	--coalesce(a.customer_bbc_profit*e.salesperson_ripei_bbc_profit_tc_rate,0)+coalesce(a.customer_fuli_profit*e.salesperson_fuli_profit_tc_rate,0) as salary_fuli_bbc_profit --奖金包_定价毛利额
from  
	(
	select 
		sales_province_name,customer_no,customer_name,work_no,sales_name,is_part_time_service_manager,
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
		salesperson_fuli_sales_value_ytd -- 销售员年度累计福利销售额
		--customer_bbc_sales_value,
		--customer_bbc_profit
	from 
		csx_tmp.tc_customer_sales_value_profit_01
	)a
	left join csx_tmp.tc_cust_over_rate b on b.customer_no=a.customer_no
	left join csx_tmp.tc_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
	left join csx_tmp.tc_service_user_over_rate d on d.service_user_name=a.service_user_name and coalesce(d.service_user_work_no,0)=coalesce(a.service_user_work_no,0)
	left join csx_tmp.tc_salesperson_profit_tc_rate e on e.work_no=a.work_no and e.sales_name=a.sales_name
;

--客户本月提成，乘分配比例
drop table csx_tmp.tc_new_cust_salary; --11
create table csx_tmp.tc_new_cust_salary
as
select 
	a.smonth,a.sales_province_name,a.customer_no,a.customer_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.customer_sales_value,a.customer_ripei_bbc_sales_value,a.customer_fuli_sales_value,
	a.customer_profit,a.customer_ripei_bbc_profit,a.customer_fuli_profit,
	a.customer_prorate,a.customer_ripei_bbc_prorate,a.customer_fuli_prorate,
	a.salesperson_ripei_bbc_prorate,a.salesperson_fuli_prorate,
	a.salary_sales_value, -- 奖金包_销售额
	a.salary_profit, --奖金包_定价毛利额
	a.receivable_amount,a.overdue_amount,
	a.assigned_type, --分配类别
	a.salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	a.salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
	a.service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	a.service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
	a.salesperson_over_rate,
	a.service_user_over_rate,
	--提成_销售额_销售员
	a.salary_sales_value*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_sales_value_fp_rate,0) as salary_sales_value_salesperson, --提成_销售额_销售员
	--提成_销售额_服务管家
	a.salary_sales_value*(1-coalesce(if(a.service_user_over_rate<=0.5,a.service_user_over_rate,1),0))*coalesce(a.service_user_sales_value_fp_rate,0) salary_sales_value_service,--提成_销售额_服务管家
	--提成_定价毛利额_销售员
	a.salary_profit*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_profit_fp_rate,0) salary_profit_salesperson,--提成_定价毛利额_销售员
	--提成_定价毛利额_服务管家
	a.salary_profit*(1-coalesce(if(a.service_user_over_rate<=0.5,a.service_user_over_rate,1),0))*coalesce(a.service_user_profit_fp_rate,0) salary_profit_service, --提成_定价毛利额_服务管家
	a.salesperson_sales_value_ytd,-- 销售员年度累计销售额
	a.salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	a.salesperson_fuli_sales_value_ytd -- 销售员年度累计福利销售额
	--a.customer_bbc_sales_value,
	--a.customer_bbc_profit,
	--a.salary_fuli_bbc_sales_value,
	--a.salary_fuli_bbc_profit,
	--提成_销售额_销售员
	--a.salary_fuli_bbc_sales_value*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_sales_value_fp_rate,0) as salary_fuli_bbc_sales_value_salesperson, --提成_销售额_销售员
	--提成_销售额_服务管家
	--a.salary_fuli_bbc_sales_value*(1-coalesce(if(a.service_user_over_rate<=0.5,a.service_user_over_rate,1),0))*coalesce(a.service_user_sales_value_fp_rate,0) salary_fuli_bbc_sales_value_service,--提成_销售额_服务管家
	--提成_定价毛利额_销售员
	--a.salary_fuli_bbc_profit*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_profit_fp_rate,0) salary_fuli_bbc_profit_salesperson,--提成_定价毛利额_销售员
	--提成_定价毛利额_服务管家
	--a.salary_fuli_bbc_profit*(1-coalesce(if(a.service_user_over_rate<=0.5,a.service_user_over_rate,1),0))*coalesce(a.service_user_profit_fp_rate,0) salary_fuli_bbc_profit_service --提成_定价毛利额_服务管家
from
	csx_tmp.tc_customer_salary_00 a 
;


--客户本月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_kehu' row format delimited fields terminated by '\t'
select 
	smonth,sales_province_name,customer_no,customer_name,work_no,sales_name,is_part_time_service_manager,service_user_work_no,service_user_name,customer_sales_value,customer_ripei_bbc_sales_value,customer_fuli_sales_value,
	customer_profit,customer_ripei_bbc_profit,customer_fuli_profit,customer_prorate,customer_ripei_bbc_prorate,customer_fuli_prorate,salesperson_ripei_bbc_prorate,salesperson_fuli_prorate,salary_sales_value,salary_profit,
	receivable_amount,overdue_amount,assigned_type,salesperson_sales_value_fp_rate,salesperson_profit_fp_rate,service_user_sales_value_fp_rate,
	service_user_profit_fp_rate,salesperson_over_rate,service_user_over_rate,salary_sales_value_salesperson,salary_sales_value_service,salary_profit_salesperson,salary_profit_service,
	coalesce(salary_sales_value_salesperson,0)+coalesce(salary_profit_salesperson,0) as total_salesperson,
	coalesce(salary_sales_value_service,0)+coalesce(salary_profit_service,0) as total_service
	--customer_bbc_sales_value,customer_bbc_profit,
	--salary_fuli_bbc_sales_value,salary_fuli_bbc_profit,
	--salary_fuli_bbc_sales_value_salesperson,salary_fuli_bbc_sales_value_service,salary_fuli_bbc_profit_salesperson,salary_fuli_bbc_profit_service,
	--coalesce(salary_fuli_bbc_sales_value_salesperson,0)+coalesce(salary_fuli_bbc_profit_salesperson,0) as total_fuli_bbc_salesperson,
	--coalesce(salary_fuli_bbc_sales_value_service,0)+coalesce(salary_fuli_bbc_profit_service,0) as total_fuli_bbc_service
from
	csx_tmp.tc_new_cust_salary
;

--销售员本月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_xiaoshou' row format delimited fields terminated by '\t'
select
	smonth,sales_province_name,work_no,sales_name,
	salesperson_sales_value_ytd,-- 销售员年度累计销售额
	salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	salesperson_fuli_sales_value_ytd,-- 销售员年度累计福利销售额
	sum(customer_sales_value) as customer_sales_value,
	sum(customer_ripei_bbc_sales_value) as customer_ripei_bbc_sales_value,
	sum(customer_fuli_sales_value) as customer_fuli_sales_value,
	sum(customer_profit) as customer_profit,
	sum(customer_ripei_bbc_profit) as customer_ripei_bbc_profit,
	sum(customer_fuli_profit) as customer_fuli_profit,
	coalesce(sum(customer_profit)/abs(sum(customer_sales_value)),0) as customer_prorate,
	coalesce(sum(customer_ripei_bbc_profit)/abs(sum(customer_ripei_bbc_sales_value)),0) as customer_ripei_bbc_prorate,
	coalesce(sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)),0) as customer_fuli_prorate,
	sum(salary_sales_value) as salary_sales_value,
	sum(salary_profit) as salary_profit,
	sum(receivable_amount) as receivable_amount,
	sum(overdue_amount) as overdue_amount,
	salesperson_over_rate,
	sum(salary_sales_value_salesperson) as salary_sales_value_salesperson,
	sum(salary_profit_salesperson) as salary_profit_salesperson,
	coalesce(sum(salary_sales_value_salesperson),0)+coalesce(sum(salary_profit_salesperson),0) salary_sale
	--sum(customer_bbc_sales_value) as customer_bbc_sales_value,
	--sum(customer_bbc_profit) as customer_bbc_profit,
	--sum(salary_fuli_bbc_sales_value) as salary_fuli_bbc_sales_value,
	--sum(salary_fuli_bbc_profit) as salary_fuli_bbc_profit,
	--sum(salary_fuli_bbc_sales_value_salesperson) as salary_fuli_bbc_sales_value_salesperson,
	--sum(salary_fuli_bbc_profit_salesperson) as salary_fuli_bbc_profit_salesperson,
	--coalesce(sum(salary_fuli_bbc_sales_value_salesperson),0)+coalesce(sum(salary_fuli_bbc_profit_salesperson),0) tc_salesperson
from 
	csx_tmp.tc_new_cust_salary
group by 
	smonth,sales_province_name,work_no,sales_name,
	salesperson_sales_value_ytd,-- 销售员年度累计销售额
	salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	salesperson_fuli_sales_value_ytd,-- 销售员年度累计福利销售额
	salesperson_over_rate
;

--服务管家本月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_fuwuguanjia' row format delimited fields terminated by '\t'
select
	smonth,sales_province_name,service_user_work_no,service_user_name,
	sum(customer_sales_value) as customer_sales_value,
	sum(customer_ripei_bbc_sales_value) as customer_ripei_bbc_sales_value,
	sum(customer_fuli_sales_value) as customer_fuli_sales_value,
	sum(customer_profit) as customer_profit,
	sum(customer_ripei_bbc_profit) as customer_ripei_bbc_profit,
	sum(customer_fuli_profit) as customer_fuli_profit,
	coalesce(sum(customer_profit)/abs(sum(customer_sales_value)),0) as customer_prorate,
	coalesce(sum(customer_ripei_bbc_profit)/abs(sum(customer_ripei_bbc_sales_value)),0) as customer_ripei_bbc_prorate,
	coalesce(sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)),0) as customer_fuli_prorate,
	sum(salary_sales_value) as salary_sales_value,
	sum(salary_profit) as salary_profit,
	sum(receivable_amount) as receivable_amount,
	sum(overdue_amount) as overdue_amount,
	service_user_over_rate,
	sum(salary_sales_value_service) as salary_sales_value_service,
	sum(salary_profit_service) as salary_profit_service,
	coalesce(sum(salary_sales_value_service),0)+coalesce(sum(salary_profit_service),0) salary_service
	--sum(customer_bbc_sales_value) as customer_bbc_sales_value,
	--sum(customer_bbc_profit) as customer_bbc_profit,
	--sum(salary_fuli_bbc_sales_value) as salary_fuli_bbc_sales_value,
	--sum(salary_fuli_bbc_profit) as salary_fuli_bbc_profit,
	--sum(salary_fuli_bbc_sales_value_service) as salary_fuli_bbc_sales_value_service,
	--sum(salary_fuli_bbc_profit_service) as salary_fuli_bbc_profit_service,
	--coalesce(sum(salary_fuli_bbc_sales_value_service),0)+coalesce(sum(salary_fuli_bbc_profit_service),0) tc_service
from 
	csx_tmp.tc_new_cust_salary
group by 
	smonth,sales_province_name,service_user_work_no,service_user_name,service_user_over_rate
;


--===============================================================================================================================================================================


/*
-- 大客户提成：月度新客户
select 
	b.sales_province_name,b.customer_no,b.customer_name,b.attribute_desc,b.dev_source_name,b.work_no,b.sales_name,b.sign_date,
	a.first_order_date
from
	(
	select 
		attribute_desc,dev_source_name,customer_no,customer_name,channel_name,sales_name,work_no,sales_province_name,
		regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,estimate_contract_amount*10000 estimate_contract_amount
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt='current'
		and customer_no<>''
		and channel_code in('1','7','8')
	)b
	join --客户最早销售月 新客月、新客季度
		(
		select 
			customer_no,
			min(first_order_date) first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current'
		group by 
			customer_no
		having 
			min(first_order_date)>='20220101' and min(first_order_date)<='20220131'
		)a on b.customer_no=a.customer_no;

--客户对应销售员与服务管家
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	* 
from  
	csx_dw.report_crm_w_a_customer_service_manager_info_new
where  
	sdt= '20220131'
	and channel_code in('1','7')
	and (is_sale='是' or is_overdue='是')
	

--大客户销售员对照表
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	sales_province_name,customer_no,customer_name,work_no,sales_name,dev_source_name,
	city_group_name,channel_name,
	regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
	regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
from 
	csx_dw.dws_crm_w_a_customer
	--where sdt='20210617'
where 
	sdt=${hiveconf:i_sdate_11}  
	and channel_code in('1','7','8','9');




---截至上月销售员的累计销售额
drop table csx_dw.dws_cust_ytd_sale;
create table csx_dw.dws_cust_ytd_sale
as
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select b.work_no,b.sales_name,a.smonth,c.income_type,
sum(a.sales_value)sales_value,
sum(a.profit)profit
from 
  (select customer_no,substr(sdt,1,6) smonth,
  sum(sales_value) sales_value,
  sum(profit)profit
   from csx_dw.dws_sale_r_d_detail
  where sdt>='20210101' and sdt<=${hiveconf:i_sdate_11}  
  and channel_code in('1','7','9')
  and business_type_code not in('3','4')
  --福建泉州签呈，订单12月销售530181.06元，1月全部退货，不算提成
  and (order_no not in ('OM20122800005550','RH21011900000203') or order_no is null)		
  --签呈客户不考核，不算提成 2021年3月签呈取消剔除103717
  and customer_no not in('111118','102755','104023','105673','104402')
  and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')		
  --3月签呈 剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
  and customer_no not in('115721','116877','116883','116015','116556','116826')
  and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                            '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                            '106321','106325','106326','106330','104609')	
  --4月签呈 每月处理：剔除逾期系数，不算提成，每月处理
  and customer_no not in('102844','117940')  
  group by customer_no,substr(sdt,1,6)
  )a 
left join   --CRM客户信息取每月最后一天
  (select * ,
    substr(sdt,1,6) smonth,
    case when channel_code='9' then '业务代理' end as ywdl_cust,
    case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust	
  from csx_dw.dws_crm_w_a_customer 
  where sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
  and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
             regexp_replace(date_sub(current_date,1),'-',''),
             regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
             )  --sdt为每月最后一天
  )b on b.customer_no=a.customer_no and b.smonth=a.smonth 
left join (select distinct work_no,income_type from csx_tmp.sales_income_info_new where sdt=${hiveconf:i_sdate_11}) c on c.work_no=b.work_no   --上月最后1日
--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
where (b.ywdl_cust is null or b.customer_no='118689')
and b.ng_cust is null 
group by b.work_no,b.sales_name,a.smonth,c.income_type;


--1月客户销售员对照表
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	customer_no,customer_name,sales_province_name,work_no,sales_name,service_user_work_no,service_user_name,
	is_part_time_service_manager,sales_sale_rate,sales_profit_rate,service_user_sale_rate,service_user_profit_rate
from  
	csx_dw.report_crm_w_a_customer_service_manager_info_new
where  
	sdt= '20220131'
	and channel_code in('1','7')
	and (is_sale='是' or is_overdue='是')
*/
