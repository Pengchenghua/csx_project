-- 确认需对哪些销售员补充收入组
set month_start_day ='20220201';	
set month_end_day ='20220228';
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
	left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:month_end_day}) b on b.customer_no=a.customer_no
	left join (select distinct work_no,income_type from csx_tmp.sales_income_info_new where sdt=${hiveconf:last_month_end_day}) c on c.work_no=b.work_no
where 
	c.income_type is null and b.sales_name not rlike 'B|C' 
group by 
	b.work_no,b.sales_name,c.income_type;

--=============================================================================================================================================================================
-- 补充收入组并校验
load data inpath '/tmp/zhangyanpeng/sales_income_info_new_202202.csv' overwrite into table csx_tmp.sales_income_info_new partition (sdt=${hiveconf:month_end_day});
select * from csx_tmp.sales_income_info_new where sdt=${hiveconf:month_end_day};

--=============================================================================================================================================================================
-- 设置日期
set month_start_day ='20220201';	
set month_end_day ='20220228';
set last_month_end_day='20220131';	
		
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
			sum(case when business_type_code in('1','4','5','6') then sales_value else 0 end) as ripei_bbc_sales_value,
			sum(case when business_type_code in('2') then sales_value else 0 end) as fuli_sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:year_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('5') and province_name = '平台-B') --平台酒水
				--安徽省城市服务商2.0，按大客户提成方案计算
				or (business_type_code in ('4') and customer_no in
				('117817','120939','121298','121625','122567','123244','124473','124498','124601','125284')))
			and province_name in ('福建省')
			and dc_name not like '%V2DC%' --2.0 按仓库名称判断
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
	b.sales_province_name,a.customer_no,b.customer_name,a.smonth,
	-- 销售额
	sum(customer_sales_value) as customer_sales_value, -- 客户总销售额
	sum(customer_ripei_sales_value) as customer_ripei_sales_value, -- 客户日配销售额
	sum(customer_bbc_sales_value) as customer_bbc_sales_value, -- 客户bbc销售额
	sum(customer_fuli_sales_value) as customer_fuli_sales_value, -- 客户福利销售额
	sum(customer_ripei_sales_value)+sum(customer_bbc_sales_value) as customer_ripei_bbc_sales_value,
	-- 定价毛利额
	sum(customer_profit) as customer_profit,-- 客户总定价毛利额
	sum(customer_ripei_profit) as customer_ripei_profit,-- 客户日配定价毛利额
	sum(customer_bbc_profit) as customer_bbc_profit,-- 客户bbc定价毛利额
	sum(customer_fuli_profit) as customer_fuli_profit,-- 客户福利定价毛利额
	sum(customer_ripei_profit)+sum(customer_bbc_profit) as customer_ripei_bbc_profit
from 
	(
	select 
		customer_no,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sales_value) as customer_sales_value,
		sum(case when business_type_code in ('1','4','5') then sales_value else 0 end) as customer_ripei_sales_value,
		sum(case when business_type_code in('6') then sales_value else 0 end) as customer_bbc_sales_value,
		sum(case when business_type_code in('2') then sales_value else 0 end) as customer_fuli_sales_value,
		-- 各类型定价毛利额
		sum(profit) as customer_profit,
		sum(case when business_type_code in ('1','4','5') then profit else 0 end) as customer_ripei_profit,
		sum(case when business_type_code in('6') then profit else 0 end) as customer_bbc_profit,
		sum(case when business_type_code in('2') then profit else 0 end) as customer_fuli_profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:month_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('5') and province_name = '平台-B') --平台酒水
				--安徽省城市服务商2.0，按大客户提成方案计算
				or (business_type_code in ('4') and customer_no in
				('117817','120939','121298','121625','122567','123244','124473','124498','124601','125284')))
			and province_name in ('福建省')
	group by 
		customer_no,substr(sdt,1,6)
		
	--扣减定价毛利额
	--202201月签呈，每月
	--重庆市
	union all select '105569' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_sales_value,0 as customer_bbc_sales_value,0 as customer_fuli_sales_value,
	-10000.00 as customer_profit,-10000.00 as customer_ripei_profit, 0.00 as customer_bbc_profit, 0.00 as customer_fuli_profit

	--202201月签呈，当月
	--重庆市
	union all select '107806' as customer_no,'202201' as smonth,0 as customer_sales_value, 0 as customer_ripei_sales_value,0 as customer_bbc_sales_value,0 as customer_fuli_sales_value,
	-7529.00 as customer_profit,0.00 as customer_ripei_profit, 0.00 as customer_bbc_profit, -7529.00 as customer_fuli_profit
	
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
where 
	b.ywdl_cust is null -- or b.customer_no in ('118689','116957','116629'))
	and b.ng_cust is null
group by 
	b.sales_province_name,a.customer_no,b.customer_name,a.smonth
;

-- 客户定价毛利额扣点
drop table csx_tmp.tc_customer_sales_value_profit_01;
create table csx_tmp.tc_customer_sales_value_profit_01
as
select 
	a.sales_province_name,a.customer_no,a.customer_name,a.smonth,
	c.service_user_work_no,c.service_user_name,c.work_no,c.sales_name,c.is_part_time_service_manager,
	c.salesperson_sales_value_fp_rate,  --销售员_销售额_分配比例
	c.salesperson_profit_fp_rate,  --销售员_定价毛利额_分配比例
	c.service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	c.service_user_profit_fp_rate,	 --服务管家_定价毛利额_分配比例
	-- 销售额
	customer_sales_value, -- 客户总销售额
	customer_ripei_sales_value, -- 客户日配销售额
	customer_bbc_sales_value, -- 客户bbc销售额
	customer_fuli_sales_value, -- 客户福利销售额
	customer_ripei_bbc_sales_value, -- 客户日配&bbc销售额
	-- 定价毛利额
	(customer_ripei_profit-coalesce(customer_ripei_sales_value*ripei_kd_rate,0))+
	(customer_bbc_profit-coalesce(customer_bbc_sales_value*bbc_kd_rate,0))+
	(customer_fuli_profit-coalesce(customer_fuli_sales_value*fuli_kd_rate,0)) as customer_profit,-- 客户总定价毛利额
	
	customer_ripei_profit-coalesce(customer_ripei_sales_value*ripei_kd_rate,0) as customer_ripei_profit,-- 客户日配定价毛利额
	customer_bbc_profit-coalesce(customer_bbc_sales_value*bbc_kd_rate,0) as customer_bbc_profit,-- 客户bbc定价毛利额
	customer_fuli_profit-coalesce(customer_fuli_sales_value*fuli_kd_rate,0) as customer_fuli_profit,-- 客户福利定价毛利额
	
	(customer_ripei_profit-coalesce(customer_ripei_sales_value*ripei_kd_rate,0))+
	(customer_bbc_profit-coalesce(customer_bbc_sales_value*bbc_kd_rate,0)) as customer_ripei_bbc_profit,
	--定价毛利率
	coalesce(((customer_ripei_profit-coalesce(customer_ripei_sales_value*ripei_kd_rate,0))+
	(customer_bbc_profit-coalesce(customer_bbc_sales_value*bbc_kd_rate,0))+
	(customer_fuli_profit-coalesce(customer_fuli_sales_value*fuli_kd_rate,0)))/abs(customer_sales_value),0) as customer_prorate, -- 客户总定价毛利率
	
	coalesce((customer_ripei_profit-coalesce(customer_ripei_sales_value*ripei_kd_rate,0))/abs(customer_ripei_sales_value),0) as customer_ripei_prorate, -- 客户日配定价毛利率
	coalesce((customer_bbc_profit-coalesce(customer_bbc_sales_value*bbc_kd_rate,0))/abs(customer_bbc_sales_value),0) as customer_bbc_prorate, -- 客户bbc定价毛利率
	coalesce((customer_fuli_profit-coalesce(customer_fuli_sales_value*fuli_kd_rate,0))/abs(customer_fuli_sales_value),0) as customer_fuli_prorate,-- 客户福利定价毛利率
	coalesce(((customer_ripei_profit-coalesce(customer_ripei_sales_value*ripei_kd_rate,0))+
	(customer_bbc_profit-coalesce(customer_bbc_sales_value*bbc_kd_rate,0)))/abs(customer_ripei_bbc_sales_value),0) as customer_ripei_bbc_prorate
from
	csx_tmp.tc_customer_sales_value_profit_00 a 
	left join
		(
		--202201月签呈，重庆市，每月
		select 'X000000' as customer_no, 0.00 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105186' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118206' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '104965' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '112813' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117753' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117727' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117728' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117729' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117748' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117773' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117776' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117782' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117790' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117791' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117795' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117800' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117805' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117918' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117920' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '119659' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120105' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '110866' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113643' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '119517' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '121061' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120924' as customer_no, 0.06 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '123084' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115206' as customer_no, 0.00 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.03 as fuli_kd_rate
		union all select '118212' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '124589' as customer_no, 0.00 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.10 as fuli_kd_rate
		union all select '112177' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115554' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '114391' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '122247' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120976' as customer_no, 0.02 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115253' as customer_no, 0.02 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '112803' as customer_no, 0.02 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		
		--202201月签呈，福建省，每月
		union all select '102734' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '102784' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '102901' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113263' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115366' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select 'PF0649' as customer_no, 0.09 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '114038' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113088' as customer_no, 0.04 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '102924' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '102524' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105703' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105750' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '106698' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '112553' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113678' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113679' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113746' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113760' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '113805' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115602' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117244' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '104281' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '107398' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '108589' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120459' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118653' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118654' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118682' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118705' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118730' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118934' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118961' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '119185' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '119172' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120666' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115961' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '115985' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '118689' as customer_no, 0.02 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '116355' as customer_no, 0.02 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105150' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105156' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105177' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105181' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105182' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '106423' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '106721' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '107404' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105164' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '105165' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '119990' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '106805' as customer_no, 0.10 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '117676' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '123252' as customer_no, 0.08 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '108557' as customer_no, 0.02 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '116707' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '116561' as customer_no, 0.05 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '120250' as customer_no, 0.03 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fuli_kd_rate
		union all select '119213' as customer_no, 0.00 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fuli_kd_rate
		union all select '113515' as customer_no, 0.00 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fuli_kd_rate
		union all select '106587' as customer_no, 0.00 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.05 as fuli_kd_rate
		union all select '116131' as customer_no, 0.00 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.06 as fuli_kd_rate
		union all select '103782' as customer_no, 0.00 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.06 as fuli_kd_rate
		union all select '115656' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fuli_kd_rate
		union all select '115826' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fuli_kd_rate
		union all select '121054' as customer_no, 0.02 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.02 as fuli_kd_rate
		union all select '116947' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fuli_kd_rate
		union all select '111207' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fuli_kd_rate
		union all select '125534' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fuli_kd_rate
		union all select '115537' as customer_no, 0.01 as ripei_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fuli_kd_rate
		
		) b on b.customer_no=a.customer_no
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
		)c on c.customer_no=a.customer_no
			
;

-- 销售员本月定价毛利率，计算销售员定价毛利额提成比例
drop table csx_tmp.tc_salesperson_profit_tc_rate;
create table csx_tmp.tc_salesperson_profit_tc_rate
as
select
	work_no,sales_name,salesperson_sales_value,salesperson_ripei_bbc_sales_value,salesperson_fuli_sales_value,salesperson_profit,salesperson_ripei_bbc_profit,
	salesperson_fuli_profit,salesperson_prorate,salesperson_ripei_bbc_prorate,salesperson_fuli_prorate,
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
	a.customer_sales_value,a.customer_ripei_sales_value,a.customer_bbc_sales_value,a.customer_fuli_sales_value,a.customer_ripei_bbc_sales_value,
	a.customer_profit,a.customer_ripei_profit,a.customer_bbc_profit,a.customer_fuli_profit,a.customer_ripei_bbc_profit,
	a.customer_prorate,a.customer_ripei_prorate,a.customer_bbc_prorate,a.customer_fuli_prorate,a.customer_ripei_bbc_prorate,
	coalesce(e.salesperson_ripei_bbc_prorate,0) as salesperson_ripei_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
	coalesce(e.salesperson_fuli_prorate,0) as salesperson_fuli_prorate, -- 销售员本月福利定价毛利率
	--奖金包_福利业务
	coalesce(a.customer_fuli_sales_value*f.salesperson_fuli_sales_value_tc_rate,0) as salary_fuli_sales_value, --奖金包_福利销售额
	coalesce(a.customer_fuli_profit*e.salesperson_fuli_profit_tc_rate,0) as salary_fuli_profit, --奖金包_福利定价毛利额
	--奖金包_日配业务
	coalesce(a.customer_ripei_sales_value*f.salesperson_ripei_bbc_sales_value_tc_rate,0) as salary_ripei_sales_value, -- 奖金包_日配销售额
	coalesce(a.customer_ripei_profit*e.salesperson_ripei_bbc_profit_tc_rate,0) as salary_ripei_profit, --奖金包_日配定价毛利额
	--奖金包_BBC业务
	coalesce(a.customer_bbc_sales_value*f.salesperson_ripei_bbc_sales_value_tc_rate,0) as salary_bbc_sales_value, -- 奖金包_bbc销售额
	coalesce(a.customer_bbc_profit*e.salesperson_ripei_bbc_profit_tc_rate,0) as salary_bbc_profit, --奖金包_bbc定价毛利额	
	--奖金包_日配&BBC业务
	coalesce(a.customer_ripei_bbc_sales_value*f.salesperson_ripei_bbc_sales_value_tc_rate,0) as salary_sales_value, -- 奖金包_销售额
	coalesce(a.customer_ripei_bbc_profit*e.salesperson_ripei_bbc_profit_tc_rate,0) as salary_profit, --奖金包_定价毛利额
	b.receivable_amount,b.overdue_amount,
	if(a.service_user_work_no<>'','服务管家有提成','服务管家无提成') assigned_type, --分配类别
	a.salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
	a.salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
	a.service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
	a.service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
	coalesce(c.over_rate,0) as salesperson_over_rate,
	coalesce(d.over_rate,0) as service_user_over_rate,
	f.salesperson_sales_value_ytd,-- 销售员年度累计销售额
	f.salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	f.salesperson_fuli_sales_value_ytd -- 销售员年度累计福利销售额
from  
	(
	select 
		sales_province_name,customer_no,customer_name,work_no,sales_name,is_part_time_service_manager,service_user_work_no,service_user_name,
		salesperson_sales_value_fp_rate, --销售员_销售额_分配比例
		salesperson_profit_fp_rate,  --销售员_定价毛利额分配比例
		service_user_sales_value_fp_rate,  --服务管家_销售额_分配比例
		service_user_profit_fp_rate, --服务管家_定价毛利额_分配比例
		smonth,customer_sales_value,customer_ripei_sales_value,customer_bbc_sales_value,customer_fuli_sales_value,customer_ripei_bbc_sales_value,
		customer_profit,customer_ripei_profit,customer_bbc_profit,customer_fuli_profit,customer_ripei_bbc_profit,
		customer_prorate,customer_ripei_prorate,customer_bbc_prorate,customer_fuli_prorate,customer_ripei_bbc_prorate
	from 
		csx_tmp.tc_customer_sales_value_profit_01
	)a
	left join csx_tmp.tc_cust_over_rate b on b.customer_no=a.customer_no
	left join csx_tmp.tc_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
	left join csx_tmp.tc_service_user_over_rate d on d.service_user_name=a.service_user_name and coalesce(d.service_user_work_no,0)=coalesce(a.service_user_work_no,0)
	left join csx_tmp.tc_salesperson_profit_tc_rate e on e.work_no=a.work_no and e.sales_name=a.sales_name
	left join csx_tmp.tc_salesperson_rate_ytd f on f.work_no=a.work_no and f.sales_name=a.sales_name
;

--客户本月提成，乘分配比例
drop table csx_tmp.tc_new_cust_salary; --11
create table csx_tmp.tc_new_cust_salary
as
select 
	a.smonth,a.sales_province_name,a.customer_no,a.customer_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.customer_sales_value,a.customer_ripei_sales_value,a.customer_bbc_sales_value,a.customer_fuli_sales_value,a.customer_ripei_bbc_sales_value,
	a.customer_profit,a.customer_ripei_profit,a.customer_bbc_profit,a.customer_fuli_profit,a.customer_ripei_bbc_profit,
	a.customer_prorate,a.customer_ripei_prorate,a.customer_bbc_prorate,a.customer_fuli_prorate,a.customer_ripei_bbc_prorate,
	a.salesperson_ripei_bbc_prorate,a.salesperson_fuli_prorate,
	--奖金包_福利业务
	a.salary_fuli_sales_value,
	a.salary_fuli_profit,
	--奖金包_日配业务
	a.salary_ripei_sales_value,
	a.salary_ripei_profit,
	--奖金包_BBC业务
	a.salary_bbc_sales_value,
	a.salary_bbc_profit, 
	--奖金包_日配&BBC业务
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
	a.salary_fuli_sales_value*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0)) as salary_fuli_sales_value_salesperson,
	a.salary_fuli_profit*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0)) as salary_fuli_profit_salesperson,
	--提成_销售额_销售员
	--a.salary_sales_value*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_sales_value_fp_rate,0) as salary_sales_value_salesperson, --提成_销售额_销售员
	a.salary_sales_value*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_sales_value_fp_rate,0) as salary_sales_value_salesperson, --提成_销售额_销售员
	--提成_销售额_服务管家
	a.salary_sales_value*(1-coalesce(if(a.service_user_over_rate<=0.5,a.service_user_over_rate,1),0))*coalesce(a.service_user_sales_value_fp_rate,0) salary_sales_value_service,--提成_销售额_服务管家
	--提成_定价毛利额_销售员
	--a.salary_profit*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_profit_fp_rate,0) salary_profit_salesperson,--提成_定价毛利额_销售员
	a.salary_profit*(1-coalesce(if(a.salesperson_over_rate<=0.5,a.salesperson_over_rate,1),0))*coalesce(a.salesperson_profit_fp_rate,0) salary_profit_salesperson,--提成_定价毛利额_销售员
	--提成_定价毛利额_服务管家
	a.salary_profit*(1-coalesce(if(a.service_user_over_rate<=0.5,a.service_user_over_rate,1),0))*coalesce(a.service_user_profit_fp_rate,0) salary_profit_service, --提成_定价毛利额_服务管家
	a.salesperson_sales_value_ytd,-- 销售员年度累计销售额
	a.salesperson_ripei_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	a.salesperson_fuli_sales_value_ytd -- 销售员年度累计福利销售额
from
	csx_tmp.tc_customer_salary_00 a 
;


--客户本月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_kehu' row format delimited fields terminated by '\t'
select 
	smonth,sales_province_name,customer_no,customer_name,work_no,sales_name,is_part_time_service_manager,service_user_work_no,service_user_name,
	customer_sales_value,customer_ripei_sales_value,customer_bbc_sales_value,customer_fuli_sales_value,customer_ripei_bbc_sales_value,
	customer_profit,customer_ripei_profit,customer_bbc_profit,customer_fuli_profit,customer_ripei_bbc_profit,
	customer_prorate,customer_ripei_prorate,customer_bbc_prorate,customer_fuli_prorate,customer_ripei_bbc_prorate,
	salesperson_ripei_bbc_prorate,salesperson_fuli_prorate,
	salary_fuli_sales_value,salary_fuli_profit,
	salary_ripei_sales_value,salary_ripei_profit,
	salary_bbc_sales_value,salary_bbc_profit, 
	salary_sales_value,salary_profit,
	receivable_amount,overdue_amount,assigned_type,salesperson_sales_value_fp_rate,salesperson_profit_fp_rate,service_user_sales_value_fp_rate,
	service_user_profit_fp_rate,salesperson_over_rate,service_user_over_rate,salary_sales_value_salesperson,salary_sales_value_service,salary_profit_salesperson,salary_profit_service,
	coalesce(salary_sales_value_salesperson,0)+coalesce(salary_profit_salesperson,0)+
	coalesce(salary_fuli_sales_value_salesperson,0)+coalesce(salary_fuli_profit_salesperson,0) as total_salesperson,
	coalesce(salary_sales_value_service,0)+coalesce(salary_profit_service,0) as total_service
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
	sum(customer_ripei_sales_value) as customer_ripei_sales_value,
	sum(customer_bbc_sales_value) as customer_bbc_sales_value,
	sum(customer_fuli_sales_value) as customer_fuli_sales_value,	
	sum(customer_ripei_bbc_sales_value) as customer_ripei_bbc_sales_value,
	sum(customer_profit) as customer_profit,
	sum(customer_ripei_profit) as customer_ripei_profit,
	sum(customer_bbc_profit) as customer_bbc_profit,
	sum(customer_fuli_profit) as customer_fuli_profit,	
	sum(customer_ripei_bbc_profit) as customer_ripei_bbc_profit,
	coalesce(sum(customer_profit)/abs(sum(customer_sales_value)),0) as customer_prorate,
	coalesce(sum(customer_ripei_profit)/abs(sum(customer_ripei_sales_value)),0) as customer_ripei_prorate,
	coalesce(sum(customer_bbc_profit)/abs(sum(customer_bbc_sales_value)),0) as customer_bbc_prorate,
	coalesce(sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)),0) as customer_fuli_prorate,
	coalesce(sum(customer_ripei_bbc_profit)/abs(sum(customer_ripei_bbc_sales_value)),0) as customer_ripei_bbc_prorate,
	sum(salary_fuli_sales_value) as salary_fuli_sales_value,
	sum(salary_fuli_profit) as salary_fuli_profit,
	sum(salary_ripei_sales_value) as salary_ripei_sales_value,
	sum(salary_ripei_profit) as salary_ripei_profit,
	sum(salary_bbc_sales_value) as salary_bbc_sales_value,
	sum(salary_bbc_profit) as salary_bbc_profit, 
	sum(salary_sales_value) as salary_sales_value,
	sum(salary_profit) as salary_profit,
	sum(receivable_amount) as receivable_amount,
	sum(overdue_amount) as overdue_amount,
	salesperson_over_rate,
	sum(salary_sales_value_salesperson) as salary_sales_value_salesperson,
	sum(salary_profit_salesperson) as salary_profit_salesperson,
	coalesce(sum(salary_sales_value_salesperson),0)+coalesce(sum(salary_profit_salesperson),0)+
	coalesce(sum(salary_fuli_sales_value_salesperson),0)+coalesce(sum(salary_fuli_profit_salesperson),0) salary_sale
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
	sum(customer_ripei_sales_value) as customer_ripei_sales_value,
	sum(customer_bbc_sales_value) as customer_bbc_sales_value,
	sum(customer_fuli_sales_value) as customer_fuli_sales_value,	
	sum(customer_ripei_bbc_sales_value) as customer_ripei_bbc_sales_value,
	sum(customer_profit) as customer_profit,
	sum(customer_ripei_profit) as customer_ripei_profit,
	sum(customer_bbc_profit) as customer_bbc_profit,
	sum(customer_fuli_profit) as customer_fuli_profit,	
	sum(customer_ripei_bbc_profit) as customer_ripei_bbc_profit,
	coalesce(sum(customer_profit)/abs(sum(customer_sales_value)),0) as customer_prorate,
	coalesce(sum(customer_ripei_profit)/abs(sum(customer_ripei_sales_value)),0) as customer_ripei_prorate,
	coalesce(sum(customer_bbc_profit)/abs(sum(customer_bbc_sales_value)),0) as customer_bbc_prorate,
	coalesce(sum(customer_fuli_profit)/abs(sum(customer_fuli_sales_value)),0) as customer_fuli_prorate,
	coalesce(sum(customer_ripei_bbc_profit)/abs(sum(customer_ripei_bbc_sales_value)),0) as customer_ripei_bbc_prorate,
	sum(salary_sales_value) as salary_sales_value,
	sum(salary_profit) as salary_profit,
	sum(receivable_amount) as receivable_amount,
	sum(overdue_amount) as overdue_amount,
	service_user_over_rate,
	sum(salary_sales_value_service) as salary_sales_value_service,
	sum(salary_profit_service) as salary_profit_service,
	coalesce(sum(salary_sales_value_service),0)+coalesce(sum(salary_profit_service),0) salary_service
from 
	csx_tmp.tc_new_cust_salary
where
	service_user_work_no !=''
group by 
	smonth,sales_province_name,service_user_work_no,service_user_name,service_user_over_rate
;


--===============================================================================================================================================================================

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
			customer_no,min(first_order_date) first_order_date
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
	sdt= '20220228'
	and channel_code in('1','7')
	and (is_sale='是' or is_overdue='是')
	and sales_province_name in ('福建省')
;

--1月客户销售员对照表
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	customer_no,customer_name,sales_province_name,work_no,sales_name,service_user_work_no,service_user_name,
	is_part_time_service_manager,sales_sale_rate,sales_profit_rate,service_user_sale_rate,service_user_profit_rate
from  
	csx_dw.report_crm_w_a_customer_service_manager_info_new
where  
	sdt= '20220228'
	and channel_code in('1','7')
	and (is_sale='是' or is_overdue='是')
*/
