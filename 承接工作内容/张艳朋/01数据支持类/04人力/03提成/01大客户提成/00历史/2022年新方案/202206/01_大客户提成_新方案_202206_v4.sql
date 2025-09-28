-- 确认需对哪些销售员补充收入组
--set month_start_day ='20220601';	
--set month_end_day ='20220630';
--set last_month_end_day='20220531';
--
--select 
--	b.work_no,b.sales_name,c.income_type,sum(sales_value) sales_value,sum(profit) profit
--from
--	(
--	select 
--		province_code,province_name,customer_no,substr(sdt,1,6) smonth,sum(sales_value) sales_value,sum(profit) profit
--	from 
--		csx_dw.dws_sale_r_d_detail
--	where 
--		sdt>=${hiveconf:month_start_day}
--		and sdt<=${hiveconf:month_end_day}
--		and channel_code in('1','7')
--	group by 
--		province_code,province_name,customer_no,substr(sdt,1,6)
--	)a	
--	left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:month_end_day}) b on b.customer_no=a.customer_no
--	left join (select distinct work_no,income_type from csx_tmp.sales_income_info_new where sdt=${hiveconf:last_month_end_day}) c on c.work_no=b.work_no
--where 
--	c.income_type is null and b.sales_name not rlike 'B|C' 
--group by 
--	b.work_no,b.sales_name,c.income_type;
--
----=============================================================================================================================================================================
---- 补充收入组并校验
--load data inpath '/tmp/zhangyanpeng/sales_income_info_new_202206.csv' overwrite into table csx_tmp.sales_income_info_new partition (sdt=${hiveconf:month_end_day});
--select * from csx_tmp.sales_income_info_new where sdt=${hiveconf:month_end_day};

--=============================================================================================================================================================================
-- 设置日期
set month_start_day ='20220601';	
set month_end_day ='20220630';
set last_month_end_day='20220531';
set year_start_day ='20220101';	
		
-- 销售员年度累计销额提成比例
drop table csx_tmp.tc_sales_rate_ytd; --5
create table csx_tmp.tc_sales_rate_ytd
as
select 
	sales_id,work_no,sales_name,income_type,sales_sales_value_ytd,sales_rp_bbc_sales_value_ytd,sales_fl_sales_value_ytd,
	case when 
			((sales_rp_bbc_sales_value_ytd<=10000000 and income_type in('Q1','Q2','Q3')) 
			or (sales_rp_bbc_sales_value_ytd>10000000 and sales_rp_bbc_sales_value_ytd<=20000000 and income_type in('Q2','Q3'))
			or (sales_rp_bbc_sales_value_ytd>20000000 and sales_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q3'))) then 0.002
		when ((sales_rp_bbc_sales_value_ytd>10000000 and sales_rp_bbc_sales_value_ytd<=20000000 and income_type in('Q1'))
			or (sales_rp_bbc_sales_value_ytd>20000000 and sales_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q2'))
			or (sales_rp_bbc_sales_value_ytd>30000000 and sales_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q3'))) then 0.0025
		when ((sales_rp_bbc_sales_value_ytd>20000000 and sales_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q1'))
			or (sales_rp_bbc_sales_value_ytd>30000000 and sales_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q2'))
			or (sales_rp_bbc_sales_value_ytd>40000000 and income_type in('Q3'))) then 0.003
		when ((sales_rp_bbc_sales_value_ytd>30000000 and sales_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q1'))
			or (sales_rp_bbc_sales_value_ytd>40000000 and income_type in('Q2'))) then 0.0035
		when (sales_rp_bbc_sales_value_ytd>40000000 and income_type in('Q1')) then 0.004			
		else 0.002 end sales_rp_bbc_sales_value_tc_rate,
	0.002 as sales_fl_sales_value_tc_rate
from 
	(
	select 
		b.sales_id,b.work_no,b.sales_name,coalesce(c.income_type,'Q1') as income_type,
		sum(a.sales_value) as sales_sales_value_ytd,
		sum(a.rp_bbc_sales_value) as sales_rp_bbc_sales_value_ytd,
		sum(a.fl_sales_value) as sales_fl_sales_value_ytd
	from 
		(
		select 
			customer_no,regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') as sdt_last,
			sum(sales_value) as sales_value,
			sum(case when business_type_code in('1','4','5','6') then sales_value else 0 end) as rp_bbc_sales_value,
			sum(case when business_type_code in('2') then sales_value else 0 end) as fl_sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:year_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and province_name = '平台-B') --平台酒水
				--安徽省城市服务商2.0，按大客户提成方案计算，当月 
				--福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				--福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				or (business_type_code in ('4') and customer_no in
				('123244','127923','126690','115941','120768','114853','124351','121020','121384','121337','124558','124193','120872','113829','111881','115904','114859',
				'112410','125719','127954','120879','125722','112948','121287'
				)))
			--and province_name in ('福建省')
			and (province_name !='福建省' or (province_name='福建省' and dc_name not like '%V2DC%')) --2.0 按仓库名称判断
			--202202月签呈，该客户已转代理人，不算提成，每月
			and customer_no not in ('122221','123086')
			--202202月签呈，公司BBC客户，不算提成，每月
			and customer_no not in ('123623')
			--202202月签呈，客户地采产品较多，不算提成，当月
			--and customer_no not in ('102866')
			--202203月签呈 不算提成和逾期 每月 
			and customer_no not in ('104192','123395','117927','126154')
			--202203月签呈 不算提成和逾期 当月 
			--and customer_no not in ('123859')
			--202204月签呈 不算提成和逾期 每月 
			and customer_no not in ('120459','121206')	
			--202205月签呈 不算提成 每月 
			and customer_no not in ('109435','111255')				
		group by 
			customer_no,regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
		)a 
		left join   --CRM客户信息取每月最后一天
			(
			select 
				sdt,customer_id,customer_no,customer_name,sales_id,work_no,sales_name,
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
		b.sales_id,b.work_no,b.sales_name,coalesce(c.income_type,'Q1')
	)a
where
	work_no !=''
;--6

-- 客户本月销售额、定价毛利额统计
drop table csx_tmp.tc_sales_value_profit_00;--7
create table csx_tmp.tc_sales_value_profit_00
as
select 
	b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,a.customer_no,b.customer_name,a.smonth,
	-- 销售额
	sum(sales_value) as sales_value, -- 客户总销售额
	sum(rp_sales_value) as rp_sales_value, -- 客户日配销售额
	sum(bbc_sales_value) as bbc_sales_value, -- 客户bbc销售额
	sum(fl_sales_value) as fl_sales_value, -- 客户福利销售额
	sum(rp_sales_value)+sum(bbc_sales_value) as rp_bbc_sales_value,
	-- 定价毛利额
	sum(profit) as profit,-- 客户总定价毛利额
	sum(rp_profit) as rp_profit,-- 客户日配定价毛利额
	sum(bbc_profit) as bbc_profit,-- 客户bbc定价毛利额
	sum(fl_profit) as fl_profit,-- 客户福利定价毛利额
	sum(rp_profit)+sum(bbc_profit) as rp_bbc_profit
from 
	(
	select 
		customer_no,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sales_value) as sales_value,
		sum(case when business_type_code in ('1','4','5') then sales_value else 0 end) as rp_sales_value,
		sum(case when business_type_code in('6') then sales_value else 0 end) as bbc_sales_value,
		sum(case when business_type_code in('2') then sales_value else 0 end) as fl_sales_value,
		-- 各类型定价毛利额
		sum(case when dc_code <>'W0K4' then profit else 0 end) as profit, --W0K4只计算销售额 不计算定价毛利额 每月
		sum(case when business_type_code in ('1','4','5') and dc_code <>'W0K4' then profit else 0 end) as rp_profit,
		sum(case when business_type_code in('6') and dc_code <>'W0K4' then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in('2') and dc_code <>'W0K4' then profit else 0 end) as fl_profit	
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:month_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and province_name = '平台-B') --平台酒水
				--安徽省城市服务商2.0，按大客户提成方案计算，当月 
				--福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				--福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				or (business_type_code in ('4') and customer_no in
				('123244','127923','126690','115941','120768','114853','124351','121020','121384','121337','124558','124193','120872','113829','111881','115904','114859',
				'112410','125719','127954','120879','125722','112948','121287'
				)))
			--and province_name in ('福建省')
			and (province_name !='福建省' or (province_name='福建省' and dc_name not like '%V2DC%')) --2.0 按仓库名称判断
			--202202月签呈，该客户已转代理人，不算提成，每月
			and customer_no not in ('122221','123086')
			--202202月签呈，不算提成，当月
			--and customer_no not in ('125613','124379','125247','125256','124025','124667','124621','124370','125469','123599','124782')
			--202202月签呈，公司BBC客户，不算提成，每月
			and customer_no not in ('123623')
			--202202月签呈，客户地采产品较多，不算提成，当月
			--and customer_no not in ('102866')
			--202202月签呈，剔除直送客户，当月
			--and customer_no not in ('114834','111832','123685','120723','124367','124387','124416','119760','121425','121841','123553')	
			--202203月签呈 不算提成和逾期 每月 
			and customer_no not in ('104192','123395','117927','126154')
			--202203月签呈 不算提成和逾期 当月 
			--and customer_no not in ('123859')
			--202204月签呈 不算提成和逾期 每月 
			and customer_no not in ('120459','121206')	
			--202205月签呈 不算提成 每月 
			and customer_no not in ('109435','111255')
			--202206月签呈 剔除销售额及毛利额 当月
			and customer_no not in ('125111','127156')
			
	group by 
		customer_no,substr(sdt,1,6)
		
	--扣减定价毛利额
	--重庆市 每月
	union all select '105569' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-10000.00 as profit,-10000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	--四川省 每月
	union all select '124403' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-6300.00 as profit,-6300.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '108835' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3500.00 as profit,-3500.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit

	-- 重庆市 当月
	union all select '121061' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-31227.60 as profit,-31227.60 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '122129' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-14977.80 as profit,-14977.80 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '121061' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-8971.99 as profit,-8971.99 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125412' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-4305.77 as profit,-4305.77 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '122606' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2633.03 as profit,-2633.03 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125089' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1619.94 as profit,-1619.94 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '106898' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1154.40 as profit,-1154.40 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '127515' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-907.84 as profit,-907.84 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '113366' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-841.33 as profit,-841.33 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '121397' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-773.86 as profit,-773.86 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '124039' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-634.59 as profit,-634.59 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '121318' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-587.87 as profit,-587.87 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '105085' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-576.13 as profit,-576.13 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '127572' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-503.59 as profit,-503.59 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '105186' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-6052.76 as profit,-6052.76 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '118206' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2847.27 as profit,-2847.27 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '104965' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2276.98 as profit,-2276.98 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '112813' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2220.19 as profit,-2220.19 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117753' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1141.95 as profit,-1141.95 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117727' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1869.03 as profit,-1869.03 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117728' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3250.02 as profit,-3250.02 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117729' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2855.17 as profit,-2855.17 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117748' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3135.61 as profit,-3135.61 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117761' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-43.22 as profit,-43.22 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117773' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1178.72 as profit,-1178.72 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117776' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2727.74 as profit,-2727.74 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117782' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2482.70 as profit,-2482.70 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117790' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1684.71 as profit,-1684.71 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117791' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1295.17 as profit,-1295.17 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117795' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1342.90 as profit,-1342.90 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117800' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1903.47 as profit,-1903.47 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117805' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-9664.44 as profit,-9664.44 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117918' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-388.30 as profit,-388.30 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '124044' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2080.73 as profit,-2080.73 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '124004' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1571.67 as profit,-1571.67 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '124899' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-745.32 as profit,-745.32 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '124467' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-18699.79 as profit,-18699.79 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125718' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-21045.53 as profit,-21045.53 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117920' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1908.89 as profit,-1908.89 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117920' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-14.79 as profit,0.00 as rp_profit, -14.79 as bbc_profit, 0.00 as fl_profit
	union all select '121113' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-8181.87 as profit,-8181.87 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125572' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-9546.47 as profit,-9546.47 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125429' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-119.15 as profit,-119.15 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125434' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1958.19 as profit,-1958.19 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125428' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-74.37 as profit,-74.37 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125412' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-22376.40 as profit,-22376.40 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '124505' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-4695.37 as profit,-4695.37 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '124353' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-4969.26 as profit,-4969.26 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '124474' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3125.25 as profit,-3125.25 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '107099' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3745.49 as profit,-3745.49 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '122606' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-5949.63 as profit,-5949.63 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '127165' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-797.92 as profit,-797.92 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '127333' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-7064.08 as profit,-7064.08 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125311' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2334.47 as profit,-2334.47 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '112160' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-11038.87 as profit,-11038.87 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '112160' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-4525.13 as profit,0.00 as rp_profit, -4525.13 as bbc_profit, 0.00 as fl_profit
	union all select '127449' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-458.57 as profit,0.00 as rp_profit, -458.57 as bbc_profit, 0.00 as fl_profit
	union all select '127613' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1090.00 as profit,0.00 as rp_profit, -1090.00 as bbc_profit, 0.00 as fl_profit
	union all select '127569' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-517.38 as profit,0.00 as rp_profit, -517.38 as bbc_profit, 0.00 as fl_profit
	union all select '127655' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2042.99 as profit,0.00 as rp_profit, -2042.99 as bbc_profit, 0.00 as fl_profit
	union all select '127668' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-4425.34 as profit,0.00 as rp_profit, -4425.34 as bbc_profit, 0.00 as fl_profit
	union all select '119659' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1776.78 as profit,-1776.78 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '120105' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1113.26 as profit,-1113.26 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '120623' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-10720.99 as profit,-10720.99 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '110866' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1256.63 as profit,-1256.63 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '113643' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-5125.68 as profit,-5125.68 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '113643' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-896.61 as profit,0.00 as rp_profit, -896.61 as bbc_profit, 0.00 as fl_profit
	union all select '120924' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-15243.16 as profit,-15243.16 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '123084' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3613.45 as profit,0.00 as rp_profit, -3613.45 as bbc_profit, 0.00 as fl_profit
	union all select '122103' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-6874.23 as profit,-6874.23 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '113423' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2938.53 as profit,-2938.53 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '118212' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1241.43 as profit,0.00 as rp_profit, -1241.43 as bbc_profit, 0.00 as fl_profit
	union all select '112177' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-8540.95 as profit,-8540.95 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '115554' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-6534.58 as profit,-6534.58 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '114287' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2150.45 as profit,-2150.45 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '121061' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-49410.34 as profit,-49410.34 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '122247' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2549.85 as profit,-2549.85 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '120976' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-522.31 as profit,-522.31 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125679' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-495.33 as profit,-495.33 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '118738' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3430.27 as profit,-3430.27 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125621' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-4622.15 as profit,-4622.15 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '115253' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-807.20 as profit,-807.20 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '112803' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1537.89 as profit,-1537.89 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '119042' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-554.11 as profit,-554.11 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit

	--上海松江 当月
	union all select '104901' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-9563.00 as profit,-9563.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '105381' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-7631.71 as profit,-7631.71 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '107059' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-8102.08 as profit,-8102.08 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '117755' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-5714.46 as profit,-5714.46 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '118288' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-5262.42 as profit,0.00 as rp_profit, -5262.42 as bbc_profit, 0.00 as fl_profit
	

	--202202月签呈，河北省，1-4月
	--union all select '124519' as customer_no,'202204' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	---5000.00 as profit,-5000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--河北省 当月
	--union all select '125105' as customer_no,'202202' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	---10000.00 as profit,-10000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--安徽省 当月
	union all select '113550' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-7743.67 as profit,-7743.67 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '107411' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-6706.19 as profit,-6706.19 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '114777' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2072.52 as profit,-2072.52 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '121233' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-7467.80 as profit,-7467.80 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '115883' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-6249.71 as profit,-6249.71 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '120428' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3707.47 as profit,-3707.47 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '119539' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-9000.00 as profit,-9000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '120430' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1048.66 as profit,-1048.66 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '112207' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-280.18 as profit,-280.18 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '109460' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2397.72 as profit,-2397.72 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '125686' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2023.12 as profit,-2023.12 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '121967' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-2176.90 as profit,-2176.90 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '105870' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1122.00 as profit,-1122.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '114853' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-5600.00 as profit,0.00 as rp_profit, -5600.00 as bbc_profit, 0.00 as fl_profit
	union all select '124046' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-951.73 as profit,0.00 as rp_profit, -951.73 as bbc_profit, 0.00 as fl_profit	
	--福建省 当月
	union all select '123540' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-1869.40 as profit,-1869.40 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '108097' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-231.60 as profit,-231.60 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '120549' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-94.00 as profit,-94.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '106000' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-313.52 as profit,-313.52 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit	
	--江苏苏州 当月
	union all select '127866' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-48282.30 as profit,0.00 as rp_profit, -48282.30 as bbc_profit, 0.00 as fl_profit
	--河南省 当月
	union all select '127312' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-314.99 as profit,-314.99 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '126978' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-552.00 as profit,0.00 as rp_profit, -552.00 as bbc_profit, 0.00 as fl_profit
	--陕西省 当月
	union all select '124085' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	7144.18 as profit,7144.18 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	)a
	left join 
		(
		select 
			distinct customer_id,customer_no,customer_name,work_no,sales_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day}
			and customer_no !=''
		)b on b.customer_no=a.customer_no
where 
	b.ywdl_cust is null
	and b.ng_cust is null
group by 
	b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,a.customer_no,b.customer_name,a.smonth
;--8

-- 客户定价毛利额扣点、退货金额统计
drop table csx_tmp.tc_sales_value_profit_01;--9
create table csx_tmp.tc_sales_value_profit_01
as
select 
	a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_no,a.customer_name,a.smonth,
	c.customer_id,c.sales_id,c.work_no,c.sales_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	c.rp_sales_sale_fp_rate,
	c.rp_sales_profit_fp_rate,
	c.fl_sales_sale_fp_rate,
	c.fl_sales_profit_fp_rate,
	c.bbc_sales_sale_fp_rate,
	c.bbc_sales_profit_fp_rate,
	c.rp_service_user_sale_fp_rate,
	c.rp_service_user_profit_fp_rate,
	c.fl_service_user_sale_fp_rate,
	c.fl_service_user_profit_fp_rate,
	c.bbc_service_user_sale_fp_rate,
	c.bbc_service_user_profit_fp_rate,
	-- 销售额
	sales_value, -- 客户总销售额
	rp_sales_value, -- 客户日配销售额
	bbc_sales_value, -- 客户bbc销售额
	fl_sales_value, -- 客户福利销售额
	rp_bbc_sales_value, -- 客户日配&bbc销售额
	-- 定价毛利额
	(rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))+
	(bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0))+
	(fl_profit-coalesce(fl_sales_value*fl_kd_rate,0)) as profit,-- 客户总定价毛利额
	
	rp_profit-coalesce(rp_sales_value*rp_kd_rate,0) as rp_profit,-- 客户日配定价毛利额
	bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0) as bbc_profit,-- 客户bbc定价毛利额
	fl_profit-coalesce(fl_sales_value*fl_kd_rate,0) as fl_profit,-- 客户福利定价毛利额
	
	(rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))+
	(bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0)) as rp_bbc_profit,
	--定价毛利率
	coalesce(((rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))+
	(bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0))+
	(fl_profit-coalesce(fl_sales_value*fl_kd_rate,0)))/abs(sales_value),0) as prorate, -- 客户总定价毛利率
	
	coalesce((rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))/abs(rp_sales_value),0) as rp_prorate, -- 客户日配定价毛利率
	coalesce((bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0))/abs(bbc_sales_value),0) as bbc_prorate, -- 客户bbc定价毛利率
	coalesce((fl_profit-coalesce(fl_sales_value*fl_kd_rate,0))/abs(fl_sales_value),0) as fl_prorate,-- 客户福利定价毛利率
	coalesce(((rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))+
	(bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0)))/abs(rp_bbc_sales_value),0) as rp_bbc_prorate,
	coalesce(d.refund_sales_value,0) as refund_sales_value,
	coalesce(d.refund_rp_sales_value,0) as refund_rp_sales_value,
	coalesce(d.refund_bbc_sales_value,0) as refund_bbc_sales_value,
	coalesce(d.refund_rp_bbc_sales_value,0) as refund_rp_bbc_sales_value,
	coalesce(d.refund_fl_sales_value,0) as refund_fl_sales_value,
	coalesce(d.w0k4_sales_value,0) as w0k4_sales_value,
	coalesce(d.w0k4_rp_sales_value,0) as w0k4_rp_sales_value,
	coalesce(d.w0k4_bbc_sales_value,0) as w0k4_bbc_sales_value,
	coalesce(d.w0k4_rp_bbc_sales_value,0) as w0k4_rp_bbc_sales_value,
	coalesce(d.w0k4_fl_sales_value,0) as w0k4_fl_sales_value	
from
	csx_tmp.tc_sales_value_profit_00 a 
	left join
		(
		--202201月签呈，重庆市 每月
		select 'X000000' as customer_no, 0.00 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--union all select '105186' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--union all select '118206' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		
		--202201月签呈，福建省 每月
		union all select '102734' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '102784' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '102901' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113263' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '115366' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select 'PF0649' as customer_no, 0.09 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '114038' as customer_no, 0.00 as rp_kd_rate, 0.048 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113088' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '102924' as customer_no, 0.00 as rp_kd_rate, 0.03 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '102524' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105703' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105750' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '106698' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '112553' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113678' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113679' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113746' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113760' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113805' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '115602' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '117244' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '104281' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '107398' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '108589' as customer_no, 0.00 as rp_kd_rate, 0.028 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '120459' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118653' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118654' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118682' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118705' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118730' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118934' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118961' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119185' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119172' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '120666' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '115961' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '115985' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118689' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '116355' as customer_no, 0.028 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105150' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105156' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105177' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105181' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105182' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '106423' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '106721' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '107404' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105164' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '105165' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119990' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '106805' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '117676' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '123252' as customer_no, 0.00 as rp_kd_rate, 0.08 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '108557' as customer_no, 0.00 as rp_kd_rate, 0.02 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '116707' as customer_no, 0.00 as rp_kd_rate, 0.03 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '116561' as customer_no, 0.00 as rp_kd_rate, 0.05 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '120250' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119213' as customer_no, 0.00 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fl_kd_rate
		union all select '113515' as customer_no, 0.00 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fl_kd_rate
		union all select '106587' as customer_no, 0.00 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.05 as fl_kd_rate
		union all select '116131' as customer_no, 0.00 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.06 as fl_kd_rate
		union all select '103782' as customer_no, 0.00 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.06 as fl_kd_rate
		union all select '115656' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fl_kd_rate
		union all select '115826' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.01 as fl_kd_rate
		union all select '121054' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.02 as fl_kd_rate
		union all select '116947' as customer_no, 0.01 as rp_kd_rate, 0.01 as bbc_kd_rate, 0.01 as fl_kd_rate
		union all select '111207' as customer_no, 0.01 as rp_kd_rate, 0.01 as bbc_kd_rate, 0.01 as fl_kd_rate
		union all select '125534' as customer_no, 0.01 as rp_kd_rate, 0.01 as bbc_kd_rate, 0.01 as fl_kd_rate
		union all select '115537' as customer_no, 0.01 as rp_kd_rate, 0.01 as bbc_kd_rate, 0.01 as fl_kd_rate
		union all select '106921' as customer_no, 0.08 as rp_kd_rate, 0.08 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126331' as customer_no, 0.00 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.053 as fl_kd_rate
		union all select '125840' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select 'PF0500' as customer_no, 0.063 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '125179' as customer_no, 0.0053 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '124830' as customer_no, 0.0053 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '124278' as customer_no, 0.0053 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117671' as customer_no, 0.02 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '119235' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '127442' as customer_no, 0.000 as rp_kd_rate, 0.050 as bbc_kd_rate, 0.000 as fl_kd_rate
		
		--202201月签呈，河北省，每月
		union all select '112285' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '112024' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '123035' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--202201月签呈，河北省，当月
		--union all select '122551' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--202202月签呈，四川省，每月
		union all select '116401' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '104493' as customer_no, 0.06 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118041' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118208' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '117217' as customer_no, 0.11 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119354' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '122347' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '122860' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '123706' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '124061' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '123755' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '115191' as customer_no, 0.12 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118564' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '122628' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118770' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '120567' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118299' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113974' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '125201' as customer_no, 0.08 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '125191' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '125344' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126137' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126981' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126660' as customer_no, 0.028 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119865' as customer_no, 0.028 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126691' as customer_no, 0.028 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate

		--贵州省，每月
		union all select '125298' as customer_no, 0.08 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '125678' as customer_no, 0.08 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119663' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--浙江省 每月
		union all select '108905' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '111834' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		--贵州省 每月 
		union all select '113873' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113918' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113935' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113940' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117137' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117142' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117143' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '119589' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '115479' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '123999' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate


		) b on b.customer_no=a.customer_no
	--关联销售员、服务管家
	left join		
		(  
		select 
			*
		from 
			csx_tmp.tc_customer_service_manager_info_new
		)c on c.customer_no=a.customer_no
	--退货金额统计、W0K4仓销售金额统计
	left join
		(	
		select 
			customer_no,substr(sdt,1,6) as smonth,
			-- 各类型退货金额
			sum(case when return_flag='X' then sales_value else 0 end) as refund_sales_value, 
			sum(case when return_flag='X' and business_type_code in('1','4') then sales_value else 0 end) as refund_rp_sales_value,
			sum(case when return_flag='X' and business_type_code in('6') then sales_value else 0 end) as refund_bbc_sales_value,
			sum(case when return_flag='X' and business_type_code in('1','6','4') then sales_value else 0 end) as refund_rp_bbc_sales_value,
			sum(case when return_flag='X' and business_type_code in('2') then sales_value else 0 end) as refund_fl_sales_value,
			-- W0K4仓销售金额
			sum(case when dc_code='W0K4' then sales_value else 0 end) as w0k4_sales_value, 
			sum(case when dc_code='W0K4' and business_type_code in('1','4') then sales_value else 0 end) as w0k4_rp_sales_value,
			sum(case when dc_code='W0K4' and business_type_code in('6') then sales_value else 0 end) as w0k4_bbc_sales_value,
			sum(case when dc_code='W0K4' and business_type_code in('1','6','4') then sales_value else 0 end) as w0k4_rp_bbc_sales_value,
			sum(case when dc_code='W0K4' and business_type_code in('2') then sales_value else 0 end) as w0k4_fl_sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:month_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and (return_flag='X' or dc_code='W0K4')
		group by 
			customer_no,substr(sdt,1,6)
		)d on d.customer_no=a.customer_no and d.smonth=a.smonth
;--10

-- 销售员本月定价毛利率，计算销售员定价毛利额提成比例
drop table csx_tmp.tc_sales_profit_tc_rate;--11
create table csx_tmp.tc_sales_profit_tc_rate
as
select
	work_no,sales_name,sales_sales_value,sales_rp_bbc_sales_value,sales_fl_sales_value,sales_profit,sales_rp_bbc_profit,
	sales_fl_profit,sales_prorate,sales_rp_bbc_prorate,sales_fl_prorate,
	sales_rp_prorate,sales_bbc_prorate,
	-- 日配&bbc定价毛利额提成比例
	case when sales_rp_bbc_prorate<0.08 then 0
		when sales_rp_bbc_prorate>=0.08 and sales_rp_bbc_prorate<0.12 then 0.03
		when sales_rp_bbc_prorate>=0.12 and sales_rp_bbc_prorate<0.15 then 0.033
		when sales_rp_bbc_prorate>=0.15 and sales_rp_bbc_prorate<0.18 then 0.035
		when sales_rp_bbc_prorate>=0.18 and sales_rp_bbc_prorate<0.2 then 0.04
		when sales_rp_bbc_prorate>=0.2 then 0.05
		else 0 
	end as sales_rp_bbc_profit_tc_rate,
	-- 福利定价毛利额提成比例
	case when sales_fl_prorate<0.03 then 0
		when sales_fl_prorate>=0.03 and sales_fl_prorate<0.05 then 0.02
		when sales_fl_prorate>=0.05 and sales_fl_prorate<0.08 then 0.025
		when sales_fl_prorate>=0.08 and sales_fl_prorate<0.1 then 0.03
		when sales_fl_prorate>=0.1 and sales_fl_prorate<0.15 then 0.04
		when sales_fl_prorate>=0.15 then 0.05
		else 0 
	end as sales_fl_profit_tc_rate
from
	(
	select 	
		work_no,sales_name,
		-- 销售额
		sum(sales_value-w0k4_sales_value) as sales_sales_value, -- 总销售额
		sum(rp_sales_value-w0k4_rp_sales_value) as sales_rp_sales_value, -- 日配销售额
		sum(bbc_sales_value-w0k4_bbc_sales_value) as sales_bbc_sales_value, -- bbc销售额
		sum(rp_bbc_sales_value-w0k4_rp_bbc_sales_value) as sales_rp_bbc_sales_value, -- 日配&bbc销售额
		sum(fl_sales_value-w0k4_fl_sales_value) as sales_fl_sales_value, -- 福利销售额
		-- 定价毛利额
		sum(profit) as sales_profit,-- 总定价毛利额
		sum(rp_profit) as sales_rp_profit,-- 日配定价毛利额
		sum(bbc_profit) as sales_bbc_profit,--bbc定价毛利额
		sum(rp_bbc_profit) as sales_rp_bbc_profit,-- 日配&bbc定价毛利额
		sum(fl_profit) as sales_fl_profit,-- 福利定价毛利额
		--定价毛利率
		sum(profit)/abs(sum(sales_value-w0k4_sales_value)) as sales_prorate, -- 总定价毛利率
		sum(rp_profit)/abs(sum(rp_sales_value-w0k4_rp_sales_value)) as sales_rp_prorate,
		sum(bbc_profit)/abs(sum(bbc_sales_value-w0k4_bbc_sales_value)) as sales_bbc_prorate,
		sum(rp_bbc_profit)/abs(sum(rp_bbc_sales_value-w0k4_rp_bbc_sales_value)) as sales_rp_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
		sum(fl_profit)/abs(sum(fl_sales_value-w0k4_fl_sales_value)) as sales_fl_prorate -- 销售员本月福利定价毛利率
	from
		csx_tmp.tc_sales_value_profit_01
	where
		--202202月签呈，安徽省剔除以下客户后再算销售员的定价毛利率，服务费，当月
		--customer_no not in ('104402','104885','121452','107415','113731','113857','122147','122159','122167','122185','122186','122188','125274')
		--202202月签呈，剔除直送客户后再算销售员定价毛利率，当月
		--and customer_no not in ('114834','111832','123685','120723','124367','124387','124416','119760','121425','121841','123553')
		--202202月签呈，剔除武警客户，当月
		--and customer_no not in ('120595','124584','124784','125029','125028','125017','116932','120830','122837')
		--202203月签呈 直送客户 不计算销售员整体毛利率 每月
		customer_no not in ('124652')
		--202203月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','121452','107415','113731','113857','122147','122159','122167','122185','122186','122188','126406','113439')
		--202203月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('123685','116566','125360','123122','111832','111691','120723','115221','112906','118509','117817','121472','121994','124356',
		--	'124367','124387','124401','124416','119760','125767')
		--202204月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','123287','107415','121452','113857','122147','122167','113439','125770','126503','126571','126644','126406')
		--202204月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('114834','116566','111832','120723','112906','118509','117817','121472','121994','124356','124367','124387','124401','124416','119760','121841','125767')
		--202205月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','127396','107415','121452','113857','122147','122167','122159','122185','122186','122188')	
		--202205月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('116566','120723','112906','118509','117817','121472','121994','124356','124367','124387','124401','124416','119760','121841','115221','126084')
		--202205月签呈 河南省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126069','126113')
		--202205月签呈 河南省 剔除直送客户后再算销售员定价毛利率 每月
		and customer_no not in ('126259','127312','127336')	
		--202206月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		and customer_no not in ('104885','115286','127396','121452','107415','113857','122147','122159','122167','122185','122186','122188','125770','126644','127747','127754',
		'127543','127592','127698','127728','127729','127739','127743','127745','127746','127750','127751','127753','127755','127760','127766','127767','127775','127553',
		'127737','127756')	
		--202206月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		and customer_no not in ('126935','126084','128005','116566','120333','120723','112906','118509','122264','117817','121994','124356','124367','124387','124401','124416',
		'119760','121841','125767','127262','114859','115681','115941','120872','120879','121287','125722','120768','121384','124351','127954')
		--202206月签呈 河南省 剔除直送客户后再算销售员定价毛利率 每月
		and customer_no not in ('127043','126069','126113')	
		
	group by 
		work_no,sales_name
	) a 
where
	work_no !=''
;--12

--客户本月提成，未乘分配比例
drop table csx_tmp.tc_salary_00;--13
create table csx_tmp.tc_salary_00
as
select 
	a.smonth,a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_id,a.customer_no,a.customer_name,
	a.sales_id,a.work_no,a.sales_name,
	a.rp_service_user_id,a.rp_service_user_work_no,a.rp_service_user_name,
	a.fl_service_user_id,a.fl_service_user_work_no,a.fl_service_user_name,
	a.bbc_service_user_id,a.bbc_service_user_work_no,a.bbc_service_user_name,
	a.sales_value,a.rp_sales_value,a.bbc_sales_value,a.fl_sales_value,a.rp_bbc_sales_value,
	a.profit,a.rp_profit,a.bbc_profit,a.fl_profit,a.rp_bbc_profit,
	a.prorate,a.rp_prorate,a.bbc_prorate,a.fl_prorate,a.rp_bbc_prorate,
	coalesce(e.sales_prorate,0) as sales_prorate,
	coalesce(e.sales_rp_prorate,0) as sales_rp_prorate,
	coalesce(e.sales_bbc_prorate,0) as sales_bbc_prorate,
	coalesce(e.sales_rp_bbc_prorate,0) as sales_rp_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
	coalesce(e.sales_fl_prorate,0) as sales_fl_prorate, -- 销售员本月福利定价毛利率
	--奖金包_日配业务 --安徽直送客户 默认毛利额提成比例为2% 当月
	--'126259' 毛利单独核算 每月 
	--'127923' 销售额*0.2% 不计算毛利提成 每月
	--'126690' 销售额*0.2% 不计算毛利提成 每月
	--'127043' 销售额*0.2% 不计算毛利提成 每月
	a.rp_sales_value*if(a.customer_no in ('127923','126690','127043'),0.002,coalesce(f.sales_rp_bbc_sales_value_tc_rate,0.002)) as salary_rp_sales_value, -- 奖金包_日配销售额
	a.rp_profit*(case when a.customer_no in ('126935','126084','128005','116566','120333','120723','112906','118509','122264','117817','121994','124356','124367','124387',
	'124401','124416','119760','121841','125767','127262','114859','115681','115941','120872','120879','121287','125722','120768','121384','124351','127954') then 0.02
		--'127923' 销售额*0.2% 不计算毛利提成 每月
		--'126690' 销售额*0.2% 不计算毛利提成 每月
		when a.customer_no in ('127923','126690','127043') then 0
		when a.customer_no in ('126259') then 0.03
		else coalesce(e.sales_rp_bbc_profit_tc_rate,0.03) end) as salary_rp_profit, --奖金包_日配定价毛利额
	--奖金包_福利业务
	a.fl_sales_value*coalesce(f.sales_fl_sales_value_tc_rate,0.002) as salary_fl_sales_value, --奖金包_福利销售额
	a.fl_profit*if(a.customer_no in ('126935','126084','128005','116566','120333','120723','112906','118509','122264','117817','121994','124356','124367','124387',
	'124401','124416','119760','121841','125767','127262','114859','115681','115941','120872','120879','121287','125722','120768','121384','124351','127954'),0.02,
	coalesce(e.sales_fl_profit_tc_rate,0.02)) as salary_fl_profit, --奖金包_福利定价毛利额
	--奖金包_BBC业务
	a.bbc_sales_value*coalesce(f.sales_rp_bbc_sales_value_tc_rate,0.002) as salary_bbc_sales_value, -- 奖金包_bbc销售额
	a.bbc_profit*if(a.customer_no in ('126935','126084','128005','116566','120333','120723','112906','118509','122264','117817','121994','124356','124367','124387',
	'124401','124416','119760','121841','125767','127262','114859','115681','115941','120872','120879','121287','125722','120768','121384','124351','127954'),0.02,
	coalesce(e.sales_rp_bbc_profit_tc_rate,0.03)) as salary_bbc_profit, --奖金包_bbc定价毛利额	
	b.receivable_amount,b.overdue_amount,
	a.rp_sales_sale_fp_rate,a.rp_sales_profit_fp_rate,a.fl_sales_sale_fp_rate,a.fl_sales_profit_fp_rate,a.bbc_sales_sale_fp_rate,a.bbc_sales_profit_fp_rate,
	a.rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
	a.fl_service_user_sale_fp_rate,a.fl_service_user_profit_fp_rate,
	a.bbc_service_user_sale_fp_rate,a.bbc_service_user_profit_fp_rate,
	coalesce(c.over_rate,0) as sales_over_rate,
	coalesce(d.over_rate,0) as rp_service_user_over_rate,
	coalesce(d2.over_rate,0) as fl_service_user_over_rate,
	coalesce(d3.over_rate,0) as bbc_service_user_over_rate,
	--服务管家应收 逾期
	coalesce(d.receivable_amount,0) as rp_service_receivable_amount,
	coalesce(d.overdue_amount,0) as rp_service_overdue_amount,	
	coalesce(d2.receivable_amount,0) as fl_service_receivable_amount,
	coalesce(d2.overdue_amount,0) as fl_service_overdue_amount,
	coalesce(d3.receivable_amount,0) as bbc_service_receivable_amount,
	coalesce(d3.overdue_amount,0) as bbc_service_overdue_amount,
	f.sales_sales_value_ytd,-- 销售员年度累计销售额
	f.sales_rp_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	f.sales_fl_sales_value_ytd, -- 销售员年度累计福利销售额
	refund_sales_value,refund_rp_sales_value,refund_bbc_sales_value,
	refund_rp_bbc_sales_value,refund_fl_sales_value
from  
	(
	select 
		sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,customer_no,customer_name,smonth,customer_id,
		sales_id,work_no,sales_name,rp_service_user_id,rp_service_user_work_no,rp_service_user_name,
		fl_service_user_id,fl_service_user_work_no,fl_service_user_name,bbc_service_user_id,bbc_service_user_work_no,bbc_service_user_name,rp_sales_sale_fp_rate,rp_sales_profit_fp_rate,
		fl_sales_sale_fp_rate,fl_sales_profit_fp_rate,bbc_sales_sale_fp_rate,bbc_sales_profit_fp_rate,
		rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
		fl_service_user_sale_fp_rate,fl_service_user_profit_fp_rate,
		bbc_service_user_sale_fp_rate,bbc_service_user_profit_fp_rate,
		sales_value,rp_sales_value,bbc_sales_value,fl_sales_value,
		rp_bbc_sales_value,profit,rp_profit,bbc_profit,fl_profit,rp_bbc_profit,prorate,rp_prorate,bbc_prorate,fl_prorate,rp_bbc_prorate,refund_sales_value,
		refund_rp_sales_value,refund_bbc_sales_value,refund_rp_bbc_sales_value,refund_fl_sales_value
	from 
		csx_tmp.tc_sales_value_profit_01
	)a
	left join csx_tmp.tc_cust_over_rate b on b.customer_no=a.customer_no
	left join csx_tmp.tc_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
	left join csx_tmp.tc_service_user_over_rate d on d.service_user_name=a.rp_service_user_name and coalesce(d.service_user_work_no,0)=coalesce(a.rp_service_user_work_no,0)
	left join csx_tmp.tc_service_user_over_rate d2 on d2.service_user_name=a.fl_service_user_name and coalesce(d2.service_user_work_no,0)=coalesce(a.fl_service_user_work_no,0)
	left join csx_tmp.tc_service_user_over_rate d3 on d3.service_user_name=a.bbc_service_user_name and coalesce(d3.service_user_work_no,0)=coalesce(a.bbc_service_user_work_no,0)
	left join csx_tmp.tc_sales_profit_tc_rate e on e.work_no=a.work_no and e.sales_name=a.sales_name
	left join csx_tmp.tc_sales_rate_ytd f on f.work_no=a.work_no and f.sales_name=a.sales_name
;--14

--客户本月提成，乘分配比例
drop table csx_tmp.tc_new_cust_salary_00; --15
create table csx_tmp.tc_new_cust_salary_00
as
select
	a.smonth,a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_id,a.customer_no,a.customer_name,
	a.sales_id,a.work_no,a.sales_name,a.rp_service_user_id,a.rp_service_user_work_no,a.rp_service_user_name,
	a.fl_service_user_id,a.fl_service_user_work_no,a.fl_service_user_name,a.bbc_service_user_id,a.bbc_service_user_work_no,a.bbc_service_user_name,
	a.sales_value,a.rp_sales_value,a.bbc_sales_value,a.fl_sales_value,a.rp_bbc_sales_value,
	a.profit,a.rp_profit,a.bbc_profit,a.fl_profit,a.rp_bbc_profit,a.prorate,a.rp_prorate,a.bbc_prorate,a.fl_prorate,a.rp_bbc_prorate,
	a.sales_prorate,a.sales_rp_prorate,a.sales_bbc_prorate,a.sales_rp_bbc_prorate,a.sales_fl_prorate,
	a.salary_rp_sales_value,a.salary_rp_profit,a.salary_fl_sales_value,a.salary_fl_profit,a.salary_bbc_sales_value,a.salary_bbc_profit, 	
	a.receivable_amount,a.overdue_amount,
	a.rp_sales_sale_fp_rate,a.rp_sales_profit_fp_rate,a.fl_sales_sale_fp_rate,a.fl_sales_profit_fp_rate,a.bbc_sales_sale_fp_rate,a.bbc_sales_profit_fp_rate,
	a.rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
	a.fl_service_user_sale_fp_rate,a.fl_service_user_profit_fp_rate,
	a.bbc_service_user_sale_fp_rate,a.bbc_service_user_profit_fp_rate,
	a.sales_over_rate,
	a.rp_service_user_over_rate,
	a.fl_service_user_over_rate,
	a.bbc_service_user_over_rate,
	a.rp_service_receivable_amount,a.rp_service_overdue_amount,
	a.fl_service_receivable_amount,
	a.fl_service_overdue_amount,
	a.bbc_service_receivable_amount,
	a.bbc_service_overdue_amount,	
	--销售员各业务提成
	a.tc_rp_sales_value_sales,a.tc_rp_profit_sales,a.tc_fl_sales_value_sales,a.tc_fl_profit_sales,a.tc_bbc_sales_value_sales,a.tc_bbc_profit_sales,
	--各业务服务管家提成
	a.tc_rp_sales_value_service,a.tc_rp_profit_service,a.tc_fl_sales_value_service,a.tc_fl_profit_service,a.tc_bbc_sales_value_service,a.tc_bbc_profit_service,	
	a.sales_sales_value_ytd,a.sales_rp_bbc_sales_value_ytd,a.sales_fl_sales_value_ytd,
	if(fixed_service_fee is not null,fixed_service_fee,if(service_fee_2 is not null,service_fee_2,if(a.service_fee is not null,a.service_fee,
		coalesce(tc_rp_sales_value_sales,0)+coalesce(tc_rp_profit_sales,0)+
		coalesce(tc_fl_sales_value_sales,0)+coalesce(tc_fl_profit_sales,0)+
		coalesce(tc_bbc_sales_value_sales,0)+coalesce(tc_bbc_profit_sales,0)
		))) as tc_sales,
	if(fixed_service_fee is not null,fixed_service_fee,if(a.service_fee_2_rp is not null,service_fee_2_rp,if(a.service_fee_rp is not null,service_fee_rp,tc_rp_sales_value_service+tc_rp_profit_service))) as tc_rp_service,
	if(fixed_service_fee is not null,fixed_service_fee,if(a.service_fee_2_fl is not null,service_fee_2_fl,if(a.service_fee_fl is not null,service_fee_fl,tc_fl_sales_value_service+tc_fl_profit_service))) as tc_fl_service,
	if(fixed_service_fee is not null,fixed_service_fee,if(a.service_fee_2_bbc is not null,service_fee_2_bbc,if(a.service_fee_bbc is not null,service_fee_bbc,tc_bbc_sales_value_service+tc_bbc_profit_service))) as tc_bbc_service,
	refund_sales_value,refund_rp_sales_value,refund_bbc_sales_value,refund_rp_bbc_sales_value,refund_fl_sales_value	
from
	(
	select 
		a.smonth,a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_id,a.customer_no,a.customer_name,
		a.sales_id,a.work_no,a.sales_name,a.rp_service_user_id,a.rp_service_user_work_no,a.rp_service_user_name,
		a.fl_service_user_id,a.fl_service_user_work_no,a.fl_service_user_name,a.bbc_service_user_id,a.bbc_service_user_work_no,a.bbc_service_user_name,	
		a.sales_value,a.rp_sales_value,a.bbc_sales_value,a.fl_sales_value,a.rp_bbc_sales_value,
		a.profit,a.rp_profit,a.bbc_profit,a.fl_profit,a.rp_bbc_profit,a.prorate,a.rp_prorate,a.bbc_prorate,a.fl_prorate,a.rp_bbc_prorate,
		a.sales_prorate,a.sales_rp_prorate,a.sales_bbc_prorate,a.sales_rp_bbc_prorate,a.sales_fl_prorate,
		--a.salary_total_sales_value,a.salary_total_profit,
		--奖金包_日配业务
		a.salary_rp_sales_value,a.salary_rp_profit,
		--奖金包_福利业务
		a.salary_fl_sales_value,a.salary_fl_profit,
		--奖金包_BBC业务
		a.salary_bbc_sales_value,a.salary_bbc_profit, 
		a.receivable_amount,a.overdue_amount,
		--分配比例
		a.rp_sales_sale_fp_rate,a.rp_sales_profit_fp_rate,a.fl_sales_sale_fp_rate,a.fl_sales_profit_fp_rate,a.bbc_sales_sale_fp_rate,a.bbc_sales_profit_fp_rate,
		a.rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
		a.fl_service_user_sale_fp_rate,a.fl_service_user_profit_fp_rate,
		a.bbc_service_user_sale_fp_rate,a.bbc_service_user_profit_fp_rate,
		--销售员及各服务管家逾期系数
		a.sales_over_rate,
		a.rp_service_user_over_rate,
		a.fl_service_user_over_rate,
		a.bbc_service_user_over_rate,
		a.rp_service_receivable_amount,
		a.rp_service_overdue_amount,
		a.fl_service_receivable_amount,
		a.fl_service_overdue_amount,
		a.bbc_service_receivable_amount,
		a.bbc_service_overdue_amount,			
		--提成_日配业务_销售员
		--'123311','124033','118376' 只核算销售额提成 不核算毛利额提成 每月
		--'124524','127312','127336' 减半核算销售额提成及毛利额提成 每月
		--'124652' 只核算销售额提成 不核算毛利额提成 每月
		--'126069','126113' 只核算销售额提成 不核算毛利额提成 当月
		--'126259'
		a.salary_rp_sales_value*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0))*coalesce(a.rp_sales_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as tc_rp_sales_value_sales,
		a.salary_rp_profit*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0))*coalesce(a.rp_sales_profit_fp_rate,0)*
			if(a.customer_no in ('123311','124033','118376','124652','126069','126113'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1)) as tc_rp_profit_sales,		
				
		--提成_福利业务_销售员
		a.salary_fl_sales_value*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0))*coalesce(a.fl_sales_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as tc_fl_sales_value_sales,
		a.salary_fl_profit*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0))*coalesce(a.fl_sales_profit_fp_rate,0)*
			if(a.customer_no in ('123311','124033','118376','124652','126069','126113'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1)) as tc_fl_profit_sales,
		
		--提成_BBC业务_销售员
		a.salary_bbc_sales_value*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0))*coalesce(a.bbc_sales_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as tc_bbc_sales_value_sales,
		a.salary_bbc_profit*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0))*coalesce(a.bbc_sales_profit_fp_rate,0)*
			if(a.customer_no in ('123311','124033','118376','124652','126069','126113'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1)) as tc_bbc_profit_sales,		
		
		--提成_日配业务_服务管家
		a.salary_rp_sales_value*(1-coalesce(if(a.rp_service_user_over_rate<=0.5,a.rp_service_user_over_rate,1),0))*coalesce(a.rp_service_user_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as tc_rp_sales_value_service,
		a.salary_rp_profit*(1-coalesce(if(a.rp_service_user_over_rate<=0.5,a.rp_service_user_over_rate,1),0))*coalesce(a.rp_service_user_profit_fp_rate,0)*
			if(a.customer_no in ('123311','124033','118376','124652','126069','126113'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1)) as tc_rp_profit_service,	

		--提成_福利业务_服务管家
		a.salary_fl_sales_value*(1-coalesce(if(a.fl_service_user_over_rate<=0.5,a.fl_service_user_over_rate,1),0))*coalesce(a.fl_service_user_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as tc_fl_sales_value_service,
		a.salary_fl_profit*(1-coalesce(if(a.fl_service_user_over_rate<=0.5,a.fl_service_user_over_rate,1),0))*coalesce(a.fl_service_user_profit_fp_rate,0)*
			if(a.customer_no in ('123311','124033','118376','124652','126069','126113'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1)) as tc_fl_profit_service,

		--提成_BBC业务_服务管家
		a.salary_bbc_sales_value*(1-coalesce(if(a.bbc_service_user_over_rate<=0.5,a.bbc_service_user_over_rate,1),0))*coalesce(a.bbc_service_user_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as tc_bbc_sales_value_service,
		a.salary_bbc_profit*(1-coalesce(if(a.bbc_service_user_over_rate<=0.5,a.bbc_service_user_over_rate,1),0))*coalesce(a.bbc_service_user_profit_fp_rate,0)*
			if(a.customer_no in ('123311','124033','118376','124652','126069','126113'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1)) as tc_bbc_profit_service,			
		
		a.sales_sales_value_ytd,-- 销售员年度累计销售额
		a.sales_rp_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
		a.sales_fl_sales_value_ytd, -- 销售员年度累计福利销售额
		--'127396' 不参与逾期核算 当月
		b.service_fee*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0)) as service_fee,
		b.service_fee*(1-coalesce(if(a.rp_service_user_over_rate<=0.5,a.rp_service_user_over_rate,1),0)) as service_fee_rp,
		b.service_fee*(1-coalesce(if(a.fl_service_user_over_rate<=0.5,a.fl_service_user_over_rate,1),0)) as service_fee_fl,
		b.service_fee*(1-coalesce(if(a.bbc_service_user_over_rate<=0.5,a.bbc_service_user_over_rate,1),0)) as service_fee_bbc,
		if(c.service_fee_2 is not null,
			case when a.customer_no in('120595') then service_fee_2 --固定毛利率 服务费不变
				when a.customer_no in('122837') then service_fee_2 --*0.8 --销售员武警客户整体毛利率
				when a.customer_no in('124584','124784') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125017','125028') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125029') then service_fee_2
				when prorate<0.1 then service_fee_2*0.8
				when prorate>=0.1 and prorate<0.12 then service_fee_2
				when prorate>=0.12 and prorate<0.15 then service_fee_2+500.00
				when prorate>=0.15 then service_fee_2+1000.00 else null 
			end,null)*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0)) as service_fee_2,
			
		if(c.service_fee_2 is not null,
			case when a.customer_no in('120595') then service_fee_2 --固定毛利率 服务费不变
				when a.customer_no in('122837') then service_fee_2 --*0.8 --销售员武警客户整体毛利率
				when a.customer_no in('124584','124784') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125017','125028') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125029') then service_fee_2
				when prorate<0.1 then service_fee_2*0.8
				when prorate>=0.1 and prorate<0.12 then service_fee_2
				when prorate>=0.12 and prorate<0.15 then service_fee_2+500.00
				when prorate>=0.15 then service_fee_2+1000.00 else null 
			end,null)*(1-coalesce(if(a.rp_service_user_over_rate<=0.5,a.rp_service_user_over_rate,1),0)) as service_fee_2_rp,
			
		if(c.service_fee_2 is not null,
			case when a.customer_no in('120595') then service_fee_2 --固定毛利率 服务费不变
				when a.customer_no in('122837') then service_fee_2 --*0.8 --销售员武警客户整体毛利率
				when a.customer_no in('124584','124784') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125017','125028') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125029') then service_fee_2
				when prorate<0.1 then service_fee_2*0.8
				when prorate>=0.1 and prorate<0.12 then service_fee_2
				when prorate>=0.12 and prorate<0.15 then service_fee_2+500.00
				when prorate>=0.15 then service_fee_2+1000.00 else null 
			end,null)*(1-coalesce(if(a.fl_service_user_over_rate<=0.5,a.fl_service_user_over_rate,1),0)) as service_fee_2_fl,

		if(c.service_fee_2 is not null,
			case when a.customer_no in('120595') then service_fee_2 --固定毛利率 服务费不变
				when a.customer_no in('122837') then service_fee_2 --*0.8 --销售员武警客户整体毛利率
				when a.customer_no in('124584','124784') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125017','125028') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125029') then service_fee_2
				when prorate<0.1 then service_fee_2*0.8
				when prorate>=0.1 and prorate<0.12 then service_fee_2
				when prorate>=0.12 and prorate<0.15 then service_fee_2+500.00
				when prorate>=0.15 then service_fee_2+1000.00 else null 
			end,null)*(1-coalesce(if(a.bbc_service_user_over_rate<=0.5,a.bbc_service_user_over_rate,1),0)) as service_fee_2_bbc,
		d.fixed_service_fee,
		--if(c.service_fee_2 is not null,service_fee_2,null)*(1-coalesce(if(a.sales_over_rate<=0.5,a.sales_over_rate,1),0)) as service_fee_2,
		refund_sales_value,refund_rp_sales_value,refund_bbc_sales_value,
		refund_rp_bbc_sales_value,refund_fl_sales_value
	from
		csx_tmp.tc_salary_00 a 
		--202202月签呈，安徽省，服务费*逾期系数，每月
		--'127396' 不参与逾期核算 当月
		left join
			(
			select '999999x' as customer_no,0.00 as service_fee
			union all  select '104885' as customer_no,333.33 as service_fee
			union all  select '115286' as customer_no,333.33 as service_fee
			union all  select '127396' as customer_no,333.33 as service_fee
			union all  select '121452' as customer_no,300.00 as service_fee
			union all  select '107415' as customer_no,200.00 as service_fee
			union all  select '113857' as customer_no,200.00 as service_fee
			union all  select '122147' as customer_no,100.00 as service_fee
			union all  select '122159' as customer_no,100.00 as service_fee
			union all  select '122167' as customer_no,100.00 as service_fee
			union all  select '122185' as customer_no,100.00 as service_fee
			union all  select '122186' as customer_no,100.00 as service_fee
			union all  select '122188' as customer_no,100.00 as service_fee
			union all  select '125770' as customer_no,0.00 as service_fee
			union all  select '126644' as customer_no,0.00 as service_fee
			union all  select '127747' as customer_no,0.00 as service_fee
			union all  select '127754' as customer_no,0.00 as service_fee
			union all  select '127543' as customer_no,70.58 as service_fee
			union all  select '127592' as customer_no,70.58 as service_fee
			union all  select '127698' as customer_no,70.58 as service_fee
			union all  select '127728' as customer_no,70.58 as service_fee
			union all  select '127729' as customer_no,70.58 as service_fee
			union all  select '127739' as customer_no,70.58 as service_fee
			union all  select '127743' as customer_no,70.58 as service_fee
			union all  select '127745' as customer_no,70.58 as service_fee
			union all  select '127746' as customer_no,70.58 as service_fee
			union all  select '127750' as customer_no,70.58 as service_fee
			union all  select '127751' as customer_no,70.58 as service_fee
			union all  select '127753' as customer_no,70.58 as service_fee
			union all  select '127755' as customer_no,70.58 as service_fee
			union all  select '127760' as customer_no,70.58 as service_fee
			union all  select '127766' as customer_no,70.58 as service_fee
			union all  select '127767' as customer_no,70.58 as service_fee
			union all  select '127775' as customer_no,70.58 as service_fee
			union all  select '127553' as customer_no,166.66 as service_fee
			union all  select '127737' as customer_no,166.66 as service_fee
			union all  select '127756' as customer_no,166.66 as service_fee
			
			) b on b.customer_no=a.customer_no
		--202202月签呈 安徽省 武警客户 务费*逾期系数*折扣 每月
		left join
			(
			select '999999x' as customer_no,0.00 as service_fee_2
			union all  select '120595' as customer_no,200.00 as service_fee_2
			union all  select '124584' as customer_no,750.00 as service_fee_2
			union all  select '124784' as customer_no,750.00 as service_fee_2
			--union all  select '125028' as customer_no,1000.00 as service_fee_2
			union all  select '116932' as customer_no,1000.00 as service_fee_2
			union all  select '120830' as customer_no,500.00 as service_fee_2
			union all  select '122837' as customer_no,500.00 as service_fee_2
			union all  select '125029' as customer_no,500.00 as service_fee_2
			union all  select '125017' as customer_no,500.00 as service_fee_2
			union all  select '125028' as customer_no,500.00 as service_fee_2
			) c on c.customer_no=a.customer_no
		--固定服务费 每月
		left join
			( 
			select '999999x' as customer_no,0.00 as fixed_service_fee
			union all  select '126259' as customer_no,200.00 as fixed_service_fee
			union all  select '123311' as customer_no,500.00 as fixed_service_fee
			--union all  select '127312' as customer_no,100.00 as fixed_service_fee
			--union all  select '127336' as customer_no,100.00 as fixed_service_fee
			) d on d.customer_no=a.customer_no
	) a 
;--16

--客户提成详情
drop table csx_tmp.tc_new_cust_salary_info_202204; --17
create table csx_tmp.tc_new_cust_salary_info_202204
as
select
	a.smonth,
	--客户地区信息
	a.sales_region_code as region_code_customer,a.sales_region_name as region_name_customer,
	a.province_code as province_code_customer,a.province_name as province_name_customer,
	a.city_group_code as city_group_code_customer,a.city_group_name as city_group_name_customer,
	--销售员地区信息
	coalesce(c.region_code,'') as region_code_sales,
	coalesce(c.region_name,'') as region_name_sales,
	coalesce(c.province_code,'') as province_code_sales,
	coalesce(c.province_name,'') as province_name_sales,
	coalesce(c.city_group_code,'') as city_group_code_sales,
	coalesce(c.city_group_name,'') as city_group_name_sales,
	--日配服务管家地区信息
	coalesce(d.region_code,'') as region_code_rp_service,
	coalesce(d.region_name,'') as region_name_rp_service,
	coalesce(d.province_code,'') as province_code_rp_service,
	coalesce(d.province_name,'') as province_name_rp_service,
	coalesce(d.city_group_code,'') as city_group_code_rp_service,
	coalesce(d.city_group_name,'') as city_group_name_rp_service,
	--福利服务管家地区信息
	coalesce(d2.region_code,'') as region_code_fl_service,
	coalesce(d2.region_name,'') as region_name_fl_service,
	coalesce(d2.province_code,'') as province_code_fl_service,
	coalesce(d2.province_name,'') as province_name_fl_service,
	coalesce(d2.city_group_code,'') as city_group_code_fl_service,
	coalesce(d2.city_group_name,'') as city_group_name_fl_service,
	--BBC服务管家地区信息
	coalesce(d3.region_code,'') as region_code_bbc_service,
	coalesce(d3.region_name,'') as region_name_bbc_service,
	coalesce(d3.province_code,'') as province_code_bbc_service,
	coalesce(d3.province_name,'') as province_name_bbc_service,
	coalesce(d3.city_group_code,'') as city_group_code_bbc_service,
	coalesce(d3.city_group_name,'') as city_group_name_bbc_service,
	a.customer_id,a.customer_no,a.customer_name,
	coalesce(a.sales_id,'') as sales_id,
	coalesce(a.work_no,'') as work_no,
	coalesce(a.sales_name,'') as sales_name,
	coalesce(a.rp_service_user_id,'') as rp_service_user_id,
	coalesce(a.rp_service_user_work_no,'') as rp_service_user_work_no,
	coalesce(a.rp_service_user_name,'') as rp_service_user_name,
	coalesce(a.fl_service_user_id,'') as fl_service_user_id,
	coalesce(a.fl_service_user_work_no,'') as fl_service_user_work_no,
	coalesce(a.fl_service_user_name,'') as fl_service_user_name,
	coalesce(a.bbc_service_user_id,'') as bbc_service_user_id,
	coalesce(a.bbc_service_user_work_no,'') as bbc_service_user_work_no,
	coalesce(a.bbc_service_user_name,'') as bbc_service_user_name,
	a.sales_value,a.rp_sales_value,a.fl_sales_value,a.bbc_sales_value,a.rp_bbc_sales_value,
	a.profit,a.rp_profit,a.fl_profit,a.bbc_profit,a.rp_bbc_profit,a.prorate,a.rp_prorate,a.fl_prorate,a.bbc_prorate,a.rp_bbc_prorate,
	a.sales_prorate,a.sales_rp_prorate,a.sales_fl_prorate,a.sales_bbc_prorate,
	a.sales_rp_bbc_prorate,a.salary_rp_sales_value,a.salary_rp_profit,a.salary_fl_sales_value,a.salary_fl_profit,
	a.salary_bbc_sales_value,a.salary_bbc_profit,
	salary_rp_sales_value+salary_fl_sales_value+salary_bbc_sales_value as salary_sales_value,
	salary_rp_profit+salary_fl_profit+salary_bbc_profit as salary_profit,
	coalesce(a.receivable_amount,0) as receivable_amount,coalesce(a.overdue_amount,0) as overdue_amount,	
	a.rp_sales_sale_fp_rate,a.rp_sales_profit_fp_rate,a.fl_sales_sale_fp_rate,a.fl_sales_profit_fp_rate,a.bbc_sales_sale_fp_rate,a.bbc_sales_profit_fp_rate,
	a.rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
	a.fl_service_user_sale_fp_rate,a.fl_service_user_profit_fp_rate,
	a.bbc_service_user_sale_fp_rate,a.bbc_service_user_profit_fp_rate,	
	a.sales_over_rate,a.rp_service_user_over_rate,
	a.fl_service_user_over_rate,a.bbc_service_user_over_rate,a.rp_service_receivable_amount,a.rp_service_overdue_amount,
	a.fl_service_receivable_amount,a.fl_service_overdue_amount,a.bbc_service_receivable_amount,a.bbc_service_overdue_amount,
	a.sales_sales_value_ytd,a.sales_rp_bbc_sales_value_ytd,a.sales_fl_sales_value_ytd,
	refund_sales_value,refund_rp_sales_value,refund_bbc_sales_value,refund_rp_bbc_sales_value,refund_fl_sales_value,
	--销售员提成
	if(a.sales_id<>'',a.tc_rp_sales_value_sales,0) as tc_rp_sales_value_sales,
	if(a.sales_id<>'',a.tc_rp_profit_sales,0) as tc_rp_profit_sales,
	if(a.sales_id<>'',a.tc_fl_sales_value_sales,0) as tc_fl_sales_value_sales,
	if(a.sales_id<>'',a.tc_fl_profit_sales,0) as tc_fl_profit_sales,
	if(a.sales_id<>'',a.tc_bbc_sales_value_sales,0) as tc_bbc_sales_value_sales,
	if(a.sales_id<>'',a.tc_bbc_profit_sales,0) as tc_bbc_profit_sales,
	if(a.sales_id<>'',a.tc_sales,0) as tc_sales,
	--日配服务管家提成
	if(a.rp_service_user_id<>'',a.tc_rp_sales_value_service,0) as tc_rp_sales_value_service,
	if(a.rp_service_user_id<>'',a.tc_rp_profit_service,0) as tc_rp_profit_service,
	if(a.rp_service_user_id<>'',a.tc_rp_service,0) as tc_rp_service,
	--福利服务管家提成
	if(a.fl_service_user_id<>'',a.tc_fl_sales_value_service,0) as tc_fl_sales_value_service,
	if(a.fl_service_user_id<>'',a.tc_fl_profit_service,0) as tc_fl_profit_service,
	if(a.fl_service_user_id<>'',a.tc_fl_service,0) as tc_fl_service,
	--bbc服务管家提成
	if(a.bbc_service_user_id<>'',a.tc_bbc_sales_value_service,0) as tc_bbc_sales_value_service,
	if(a.bbc_service_user_id<>'',a.tc_bbc_profit_service,0) as tc_bbc_profit_service,
	if(a.bbc_service_user_id<>'',a.tc_bbc_service,0) as tc_bbc_service
from
	csx_tmp.tc_new_cust_salary_00 a 
	--left join(select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:month_end_day}) b on b.customer_no=a.customer_no
	left join csx_tmp.tc_person_info c on c.id=a.sales_id
	left join csx_tmp.tc_person_info d on d.user_number=split(a.rp_service_user_work_no,';')[0]
	left join csx_tmp.tc_person_info d2 on d2.user_number=split(a.fl_service_user_work_no,';')[0]
	left join csx_tmp.tc_person_info d3 on d3.user_number=split(a.bbc_service_user_work_no,';')[0]
;--18


--客户本月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_kehu' row format delimited fields terminated by '\t'
select 
	smonth,province_name_customer,city_group_name_customer,customer_no,customer_name,work_no,sales_name,rp_service_user_work_no,rp_service_user_name,
	fl_service_user_work_no,fl_service_user_name,bbc_service_user_work_no,bbc_service_user_name,
	sales_value,rp_sales_value,fl_sales_value,bbc_sales_value,rp_bbc_sales_value,
	profit,rp_profit,fl_profit,bbc_profit,rp_bbc_profit,
	prorate,rp_prorate,fl_prorate,bbc_prorate,rp_bbc_prorate,
	sales_prorate,sales_rp_prorate,sales_fl_prorate,sales_bbc_prorate,sales_rp_bbc_prorate,
	salary_rp_sales_value,salary_rp_profit,
	salary_fl_sales_value,salary_fl_profit,	
	salary_bbc_sales_value,salary_bbc_profit, 
	salary_sales_value,salary_profit,
	receivable_amount,overdue_amount,
	rp_sales_sale_fp_rate,rp_sales_profit_fp_rate,fl_sales_sale_fp_rate,fl_sales_profit_fp_rate,bbc_sales_sale_fp_rate,bbc_sales_profit_fp_rate,
	rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
	fl_service_user_sale_fp_rate,fl_service_user_profit_fp_rate,
	bbc_service_user_sale_fp_rate,bbc_service_user_profit_fp_rate,	
	sales_over_rate,rp_service_user_over_rate,fl_service_user_over_rate,bbc_service_user_over_rate,
	tc_rp_sales_value_sales,tc_rp_profit_sales,tc_fl_sales_value_sales,tc_fl_profit_sales,tc_bbc_sales_value_sales,tc_bbc_profit_sales,tc_sales,
	tc_rp_sales_value_service,tc_rp_profit_service,tc_rp_service,
	tc_fl_sales_value_service,tc_fl_profit_service,tc_fl_service,
	tc_bbc_sales_value_service,tc_bbc_profit_service,tc_bbc_service
from
	csx_tmp.tc_new_cust_salary_info_202204
;--19

--销售员本月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_xiaoshou' row format delimited fields terminated by '\t'
select
	smonth,province_name_sales,city_group_name_sales,work_no,sales_name,
	sales_sales_value_ytd,-- 销售员年度累计销售额
	sales_rp_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	sales_fl_sales_value_ytd,-- 销售员年度累计福利销售额
	sum(sales_value) as sales_value,
	sum(rp_sales_value) as rp_sales_value,
	sum(fl_sales_value) as fl_sales_value,
	sum(bbc_sales_value) as bbc_sales_value,
	sum(rp_bbc_sales_value) as rp_bbc_sales_value,
	sum(profit) as profit,
	sum(rp_profit) as rp_profit,
	sum(fl_profit) as fl_profit,
	sum(bbc_profit) as bbc_profit,
	sum(rp_bbc_profit) as rp_bbc_profit,
	coalesce(sum(profit)/abs(sum(sales_value)),0) as prorate,
	coalesce(sum(rp_profit)/abs(sum(rp_sales_value)),0) as rp_prorate,
	coalesce(sum(fl_profit)/abs(sum(fl_sales_value)),0) as fl_prorate,
	coalesce(sum(bbc_profit)/abs(sum(bbc_sales_value)),0) as bbc_prorate,
	coalesce(sum(rp_bbc_profit)/abs(sum(rp_bbc_sales_value)),0) as rp_bbc_prorate,
	sum(salary_rp_sales_value) as salary_rp_sales_value,
	sum(salary_rp_profit) as salary_rp_profit,
	sum(salary_fl_sales_value) as salary_fl_sales_value,
	sum(salary_fl_profit) as salary_fl_profit,
	sum(salary_bbc_sales_value) as salary_bbc_sales_value,
	sum(salary_bbc_profit) as salary_bbc_profit, 
	sum(salary_sales_value) as salary_sales_value,
	sum(salary_profit) as salary_profit,
	sum(receivable_amount) as receivable_amount,
	sum(overdue_amount) as overdue_amount,
	sales_over_rate,
	sum(tc_rp_sales_value_sales) as tc_rp_sales_value_sales,
	sum(tc_rp_profit_sales) as tc_rp_profit_sales,
	sum(tc_fl_sales_value_sales) as tc_fl_sales_value_sales,
	sum(tc_fl_profit_sales) as tc_fl_profit_sales,
	sum(tc_bbc_sales_value_sales) as tc_bbc_sales_value_sales,
	sum(tc_bbc_profit_sales) as tc_bbc_profit_sales,
	sum(tc_rp_sales_value_sales+tc_fl_sales_value_sales+tc_bbc_sales_value_sales) as tc_sales_value_sales,
	sum(tc_rp_profit_sales+tc_fl_profit_sales+tc_bbc_profit_sales) as tc_profit_sales,
	sum(tc_sales) as tc_sales
from 
	csx_tmp.tc_new_cust_salary_info_202204 a 
group by 
	smonth,province_name_sales,city_group_name_sales,work_no,sales_name,sales_sales_value_ytd,
	sales_rp_bbc_sales_value_ytd,sales_fl_sales_value_ytd,sales_over_rate
;--20

--服务管家本月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_fuwuguanjia' row format delimited fields terminated by '\t'
select
	smonth,province_name,city_group_name,service_user_work_no,service_user_name,
	sum(rp_sales_value+fl_sales_value+bbc_sales_value) as sales_value,
	sum(rp_sales_value) as rp_sales_value,
	sum(fl_sales_value) as fl_sales_value,	
	sum(bbc_sales_value) as bbc_sales_value,
	sum(rp_profit+fl_profit+bbc_profit) as profit,
	sum(rp_profit) as rp_profit,
	sum(fl_profit) as fl_profit,
	sum(bbc_profit) as bbc_profit,
	coalesce(sum(rp_profit+fl_profit+bbc_profit)/abs(sum(rp_sales_value+fl_sales_value+bbc_sales_value)),0) as prorate,
	coalesce(sum(rp_profit)/abs(sum(rp_sales_value)),0) as rp_prorate,
	coalesce(sum(fl_profit)/abs(sum(fl_sales_value)),0) as fl_prorate,
	coalesce(sum(bbc_profit)/abs(sum(bbc_sales_value)),0) as bbc_prorate,
	sum(salary_sales_value) as salary_sales_value,
	sum(salary_profit) as salary_profit,
	service_receivable_amount,
	service_overdue_amount,
	service_user_over_rate,
	sum(tc_sales_value_service) as tc_sales_value_service,
	sum(tc_profit_service) as tc_profit_service,
	sum(tc_service) as tc_service
from 
	(
	--日配服务管家
	select
		smonth,province_name_rp_service as province_name,city_group_name_rp_service as city_group_name,
		rp_service_user_work_no as service_user_work_no,rp_service_user_name as service_user_name,
		rp_sales_value,0 as fl_sales_value,0 as bbc_sales_value,
		rp_profit,0 as fl_profit,0 as bbc_profit,
		salary_rp_sales_value as salary_sales_value,
		salary_rp_profit as salary_profit,
		rp_service_receivable_amount as service_receivable_amount,
		rp_service_overdue_amount as service_overdue_amount,
		rp_service_user_over_rate as service_user_over_rate,
		tc_rp_sales_value_service as tc_sales_value_service,
		tc_rp_profit_service as tc_profit_service,
		tc_rp_service as tc_service
	from
		csx_tmp.tc_new_cust_salary_info_202204
	where
		rp_service_user_work_no <>''
	union all
	--福利服务管家
	select
		smonth,province_name_fl_service as province_name,city_group_name_fl_service as city_group_name,
		fl_service_user_work_no as service_user_work_no,fl_service_user_name as service_user_name,
		0 as rp_sales_value,fl_sales_value,0 as bbc_sales_value,
		0 as rp_profit,fl_profit,0 as bbc_profit,
		salary_fl_sales_value as salary_sales_value,
		salary_fl_profit as salary_profit,
		fl_service_receivable_amount as service_receivable_amount,
		fl_service_overdue_amount as service_overdue_amount,
		fl_service_user_over_rate as service_user_over_rate,
		tc_fl_sales_value_service as tc_sales_value_service,
		tc_fl_profit_service as tc_profit_service,
		tc_fl_service as tc_service
	from
		csx_tmp.tc_new_cust_salary_info_202204
	where
		fl_service_user_work_no <>''
	union all
	--BBC服务管家
	select
		smonth,province_name_bbc_service as province_name,city_group_name_bbc_service as city_group_name,
		bbc_service_user_work_no as service_user_work_no,bbc_service_user_name as service_user_name,
		0 as rp_sales_value,0 as fl_sales_value,bbc_sales_value,
		0 as rp_profit,0 as fl_profit,bbc_profit,
		salary_bbc_sales_value as salary_sales_value,
		salary_bbc_profit as salary_profit,
		bbc_service_receivable_amount as service_receivable_amount,
		bbc_service_overdue_amount as service_overdue_amount,
		bbc_service_user_over_rate as service_user_over_rate,
		tc_bbc_sales_value_service as tc_sales_value_service,
		tc_bbc_profit_service as tc_profit_service,
		tc_bbc_service as tc_service
	from
		csx_tmp.tc_new_cust_salary_info_202204
	where
		bbc_service_user_work_no <>''
	) a 
group by 
	smonth,province_name,city_group_name,service_user_work_no,service_user_name,service_receivable_amount,service_overdue_amount,service_user_over_rate
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

-- 大客户提成：月度新客户
--select 
--	b.province_name,b.customer_no,b.customer_name,b.attribute_desc,b.dev_source_name,b.work_no,b.sales_name,b.sign_date,
--	a.first_order_date
--from
--	(
--	select 
--		attribute_desc,dev_source_name,customer_no,customer_name,channel_name,sales_name,work_no,province_name,
--		regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,estimate_contract_amount*10000 estimate_contract_amount
--	from 
--		csx_dw.dws_crm_w_a_customer
--	where 
--		sdt='current'
--		and customer_no<>''
--		and channel_code in('1','7','8')
--	)b
--	join --客户最早销售月 新客月、新客季度
--		(
--		select 
--			customer_no,
--			min(first_order_date) first_order_date
--		from 
--			csx_dw.dws_crm_w_a_customer_active
--		where 
--			sdt = 'current'
--		group by 
--			customer_no
--		having 
--			min(first_order_date)>='20220601' and min(first_order_date)<='20220630'
--		)a on b.customer_no=a.customer_no;

/*

--客户对应销售员与服务管家
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	customer_id,customer_no,customer_name,province_name,
	sales_id,work_no,sales_name,rp_service_user_id,rp_service_user_work_no,rp_service_user_name,
	fl_service_user_id,fl_service_user_work_no,fl_service_user_name,bbc_service_user_id,bbc_service_user_work_no,bbc_service_user_name,
	rp_sales_sale_fp_rate,rp_sales_profit_fp_rate,fl_sales_sale_fp_rate,fl_sales_profit_fp_rate,bbc_sales_sale_fp_rate,bbc_sales_profit_fp_rate,
	rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,fl_service_user_sale_fp_rate,fl_service_user_profit_fp_rate,bbc_service_user_sale_fp_rate,     
	bbc_service_user_profit_fp_rate,is_sale,is_overdue
from 
	csx_tmp.tc_customer_service_manager_info_new
where
	is_sale='是' or is_overdue='是'
*/

--安徽省武警客户服务费
--select 
--    work_no,sales_name,customer_no,customer_name,business_type_name,sum(sales_value),sum(profit)
--from 
--    csx_dw.dws_sale_r_d_detail
--where
--    sdt>='20220601' and sdt<='20220630'
--    and channel_code in('1','7','9')
--    and customer_no in ('120595','124584','124784','117017','124668','120823','125028','125017','116932','120830','122837','125029')
--group by
--    work_no,sales_name,customer_no,customer_name,business_type_name
