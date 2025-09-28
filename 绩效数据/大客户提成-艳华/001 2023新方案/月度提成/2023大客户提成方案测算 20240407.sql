20230611 大客户提成新方案测算
月度提成=（Σ当月回款额* ①毛利率提成比例 *②回款时间系数* ③提成系数）* ④个人基准毛利额达成系数
①毛利率提成比例：根据回款追溯业务发生当月客户（分日配、福利BBC）毛利率来确定提成比例，具体见后①毛利率提成比例 
②回款时间系数：根据回款日期与结算日期差异周期不同设置系数，即客户占用资金成本时间，时间越长，成本越高。具体见后②回款时间系数 
③提成系数：用作区分是否配置服务管家，具体见③提成系数 
④个人基准毛利额达成系数：根据个人底薪+外勤补贴标准占毛利额4%倒推出个人基准毛利额，作为销售人员养活个人的依据参考，具体见后④个人基准毛利额达成系数 


计算思路：
1、当月回款核销订单的客户每月每个业务类型的回款金额
2、回款核销订单关联销售表计算客户每月每个业务类型的毛利率及对应毛利率提成比例
3、根据回款核销订单结算日期计算回款日期与结算日期的差异天数，从而计算回款时间系数
4、第1-3项需要存订单明细
5、前3项计算得到客户分配前提成，再根据是否配备服务管家乘以提成系数，得到销售员与管家的分配后提成
6、第5项结果乘以个人基准毛利额达成系数得到人员在该客户的当月最终提成

20230728处理：断约客户再激活不算新客
20230801处理：项目制订单的结算日，若项目制跨多月则以结束日期拆分月 20230808取消
20230811处理：BBC订单同时存在BBC自营和BBC联营，两种标签的，回款额按销售比例拆分；修复两个系统BBC订单号不一致的关联及关联重复问题
20230823处理：回款时间系数中回款日期由核销日期改为回款打款日期

20240315处理：调整打款日期
-- 基准毛利额 csx_analyse_tc_sales_service_profit_basic_mf

/*
-- 调整项
1、BBC区分自营联营
2、回款时间系数根据回款核销日期与结算日期间隔
			case when c.account_period_code='Z007' then 1
				when datediff(b.paid_date ,a.overdue_date)<=0 then 1
				when datediff(b.paid_date ,a.overdue_date)<=30 then 0.85
				when datediff(b.paid_date ,a.overdue_date)<=60 then 0.65
				when datediff(b.paid_date ,a.overdue_date)<=90 then 0.45
				when datediff(b.paid_date ,a.overdue_date)<=120 then 0.25
				when datediff(b.paid_date ,a.overdue_date)>120 then 0.15
			end rate
3、毛利率根据订单结算月按月算综合毛利率定相应月的毛利提成系数 订单区分日配、BBC自营、BBC联营、福利单独计算

-- 调整项0705
1、新增日配新客系数 月度提成=（Σ当月回款额* ①毛利率提成比例 *②回款时间系数* ③提成系数*④日配新客系数）* ⑤个人基准毛利额达成系数
①鼓励新客开发，对日配新客给予1.2倍系数，履约满一年后视为老客，系数还原为1
②日配断约3个月（90天）以上重新履约算新客
2、回款时间系数调整
	case when datediff(a.paid_date, a.bill_date)<=30 then 1
		when datediff(a.paid_date, a.bill_date)<=60 then 0.85
		when datediff(a.paid_date, a.bill_date)<=90 then 0.65
		when datediff(a.paid_date, a.bill_date)<=120 then 0.45
		when datediff(a.paid_date, a.bill_date)<=150 then 0.25
		when datediff(a.paid_date, a.bill_date)>150 then 0.15
	end dff_rate,
	
	case when datediff(a.paid_date, a.bill_date)<=15 then 1.1
		when datediff(a.paid_date, a.bill_date)<=31 then 1
		when datediff(a.paid_date, a.bill_date)<=60 then 0.8
		when datediff(a.paid_date, a.bill_date)<=90 then 0.6
		when datediff(a.paid_date, a.bill_date)<=120 then 0.4
		when datediff(a.paid_date, a.bill_date)<=150 then 0.2
		when datediff(a.paid_date, a.bill_date)>150 then 0.1
	end dff_rate,	
3、提成系数 销售员若为个人开发客户按原方案计算，若为公司资源或承接他人客户 则 配备服务管家40%、未配备服务管家80% 根据签呈处理
4、毛利率提成比例 增加 日配直送客户如毛利率低于8%的按0.5%进行提成 暂时不做通过签呈处理
5、平台酒水不计入整体方案计算
6、个人基准毛利额达成系数 考虑入职 
	入职时间≤1年 & 达成率<100% 则实际达成结果
	入职时间≤1年 & 达成率≥100% 则1
	入职时间>1年 & 达成率<100% 则0
	入职时间>1年 & 达成率≥100% 则1


*/



-- 确认需对哪些客服经理补充等级比例
select 
	a.performance_province_name,b.flag,b.user_work_no,b.user_name,
	d.s_level,d.level_sale_rate,d.level_profit_rate,
	sum(sale_amt) sale_amt,sum(profit) profit
from
	(
		select 
			performance_province_code,performance_province_name,customer_code,
			substr(sdt,1,6) smonth,sum(sale_amt) sale_amt,sum(profit) profit
		from csx_dws.csx_dws_sale_detail_di
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
			and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and channel_code in('1','7')
		group by performance_province_code,performance_province_name,customer_code,substr(sdt,1,6)
	)a	
left join 
(
	select distinct 
		flag,
		customer_no,
		user_work_no,
		user_name,
		user_id		
	from 
	(
		select 
			'服务管家' flag,
			customer_no,
			rp_service_user_work_no_new as user_work_no,
			rp_service_user_name_new as user_name,
			rp_service_user_id_new as user_id
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and rp_service_user_id_new is not null
		union all
		select 
			'服务管家' flag,
			customer_no,
			fl_service_user_work_no_new as user_work_no,
			fl_service_user_name_new as user_name,
			fl_service_user_id_new as user_id
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and fl_service_user_id_new is not null
		union all	
		select 
			'服务管家' flag,
			customer_no,
			bbc_service_user_work_no_new as user_work_no,
			bbc_service_user_name_new as user_name,
			bbc_service_user_id_new as user_id
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and bbc_service_user_id_new is not null
	)a
) b on b.customer_no=a.customer_code
left join 
(
	select *
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-2),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-2)),'-','')
	-- where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	-- and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	-- 不算提成 此类人员需再次确认
	and salary_level<>'不算提成'
)d on b.user_work_no=d.service_user_work_no	
where d.s_level is null
and b.user_name is not null
group by a.performance_province_name,b.flag,b.user_work_no,b.user_name,
d.s_level,d.level_sale_rate,d.level_profit_rate;	

-- =============================================================================================================================================================================
-- ★★★★★补充销售员与管家等级比例并校验★★★★★
-- ★★★★★补充管家等级并校验★★★★★
-- ★★★★★目前针对城市服务商业务要算提成 或其他规则外客户要算提成的 需要在代码中手动添加 ★★★★★
-- =============================================================================================================================================================================
-- 确认需对哪些客服经理补充等级比例


--###########################################################################################


-- 表1 csx_analyse_customer_sale_service_info_rate_qc_mi 客户对应销售员与服务管家及提成系数_签呈版--------------------------------------
-- 注：202308新旧方案提成都算，因此管家分配比例按照12 23 35，在此表中代码中调整的，202309后只算新提成管家分配比例按照2 3 4因此不可直接重刷8月提成
insert overwrite table csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi partition(smt)
select 
	distinct 
	concat_ws('-',substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6),a.customer_no) as biz_id,
	a.customer_id,a.customer_no,a.customer_name,
	a.channel_code,a.channel_name,
	a.region_code,a.region_name,
	a.province_code,a.province_name,
	a.city_group_code,a.city_group_name,
	sales_id_new as sales_id,
	work_no_new as work_no,
	sales_name_new as sales_name,
	rp_service_user_id_new as rp_service_user_id,
	rp_service_user_work_no_new as rp_service_user_work_no,		
	rp_service_user_name_new as rp_service_user_name,

	fl_service_user_id_new as fl_service_user_id,
	fl_service_user_work_no_new as fl_service_user_work_no,
	fl_service_user_name_new as fl_service_user_name,

	bbc_service_user_id_new as bbc_service_user_id,	
	bbc_service_user_work_no_new as bbc_service_user_work_no,
	bbc_service_user_name_new as bbc_service_user_name,	
	
	case 
		-- when c.income_type in('不算提成','离职') then 0
		when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
		when length(rp_service_user_id_new)<>0 and length(sales_id_new)>0 then 0.6
		when length(sales_id_new)>0 then 1
		end as rp_sales_fp_rate,
	case 
		 -- when c.income_type in('不算提成','离职') then 0
		 when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
		 when length(fl_service_user_id_new)<>0 and length(sales_id_new)>0 then 0.6
		 when length(sales_id_new)>0 then 1
		 end as fl_sales_fp_rate,	
	case 
		 -- when c.income_type in('不算提成','离职') then 0
		 when a.region_name='华南大区' and a.sales_id_new <>'' then 0.6
		 when length(bbc_service_user_id_new)<>0 and length(sales_id_new)>0 then 0.6
		 when length(sales_id_new)>0 then 1
		 end as bbc_sales_fp_rate,
		 
	b1.level_sale_rate as rp_service_user_fp_rate,
	b2.level_sale_rate as fl_service_user_fp_rate,
	b3.level_sale_rate as bbc_service_user_fp_rate,     	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6) as smt -- 统计日期
from 
(
	select *
	from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)a
left join
(
	select service_user_work_no,level_sale_rate
		-- case level_sale_rate
		-- when 0 then 0
		-- when 0.1 then 0.2
		-- when 0.2 then 0.3 
		-- when 0.3 then 0.4 end as level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b1 on a.rp_service_user_work_no_new=b1.service_user_work_no	
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b2 on a.fl_service_user_work_no_new=b2.service_user_work_no
left join
(
	select service_user_work_no,level_sale_rate
	from csx_analyse.csx_analyse_tc_service_level_mf 
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	and tc_sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
)b3 on a.bbc_service_user_work_no_new=b3.service_user_work_no
-- -- 销售员收入组为‘不算提成’或‘离职’的 提成系数为0
-- left join 
-- (
-- 	select distinct work_no,income_type
-- 	from csx_analyse.csx_analyse_report_sales_income_info_new_mf
-- 	where sdt=regexp_replace(trunc('${sdt_yes_date}','MM'),'-','')
-- 	and sdt_date=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
-- ) c on c.work_no=a.work_no_new
-- ‘调整对应人员比例’客户
left join 
(
	select * 
	-- from csx_analyse.csx_analyse_tc_customer_special_rules_mf 
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_first like '%大客户提成-调整对应人员比例%'
) d on d.customer_code=a.customer_no
where d.category_first is null
union all 
select 
	biz_id,
	customer_id,
	customer_no,
	customer_name,
	channel_code,
	channel_name,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	sales_id,
	work_no,
	sales_name,
	rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,
	bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	cast(rp_sales_sale_fp_rate as decimal(20,6)) rp_sales_sale_fp_rate,
	cast(fl_sales_sale_fp_rate as decimal(20,6)) fl_sales_sale_fp_rate,
	cast(bbc_sales_sale_fp_rate as decimal(20,6)) bbc_sales_sale_fp_rate,
	cast(rp_service_user_sale_fp_rate as decimal(20,6)) rp_service_user_fp_rate,
	cast(fl_service_user_sale_fp_rate as decimal(20,6)) fl_service_user_fp_rate,
	cast(bbc_service_user_sale_fp_rate as decimal(20,6)) bbc_service_user_fp_rate,
	
	-- cast(if(rp_sales_sale_fp_rate=0.7,0.6,rp_sales_sale_fp_rate) as decimal(20,6)) rp_sales_fp_rate,
	-- cast(if(fl_sales_sale_fp_rate=0.7,0.6,fl_sales_sale_fp_rate) as decimal(20,6)) fl_sales_fp_rate,
	-- cast(if(bbc_sales_sale_fp_rate=0.7,0.6,bbc_sales_sale_fp_rate) as decimal(20,6)) bbc_sales_fp_rate,
	-- cast(case rp_service_user_sale_fp_rate
	-- 	when 0 then 0
	-- 	when 0.1 then 0.2
	-- 	when 0.2 then 0.3 
	-- 	when 0.3 then 0.4 else rp_service_user_sale_fp_rate end		
	-- 	as decimal(20,6)) rp_service_user_fp_rate,
	-- cast(case fl_service_user_sale_fp_rate
	-- 	when 0 then 0
	-- 	when 0.1 then 0.2
	-- 	when 0.2 then 0.3 
	-- 	when 0.3 then 0.4 else fl_service_user_sale_fp_rate end		
	-- 	as decimal(20,6)) fl_service_user_fp_rate,
	-- cast(case bbc_service_user_sale_fp_rate
	-- 	when 0 then 0
	-- 	when 0.1 then 0.2
	-- 	when 0.2 then 0.3 
	-- 	when 0.3 then 0.4 else bbc_service_user_sale_fp_rate end		
	-- 	as decimal(20,6)) bbc_service_user_fp_rate,	

	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	smt
from csx_analyse.csx_analyse_tc_customer_person_rate_special_rules_mf
where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
;


-- 表2 csx_analyse_fr_tc_customer_sale_fwf_business 客户提成_业绩毛利 -----------


-- 202308以后客户本月销售额、定价毛利额统计
drop table csx_analyse_tmp.tmp_tc_customer_sales_prorate_m;
create temporary table csx_analyse_tmp.tmp_tc_customer_sales_prorate_m
as
select 
	a.customer_code,a.smonth,
	-- 销售额
	a.sale_amt, -- 客户总销售额
	a.rp_sale_amt, -- 客户日配销售额
	a.bbc_sale_amt_zy, -- 客户bbc自营销售额
	a.bbc_sale_amt_ly, -- 客户bbc联营销售额
	a.fl_sale_amt, -- 客户福利销售额
	
	-- 定价毛利额
	(a.rp_profit-coalesce(a.rp_sale_amt*b.rp_rate,0))+
	(a.bbc_profit_zy-coalesce(a.bbc_sale_amt_zy*b.bbc_rate_zy,0))+
	(a.bbc_profit_ly-coalesce(a.bbc_sale_amt_ly*b.bbc_rate_ly,0))+
	(a.fl_profit-coalesce(a.fl_sale_amt*b.fl_rate,0)) as profit,-- 客户总定价毛利额
	
	a.rp_profit-coalesce(a.rp_sale_amt*b.rp_rate,0) as rp_profit,-- 客户日配定价毛利额
	a.bbc_profit_zy-coalesce(a.bbc_sale_amt_zy*b.bbc_rate_zy,0) as bbc_profit_zy,-- 客户bbc自营定价毛利额
	a.bbc_profit_ly-coalesce(a.bbc_sale_amt_ly*b.bbc_rate_ly,0) as bbc_profit_ly,-- 客户bbc联营定价毛利额
	a.fl_profit-coalesce(a.fl_sale_amt*b.fl_rate,0) as fl_profit,-- 客户福利定价毛利额
	
	-- 定价毛利率
	coalesce(((a.rp_profit-coalesce(a.rp_sale_amt*b.rp_rate,0))+
	(a.bbc_profit_zy-coalesce(a.bbc_sale_amt_zy*b.bbc_rate_zy,0))+
	(a.bbc_profit_ly-coalesce(a.bbc_sale_amt_ly*b.bbc_rate_ly,0))+
	(a.fl_profit-coalesce(a.fl_sale_amt*b.fl_rate,0)))/abs(sale_amt),0) as prorate, -- 客户总定价毛利率
	
	coalesce((a.rp_profit-coalesce(a.rp_sale_amt*b.rp_rate,0))/abs(a.rp_sale_amt),0) as rp_prorate, -- 客户日配定价毛利率
	coalesce((a.bbc_profit_zy-coalesce(a.bbc_sale_amt_zy*b.bbc_rate_zy,0))/abs(a.bbc_sale_amt_zy),0) as bbc_prorate_zy, -- 客户bbc自营定价毛利率
	coalesce((a.bbc_profit_ly-coalesce(a.bbc_sale_amt_ly*b.bbc_rate_ly,0))/abs(a.bbc_sale_amt_ly),0) as bbc_prorate_ly, -- 客户bbc联营定价毛利率
	coalesce((a.fl_profit-coalesce(a.fl_sale_amt*b.fl_rate,0))/abs(a.fl_sale_amt),0) as fl_prorate-- 客户福利定价毛利率
from 
(
select 
	customer_code,smonth,
	-- 销售额
	sum(sale_amt) as sale_amt, -- 客户总销售额
	sum(rp_sale_amt) as rp_sale_amt, -- 客户日配销售额		
	-- sum(bbc_sale_amt) as bbc_sale_amt, -- 客户bbc销售额
	sum(bbc_sale_amt_zy) as bbc_sale_amt_zy, -- 客户bbc自营销售额
	sum(bbc_sale_amt_ly) as bbc_sale_amt_ly, -- 客户bbc联营销售额
	sum(fl_sale_amt) as fl_sale_amt, -- 客户福利销售额
	-- 定价毛利额
	sum(profit) as profit,-- 客户总定价毛利额
	sum(rp_profit) as rp_profit,-- 客户日配定价毛利额
	-- sum(bbc_profit) as bbc_profit,-- 客户bbc定价毛利额
	sum(bbc_profit_zy) as bbc_profit_zy, -- 客户bbc自营定价毛利额
	sum(bbc_profit_ly) as bbc_profit_ly, -- 客户bbc联营定价毛利额
	sum(fl_profit) as fl_profit-- 客户福利定价毛利额


from 
	(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
		-- sum(case when business_type_code in('6') then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then sale_amt else 0 end) as bbc_sale_amt_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then sale_amt else 0 end) as bbc_sale_amt_ly,
		sum(case when business_type_code in('2') then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit, 
		sum(case when business_type_code in ('1','4','5') then profit else 0 end) as rp_profit,
		-- sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then profit else 0 end) as bbc_profit_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then profit else 0 end) as bbc_profit_ly,		
		sum(case when business_type_code in('2') then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
	where 
		sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
				-- 福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202210签呈 北京 129026 129000 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202306签呈 福建省'229290','175709','125092' 项目供应商 纳入大客户提成计算
				or (business_type_code in ('4') and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))
				-- 202310签呈 河北南京部分项目供应商客户纳入大客户提成计算
			-- or (business_type_code in ('4') and customer_code in ('235949','222853','131428','131466','131202','131208','129746','128435','230788',
			-- '112846','118357','115832','125795','125831','131462','114496','117322','131421','118395','114470','130024','130430','118644','131091',
			-- '217946','129955','130226','120115','226821','129870','129865','130269','126125','129674','129880','227563','129855','129860','130955',
			-- '127521','225541','232102','233354','234828','130844','223112','129854','125545','128705','125513','126001'))
			or (business_type_code in ('4') and customer_code in (
				select customer_code
				from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
				where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and category_second like '%纳入大客户提成计算：项目供应商%'
			))
			or customer_code in (
				select customer_code
				from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
				where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and category_second='纳入大客户提成计算'
			)			
				)
			-- and performance_province_name in ('福建省')
			and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断					
	group by 
		customer_code,substr(sdt,1,6)
		
		
	-- 扣减定价毛利额 '扣减毛利额'
	union all
	select customer_code,smt_date as smonth,
	0 as sale_amt, 
	0 as rp_sale_amt,
	0 as bbc_sale_amt_zy,
	0 as bbc_sale_amt_ly,
	0 as fl_sale_amt,
	0-adjust_amount as profit,
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_profit, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_profit_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_profit_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_profit
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减毛利额%'	
	
	-- 扣减销售额与毛利额 '扣减销售额与毛利额'
	union all
	select customer_code,smt_date as smonth,
	0-adjust_amount as sale_amt, 
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_sale_amt, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_sale_amt_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_sale_amt_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_sale_amt,	
	
	0-adjust_amount as profit,	
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_profit, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_profit_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_profit_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_profit
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减销售额与毛利额%'	

	-- 扣减销售额 '扣减销售额'
	union all
	select customer_code,smt_date as smonth,
	0-adjust_amount as sale_amt, 
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_sale_amt, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_sale_amt_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_sale_amt_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_sale_amt,	
	
	0 as profit,	
	0 as rp_profit, 
	0 as bbc_profit_zy,
	0 as bbc_profit_ly,
	0 as fl_profit
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减销售额'		
	)a
	group by customer_code,smonth
	)a
	left join
		(
		-- 固定扣点
		select smt_date,customer_code,category_second,
		sum(case when adjust_business_type='日配' then adjust_rate end) as rp_rate,
		sum(case when adjust_business_type='BBC自营' then adjust_rate end) as bbc_rate_zy,
		sum(case when adjust_business_type='BBC联营' then adjust_rate end) as bbc_rate_ly,
		sum(case when adjust_business_type='福利' then adjust_rate end) as fl_rate
		from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and category_second='固定扣点'
		group by smt_date,customer_code,category_second
		) b on b.customer_code=a.customer_code;	
	



insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business partition(smt)
select 
concat_ws('-',a.smonth,a.customer_code) as biz_id,
	a.smonth,
	b.performance_region_code as region_code_customer,
	b.performance_region_name as region_name_customer,
	b.performance_province_code as province_code_customer,
	b.performance_province_name as province_name_customer,
	b.performance_city_code as city_group_code_customer,
	b.performance_city_name as city_group_name_customer,	
	
	a.customer_code,
	c.customer_name,
	c.sales_user_id,
	c.sales_user_number,
	c.sales_user_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt_zy,
	a.bbc_sale_amt_ly,
	a.fl_sale_amt,
	a.profit,
	a.rp_profit,
	a.bbc_profit_zy,
	a.bbc_profit_ly,
	a.fl_profit,
	a.profit/abs(a.sale_amt) as prorate,
	a.rp_profit/abs(a.rp_sale_amt) as rp_prorate,
	a.bbc_profit_zy/abs(a.bbc_sale_amt_zy) as bbc_prorate_zy,
	a.bbc_profit_ly/abs(a.bbc_sale_amt_ly) as bbc_prorate_ly,
	a.fl_profit/abs(a.fl_sale_amt) as fl_prorate,
	d.category_second as service_falg,
	d.service_fee,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	-- substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt,
	cast(a.smonth as string) as smt	
from 
	(
	select
		a.customer_code,a.smonth,
		-- 销售额
		a.sale_amt, -- 客户总销售额
		a.rp_sale_amt, -- 客户日配销售额
		a.bbc_sale_amt_zy, -- 客户bbc自营销售额
		a.bbc_sale_amt_ly, -- 客户bbc联营销售额
		a.fl_sale_amt, -- 客户福利销售额
		if(a2.rp_rate is null,if(a1.rp_rate is null,a.rp_profit,if(a.rp_profit>a1.rp_rate*a.rp_sale_amt,a1.rp_rate*a.rp_sale_amt,a.rp_profit)),a2.rp_rate*a.rp_sale_amt)+
		if(a2.bbc_rate_zy is null,if(a1.bbc_rate_zy is null,a.bbc_profit_zy,if(a.bbc_profit_zy>a1.bbc_rate_zy*a.bbc_sale_amt_zy,a1.bbc_rate_zy*a.bbc_sale_amt_zy,a.bbc_profit_zy)),a2.bbc_rate_zy*a.bbc_sale_amt_zy)+
		if(a2.bbc_rate_ly is null,if(a1.bbc_rate_ly is null,a.bbc_profit_ly,if(a.bbc_profit_ly>a1.bbc_rate_ly*a.bbc_sale_amt_ly,a1.bbc_rate_ly*a.bbc_sale_amt_ly,a.bbc_profit_ly)),a2.bbc_rate_ly*a.bbc_sale_amt_ly)+
		if(a2.fl_rate is null,if(a1.fl_rate is null,a.fl_profit,if(a.fl_profit>a1.fl_rate*a.fl_sale_amt,a1.fl_rate*a.fl_sale_amt,a.fl_profit)),a2.fl_rate*a.fl_sale_amt) as profit,
			
		if(a2.rp_rate is null,if(a1.rp_rate is null,a.rp_profit,if(a.rp_profit>a1.rp_rate*a.rp_sale_amt,a1.rp_rate*a.rp_sale_amt,a.rp_profit)),a2.rp_rate*a.rp_sale_amt) as rp_profit,-- 客户日配定价毛利额
		if(a2.bbc_rate_zy is null,if(a1.bbc_rate_zy is null,a.bbc_profit_zy,if(a.bbc_profit_zy>a1.bbc_rate_zy*a.bbc_sale_amt_zy,a1.bbc_rate_zy*a.bbc_sale_amt_zy,a.bbc_profit_zy)),a2.bbc_rate_zy*a.bbc_sale_amt_zy) as bbc_profit_zy,-- 客户bbc自营定价毛利额
		if(a2.bbc_rate_ly is null,if(a1.bbc_rate_ly is null,a.bbc_profit_ly,if(a.bbc_profit_ly>a1.bbc_rate_ly*a.bbc_sale_amt_ly,a1.bbc_rate_ly*a.bbc_sale_amt_ly,a.bbc_profit_ly)),a2.bbc_rate_ly*a.bbc_sale_amt_ly) as bbc_profit_ly,-- 客户bbc联营定价毛利额
		if(a2.fl_rate is null,if(a1.fl_rate is null,a.fl_profit,if(a.fl_profit>a1.fl_rate*a.fl_sale_amt,a1.fl_rate*a.fl_sale_amt,a.fl_profit)),a2.fl_rate*a.fl_sale_amt) as fl_profit-- 客户福利定价毛利额
	
	from csx_analyse_tmp.tmp_tc_customer_sales_prorate_m a
	-- 关联签呈'最高毛利率'
	left join
		(
		select smt_date,customer_code,category_second,
			sum(case when adjust_business_type='日配' then adjust_rate end) as rp_rate,
			sum(case when adjust_business_type='BBC自营' then adjust_rate end) as bbc_rate_zy,
			sum(case when adjust_business_type='BBC联营' then adjust_rate end) as bbc_rate_ly,
			sum(case when adjust_business_type='福利' then adjust_rate end) as fl_rate
		from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and category_second like '%最高毛利率%'
		group by smt_date,customer_code,category_second		
		)a1 on a1.customer_code=a.customer_code
	-- 关联签呈'部分业务固定毛利率'
	left join		
		(  
		select smt_date,customer_code,category_second,
			sum(case when adjust_business_type='日配' then adjust_rate end) as rp_rate,
			sum(case when adjust_business_type='BBC自营' then adjust_rate end) as bbc_rate_zy,
			sum(case when adjust_business_type='BBC联营' then adjust_rate end) as bbc_rate_ly,
			sum(case when adjust_business_type='福利' then adjust_rate end) as fl_rate
		from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and category_second like '%部分业务固定毛利率%'
		group by smt_date,customer_code,category_second
		)a2 on a2.customer_code=a.customer_code		
	) a
	left join 
		(
		select 
			distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			-- 202303签呈 上海 '130733','128865','130078','114872','124484' 每月纳入大客户提成计算 仅管家拿提成
			-- case when channel_code='9' and customer_code not in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260') then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from csx_dim.csx_dim_crm_customer_info 
		where 
			sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and customer_type_code=4
		)b on b.customer_code=a.customer_code
	left join 
		(
		select
			region_code,region_name,
			province_code,province_name,
			city_group_code,city_group_name,
			customer_no,customer_name,
			sales_id sales_user_id,
			work_no sales_user_number,
			sales_name sales_user_name,
			rp_service_user_id,
			rp_service_user_work_no,
			rp_service_user_name,
			fl_service_user_id,
			fl_service_user_work_no,
			fl_service_user_name,		
			bbc_service_user_id,
			bbc_service_user_work_no,
			bbc_service_user_name
			
		-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
		from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
		where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
		) c on c.customer_no=a.customer_code		
	left join 
		(
	select customer_code,smt_date as smonth,category_second,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%服务费%'		
	)d on a.customer_code=d.customer_code
left join 
		(
	select customer_code,smt_date as smonth,category_second
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%剔除客户%'		
	)e on a.customer_code=e.customer_code
where e.category_second is null	
;




-- 表3 csx_analyse_fr_tc_customer_credit_order_unpay_mi 大客户提成-核销订单的待核销金额-----------不含BBC纯现金客户---------------------------

set hive.tez.container.size=8192;

-- 销售单业绩毛利
-- oc返利单的可能一个返利单对应多个原单号，original_order_code
drop table if exists csx_analyse_tmp.tmp_tc_cust_sale_order;
create temporary table csx_analyse_tmp.tmp_tc_cust_sale_order
as
select 
	order_code,business_type_code,business_type_name,status,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit,
	sum(sale_amt_jiushui) as sale_amt_jiushui,
	sum(profit_jiushui) as profit_jiushui	
from 
(
	select order_code,
	-- operation_mode_code,
	case when business_type_code in(1,4,5) then 1 
		when business_type_code in(6) and operation_mode_code=1 then 6.1
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
		else business_type_code end business_type_code,
	
	case when business_type_code in(1,4,5) then '日配业务'
		when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
		else business_type_name end business_type_name,
	
	if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1) status,  -- 是否有效 0.无效 1.有效
	-- sum(sale_amt) as sale_amt,
	-- sum(profit) as profit,
	sum(case when goods_code not in ('8718','8708','8649','840509') then sale_amt end) as sale_amt,
	sum(case when goods_code not in ('8718','8708','8649','840509') then profit end) as profit,
	sum(case when goods_code in ('8718','8708','8649','840509') then sale_amt end) as sale_amt_jiushui,
	sum(case when goods_code in ('8718','8708','8649','840509') then profit end) as profit_jiushui
	from csx_dws.csx_dws_sale_detail_di
	where channel_code in('1','7','9')
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	and (business_type_code in('1','2','6')
		or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
		or (business_type_code in ('4') and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))
		-- 202310签呈 河北南京部分项目供应商客户纳入大客户提成计算
		-- or (business_type_code in ('4') and customer_code in ('235949','222853','131428','131466','131202','131208','129746','128435','230788',
		-- '112846','118357','115832','125795','125831','131462','114496','117322','131421','118395','114470','130024','130430','118644','131091',
		-- '217946','129955','130226','120115','226821','129870','129865','130269','126125','129674','129880','227563','129855','129860','130955',
		-- '127521','225541','232102','233354','234828','130844','223112','129854','125545','128705','125513','126001'))
		or (business_type_code in ('4') and customer_code in (
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second like '%纳入大客户提成计算：项目供应商%'
		))
		or customer_code in (
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second='纳入大客户提成计算'
		)			
		-- 202303签呈 上海 '130733','128865','130078' 每月纳入大客户提成计算 仅管家拿提成
		or customer_code in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260'))
	-- and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断
	group by order_code,
	case when business_type_code in(1,4,5) then 1 
		when business_type_code in(6) and operation_mode_code=1 then 6.1
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
		else business_type_code end,
	
	case when business_type_code in(1,4,5) then '日配业务'  
		when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
		else business_type_name end,
	if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)
	
	union all
	-- 价格补救需用原单
	select original_order_code order_code,
	case when business_type_code in(1,4,5) then 1 
		when business_type_code in(6) and operation_mode_code=1 then 6.1
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
		else business_type_code end business_type_code,
	
	case when business_type_code in(1,4,5) then '日配业务' 
		when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
		else business_type_name end business_type_name,
	
	if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1) status,  -- 是否有效 0.无效 1.有效
	-- sum(sale_amt) as sale_amt,
	-- sum(profit) as profit,
	sum(case when goods_code not in ('8718','8708','8649','840509') then sale_amt end) as sale_amt,
	sum(case when goods_code not in ('8718','8708','8649','840509') then profit end) as profit,
	sum(case when goods_code in ('8718','8708','8649','840509') then sale_amt end) as sale_amt_jiushui,
	sum(case when goods_code in ('8718','8708','8649','840509') then profit end) as profit_jiushui
	from csx_dws.csx_dws_sale_detail_di
	where channel_code in('1','7','9')
	-- 订单来源渠道: 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
	and order_channel_code=5
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	and (business_type_code in('1','2','6')
		or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
		or (business_type_code in ('4') and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))
		-- 202310签呈 河北南京部分项目供应商客户纳入大客户提成计算
		-- or (business_type_code in ('4') and customer_code in ('235949','222853','131428','131466','131202','131208','129746','128435','230788',
		-- '112846','118357','115832','125795','125831','131462','114496','117322','131421','118395','114470','130024','130430','118644','131091',
		-- '217946','129955','130226','120115','226821','129870','129865','130269','126125','129674','129880','227563','129855','129860','130955',
		-- '127521','225541','232102','233354','234828','130844','223112','129854','125545','128705','125513','126001'))
		or (business_type_code in ('4') and customer_code in (
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second like '%纳入大客户提成计算：项目供应商%'
		))	
		or customer_code in (
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second='纳入大客户提成计算'
		)			
		-- 202303签呈 上海 '130733','128865','130078' 每月纳入大客户提成计算 仅管家拿提成
		or customer_code in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260'))
	group by original_order_code,
	case when business_type_code in(1,4,5) then 1 
		when business_type_code in(6) and operation_mode_code=1 then 6.1
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
		else business_type_code end,
	
	case when business_type_code in(1,4,5) then '日配业务' 
		when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
		else business_type_name end,
	if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)
)a
group by order_code,business_type_code,business_type_name,status
;





-- 注意：调价返利数据快照中订单号可能变化，8月OC23072500033-1，9月OC23072500033
-- 结算单中本月回款核销金额 限定本月核销单但是以认领单中的打款日期计算回款时间系数
drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale;
create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale
as
select 
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
	split(a.source_bill_no,'-')[0] source_bill_no,	-- 来源单号
	case when source_sys='BBC' and substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-2)
		 when source_sys='BBC' and substr(split(a.source_bill_no,'-')[0],1,1)='B' and substr(split(a.source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(a.source_bill_no,'-')[0],2,length(split(a.source_bill_no,'-')[0])-1)
		 else split(a.source_bill_no,'-')[0]
		 end as source_bill_no_new,

	-- a.source_bill_no,	-- 来源单号
	-- case when source_sys='BBC' and substr(a.source_bill_no,1,1)='B' and substr(a.source_bill_no,-1,1) in ('A','B','C','D','E') then substr(a.source_bill_no,2,length(a.source_bill_no)-2)
	-- 	 when source_sys='BBC' and substr(a.source_bill_no,1,1)='B' and substr(a.source_bill_no,-1,1) not in ('A','B','C','D','E')then substr(a.source_bill_no,2,length(a.source_bill_no)-1)
	-- 	 else a.source_bill_no
	-- 	 end as source_bill_no_new,
		 
	a.customer_code,	-- 客户编码
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	-- a.account_period_code,	-- 账期编码
	-- a.account_period_name,	-- 账期名称
	-- a.account_period_value,	-- 账期值
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	a.bill_date, -- 结算日期
	a.overdue_date,	-- 逾期开始日期	
	b.paid_date,	-- 核销日期
	a.order_amt,	-- 源单据对账金额
	c.unpay_amt, -- 历史核销剩余金额
	sum(b.pay_amt) pay_amt_old,	-- 核销金额
	-- 如果本月核销金额大于历史核销剩余金额则按比例折算
	sum(if(c.unpay_amt is null,b.pay_amt,
		if(c.unpay_amt=0,0,
		if(c.unpay_amt<>0 and b.pay_amt_bill<>0 and abs(b.pay_amt_bill/c.unpay_amt)>1,b.pay_amt/abs(b.pay_amt_bill/c.unpay_amt),b.pay_amt)
		))) pay_amt  -- 核销金额
from 
(
	select 
		bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		sdt,
		source_bill_no,	-- 来源单号
		-- case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
		-- 	when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
		-- 	else split(source_bill_no,'-')[0]
		-- 	end as source_bill_no_new,		
		customer_code,	-- 客户编码
		credit_code,	-- 信控号
		happen_date,	-- 发生时间
		order_amt,	-- 源单据对账金额
		company_code,	-- 签约公司编码
		residue_amt,	-- 剩余预付款金额_预付款客户抵消订单金额后
		residue_amt_sss,	-- 剩余预付款金额_原销售结算
		unpaid_amount,	-- 未回款金额_抵消预付款后
		unpaid_amount_sss,	-- 未回款金额_原销售结算
		bad_debt_amount,	-- 坏账金额
		account_period_code,	-- 账期编码
		account_period_name,	-- 账期名称
		account_period_value,	-- 账期值
		source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
		close_bill_amount pay_amt,	-- 核销金额
		reconciliation_period,  -- 对账周期
		project_end_date,	-- 项目制结束日期
		project_begin_date,	-- 项目制开始日期
		bill_start_date,	-- 账期周期开始时间
		bill_end_date,	-- 账期周期结束时间
		-- date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1) bill_date, -- 结算日期
		
		-- 若不是项目制或者项目制且项目制结束日期距离业务发生日期小于等于31天，则常规结算日不变（账期周期结束时间+1）
		--  否则看项目制结束日期距离发生日期大于31天：①日期部分业务发生日期≤项目制结束日期，则业务发生月+项目制结束日期的日期；②期部分业务发生日期大于项目制结束日期，则业务发生次月+项目制结束日期的日期
	case when coalesce(project_end_date,'')='' then date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1)
	     when coalesce(project_end_date,'')<>'' then 
		 (case when datediff(date_format(project_end_date, 'yyyy-MM-dd'),date_format(happen_date, 'yyyy-MM-dd'))<=31 then date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1)
		     when substr(project_end_date,9,2)>=substr(happen_date,9,2) and reconciliation_period=1 then trunc(add_months(date_format(happen_date, 'yyyy-MM-dd'),1),'MM')
		      when substr(project_end_date,9,2)>=substr(happen_date,9,2) then date_add(date_format(concat(substr(happen_date,1,8),substr(project_end_date,9,2)), 'yyyy-MM-dd'), 1)
			  else date_add(trunc(add_months(date_format(happen_date, 'yyyy-MM-dd'),1),'MM'),cast(substr(project_end_date,9,2) as int)) end)
		else date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1) end as bill_date, -- 结算日期
			
		overdue_date	-- 逾期开始日期	  
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	-- where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	-- and date_format(happen_date,'yyyy-MM-dd')>='2020-06-01'
	and date_format(happen_date,'yyyy-MM-dd')>='2022-06-01'
	-- and customer_code='127307'
)a
join
(
	select a.close_bill_code,
		-- 用核销日期还是交易日期二选一
		coalesce(b.trade_date,a.paid_date) paid_date,
		-- a.paid_date,		
		max(a.pay_amt_bill) pay_amt_bill,
		sum(a.pay_amt) pay_amt
	from 
		(-- 核销流水明细表:本月核销金额
			select *,sum (pay_amt) over(partition by close_bill_code) as pay_amt_bill
			from 
			(
				select 
					close_bill_code,				
					claim_bill_code,close_account_code,
					-- customer_code,credit_code,company_code,
					date_format(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')), 'yyyy-MM-dd') paid_date,
					-- sum (pay_amt) over(partition by close_bill_code) as pay_amt_bill,
					sum(pay_amt) pay_amt
				-- from csx_dwd.csx_dwd_sss_close_bill_account_record_di
				-- 单据核销流水明细月快照表
				from csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf
				-- 核销日期分区
				where smt=regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','')
				and sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
				and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
				and date_format(happen_date,'yyyy-MM-dd')>='2022-06-01'
				and delete_flag ='0'
				group by 
					close_bill_code,
				claim_bill_code,close_account_code,
				date_format(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')), 'yyyy-MM-dd')
			)a
		)a
	-- 核销单中根据认领单找打款日期
	left join
		(
		select  
			claim_bill_code,substr(trade_time,1,10) trade_date,
			count(1) aa
		from csx_dwd.csx_dwd_sss_money_back_di
		where sdt>='20200601' 
		and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		-- and (paid_amt<>0 or residue_amt<>0) -- 剔除补救单和对应原单
		and customer_code not like'G%'
		group by claim_bill_code,substr(trade_time,1,10)
		)b on a.claim_bill_code=b.claim_bill_code
	group by a.close_bill_code,
	-- 用核销日期还是交易日期二选一
	coalesce(b.trade_date,a.paid_date)
	-- a.paid_date
)b on b.close_bill_code=a.source_bill_no
-- 核销月度快照表中202308后历史月快照中订单合计核销金额，被计算过的核销金额大于原单金额的，不再计算本月核销金额
left join
(
	select *
	from 
	(
		select 
			close_bill_code,
			max(bill_amt) bill_amt,
			sum(pay_amt) pay_amt,
			if(max(bill_amt)>0,if(max(bill_amt)-sum(pay_amt)>0,max(bill_amt)-sum(pay_amt),0),
			if(max(bill_amt)-sum(pay_amt)<0,max(bill_amt)-sum(pay_amt),0)) unpay_amt
		-- 单据核销流水明细月快照表
		from csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf
		-- 202308及以后月快照中8月所有核销情况及以后每月快照中当月核销
		where smt>='202308'
		and smt<regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','')
		-- 8月作为期初核销情况及以后每个计算月当月的核销，作为历史核销过的依据
		and (smt='202308'
		or (smt>'202308' and smt<=regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','') 
			and substr(sdt,1,6)=smt and substr(sdt,1,6)<regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','') ))
		and date_format(happen_date,'yyyy-MM-dd')>='2022-06-01'
		and delete_flag ='0'
		group by 
			close_bill_code
	)a -- where abs(bill_amt)<=abs(pay_amt)
)c on c.close_bill_code=b.close_bill_code
-- 本月新增订单或历史订单剩余未核销金额不为0
-- where c.bill_amt is null or c.unpay_amt<>0
group by 
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
	a.source_bill_no,	-- 来源单号
	case when source_sys='BBC' and substr(a.source_bill_no,1,1)='B' and substr(a.source_bill_no,-1,1) in ('A','B','C','D','E') then substr(a.source_bill_no,2,length(a.source_bill_no)-2)
		 when source_sys='BBC' and substr(a.source_bill_no,1,1)='B' and substr(a.source_bill_no,-1,1) not in ('A','B','C','D','E')then substr(a.source_bill_no,2,length(a.source_bill_no)-1)
		 else a.source_bill_no
		 end,		
	a.customer_code,	-- 客户编码
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	a.bill_date, -- 结算日期
	a.overdue_date,	-- 逾期开始日期	
	b.paid_date,
	a.order_amt,
	c.unpay_amt;


-- 结算单中本月回款核销金额 限定本月核销单但是以认领单中的打款日期计算回款时间系数
drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill;
create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill
as
select distinct
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
	a.source_bill_no,	-- 来源单号
	a.customer_code,	-- 客户编码
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	-- a.account_period_code,	-- 账期编码
	-- a.account_period_name,	-- 账期名称
	-- a.account_period_value,	-- 账期值
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	a.bill_date, -- 结算日期
	a.overdue_date,	-- 逾期开始日期	
	a.paid_date,	-- 核销日期	
	a.order_amt,	-- 源单据对账金额 
	a.unpay_amt,
	a.pay_amt_old,
	a.pay_amt,	-- 核销金额
	if(a.source_sys='BEGIN',1,b.business_type_code) business_type_code,
	if(a.source_sys='BEGIN','日配业务',b.business_type_name) business_type_name,
	b.status,  -- 是否有效 0.无效 1.有效
	b.sale_amt,
	b.profit,
	b.sale_amt_jiushui,
	b.profit_jiushui
from csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale a
-- 销售单业绩毛利
left join csx_analyse_tmp.tmp_tc_cust_sale_order b on b.order_code=a.source_bill_no
where b.sale_amt is not null

union all
select distinct
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
	a.source_bill_no,	-- 来源单号
	a.customer_code,	-- 客户编码
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	-- a.account_period_code,	-- 账期编码
	-- a.account_period_name,	-- 账期名称
	-- a.account_period_value,	-- 账期值
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	a.bill_date, -- 结算日期
	a.overdue_date,	-- 逾期开始日期	
	a.paid_date,	-- 核销日期	
	a.order_amt,	-- 源单据对账金额 
	a.unpay_amt,
	a.pay_amt_old,
	a.pay_amt,	-- 核销金额
	if(a.source_sys='BEGIN',1,c.business_type_code) business_type_code,
	if(a.source_sys='BEGIN','日配业务',c.business_type_name) business_type_name,
	c.status,  -- 是否有效 0.无效 1.有效
	c.sale_amt,
	c.profit,
	c.sale_amt_jiushui,
	c.profit_jiushui
from csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale a
left join csx_analyse_tmp.tmp_tc_cust_sale_order b on b.order_code=a.source_bill_no
left join csx_analyse_tmp.tmp_tc_cust_sale_order c on c.order_code=a.source_bill_no_new 
where b.sale_amt is null;



insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi partition(smt)
select 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.customer_code,a.credit_code,a.source_bill_no) biz_id,	
	b.channel_code,
	b.channel_name,
	b.region_code,
	b.region_name,
	b.province_code,
	b.province_name,
	b.city_group_code,
	b.city_group_name,
	b.sales_id,
	b.work_no,
	b.sales_name,
	b.rp_service_user_id,
	b.rp_service_user_work_no,		
	b.rp_service_user_name,

	b.fl_service_user_id,
	b.fl_service_user_work_no,
	b.fl_service_user_name,

	b.bbc_service_user_id,	
	b.bbc_service_user_work_no,
	b.bbc_service_user_name,	
	
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
	a.source_bill_no,	-- 来源单号
	a.customer_code,	-- 客户编码
	b.customer_name,
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	a.bill_date, -- 结算日期
	a.overdue_date,	-- 逾期开始日期	
	case 
	when (regexp_replace(substr(a.paid_date,1,10),'-','') between c.date_star and c.date_end) and c.adjust_business_type='全业务' then c.paid_date_new
	when (regexp_replace(substr(a.paid_date,1,10),'-','') between c.date_star and c.date_end) and c.adjust_business_type='日配' and a.business_type_name like '日配%' then c.paid_date_new
	when (regexp_replace(substr(a.paid_date,1,10),'-','') between c.date_star and c.date_end) and c.adjust_business_type='BBC' and a.business_type_name like 'BBC%' then c.paid_date_new
	when (regexp_replace(substr(a.paid_date,1,10),'-','') between c.date_star and c.date_end) and c.adjust_business_type='福利' and a.business_type_name like '福利%' then c.paid_date_new
	else a.paid_date end as paid_date,	-- 核销日期	
	
	-- paid_date,	-- 核销日期	 
	a.order_amt,	-- 源单据对账金额 
	a.unpay_amt,
	a.pay_amt_old,
	a.pay_amt,	-- 核销金额
	a.business_type_code,
	a.business_type_name,
	a.status,  -- 是否有效 0.无效 1.有效
	a.sale_amt,
	a.profit,
	a.sale_amt_jiushui,
	a.profit_jiushui,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 	
from csx_analyse_tmp.tmp_tc_cust_credit_bill a 
left join
( 
	select *
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','')
)b on a.customer_code=b.customer_no
left join 
		(
	select customer_code,smt_date as smonth,category_second,adjust_business_type,
	  date_star,date_end,
	  date_format(from_unixtime(unix_timestamp(paid_date_new,'yyyyMMdd')),'yyyy-MM-dd') as paid_date_new 
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整打款日期%'		
	)c on a.customer_code=c.customer_code
left join 
		(
	select customer_code,smt_date as smonth,category_second
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%剔除客户%'		
	)e on a.customer_code=e.customer_code
where e.category_second is null
;




-- 表4 csx_analyse_fr_tc_customer_credit_order_detail 大客户提成-回款订单明细--------------------------------------

set hive.tez.container.size=8192;

-- 纯现金客户标记 月BBC销售金额大于0，月授信支付金额等于0
drop table if exists csx_analyse_tmp.tmp_tc_cust_chunxianjin;
create temporary table csx_analyse_tmp.tmp_tc_cust_chunxianjin
as
select a.customer_code,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) smonth,
	a.sale_amt,
	a.profit,
	if(a.bbc_sale_amt>0 and a.sale_amt=a.bbc_sale_amt and coalesce(a.credit_settle_amount,0)=0,'是','否') is_chunxianjin
from 
(
	select a.customer_code,a.smonth,
		sum(a.sale_amt) as sale_amt,
		sum(a.profit) as profit,
		sum(a.bbc_sale_amt) as bbc_sale_amt,
		sum(b.credit_settle_amount) as credit_settle_amount -- 授信结算金额
	from
	(
		select 
			business_type_name,performance_province_name,customer_code,customer_name,original_order_code,substr(sdt,1,6) as smonth,
			sum(sale_amt) as sale_amt,
			sum(profit) as profit,
			sum(case when business_type_code in('1','4','5','6') then sale_amt else 0 end) as rp_bbc_sale_amt,
			sum(case when business_type_code in(6) then sale_amt else 0 end) as bbc_sale_amt,
			sum(case when business_type_code in(2) then sale_amt else 0 end) as fl_sale_amt
		from csx_dws.csx_dws_sale_detail_di
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
		and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and channel_code in('1','7','9')	
		group by business_type_name,performance_province_name,customer_code,customer_name,original_order_code,substr(sdt,1,6)
	)a 
	left join 
	(
		select
		order_code,
		sum(credit_settle_amt) credit_settle_amount
		from
		(
		select distinct
			order_code,
			bill_code,
			credit_settle_amt
		from csx_dwd.csx_dwd_bbc_wshop_bill_order_detail_di
		where sdt >=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-5),'-','')
		) tmp group by order_code	
	)b on b.order_code=a.original_order_code
	group by a.customer_code,a.smonth
)a
where a.bbc_sale_amt>0 
and a.sale_amt=a.bbc_sale_amt 
and coalesce(a.credit_settle_amount,0)=0;


-- 核销订单+纯现金客户本月销售明细
drop table if exists csx_analyse_tmp.tmp_tc_cust_order_detail;
create temporary table csx_analyse_tmp.tmp_tc_cust_order_detail
as
select 
	bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	sdt,
	source_bill_no,	-- 来源单号
	customer_code,	-- 客户编码
	credit_code,	-- 信控号
	happen_date,	-- 发生时间		
	company_code,	-- 签约公司编码
	source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	reconciliation_period,  -- 对账周期
	date_format(bill_date, 'yyyy-MM-dd') bill_date, -- 结算日期
	overdue_date,	-- 逾期开始日期	
	paid_date,	-- 核销日期	
	order_amt,	-- 源单据对账金额
	unpay_amt,
	pay_amt,	-- 核销金额
	cast(business_type_code as decimal(2,1)) business_type_code,
	business_type_name,
	cast(status as int) status,  -- 是否有效 0.无效 1.有效
	sale_amt,
	profit,
	sale_amt_jiushui,
	profit_jiushui
from csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi
where smt=regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','')

-- 纯现金
union all
select 
	null as bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	b.sdt,
	b.order_code as source_bill_no,	-- 来源单号
	b.customer_code,	-- 客户编码
	b.credit_code,	-- 信控号
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') happen_date,	-- 发生时间		
	b.company_code,	-- 签约公司编码
	'纯现金' as source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	null reconciliation_period,  -- 对账周期
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') bill_date, -- 结算日期
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') overdue_date,	-- 逾期开始日期	
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') paid_date,	-- 核销日期	
	b.sale_amt order_amt,	-- 源单据对账金额
	null as unpay_amt,
	b.sale_amt pay_amt,	-- 核销金额
	b.business_type_code,
	b.business_type_name,
	b.status,  -- 是否有效 0.无效 1.有效
	b.sale_amt,
	b.profit,
	b.sale_amt_jiushui,
	b.profit_jiushui
from csx_analyse_tmp.tmp_tc_cust_chunxianjin a 
join
(
	select 
		company_code,
		case when business_type_code in(6) and operation_mode_code=1 then 6.1
			when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
			end business_type_code,
		
		case when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
			when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
			end business_type_name,			

		performance_province_name,customer_code,credit_code,order_code,sdt,
		if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1) status,  -- 是否有效 0.无效 1.有效
		-- sum(sale_amt) as sale_amt,
		-- sum(profit) as profit,
		sum(case when goods_code not in ('8718','8708','8649','840509') then sale_amt end) as sale_amt,
		sum(case when goods_code not in ('8718','8708','8649','840509') then profit end) as profit,
		sum(case when goods_code in ('8718','8708','8649','840509') then sale_amt end) as sale_amt_jiushui,
		sum(case when goods_code in ('8718','8708','8649','840509') then profit end) as profit_jiushui
	from csx_dws.csx_dws_sale_detail_di
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
	and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and channel_code in('1','7','9')	
	group by company_code,
	case when business_type_code in(6) and operation_mode_code=1 then 6.1
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
		end,
	case when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
		when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
		end,
	performance_province_name,customer_code,credit_code,order_code,sdt,
	if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)  -- 是否有效 0.无效 1.有效
)b on a.customer_code=b.customer_code;



-- 结算单回款+BBC纯现金客户
drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu;
create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu
as
select
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	-- a.sdt,
	a.source_bill_no,	-- 来源单号
	a.customer_code,	-- 客户编码
	b.customer_name,
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	c.account_period_code,	-- 账期编码
	c.account_period_name,	-- 账期名称
	c.account_period_value,	-- 账期值
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	a.bill_date, -- 结算日期
	a.overdue_date,	-- 逾期开始日期	
	a.paid_date,	-- 核销日期	
	datediff(a.paid_date, a.bill_date) dff,
	case when datediff(a.paid_date, a.bill_date)<=15 then 1.1
		when datediff(a.paid_date, a.bill_date)<=31 then 1
		when datediff(a.paid_date, a.bill_date)<=60 then 0.8
		when datediff(a.paid_date, a.bill_date)<=90 then 0.6
		when datediff(a.paid_date, a.bill_date)<=120 then 0.4
		when datediff(a.paid_date, a.bill_date)<=150 then 0.2
		when datediff(a.paid_date, a.bill_date)>150 then 0.1
	end dff_rate,
	if(a.sale_amt_jiushui/(a.sale_amt_jiushui+a.sale_amt)>0,
		a.order_amt* (a.sale_amt/(a.sale_amt_jiushui+a.sale_amt)),a.order_amt) order_amt,	-- 源单据对账金额
	if(a.sale_amt_jiushui/(a.sale_amt_jiushui+a.sale_amt)>0,
		a.unpay_amt* (a.sale_amt/(a.sale_amt_jiushui+a.sale_amt)),a.unpay_amt) unpay_amt,	-- 历史核销剩余金额
	if(a.sale_amt_jiushui/(a.sale_amt_jiushui+a.sale_amt)>0,
		a.pay_amt* (a.sale_amt/(a.sale_amt_jiushui+a.sale_amt)),a.pay_amt) pay_amt,	-- 核销金额
	a.business_type_code,
	a.business_type_name,
	a.status,  -- 是否有效 0.无效 1.有效
	a.sale_amt,
	a.profit,
	a.sale_amt_jiushui,
	a.profit_jiushui,
	b.region_code,b.region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,
	b.sales_id,
	b.work_no,
	b.sales_name,
	b.rp_service_user_id,
	b.rp_service_user_work_no,
	b.rp_service_user_name,
	b.fl_service_user_id,
	b.fl_service_user_work_no,
	b.fl_service_user_name,		
	b.bbc_service_user_id,
	b.bbc_service_user_work_no,
	b.bbc_service_user_name,
	-- 提成分配系数
	b.rp_sales_fp_rate,
	b.fl_sales_fp_rate,
	b.bbc_sales_fp_rate,
	b.rp_service_user_fp_rate,
	b.fl_service_user_fp_rate,
	b.bbc_service_user_fp_rate	
from csx_analyse_tmp.tmp_tc_cust_order_detail a
-- 客户信息与提成系数
left join
(
	select
		region_code,region_name,province_code,province_name,city_group_code,city_group_name,
		regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') as sdt,customer_no,customer_name,
		sales_id,
		work_no,
		sales_name,
		rp_service_user_id,
		rp_service_user_work_no,
		rp_service_user_name,
		fl_service_user_id,
		fl_service_user_work_no,
		fl_service_user_name,		
		bbc_service_user_id,
		bbc_service_user_work_no,
		bbc_service_user_name,
		-- 提成系数按
		rp_sales_fp_rate,
		fl_sales_fp_rate,
		bbc_sales_fp_rate,
		rp_service_user_fp_rate,
		fl_service_user_fp_rate,
		bbc_service_user_fp_rate
		-- if(rp_sales_sale_fp_rate=0.7,0.6,if(rp_sales_sale_fp_rate=0.3,0.4,if(rp_sales_sale_fp_rate=0.2,0.3,if(rp_sales_sale_fp_rate=0.1,0.2,rp_sales_sale_fp_rate)))) as rp_sales_fp_rate,
		-- if(fl_sales_fp_rate=0.7,0.6,if(fl_sales_fp_rate=0.3,0.4,if(fl_sales_fp_rate=0.2,0.3,if(fl_sales_fp_rate=0.1,0.2,fl_sales_fp_rate)))) as fl_sales_fp_rate,
		-- if(bbc_sales_fp_rate=0.7,0.6,if(bbc_sales_fp_rate=0.3,0.4,if(bbc_sales_fp_rate=0.2,0.3,if(bbc_sales_fp_rate=0.1,0.2,bbc_sales_fp_rate)))) as bbc_sales_fp_rate,
		-- if(rp_service_user_sale_fp_rate=0.7,0.6,if(rp_service_user_sale_fp_rate=0.3,0.4,if(rp_service_user_sale_fp_rate=0.2,0.3,if(rp_service_user_sale_fp_rate=0.1,0.2,rp_service_user_sale_fp_rate)))) as rp_service_user_fp_rate,
		-- if(fl_service_user_sale_fp_rate=0.7,0.6,if(fl_service_user_sale_fp_rate=0.3,0.4,if(fl_service_user_sale_fp_rate=0.2,0.3,if(fl_service_user_sale_fp_rate=0.1,0.2,fl_service_user_sale_fp_rate)))) as fl_service_user_fp_rate,
		-- if(bbc_service_user_sale_fp_rate=0.7,0.6,if(bbc_service_user_sale_fp_rate=0.3,0.4,if(bbc_service_user_sale_fp_rate=0.2,0.3,if(bbc_service_user_sale_fp_rate=0.1,0.2,bbc_service_user_sale_fp_rate)))) as bbc_service_user_fp_rate	
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
)b on b.customer_no=a.customer_code
-- 客户信控的账期
left join
     (
         select customer_code             customer_no,
                customer_name,
                company_code,
                company_name,
                credit_code,
                performance_city_code     city_group_code,
                performance_city_name     city_group_name,
                performance_province_code province_code,
                performance_province_name province_name,
                performance_region_code   region_code,
                performance_region_name   region_name,
                channel_code,                            --  渠道编码
                channel_name,                            --  渠道名称
                sales_user_id             sales_id,      --  销售员id
                sales_user_number         work_no,       --  销售员工号
                sales_user_name           sales_name,    --  销售员名称
                account_period_code, --  账期类型
                account_period_name,  --  账期名称
                account_period_value,  --  帐期天数
                credit_limit,                            --  信控额度
                temp_credit_limit,                       --  临时额度
                temp_begin_time,                         --  临时额度起始时间
                temp_end_time,                            --  临时额度截止时间
                business_attribute_code,                 -- 信控业务属性编码
                business_attribute_name                  -- 信控业务属性名称
         from csx_dim.csx_dim_crm_customer_company_details
         where sdt = 'current' and status = 1
     ) c on a.customer_code = c.customer_no and a.company_code = c.company_code and a.credit_code = c.credit_code
left join   -- CRM客户信息取月最后一天
	(
		select 
			customer_code,customer_name,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			-- 202302签呈 上海 130733 每月纳入大客户提成计算 仅管家拿提成
			case when channel_code='9' and customer_code not in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260') then '业务代理' end as ywdl_cust,				
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from csx_dim.csx_dim_crm_customer_info 
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and channel_code in('1','7','9')
		and ((customer_type_code=4
		and customer_name not like '%内%购%'
		and customer_name not like '%临保%'
		and channel_code<>'9') 
		or (customer_code in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260')
		or customer_code in (
				select customer_code
				from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
				where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and category_second='纳入大客户提成计算'
			)))		
	)d on d.customer_code=a.customer_code
where d.customer_code is not null
;




-- BBC一个订单部分自营部分联营，拆分比例
-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail;
-- create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail partition(smt)
select
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),
		a.source_bill_no,a.paid_date,a.customer_code) biz_id,
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.source_bill_no,	-- 来源单号
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号
	a.happen_date,	-- 发生时间		
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称
	a.account_period_value,	-- 账期值
	a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,  -- 对账周期
	-- a.bill_date, -- 结算日期
	
	-- 调整结算日 暂时在代码中调整
	-- 126275 将销售日期为6.15-8.15期间的BBC，结算日调整为8.16，且最高回款系数100%
	case when a.customer_code='126275' and a.business_type_name like 'BBC%' 
	and a.happen_date >='2023-06-15' and a.happen_date <='2023-08-15' then '2023-08-16'
	else a.bill_date end as bill_date, -- 结算日期
	
	a.overdue_date,	-- 逾期开始日期	
	-- a.paid_date,	-- 核销日期	
	case when (regexp_replace(substr(a.happen_date,1,10),'-','') between f.date_star and f.date_end) and f.category_second is not null 
		 and (f.adjust_business_type=substr(a.business_type_name,1,2) or f.adjust_business_type='全业务') 
		 then trunc(add_months('${sdt_yes_date}',-1),"MM")
	else a.paid_date end paid_date,	
	a.dff,
	-- a.dff_rate,
	case when (regexp_replace(substr(a.happen_date,1,10),'-','') between f.date_star and f.date_end) and f.category_second is not null 
		 and (f.adjust_business_type=substr(a.business_type_name,1,2) or f.adjust_business_type='全业务') then f.hk_dff_rate 
		 when g.customer_code is not null then if(a.dff_rate>1,1,a.dff_rate)
		 -- 回款金额负数，回款时间系数为110%时按100%算
		 when a.pay_amt<0 and a.dff_rate=1.1 then 1	
	else a.dff_rate end dff_rate,
	-- a.order_amt,	-- 源单据对账金额
	-- a.pay_amt,	-- 核销金额
	case when b.sale_amt is not null and a.business_type_name='BBC联营' then a.order_amt*b.sale_amt_bbc_ly_rate 
		 when b.sale_amt is not null and a.business_type_name='BBC自营' then a.order_amt*b.sale_amt_bbc_zy_rate 
	else a.order_amt end order_amt,	-- 源单据对账金额
	
	case when b.sale_amt is not null and a.business_type_name='BBC联营' then a.pay_amt*b.sale_amt_bbc_ly_rate 
		 when b.sale_amt is not null and a.business_type_name='BBC自营' then a.pay_amt*b.sale_amt_bbc_zy_rate 
	else a.pay_amt end pay_amt,	-- 核销金额	
	
	a.business_type_code,
	a.business_type_name,
	a.status,  -- 是否有效 0.无效 1.有效
	a.sale_amt,
	a.profit,
	a.sale_amt_jiushui,
	a.profit_jiushui,
	a.region_code,a.region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	

	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	-- a.unpay_amt,	-- 历史核销剩余金额
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu a
left join 
(
select source_bill_no,
sum(sale_amt) sale_amt,
sum(case when business_type_name='BBC联营' then sale_amt end) sale_amt_bbc_ly,
sum(case when business_type_name='BBC自营' then sale_amt end) sale_amt_bbc_zy,
sum(case when business_type_name='BBC联营' then sale_amt end)/sum(sale_amt) sale_amt_bbc_ly_rate,
sum(case when business_type_name='BBC自营' then sale_amt end)/sum(sale_amt) sale_amt_bbc_zy_rate
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu
where business_type_name like 'BBC%' 
group by source_bill_no
)b on a.source_bill_no=b.source_bill_no
left join 
		(
	select customer_code,smt_date as smonth,category_second
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%剔除客户%'		
	)e on a.customer_code=e.customer_code
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_first like '%调整回款时间系数%'		
	)f on a.customer_code=f.customer_code
-- 直送客户和项目供应商客户回款系数调整：110%调整为100% 安徽签呈，其他省区相同处理	
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,remark
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and (remark like '%直送客户%' or remark like '%项目供应商客户%')		
	)g on a.customer_code=g.customer_code	
where (a.sale_amt is not null 
or a.source_sys='BEGIN')
and e.category_second is null;	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初



-- 表5 csx_analyse_fr_tc_customer_person_profit_real_mi 大客户提成-客户人员毛利完成值--------------------------------------
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_person_profit_real_mi partition(smt)
select 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.customer_no) biz_id,	
	a.smt as smonth,
	b.performance_region_code,
	b.performance_region_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_city_code,
	b.performance_city_name,	
	a.customer_no as customer_code,
	b.customer_name,
	c.sales_id as sales_user_id,
	c.work_no as sales_user_number,
	c.sales_name as sales_user_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,		
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	-- 各类型销售额
	d.sale_amt,
	d.rp_sale_amt,
	(d.bbc_sale_amt_ly+d.bbc_sale_amt_zy) bbc_sale_amt,
	d.bbc_sale_amt_zy as bbc_zy_sale_amt,
	d.bbc_sale_amt_ly as bbc_ly_sale_amt,
	d.fl_sale_amt,
	-- 各类型定价毛利额
	-- 个人实际毛利额核算时福利及联营bbc业务按照1.2系数上浮
	(d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly*1.2+d.fl_profit*1.2) as profit,
	d.rp_profit,
	(d.bbc_profit_zy+d.bbc_profit_ly*1.2) as bbc_profit,
	d.bbc_profit_zy as bbc_zy_profit,
	d.bbc_profit_ly*1.2 as bbc_ly_profit,
	d.fl_profit*1.2 as fl_profit,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 	
from
-- 客户对应的销售员、客服经理	
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)a
left join  
	(
	-- 本月销售额毛利额 毛利目标达成用签呈后的
	select *
	from csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	-- 算基准毛利额达成系数时，剔除特定福建监狱客户
	and	customer_code not in('105150','105156','105164','105165','105177','105181','105182','106423','106721','107404','119990')
	and customer_code not in(
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second like '%不参与人员基准毛利额达成计算%'	
		)
	)d on a.customer_no=d.customer_code
left join 
	(
	select 
		distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		-- case when channel_code='9' then '业务代理' end as ywdl_cust,
		case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and customer_type_code=4
	)b on b.customer_code=a.customer_no
-- 关联对应各月销售员
join		
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)c on c.customer_no=a.customer_no	
where b.ng_cust is null
and d.sale_amt is not null;





-- 表6 csx_analyse_tc_person_profit_target_rate 大客户提成-人员毛利目标达成情况--------------------------------------

-- 目标毛利系数-客户月度毛利
drop table if exists csx_analyse_tmp.tmp_tc_cust_profit_month;
create temporary table csx_analyse_tmp.tmp_tc_cust_profit_month
as
select 
	-- b.performance_region_code,b.performance_region_name,
	-- b.performance_province_code,b.performance_province_name,
	-- b.performance_city_code,b.performance_city_name,
	a.customer_no as customer_code,b.customer_name,
	a.smt as smonth,
	c.sales_id,c.work_no,c.sales_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,		
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	-- 各类型销售额
	d.sale_amt,
	d.rp_sale_amt,
	(d.bbc_sale_amt_ly+d.bbc_sale_amt_zy) bbc_sale_amt,
	d.bbc_sale_amt_ly as bbc_ly_sale_amt,
	d.bbc_sale_amt_zy as bbc_zy_sale_amt,
	d.fl_sale_amt,
	-- 各类型定价毛利额
	-- 个人实际毛利额核算时福利及联营bbc业务按照1.2系数上浮
	(d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly*1.2+d.fl_profit*1.2) as profit,
	d.rp_profit,
	(d.bbc_profit_ly*1.2+d.bbc_profit_zy) as bbc_profit,
	d.bbc_profit_ly*1.2 as bbc_ly_profit,
	d.bbc_profit_zy as bbc_zy_profit,
	d.fl_profit*1.2 as fl_profit
from
-- 客户对应的销售员、客服经理	
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)a
left join  
	(
	-- 本月销售额毛利额 毛利目标达成用签呈后的
	select *
	from csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	-- 算基准毛利额达成系数时，剔除特定福建监狱客户
	and	customer_code not in('105150','105156','105164','105165','105177','105181','105182','106423','106721','107404','119990')
	and customer_code not in(
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second like '%不参与人员基准毛利额达成计算%'	
		)	
	)d on a.customer_no=d.customer_code
left join 
	(
	select 
		distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		-- case when channel_code='9' then '业务代理' end as ywdl_cust,
		case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and customer_type_code=4
	)b on b.customer_code=a.customer_no
-- 关联对应各月销售员
join		
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)c on c.customer_no=a.customer_no	
where b.ng_cust is null;
-- where b.ywdl_cust is null and b.ng_cust is null;


-- 毛利额汇总-销售员与客服经理：未处理多管家情况-顿号隔开
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_total
as
select 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	sum(a.sale_amt) as sale_amt, -- 客户总销售额
	sum(a.profit) as profit-- 客户总定价毛利额
from 
(
	select 
		smonth,customer_code,customer_name,
		sales_id,work_no,sales_name,
		sale_amt,profit
	from csx_analyse_tmp.tmp_tc_cust_profit_month
	where work_no<>''
	union all
	select 
		smonth,customer_code,customer_name,
		rp_service_user_id as sales_id,
		rp_service_user_work_no as work_no,
		rp_service_user_name as sales_name,
		rp_sale_amt as sale_amt,
		rp_profit as profit
	from csx_analyse_tmp.tmp_tc_cust_profit_month
	where rp_service_user_work_no<>''
	
	union all
	select 
		smonth,customer_code,customer_name,
		fl_service_user_id as sales_id,
		fl_service_user_work_no as work_no,
		fl_service_user_name as sales_name,
		fl_sale_amt as sale_amt,
		fl_profit as profit
	from csx_analyse_tmp.tmp_tc_cust_profit_month
	where fl_service_user_work_no<>''
	
	union all
	select 
		smonth,customer_code,customer_name,
		bbc_service_user_id as sales_id,
		bbc_service_user_work_no as work_no,
		bbc_service_user_name as sales_name,
		bbc_sale_amt as sale_amt,
		bbc_profit as profit
	from csx_analyse_tmp.tmp_tc_cust_profit_month
	where bbc_service_user_work_no<>''
)a
group by 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name;
	

-- 毛利额汇总-销售员与客服经理：多管家-拆分到单人
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total_split;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_total_split
as
select
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	sum(a.sale_amt/a.count_person) sale_amt, -- 客户总销售额
	sum(a.profit/a.count_person) profit,-- 客户总定价毛利额
	d.profit_basic,
	e.begin_date,
	e.begin_less_1year_flag,
	round(sum(a.profit/a.count_person)/d.profit_basic,6) as profit_target_rate		
from
(		
select size(split(sales_id,'、')) as count_person,
	smonth,
	sales_id,work_no,sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))=1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[0] sales_id,
	split(work_no,'、')[0] work_no,
	split(sales_name,'、')[0] sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[1] sales_id,
	split(work_no,'、')[1] work_no,
	split(sales_name,'、')[1] sales_name,		
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[2] sales_id,
	split(work_no,'、')[2] work_no,
	split(sales_name,'、')[2] sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[3] sales_id,
	split(work_no,'、')[3] work_no,
	split(sales_name,'、')[3] sales_name,		
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1
)a 
left join 
	(	select *
		from csx_analyse.csx_analyse_tc_sales_service_profit_basic_mf
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_c=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	)	d on d.work_no=a.work_no 
		and d.smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	left join 
	(
	select employee_code,employee_status,begin_date,
	case when begin_date>=regexp_replace(trunc(add_months('${sdt_yes_date}',-12),'MM'),'-','') then '是' else '否' end as begin_less_1year_flag
	from csx_dim.csx_dim_basic_employee
	where sdt='current'
	-- and employee_status=0
	and card_type='0'
	) e on a.work_no=e.employee_code		
where coalesce(sales_id,'')<>''
group by 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	d.profit_basic,
	e.begin_date,
	e.begin_less_1year_flag	
;

-- 创建人员信息表，获取销售员和客服经理的城市，因为存在一个业务员名下客户跨城市的情况
drop table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info;
create temporary table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info
as
select distinct a.user_id,a.user_number,a.user_name,
	b.performance_city_code,
	b.performance_city_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_region_code,
	b.performance_region_name
from
	(
	select 	user_id,user_number,user_name,user_position,city_name,province_name
	from csx_dim.csx_dim_uc_user
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	-- and status = 0 
	and delete_flag = '0'
	) a
	left join -- 区域表
	( 
	select distinct
		city_code,city_name,
		province_code,province_name,
		performance_city_code,
		performance_city_name,
		performance_province_code,
		performance_province_name,
		performance_region_code,
		performance_region_name
	from csx_dim.csx_dim_sales_area_belong_mapping
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	) b on b.city_name=a.city_name and b.province_name=a.province_name;
	
	
	
-- 大客户提成-人员毛利目标达成情况
insert overwrite table csx_analyse.csx_analyse_tc_person_profit_target_rate partition(smt)
select
	concat_ws('-',a.smonth,b.performance_province_code,a.sales_id) biz_id,	
	b.performance_region_code as region_code,
	b.performance_region_name as region_name,
	b.performance_province_code as province_code,
	b.performance_province_name as province_name,
	b.performance_city_code as city_group_code,
	b.performance_city_name as city_group_name,	
	case when length(c.sales_id)>0 then '销售员' else '服务管家' end as user_position,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.begin_date,
	a.begin_less_1year_flag,	
	a.sale_amt, -- 总销售额
	a.profit,-- 总定价毛利额
	a.profit_basic,
	a.profit_target_rate,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	a.smonth as smt_ct,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 	
from csx_analyse_tmp.tmp_tc_person_profit_total_split a 
left join csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info b on a.sales_id=b.user_id
left join 
(
	select distinct sales_id
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
)c on a.sales_id=c.sales_id
where length(a.sales_id)>0
and (a.sale_amt is not null or a.profit_basic is not null);



-- 表7 csx_analyse_fr_tc_customer_bill_month_dff_rate_detail 大客户提成-客户结算月回款时间系数明细--------------------------------------

set hive.tez.container.size=8192;

-- 目标毛利系数-客户月度毛利
drop table if exists csx_analyse_tmp.tmp_tc_cust_profit_month;
create temporary table csx_analyse_tmp.tmp_tc_cust_profit_month
as
select 
	-- b.performance_region_code,b.performance_region_name,
	-- b.performance_province_code,b.performance_province_name,
	-- b.performance_city_code,b.performance_city_name,
	a.customer_no as customer_code,b.customer_name,
	a.smt as smonth,
	c.sales_id,c.work_no,c.sales_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,		
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	-- 各类型销售额
	d.sale_amt,
	d.rp_sale_amt,
	(d.bbc_sale_amt_ly+d.bbc_sale_amt_zy) bbc_sale_amt,
	d.bbc_sale_amt_ly as bbc_ly_sale_amt,
	d.bbc_sale_amt_zy as bbc_zy_sale_amt,
	d.fl_sale_amt,
	-- 各类型定价毛利额
	-- 个人实际毛利额核算时福利及联营bbc业务按照1.2系数上浮
	(d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly*1.2+d.fl_profit*1.2) as profit,
	d.rp_profit,
	(d.bbc_profit_ly*1.2+d.bbc_profit_zy) as bbc_profit,
	d.bbc_profit_ly*1.2 as bbc_ly_profit,
	d.bbc_profit_zy as bbc_zy_profit,
	d.fl_profit*1.2 as fl_profit
from
-- 客户对应的销售员、客服经理	
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)a
left join  
	(
	-- 本月销售额毛利额 毛利目标达成用签呈后的
	select *
	from csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	-- 算基准毛利额达成系数时，剔除特定福建监狱客户
	and	customer_code not in('105150','105156','105164','105165','105177','105181','105182','106423','106721','107404','119990')
	and customer_code not in(
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second like '%不参与人员基准毛利额达成计算%'	
		)	
	)d on a.customer_no=d.customer_code
left join 
	(
	select 
		distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		-- case when channel_code='9' then '业务代理' end as ywdl_cust,
		case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and customer_type_code=4
	)b on b.customer_code=a.customer_no
-- 关联对应各月销售员
join		
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)c on c.customer_no=a.customer_no	
where b.ng_cust is null;
-- where b.ywdl_cust is null and b.ng_cust is null;


-- 毛利额汇总-销售员与客服经理：未处理多管家情况-顿号隔开
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_total
as
select 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	sum(a.sale_amt) as sale_amt, -- 客户总销售额
	sum(a.profit) as profit-- 客户总定价毛利额
from 
(
	select 
		smonth,customer_code,customer_name,
		sales_id,work_no,sales_name,
		sale_amt,profit
	from csx_analyse_tmp.tmp_tc_cust_profit_month
	where work_no<>''
	union all
	select 
		smonth,customer_code,customer_name,
		rp_service_user_id as sales_id,
		rp_service_user_work_no as work_no,
		rp_service_user_name as sales_name,
		rp_sale_amt as sale_amt,
		rp_profit as profit
	from csx_analyse_tmp.tmp_tc_cust_profit_month
	where rp_service_user_work_no<>''
	
	union all
	select 
		smonth,customer_code,customer_name,
		fl_service_user_id as sales_id,
		fl_service_user_work_no as work_no,
		fl_service_user_name as sales_name,
		fl_sale_amt as sale_amt,
		fl_profit as profit
	from csx_analyse_tmp.tmp_tc_cust_profit_month
	where fl_service_user_work_no<>''
	
	union all
	select 
		smonth,customer_code,customer_name,
		bbc_service_user_id as sales_id,
		bbc_service_user_work_no as work_no,
		bbc_service_user_name as sales_name,
		bbc_sale_amt as sale_amt,
		bbc_profit as profit
	from csx_analyse_tmp.tmp_tc_cust_profit_month
	where bbc_service_user_work_no<>''
)a
group by 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name;
	

-- 毛利额汇总-销售员与客服经理：多管家-拆分到单人
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total_split;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_total_split
as
select
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	sum(a.sale_amt/a.count_person) sale_amt, -- 客户总销售额
	sum(a.profit/a.count_person) profit,-- 客户总定价毛利额
	d.profit_basic,
	e.begin_date,
	e.begin_less_1year_flag,
	round(sum(a.profit/a.count_person)/d.profit_basic,6) as profit_target_rate		
from
(		
select size(split(sales_id,'、')) as count_person,
	smonth,
	sales_id,work_no,sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))=1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[0] sales_id,
	split(work_no,'、')[0] work_no,
	split(sales_name,'、')[0] sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[1] sales_id,
	split(work_no,'、')[1] work_no,
	split(sales_name,'、')[1] sales_name,		
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[2] sales_id,
	split(work_no,'、')[2] work_no,
	split(sales_name,'、')[2] sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[3] sales_id,
	split(work_no,'、')[3] work_no,
	split(sales_name,'、')[3] sales_name,		
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

-- 手工调整：调整业务员基准毛利额
-- 202402签呈：124484 业务代理人毛利额归属到於佳作为考核其毛利额是否达标的标准：28052.88
-- 202402签呈：128865 业务代理人毛利额归属到於佳作为考核其毛利额是否达标的标准：67249.86
-- 202402签呈：128865 业务代理人毛利额归属到於佳作为考核其毛利额是否达标的标准：-220.5
-- 202402签呈：128336业务代理人毛利额归属到刘斌作为考核其毛利额是否达标的标准：12588.17

union all
select 1 as count_person,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smonth,
	'1000000211077' as  sales_id,
	'80946479' as work_no,
	'於佳' as sales_name,		
	0 sale_amt, -- 客户总销售额
	if(substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)='202402',95082.24,0) as profit-- 客户总定价毛利额
	
union all
select 1 as count_person,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smonth,
	'1000000575185' as  sales_id,
	'81160930' as work_no,
	'刘斌' as sales_name,		
	0 sale_amt, -- 客户总销售额
	if(substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)='202402',12588.17,0) as profit-- 客户总定价毛利额	
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1
)a 
left join 
	(	select *
		from csx_analyse.csx_analyse_tc_sales_service_profit_basic_mf
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_c=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	)	d on d.work_no=a.work_no 
		and d.smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	left join 
	(
	select employee_code,employee_status,begin_date,
	case when begin_date>=regexp_replace(trunc(add_months('${sdt_yes_date}',-12),'MM'),'-','') then '是' else '否' end as begin_less_1year_flag
	from csx_dim.csx_dim_basic_employee
	where sdt='current'
	-- and employee_status=0
	and card_type='0'
	) e on a.work_no=e.employee_code		
where coalesce(sales_id,'')<>''
group by 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	d.profit_basic,
	e.begin_date,
	e.begin_less_1year_flag	
;

-- 创建人员信息表，获取销售员和客服经理的城市，因为存在一个业务员名下客户跨城市的情况
drop table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info;
create temporary table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info
as
select distinct a.user_id,a.user_number,a.user_name,
	b.performance_city_code,
	b.performance_city_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_region_code,
	b.performance_region_name
from
	(
	select 	user_id,user_number,user_name,user_position,city_name,province_name
	from csx_dim.csx_dim_uc_user
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	-- and status = 0 
	and delete_flag = '0'
	) a
	left join -- 区域表
	( 
	select distinct
		city_code,city_name,
		province_code,province_name,
		performance_city_code,
		performance_city_name,
		performance_province_code,
		performance_province_name,
		performance_region_code,
		performance_region_name
	from csx_dim.csx_dim_sales_area_belong_mapping
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	) b on b.city_name=a.city_name and b.province_name=a.province_name;
	
	
	
-- 目标毛利系数-销售员与客服经理
-- 过度期内入职时间超过1年的个人基准毛利率低于100%的按照实际率核算; 湖北8月过渡期，9月生效；其他省区都是8-10月过渡期，11月生效
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc
as
-- 手工调整：调整业务员基准毛利率达成
-- 202310签呈：销售员詹成宏81147160不考核毛利额达成率 每月
-- 202311签呈：81131450 张珠妹、81133021 黄升、80952742 王秀云、80010438 林秀丽 不考核毛利额达成率 每月

-- 202311签呈：81090419 葛立新   2023年8月1日从湖北调动至合肥，申请保护期一年，2023年8月1日至2024年7月31日
-- 202311签呈：81014012 张鲲     2023年11月毛利额系数100%
-- 202311签呈：81048704 关毓萱   2023年11月毛利额系数100%
-- 202311签呈：80892167 李烁岚   2023年11-12月毛利额系数100%

-- 202312签呈："服务管家冯子玲（81089289）于 2023 年 11 月从运营部转岗到服务管家团队，按实际达成发放员工提成 2023.11-2024.10"
-- 202312签呈：81022821 杨程销售岗不考核基准毛利率
-- 202312签呈：80880757 张标华销售岗不考核基准毛利率
-- 202312签呈：81190209 张敏销售岗不考核基准毛利率
-- 202312签呈：80897025 耿海娜销售岗不考核基准毛利率
-- 202312签呈：81214655 贾莉莉服务管家不考核基准毛利率
-- 202312签呈：81125195 常文棋服务管家不考核基准毛利率

-- 202402签呈：服务管家周凤云（工号：80894299）为省区应收对账专员转岗到服务管家团队人员，转岗时间为：2023年9月1日，过渡期间未达成毛利额指标的按照实际达成率进行提成核算，现申请予以执行，2024年9月1日之后如果未达成，按照方案执行
-- 202402签呈：服务管家陈燕红（工号：80981355），2023年9月1日转岗为销售员,自2023年9月1日至 2024年8月31日期间陈燕红个人基准毛利率达成低于 100%的按照实际达成结果作为达成系数计算其月度提成
-- 202402签呈：BP岗李芬娟（工号：80948173），2023年11月1日起转岗为服务管家,自2023年11月至 2024年10月个人基准毛利率达成低于 100%的按照实际达成系数计算其月度提成
-- 202402签呈：产品运营张楠林（工号：81225749），2024年1月1日起转岗为服务管家,自2024年1月至 2024年12月个人基准毛利率达成低于 100%的按照实际达成系数计算其月度提成

select *,
	case 
		when a.work_no in('81147160','81131450','81133021','80952742','80010438') then 1
		when a.work_no in('81022821','80880757','81190209','80897025','81214655','81125195') then 1
		when a.work_no in('81090419') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)<='202407' then if(profit_target_rate<1,profit_target_rate,1)
		when a.work_no in('81014012','81048704') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)='202311' then 1
		when a.work_no in('80892167') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) in('202311','202312') then 1
		when a.work_no in('81089289') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) between '202311' and '202410' then if(profit_target_rate<1,profit_target_rate,1)

		when a.work_no in('80894299','80981355') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) between '202309' and '202408' then if(profit_target_rate<1,profit_target_rate,1)
		when a.work_no in('80948173') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) between '202311' and '202410' then if(profit_target_rate<1,profit_target_rate,1)
		when a.work_no in('81225749') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) between '202401' and '202412' then if(profit_target_rate<1,profit_target_rate,1)
		
		when b.performance_province_name in('湖北省') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)<='202308' then if(profit_target_rate<1,profit_target_rate,1)
		when b.performance_province_name not in('湖北省') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)<='202310' then if(profit_target_rate<1,profit_target_rate,1)
	else (case 	
		when coalesce(begin_less_1year_flag,'否')='是' and profit_target_rate<1 then profit_target_rate
		when coalesce(begin_less_1year_flag,'否')='是' and profit_target_rate>=1 then 1
		when coalesce(begin_less_1year_flag,'否')='否' and profit_target_rate<1 then 0
		when coalesce(begin_less_1year_flag,'否')='否' and profit_target_rate>=1 then 1
		else 1 end) end as profit_target_rate_tc	
	-- coalesce(if(profit_target_rate>=1,1,profit_target_rate),1) as profit_target_rate_tc			
from 
(
	select
		a.smonth,
		a.sales_id,a.work_no,a.sales_name,
		-- 多管家毛利额达成排序中的最大值，就高原则
		case when arr[3]=profit_target_rate_1 then begin_date_1
			 when arr[3]=profit_target_rate_2 then begin_date_2
			 when arr[3]=profit_target_rate_3 then begin_date_3
			 when arr[3]=profit_target_rate_4 then begin_date_4
			 when arr[3] is null then begin_date_1
			 end as begin_date,	
		case when arr[3]=profit_target_rate_1 then begin_less_1year_flag_1
			 when arr[3]=profit_target_rate_2 then begin_less_1year_flag_2
			 when arr[3]=profit_target_rate_3 then begin_less_1year_flag_3
			 when arr[3]=profit_target_rate_4 then begin_less_1year_flag_4
			 when arr[3] is null then begin_less_1year_flag_1
			 end as begin_less_1year_flag,	
		
		case when arr[3]=profit_target_rate_1 then profit_basic_1
			 when arr[3]=profit_target_rate_2 then profit_basic_2
			 when arr[3]=profit_target_rate_3 then profit_basic_3
			 when arr[3]=profit_target_rate_4 then profit_basic_4
			 -- 如果没有业绩毛利，则取第一项毛利目标
			 when arr[3] is null then profit_basic_1			 
			 end as profit_basic,
		case when arr[3]=profit_target_rate_1 then profit_1
			 when arr[3]=profit_target_rate_2 then profit_2
			 when arr[3]=profit_target_rate_3 then profit_3
			 when arr[3]=profit_target_rate_4 then profit_4
			 when arr[3] is null then 0
			 end as profit,
		case when arr[3]=profit_target_rate_1 then profit_target_rate_1
			 when arr[3]=profit_target_rate_2 then profit_target_rate_2
			 when arr[3]=profit_target_rate_3 then profit_target_rate_3
			 when arr[3]=profit_target_rate_4 then profit_target_rate_4
			 when arr[3] is null then 0
			 end as profit_target_rate
	from 
	(
	select 
		a.smonth,
		a.sales_id,a.work_no,a.sales_name,
		a.sale_amt,
		a.profit,		
		b1.sale_amt as sale_amt_1,
		b1.profit as profit_1,
		b1.profit_basic as profit_basic_1,
		b1.profit_target_rate as profit_target_rate_1,
		b1.begin_date as begin_date_1,
		b1.begin_less_1year_flag as begin_less_1year_flag_1,
	
		b2.sale_amt as sale_amt_2,
		b2.profit as profit_2,
		b2.profit_basic as profit_basic_2,
		b2.profit_target_rate as profit_target_rate_2,	
		b2.begin_date as begin_date_2,
		b2.begin_less_1year_flag as begin_less_1year_flag_2,
		
		b3.sale_amt as sale_amt_3,
		b3.profit as profit_3,
		b3.profit_basic as profit_basic_3,
		b3.profit_target_rate as profit_target_rate_3,	
		b3.begin_date as begin_date_3,
		b3.begin_less_1year_flag as begin_less_1year_flag_3,
		
		b4.sale_amt as sale_amt_4,
		b4.profit as profit_4,
		b4.profit_basic as profit_basic_4,
		b4.profit_target_rate as profit_target_rate_4,	
		b4.begin_date as begin_date_4,
		b4.begin_less_1year_flag as begin_less_1year_flag_4,	
		-- 多个管家的毛利额达成排序
		sort_array(array(b1.profit_target_rate,b2.profit_target_rate,b3.profit_target_rate,b4.profit_target_rate)) as arr
	from csx_analyse_tmp.tmp_tc_person_profit_total a 
	left join csx_analyse_tmp.tmp_tc_person_profit_total_split b1 on split(a.sales_id,'、')[0]=b1.sales_id
	left join csx_analyse_tmp.tmp_tc_person_profit_total_split b2 on split(a.sales_id,'、')[1]=b2.sales_id
	left join csx_analyse_tmp.tmp_tc_person_profit_total_split b3 on split(a.sales_id,'、')[2]=b3.sales_id
	left join csx_analyse_tmp.tmp_tc_person_profit_total_split b4 on split(a.sales_id,'、')[3]=b4.sales_id
	)a
)a
left join csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info b on split(a.sales_id,'、')[0] =b.user_id;




-- 销售月金额与毛利率
drop table if exists csx_analyse_tmp.tmp_tc_customer_sale_profit_ls;
create temporary table csx_analyse_tmp.tmp_tc_customer_sale_profit_ls
as
select 
customer_code,smonth,
	-- 销售额
	sale_amt, -- 客户总销售额
	rp_sale_amt, -- 客户日配销售额		
	-- sum(bbc_sale_amt) as bbc_sale_amt, -- 客户bbc销售额
	bbc_sale_amt_zy, -- 客户bbc自营销售额
	bbc_sale_amt_ly, -- 客户bbc联营销售额
	fl_sale_amt, -- 客户福利销售额
	-- 定价毛利额
	profit,-- 客户总定价毛利额
	rp_profit,-- 客户日配定价毛利额
	-- sum(bbc_profit) as bbc_profit,-- 客户bbc定价毛利额
	bbc_profit_zy, -- 客户bbc自营定价毛利额
	bbc_profit_ly, -- 客户bbc联营定价毛利额
	fl_profit,  -- 客户福利定价毛利额
	-- 定价毛利率
	profit/abs(sale_amt) as prorate,-- 客户总定价毛利率
	rp_profit/abs(rp_sale_amt) as rp_prorate,-- 客户日配定价毛利率
	bbc_profit_zy/abs(bbc_sale_amt_zy) as bbc_prorate_zy, -- 客户bbc自营定价毛利率
	bbc_profit_ly/abs(bbc_sale_amt_ly) as bbc_prorate_ly, -- 客户bbc联营定价毛利率
	fl_profit/abs(fl_sale_amt) as fl_prorate  -- 客户福利定价毛利率	
from 
(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
		-- sum(case when business_type_code in('6') then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then sale_amt else 0 end) as bbc_sale_amt_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then sale_amt else 0 end) as bbc_sale_amt_ly,
		sum(case when business_type_code in('2') then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit, 
		sum(case when business_type_code in ('1','4','5') then profit else 0 end) as rp_profit,
		-- sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then profit else 0 end) as bbc_profit_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then profit else 0 end) as bbc_profit_ly,		
		sum(case when business_type_code in('2') then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='20220601' 
		and sdt<regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and channel_code in('1','7','9')
		and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
		and (business_type_code in('1','2','6')
			or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
			-- 福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
			-- 福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
			-- 202210签呈 北京 129026 129000 城市服务商业务销售额*0.2% 不计算毛利提成 每月
			-- 202306签呈 福建省'229290','175709','125092' 项目供应商 纳入大客户提成计算
			or (business_type_code in ('4') and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))
			-- 202310签呈 河北南京部分项目供应商客户纳入大客户提成计算
			-- or (business_type_code in ('4') and customer_code in ('235949','222853','131428','131466','131202','131208','129746','128435','230788',
			-- '112846','118357','115832','125795','125831','131462','114496','117322','131421','118395','114470','130024','130430','118644','131091',
			-- '217946','129955','130226','120115','226821','129870','129865','130269','126125','129674','129880','227563','129855','129860','130955',
			-- '127521','225541','232102','233354','234828','130844','223112','129854','125545','128705','125513','126001'))
			or (business_type_code in ('4') and customer_code in (
				select customer_code
				from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
				where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and category_second like '%纳入大客户提成计算：项目供应商%'
			))
			or customer_code in (
				select customer_code
				from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
				where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and category_second='纳入大客户提成计算'
			)				
			)		
		-- and performance_province_name in ('福建省')
		and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断					
	group by customer_code,substr(sdt,1,6)
)a ;





-- 签呈处理：部分订单按实际回款时间系数计算（给预付款标签）
drop table if exists csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2;
create temporary table csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2
as
select a.*,
	case 
		 -- 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e.date_star and e.date_end) and e.category_second is not null 
		 and (e.adjust_business_type=substr(a.business_type_name,1,2) or e.adjust_business_type='全业务') then '是' 
		 -- 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f.date_star and f.date_end) and f.category_second is not null 
		 and (f.adjust_business_type=substr(a.business_type_name,1,2) or f.adjust_business_type='全业务') then '是' 
		 
		 else '否' end yufu_flag	
from
(
	select *
	from csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and status=1
)a
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按实际回款时间系数-发生日期%'		
	)e on a.customer_code=e.customer_code 
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按实际回款时间系数-打款日期%'		
	)f on a.customer_code=f.customer_code	
;	
	

	

-- 客户+结算月+回款时间系数：各业务类型毛利率提成比例
drop table if exists csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc;
create temporary table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc
as
select a.*,

	coalesce(e.rp_rate,
	case when rp_profit_rate<0.08 then 0.002
		when rp_profit_rate>=0.08 and rp_profit_rate<0.12 then 0.005
		when rp_profit_rate>=0.12 and rp_profit_rate<0.16 then 0.007
		when rp_profit_rate>=0.16 and rp_profit_rate<0.2 then 0.009
		when rp_profit_rate>=0.2 and rp_profit_rate<0.25 then 0.013
		when rp_profit_rate>=0.25 then 0.015
		else 0.002 end) as cust_rp_profit_rate_tc,

	coalesce(e.bbc_rate_zy,
	case when bbc_zy_profit_rate<0.08 then 0.002
		when bbc_zy_profit_rate>=0.08 and bbc_zy_profit_rate<0.12 then 0.005
		when bbc_zy_profit_rate>=0.12 and bbc_zy_profit_rate<0.16 then 0.007
		when bbc_zy_profit_rate>=0.16 and bbc_zy_profit_rate<0.2 then 0.009
		when bbc_zy_profit_rate>=0.2 and bbc_zy_profit_rate<0.25 then 0.013
		when bbc_zy_profit_rate>=0.25 then 0.015
		else 0.002 end) as cust_bbc_zy_profit_rate_tc,		
		
	coalesce(e.bbc_rate_ly,
	case when bbc_ly_profit_rate<0.03 then 0.002
		when bbc_ly_profit_rate>=0.03 and bbc_ly_profit_rate<0.07 then 0.0035
		when bbc_ly_profit_rate>=0.07 and bbc_ly_profit_rate<0.1 then 0.0045
		when bbc_ly_profit_rate>=0.1 and bbc_ly_profit_rate<0.13 then 0.0065
		when bbc_ly_profit_rate>=0.13 and bbc_ly_profit_rate<0.17 then 0.0095
		when bbc_ly_profit_rate>=0.17 and bbc_ly_profit_rate<0.23 then 0.013
		when bbc_ly_profit_rate>=0.23 then 0.015
		else 0.002 end) as cust_bbc_ly_profit_rate_tc,

	coalesce(e.fl_rate,
	case when fl_profit_rate<0.03 then 0.002
		when fl_profit_rate>=0.03 and fl_profit_rate<0.07 then 0.0035
		when fl_profit_rate>=0.07 and fl_profit_rate<0.1 then 0.0045
		when fl_profit_rate>=0.1 and fl_profit_rate<0.13 then 0.0065
		when fl_profit_rate>=0.13 and fl_profit_rate<0.17 then 0.0095
		when fl_profit_rate>=0.17 and fl_profit_rate<0.23 then 0.013
		when fl_profit_rate>=0.23 then 0.015
		else 0.002 end) as cust_fl_profit_rate_tc		

from 
(
select a.*,
	-- profit/abs(sale_amt) as profit_rate,
	-- rp_profit/abs(rp_sale_amt) as rp_profit_rate,
	-- bbc_profit/abs(bbc_sale_amt) as bbc_profit_rate,
	-- bbc_ly_profit/abs(bbc_ly_sale_amt) as bbc_ly_profit_rate,
	-- bbc_zy_profit/abs(bbc_zy_sale_amt) as bbc_zy_profit_rate,
	-- fl_profit/abs(fl_sale_amt) as fl_profit_rate
	if(b.sale_amt is not null,b.prorate,b2.prorate) as profit_rate,
	if(b.sale_amt is not null,b.rp_prorate,b2.rp_prorate) as rp_profit_rate,
	if(b.sale_amt is not null,(b.bbc_profit_zy+b.bbc_profit_ly)/abs(b.bbc_sale_amt_zy+b.bbc_sale_amt_ly),
		(b2.bbc_profit_zy+b2.bbc_profit_ly)/abs(b2.bbc_sale_amt_zy+b2.bbc_sale_amt_ly))as bbc_profit_rate,
	if(b.sale_amt is not null,b.bbc_prorate_zy,b2.bbc_prorate_zy) as bbc_zy_profit_rate,
	if(b.sale_amt is not null,b.bbc_prorate_ly,b2.bbc_prorate_ly) as bbc_ly_profit_rate,	
	if(b.sale_amt is not null,b.fl_prorate,b2.fl_prorate) as fl_profit_rate,

	-- 历史月销售额
	if(b.sale_amt is not null,b.sale_amt,b2.sale_amt) as sale_amt_real,
	if(b.sale_amt is not null,b.rp_sale_amt,b2.rp_sale_amt) as rp_sale_amt_real,
	if(b.sale_amt is not null,b.bbc_sale_amt_zy,b2.bbc_sale_amt_zy) as bbc_sale_amt_zy_real,
	if(b.sale_amt is not null,b.bbc_sale_amt_ly,b2.bbc_sale_amt_ly) as bbc_sale_amt_ly_real,
	if(b.sale_amt is not null,b.fl_sale_amt,b2.fl_sale_amt) as fl_sale_amt_real,	
	
	-- 服务费
	-- b.service_falg,
	-- b.service_fee,
	
	if(a.customer_code in('127543','127592','127728','127729','127737','127743','127745'),'',
			if(a.province_name='安徽' and a.happen_month<='202308',d.service_falg,b.service_falg)) as service_falg,
	if(a.customer_code in('127543','127592','127728','127729','127737','127743','127745'),null,
			if(a.province_name='安徽' and a.happen_month<='202308',d.service_fee,b.service_fee)) as service_fee,
	-- 本月销售额毛利额
	c.sale_amt as by_sale_amt,
	c.rp_sale_amt as by_rp_sale_amt,
	c.bbc_sale_amt_zy as by_bbc_sale_amt_zy,
	c.bbc_sale_amt_ly as by_bbc_sale_amt_ly,
	c.fl_sale_amt as by_fl_sale_amt,
	c.profit as by_profit,
	c.rp_profit as by_rp_profit,
	c.bbc_profit_zy as by_bbc_profit_zy,
	c.bbc_profit_ly as by_bbc_profit_ly,
	c.fl_profit as by_fl_profit
	
from 
(
	select
		region_code,
		region_name,
		province_code,
		province_name,
		city_group_code,
		city_group_name,
		customer_code,	-- 客户编码
		customer_name,
		credit_code,	-- 信控号	
		company_code,	-- 签约公司编码
		account_period_code,	-- 账期编码
		account_period_name,	-- 账期名称		
		sales_id,
		work_no,
		sales_name,
		rp_service_user_id,
		rp_service_user_work_no,
		rp_service_user_name,
		fl_service_user_id,
		fl_service_user_work_no,
		fl_service_user_name,		
		bbc_service_user_id,
		bbc_service_user_work_no,
		bbc_service_user_name,
		-- 提成分配系数
		rp_sales_fp_rate,
		fl_sales_sale_fp_rate as fl_sales_fp_rate,
		bbc_sales_sale_fp_rate as bbc_sales_fp_rate,
		rp_service_user_fp_rate,
		fl_service_user_fp_rate,
		bbc_service_user_fp_rate,	
		substr(regexp_replace(bill_date,'-',''),1,6) as bill_month, -- 结算月
		bill_date,  -- 结算日期
		paid_date,  -- 核销日期（打款日期）
		yufu_flag,
		-- '否' as yufu_flag,
		-- if(paid_date<happen_date,'是','否') as yufu_flag,
		substr(regexp_replace(happen_date,'-',''),1,6) as happen_month, -- 销售月		
		-- 202308签呈 126275 将销售日期为6.15-8.15期间的BBC，结算日调整为8.16，且最高回款系数100%
		case when customer_code='126275' and dff_rate>1 then 1 
			else dff_rate end as dff_rate,  -- 回款时间系数
		sum(pay_amt) pay_amt,	-- 核销金额
		sum(case when business_type_code in (1,4,5) then pay_amt else 0 end) as rp_pay_amt,
		sum(case when business_type_name like 'BBC%' then pay_amt else 0 end) as bbc_pay_amt,
		sum(case when business_type_name='BBC联营' then pay_amt else 0 end) as bbc_ly_pay_amt,
		sum(case when business_type_name='BBC自营' then pay_amt else 0 end) as bbc_zy_pay_amt,
		sum(case when business_type_code in(2) then pay_amt else 0 end) as fl_pay_amt,
		
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in (1,4,5) then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_name like 'BBC%' then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_name='BBC联营' then sale_amt else 0 end) as bbc_ly_sale_amt,
		sum(case when business_type_name='BBC自营' then sale_amt else 0 end) as bbc_zy_sale_amt,
		sum(case when business_type_code in(2) then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit,
		sum(case when business_type_code in (1,4,5) then profit else 0 end) as rp_profit,
		sum(case when business_type_name like 'BBC%' then profit else 0 end) as bbc_profit,
		sum(case when business_type_name='BBC联营' then profit else 0 end) as bbc_ly_profit,
		sum(case when business_type_name='BBC自营' then profit else 0 end) as bbc_zy_profit,
		sum(case when business_type_code in(2) then profit else 0 end) as fl_profit	
	from csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2
	group by 	region_code,
		region_name,
		province_code,
		province_name,
		city_group_code,
		city_group_name,
		customer_code,	-- 客户编码
		customer_name,
		credit_code,	-- 信控号	
		company_code,	-- 签约公司编码
		account_period_code,	-- 账期编码
		account_period_name,	-- 账期名称			
		sales_id,
		work_no,
		sales_name,
		rp_service_user_id,
		rp_service_user_work_no,
		rp_service_user_name,
		fl_service_user_id,
		fl_service_user_work_no,
		fl_service_user_name,		
		bbc_service_user_id,
		bbc_service_user_work_no,
		bbc_service_user_name,
		-- 提成分配系数
		rp_sales_fp_rate,
		fl_sales_sale_fp_rate,
		bbc_sales_sale_fp_rate,
		rp_service_user_fp_rate,
		fl_service_user_fp_rate,
		bbc_service_user_fp_rate,
		substr(regexp_replace(bill_date,'-',''),1,6), -- 结算月
		bill_date,	
		paid_date,
		yufu_flag,
		-- if(paid_date<happen_date,'是','否'),
		substr(regexp_replace(happen_date,'-',''),1,6),  -- 销售月
		case when customer_code='126275' and dff_rate>1 then 1 else dff_rate end
)a	
left join csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business b on a.customer_code=b.customer_code and a.happen_month=b.smonth
left join csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business c on a.customer_code=c.customer_code and c.smonth=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
-- 因某客户可能后期纳入大客户提成计算，无历史处理签呈后的毛利率，若没有历史月的毛利率 则取最新计算的历史月毛利率
left join csx_analyse_tmp.tmp_tc_customer_sale_profit_ls b2 on a.customer_code=b2.customer_code and a.happen_month=b2.smonth
-- 安徽历史月份服务费按照当月处理历史月
	left join 
		(
	select customer_code,smt_date as smonth,category_second as service_falg,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt='202308'
	and smt_date='202308'
	-- where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	-- and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%服务费%'
	and customer_code not in('127543','127592','127728','127729','127737','127743','127745')
	)d on a.customer_code=d.customer_code
)a	
left join
	(
	select smt_date,customer_code,category_second,
		max(case when adjust_business_type in('日配','全业务') then back_amt_tc_rate end) as rp_rate,
		max(case when adjust_business_type in('BBC','BBC自营','全业务') then back_amt_tc_rate end) as bbc_rate_zy,
		max(case when adjust_business_type in('BBC','BBC联营','全业务') then back_amt_tc_rate end) as bbc_rate_ly,
		max(case when adjust_business_type in('福利','全业务') then back_amt_tc_rate end) as fl_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%调整提成比例%'
	group by smt_date,customer_code,category_second
	) e on e.customer_code=a.customer_code
;	



drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0;
create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0
as
select a.*,
	if(abs(a.pay_amt)<abs(a.sale_amt),a.pay_amt,a.sale_amt) as pay_amt_use,
	-- 目标毛利系数-销售员与客服经理
	d1.profit_basic as sales_profit_basic,
	d1.profit as sales_profit_finish,
	d1.profit_target_rate as sales_target_rate,
	d1.profit_target_rate_tc as sales_target_rate_tc,
	
	d2.profit_basic as rp_service_profit_basic,
	d2.profit as rp_service_profit_finish,
	d2.profit_target_rate as rp_service_target_rate,
	d2.profit_target_rate_tc as rp_service_target_rate_tc,

	d3.profit_basic as fl_service_profit_basic,
	d3.profit as fl_service_profit_finish,
	d3.profit_target_rate as fl_service_target_rate,
	d3.profit_target_rate_tc as fl_service_target_rate_tc,

	d4.profit_basic as bbc_service_profit_basic,
	d4.profit as bbc_service_profit_finish,
	d4.profit_target_rate as bbc_service_target_rate,
	d4.profit_target_rate_tc as bbc_service_target_rate_tc,
	
	(rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_sales_fp_rate)*if(f.category_second<>'',f.adjust_rate,coalesce(e.new_cust_rate,1))*coalesce(d1.profit_target_rate_tc,1) as tc_sales_rp,
	(bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_sales_fp_rate)*coalesce(d1.profit_target_rate_tc,1) as tc_sales_bbc_zy,
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_sales_fp_rate)*coalesce(d1.profit_target_rate_tc,1) as tc_sales_bbc_ly,
	(fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_sales_fp_rate)*coalesce(d1.profit_target_rate_tc,1) as tc_sales_fl,
	
	((rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_sales_fp_rate)*if(f.category_second<>'',f.adjust_rate,coalesce(e.new_cust_rate,1))+
	(bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_sales_fp_rate)+
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_sales_fp_rate)+
	(fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_sales_fp_rate))*coalesce(d1.profit_target_rate_tc,1) as tc_sales,
		
	rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_service_user_fp_rate*coalesce(d2.profit_target_rate_tc,1) as tc_rp_service,		
		
	fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_service_user_fp_rate*coalesce(d3.profit_target_rate_tc,1) as tc_fl_service,	

	bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_service_user_fp_rate*coalesce(d4.profit_target_rate_tc,1) as tc_bbc_service_zy,
	bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_service_user_fp_rate*coalesce(d4.profit_target_rate_tc,1) as tc_bbc_service_ly,
	
	((bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_service_user_fp_rate)+
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_service_user_fp_rate))*coalesce(d4.profit_target_rate_tc,1) as tc_bbc_service,


	-- 增加字段，计算不考虑毛利目标达成情况的提成
	(rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_sales_fp_rate)*if(f.category_second<>'',f.adjust_rate,coalesce(e.new_cust_rate,1)) as original_tc_sales_rp,
	(bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_sales_fp_rate) as original_tc_sales_bbc_zy,
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_sales_fp_rate) as original_tc_sales_bbc_ly,
	(fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_sales_fp_rate) as original_tc_sales_fl,
	
	((rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_sales_fp_rate)*if(f.category_second<>'',f.adjust_rate,coalesce(e.new_cust_rate,1))+
	(bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_sales_fp_rate)+
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_sales_fp_rate)+
	(fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_sales_fp_rate)) as original_tc_sales,
		
	rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_service_user_fp_rate as original_tc_rp_service,		
		
	fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_service_user_fp_rate as original_tc_fl_service,	

	bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_service_user_fp_rate as original_tc_bbc_service_zy,
	bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_service_user_fp_rate as original_tc_bbc_service_ly,
	
	((bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_service_user_fp_rate)+
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_service_user_fp_rate)) as original_tc_bbc_service,	
	-- from_utc_timestamp(current_timestamp(),'GMT') update_time,
	-- substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	if(f.category_second<>'',f.adjust_rate,coalesce(e.new_cust_rate,1)) new_cust_rate
	-- substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 		
	
-- 客户+结算月+回款时间系数：各业务类型毛利率提成比例	
from csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc a 
-- 目标毛利系数-销售员与客服经理
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d1 on d1.work_no=a.work_no
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d2 on d2.work_no=a.rp_service_user_work_no
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d3 on d3.work_no=a.fl_service_user_work_no
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d4 on d4.work_no=a.bbc_service_user_work_no

-- 日配新客系数
left join 
(
select distinct customer_code,1.2 as new_cust_rate
	from 
	(
		-- select customer_code,sale_month as first_sale_month from csx_analyse_tmp.tmp_tc_cust_dy_jh_list
		-- union all 
		select *
		from (
			select 
				customer_code,
				substr(min(first_business_sale_date),1,6) first_sale_month
			from csx_dws.csx_dws_crm_customer_business_active_di
			where sdt ='current' 
			and business_type_code='1'
			group by customer_code
		)a	
		where first_sale_month>=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-12)),'-',''),1,6)
		and first_sale_month>='202308'
	)a
) e on a.customer_code=e.customer_code
left join
	(
	select smt_date,customer_code,category_second,
		adjust_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%调整日配新客系数%'
	) f on f.customer_code=a.customer_code;




drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1;
create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1
as
select 
	-- concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.region_code,a.customer_code,a.bill_month,cast(a.dff_rate as string)) biz_id,
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号	
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称		
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month, -- 结算月
	cast(a.dff_rate as decimal(20,6)) dff_rate,		
	a.pay_amt,	-- 核销金额
	a.rp_pay_amt,
	a.bbc_pay_amt,
	a.bbc_ly_pay_amt,
	a.bbc_zy_pay_amt,
	a.fl_pay_amt,		
	
	-- 各类型销售额
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt,
	a.bbc_ly_sale_amt,
	a.bbc_zy_sale_amt,
	a.fl_sale_amt,
	-- 各类型定价毛利额
	a.profit,
	a.rp_profit,
	a.bbc_profit,
	a.bbc_ly_profit,
	a.bbc_zy_profit,
	a.fl_profit,
	a.profit_rate,
	a.rp_profit_rate,
	a.bbc_profit_rate,
	a.bbc_ly_profit_rate,
	a.bbc_zy_profit_rate,
	a.fl_profit_rate,
	
	coalesce(a.cust_rp_profit_rate_tc,0.002) as cust_rp_profit_rate_tc, 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,
	
	-- 若系统账期为预付货款，则按原回款时间系数
	-- 若是预付款客户，打款日期小于上月1号则，按原回款时间系数但最高100%
	-- 若打款日期小于上月1号则提成为0，若为服务费则=当月回款额/当月销售额*服务费标准*回款系数
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,a.tc_sales_rp)+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,a.tc_sales_bbc_zy)+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,a.tc_sales_bbc_ly)+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,a.tc_sales_fl)
				))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_sales,
	
	-- 取消打款
	-- if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_rp_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_rp_service,		


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_fl_service))
		)*if(d.category_second like'%提成减半%',0.5,1) as tc_fl_service,	


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee
		*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_bbc_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,	
	
	-- 增加字段，计算不考虑毛利目标达成情况的提成
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,a.original_tc_sales_rp)+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,a.original_tc_sales_bbc_zy)+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,a.original_tc_sales_bbc_ly)+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,a.original_tc_sales_fl)
				))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_sales,
		
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_rp_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_rp_service,		


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_fl_service))
		)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_fl_service,	


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee
		*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_bbc_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_bbc_service,
	
	
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 		
	
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0 a
-- 系统账期预付款客户
 -- 信控公司账期 
left join 
(
	select customer_code,credit_code,company_code,
		account_period_code,account_period_name,		-- 账期编码,账期名称
		account_period_value,		-- 账期值
		account_period_abbreviation_name,		-- 账期简称
		credit_limit,temp_credit_limit
		from csx_dim.csx_dim_crm_customer_company_details
		where sdt='current'
)e1 on a.credit_code=e1.credit_code and a.company_code=e1.company_code
-- 预付款客户
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('日配','全业务')
	)b1 on b1.customer_code=a.customer_code
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('BBC','全业务')
	)b2 on b2.customer_code=a.customer_code
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('福利','全业务')
	)b3 on b3.customer_code=a.customer_code
-- 调整打款日期：按打款日期对应的实际回款时间系数计算	
left join 
		(
	select customer_code,smt_date as smonth,category_second,adjust_business_type,
	  date_star,date_end,
	  date_format(from_unixtime(unix_timestamp(paid_date_new,'yyyyMMdd')),'yyyy-MM-dd') as paid_date_new 
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整打款日期%'		
	)c on a.customer_code=c.customer_code and a.paid_date=c.paid_date_new
left join
	(
	select smt_date,customer_code,
		concat(customer_code,effective_period,remark) as dd,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and (category_second like'%不算提成%'
	-- or category_second like'%服务费%'
	or category_second like'%提成减半%')
	)d on d.customer_code=a.customer_code
;




-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail;
-- create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail partition(smt)
select 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.province_code,a.customer_code,
	a.bill_month,a.happen_month,a.bill_date,a.paid_date,cast(a.dff_rate as string)) biz_id,
	
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month, -- 结算月
	cast(a.dff_rate as decimal(20,6)) dff_rate,		
	sum(pay_amt) as pay_amt,	-- 核销金额
	sum(rp_pay_amt) as rp_pay_amt,
	sum(bbc_pay_amt) as bbc_pay_amt,
	sum(bbc_ly_pay_amt) as bbc_ly_pay_amt,
	sum(bbc_zy_pay_amt) as bbc_zy_pay_amt,
	sum(fl_pay_amt) as fl_pay_amt,		
	
	-- 各类型销售额
	sum(sale_amt) as sale_amt,
	sum(rp_sale_amt) as rp_sale_amt,
	sum(bbc_sale_amt) as bbc_sale_amt,
	sum(bbc_ly_sale_amt) as bbc_ly_sale_amt,
	sum(bbc_zy_sale_amt) as bbc_zy_sale_amt,
	sum(fl_sale_amt) as fl_sale_amt,
	-- 各类型定价毛利额
	sum(profit) as profit,
	sum(rp_profit) as rp_profit,
	sum(bbc_profit) as bbc_profit,
	sum(bbc_ly_profit) as bbc_ly_profit,
	sum(bbc_zy_profit) as bbc_zy_profit,
	sum(fl_profit) as fl_profit,
	
	profit_rate,
	rp_profit_rate,
	bbc_profit_rate,
	bbc_ly_profit_rate,
	bbc_zy_profit_rate,
	fl_profit_rate,
	
	coalesce(a.cust_rp_profit_rate_tc,0.002) as cust_rp_profit_rate_tc, 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,
	
	sum(tc_sales) as tc_sales,
	sum(tc_rp_service) as tc_rp_service,		
	sum(tc_fl_service) as tc_fl_service,	
	sum(tc_bbc_service) as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,	

	-- 增加字段，计算不考虑毛利目标达成情况的提成
	sum(original_tc_sales) as original_tc_sales,
	sum(original_tc_rp_service) as original_tc_rp_service,		
	sum(original_tc_fl_service) as original_tc_fl_service,	
	sum(original_tc_bbc_service) as original_tc_bbc_service,	
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 	
	
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1 a
group by 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.province_code,a.customer_code,
	a.bill_month,a.happen_month,a.bill_date,a.paid_date,cast(a.dff_rate as string)),
	
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month,

	profit_rate,
	rp_profit_rate,
	bbc_profit_rate,
	bbc_ly_profit_rate,
	bbc_zy_profit_rate,
	fl_profit_rate,
	
	cast(a.dff_rate as decimal(20,6)),
	coalesce(a.cust_rp_profit_rate_tc,0.002), 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,	
	
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit;	
	

-- 8月销售额
select
from csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business
where smt='202309'








-- hive 销售员与客服经理基准毛利额
drop table if exists csx_analyse.csx_analyse_tc_sales_service_profit_basic_mf;
create table csx_analyse.csx_analyse_tc_sales_service_profit_basic_mf(
`province_name`	string	COMMENT	'省区',
`work_no`	string	COMMENT	'工号',
`sales_name`	string	COMMENT	'姓名',
`begin_date`	string	COMMENT	'入职时间',
`user_position`	string	COMMENT	'职位',
`profit_basic`	decimal(20,6)	COMMENT	'基准毛利额',
`smt_c`	string	COMMENT	'日期_c'
) COMMENT '销售员与客服经理基准毛利额'
PARTITIONED BY (smt string COMMENT '日期分区');

select customer_code,
claim_bill_code,
close_bill_code,
happen_date,
paid_time,
posting_time
from csx_dwd.csx_dwd_sss_close_bill_account_record_di
where sdt>='20230804'
and substr(happen_date,1,10)<>substr(posting_time,1,10)
limit 200;

describe csx_dwd.csx_dwd_sss_money_back_di
trade_time	timestamp	交易日期
