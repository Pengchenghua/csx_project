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

--目标毛利系数-客户月度毛利
drop table csx_analyse_tmp.tmp_tc_cust_profit_month;
create temporary table csx_analyse_tmp.tmp_tc_cust_profit_month
as
select 
	b.performance_region_code,b.performance_region_name,
	b.performance_province_code,b.performance_province_name,
	b.performance_city_code,b.performance_city_name,
	a.customer_code,b.customer_name,
	a.smonth,
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
	sale_amt,
	rp_sale_amt,
	bbc_sale_amt,
	fl_sale_amt,
	-- 各类型定价毛利额
	profit,
	rp_profit,
	bbc_profit,
	fl_profit
from 
	(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_code in(6) then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in(2) then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit,
		sum(case when business_type_code in ('1','4','5') then profit else 0 end) as rp_profit,
		sum(case when business_type_code in(6) then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in(2) then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
	and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and channel_code in('1','7','9')
	and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'					
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	and (business_type_code in('1','2','6')
	or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
	-- 202303签呈 上海 '130733','128865','130078' 每月纳入大客户提成计算 仅管家拿提成
	or customer_code in ('130733','128865','130078'))	
	and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断	
	group by customer_code,substr(sdt,1,6)	
	)a
left join 
	(
	select 
		distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		case when channel_code='9' then '业务代理' end as ywdl_cust,
		case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and customer_code !=''
	)b on b.customer_code=a.customer_code
-- 关联对应各月销售员
join		
	(  
	select *
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)c on c.customer_no=a.customer_code	
where b.ywdl_cust is null and b.ng_cust is null;



--目标毛利系数-销售员与客服经理
drop table csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc
as
select *,
	case when profit_target_rate<0.6 then 0
		when profit_target_rate>=0.6 and profit_target_rate<1 then profit_target_rate
		when profit_target_rate>=1 and profit_target_rate<1.5 then 1.1
		when profit_target_rate>=1.5 then 1.2
		else 1 end as profit_target_rate_tc
from 
(
	select 
		a.performance_region_code,a.performance_region_name,
		a.performance_province_code,a.performance_province_name,
		a.smonth,
		a.sales_id,a.work_no,a.sales_name,	
		d.profit_basic,
		sum(a.sale_amt) as sale_amt, -- 客户总销售额
		sum(a.profit) as profit,-- 客户总定价毛利额
		round(sum(a.profit)/d.profit_basic,6) as profit_target_rate
	from 
	(
		select performance_region_code,performance_region_name,
			performance_province_code,performance_province_name,
			smonth,customer_code,customer_name,
			sales_id,work_no,sales_name,
			sale_amt,profit
		from csx_analyse_tmp.tmp_tc_cust_profit_month
		where work_no<>''
		union all
		select performance_region_code,performance_region_name,
			performance_province_code,performance_province_name,
			smonth,customer_code,customer_name,
			rp_service_user_id as sales_id,
			rp_service_user_work_no as work_no,
			rp_service_user_name as sales_name,
			rp_sale_amt as sale_amt,
			rp_profit as profit
		from csx_analyse_tmp.tmp_tc_cust_profit_month
		where rp_service_user_work_no<>''
		
		union all
		select performance_region_code,performance_region_name,
			performance_province_code,performance_province_name,
			smonth,customer_code,customer_name,
			fl_service_user_id as sales_id,
			fl_service_user_work_no as work_no,
			fl_service_user_name as sales_name,
			fl_sale_amt as sale_amt,
			fl_profit as profit
		from csx_analyse_tmp.tmp_tc_cust_profit_month
		where fl_service_user_work_no<>''
		
		union all
		select performance_region_code,performance_region_name,
			performance_province_code,performance_province_name,
			smonth,customer_code,customer_name,
			bbc_service_user_id as sales_id,
			bbc_service_user_work_no as work_no,
			bbc_service_user_name as sales_name,
			bbc_sale_amt as sale_amt,
			bbc_profit as profit
		from csx_analyse_tmp.tmp_tc_cust_profit_month
		where bbc_service_user_work_no<>''
	) a
	left join csx_analyse_tmp.tc_sales_service_profit_basic d on d.work_no=a.work_no 
	group by a.performance_region_code,a.performance_region_name,
		a.performance_province_code,a.performance_province_name,
		a.smonth,a.sales_id,a.work_no,a.sales_name,d.profit_basic
)a;







-- 纯现金客户标记 月BBC销售金额大于0，月授信支付金额等于0
drop table csx_analyse_tmp.tmp_tc_cust_chunxianjin;
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
		select order_code,sum(credit_settle_amount) credit_settle_amount
		from csx_ods.csx_ods_csxprd_common_wshop_bill_order_df
		-- where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		-- 修改了固定分区取最新
		where sdt='19990101'
		group by order_code
	)b on b.order_code=a.original_order_code
	group by a.customer_code,a.smonth
)a
where a.bbc_sale_amt>0 
and a.sale_amt=a.bbc_sale_amt 
and coalesce(a.credit_settle_amount,0)=0;



-- 结算单中本月回款核销金额
drop table csx_analyse_tmp.tmp_tc_cust_credit_bill;
create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill
as
select
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
	a.pay_amt,	-- 核销金额
	b.business_type_code,
	b.business_type_name,
	b.status,  -- 是否有效 0.无效 1.有效
	b.sale_amt,
	b.profit,
	b.sale_amt_jiushui,
	b.profit_jiushui
from 
(
	select 
		a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		a.sdt,
		split(a.source_bill_no,'-')[0] source_bill_no,	-- 来源单号
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
		sum(a.order_amt) order_amt,	-- 源单据对账金额
		sum(a.pay_amt) pay_amt	-- 核销金额
	from 
	(
		select 
			bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
			sdt,
			source_bill_no,	-- 来源单号
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
			date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1) bill_date, -- 结算日期
			overdue_date	-- 逾期开始日期	  
		from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
		-- where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
		and date_format(happen_date,'yyyy-MM-dd')>='2020-06-01'
		-- and customer_code='127307'
	)a
	join
	(
		-- 核销流水明细表:本月核销金额
		select customer_code,credit_code,company_code,close_bill_code,
		date_format(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')), 'yyyy-MM-dd') paid_date,
		sum(pay_amt) pay_amt
		from csx_dwd.csx_dwd_sss_close_bill_account_record_di
		-- 核销日期分区
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','')
		and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		-- and date_format(happen_date,'yyyy-MM-dd')>='2022-03-01'
		and delete_flag ='0'
		group by customer_code,credit_code,company_code,close_bill_code,
		date_format(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')), 'yyyy-MM-dd')
	)b on b.close_bill_code=a.source_bill_no
	group by 
		a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		a.sdt,
		split(a.source_bill_no,'-')[0],	-- 来源单号
		a.customer_code,	-- 客户编码
		a.credit_code,	-- 信控号
		a.happen_date,	-- 发生时间		
		a.company_code,	-- 签约公司编码
		a.source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
		a.reconciliation_period,  -- 对账周期
		a.bill_date, -- 结算日期
		a.overdue_date,	-- 逾期开始日期	
		b.paid_date
)a
-- 销售单业绩毛利
-- oc返利单的可能一个返利单对应多个原单号，original_order_code
left join
(
select order_code,
case when business_type_code in(1,4,5) then 1 else business_type_code end business_type_code,
case when business_type_code in(1,4,5) then '日配业务' else business_type_name end business_type_name,
if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1) status,  -- 是否有效 0.无效 1.有效
sum(case when goods_code not in ('8718','8708','8649','840509') then sale_amt end) as sale_amt,
sum(case when goods_code not in ('8718','8708','8649','840509') then profit end) as profit,
sum(case when goods_code in ('8718','8708','8649','840509') then sale_amt end) as sale_amt_jiushui,
sum(case when goods_code in ('8718','8708','8649','840509') then profit end) as profit_jiushui
from csx_dws.csx_dws_sale_detail_di
where channel_code in('1','7','9')
-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
and (business_type_code in('1','2','6')
	or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
	-- 202303签呈 上海 '130733','128865','130078' 每月纳入大客户提成计算 仅管家拿提成
	or customer_code in ('130733','128865','130078'))
-- and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断
group by order_code,
case when business_type_code in(1,4,5) then 1 else business_type_code end,
case when business_type_code in(1,4,5) then '日配业务' else business_type_name end,
if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)
)b on b.order_code=a.source_bill_no;


--结算单回款+BBC纯现金客户+价格补救单
drop table csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu;
create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu
as
select
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
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
	round(pow(0.95,if(datediff(a.paid_date, a.bill_date)<0,0,floor(datediff(a.paid_date, a.bill_date)/30))),2) dff_rate,
	a.order_amt,	-- 源单据对账金额
	a.pay_amt,	-- 核销金额
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
	b.fl_sales_sale_fp_rate,
	b.bbc_sales_sale_fp_rate,
	b.rp_service_user_fp_rate,
	b.fl_service_user_fp_rate,
	b.bbc_service_user_fp_rate	
from 
(
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
	pay_amt,	-- 核销金额
	business_type_code,
	business_type_name,
	status,  -- 是否有效 0.无效 1.有效
	sale_amt,
	profit,
	sale_amt_jiushui,
	profit_jiushui
from csx_analyse_tmp.tmp_tc_cust_credit_bill

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
			company_code,business_type_code,business_type_name,
			performance_province_name,customer_code,credit_code,order_code,sdt,
			if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1) status,  -- 是否有效 0.无效 1.有效
			sum(case when goods_code not in ('8718','8708','8649','840509') then sale_amt end) as sale_amt,
			sum(case when goods_code not in ('8718','8708','8649','840509') then profit end) as profit,
			sum(case when goods_code in ('8718','8708','8649','840509') then sale_amt end) as sale_amt_jiushui,
			sum(case when goods_code in ('8718','8708','8649','840509') then profit end) as profit_jiushui
		from csx_dws.csx_dws_sale_detail_di
		where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') 
		and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and channel_code in('1','7','9')	
		group by company_code,business_type_code,business_type_name,
		performance_province_name,customer_code,credit_code,order_code,sdt,
		if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)  -- 是否有效 0.无效 1.有效
)b on a.customer_code=b.customer_code

-- 价格补救
union all
select 
	null as bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	b.sdt,
	b.order_code as source_bill_no,	-- 来源单号
	b.customer_code,	-- 客户编码
	b.credit_code,	-- 信控号
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') happen_date,	-- 发生时间		
	b.company_code,	-- 签约公司编码
	'价格补救' as source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	null reconciliation_period,  -- 对账周期
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') bill_date, -- 结算日期
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') overdue_date,	-- 逾期开始日期	
	date_format(from_unixtime(unix_timestamp(b.sdt,'yyyyMMdd')), 'yyyy-MM-dd') paid_date,	-- 核销日期	
	b.sale_amt order_amt,	-- 源单据对账金额
	b.sale_amt pay_amt,	-- 核销金额
	b.business_type_code,
	b.business_type_name,
	b.status,  -- 是否有效 0.无效 1.有效
	b.sale_amt,
	b.profit,
	b.sale_amt_jiushui,
	b.profit_jiushui
from csx_analyse_tmp.tmp_tc_cust_credit_bill a 
-- 找回款单对应的价格补救单
join 
(
select sdt,customer_code,credit_code,company_code,original_order_code,order_code,
case when business_type_code in(1,4,5) then 1 else business_type_code end business_type_code,
case when business_type_code in(1,4,5) then '日配业务' else business_type_name end business_type_name,
if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1) status,  -- 是否有效 0.无效 1.有效
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
	-- 202303签呈 上海 '130733','128865','130078' 每月纳入大客户提成计算 仅管家拿提成
	or customer_code in ('130733','128865','130078'))
group by sdt,customer_code,credit_code,company_code,original_order_code,order_code,
case when business_type_code in(1,4,5) then 1 else business_type_code end,
case when business_type_code in(1,4,5) then '日配业务' else business_type_name end,
if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)
)b on a.source_bill_no=b.original_order_code
)a 
--客户信息与提成系数
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
		if(rp_sales_sale_fp_rate=0.7,0.6,if(rp_sales_sale_fp_rate=0.3,0.4,if(rp_sales_sale_fp_rate=0.2,0.3,if(rp_sales_sale_fp_rate=0.1,0.2,rp_sales_sale_fp_rate)))) as rp_sales_fp_rate,
		if(fl_sales_sale_fp_rate=0.7,0.6,if(fl_sales_sale_fp_rate=0.3,0.4,if(fl_sales_sale_fp_rate=0.2,0.3,if(fl_sales_sale_fp_rate=0.1,0.2,fl_sales_sale_fp_rate)))) as fl_sales_sale_fp_rate,
		if(bbc_sales_sale_fp_rate=0.7,0.6,if(bbc_sales_sale_fp_rate=0.3,0.4,if(bbc_sales_sale_fp_rate=0.2,0.3,if(bbc_sales_sale_fp_rate=0.1,0.2,bbc_sales_sale_fp_rate)))) as bbc_sales_sale_fp_rate,
		if(rp_service_user_sale_fp_rate=0.7,0.6,if(rp_service_user_sale_fp_rate=0.3,0.4,if(rp_service_user_sale_fp_rate=0.2,0.3,if(rp_service_user_sale_fp_rate=0.1,0.2,rp_service_user_sale_fp_rate)))) as rp_service_user_fp_rate,
		if(fl_service_user_sale_fp_rate=0.7,0.6,if(fl_service_user_sale_fp_rate=0.3,0.4,if(fl_service_user_sale_fp_rate=0.2,0.3,if(fl_service_user_sale_fp_rate=0.1,0.2,fl_service_user_sale_fp_rate)))) as fl_service_user_fp_rate,
		if(bbc_service_user_sale_fp_rate=0.7,0.6,if(bbc_service_user_sale_fp_rate=0.3,0.4,if(bbc_service_user_sale_fp_rate=0.2,0.3,if(bbc_service_user_sale_fp_rate=0.1,0.2,bbc_service_user_sale_fp_rate)))) as bbc_service_user_fp_rate	
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
)b on b.customer_no=a.customer_code
--客户信控的账期
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
			distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			-- 202302签呈 上海 130733 每月纳入大客户提成计算 仅管家拿提成
			case when channel_code='9' and customer_code not in ('130733','128865','130078') then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from csx_dim.csx_dim_crm_customer_info 
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and channel_code in('1','7','9')
		and customer_type_code=4
	)d on d.customer_code=a.customer_code
where d.ywdl_cust is null  and d.ng_cust is null and d.customer_code is not null
;




drop table csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_1;
create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_1
as
select *
from (
select *,
row_number() over(partition by customer_code,source_bill_no order by happen_date desc)	as num
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu
)a
where num=1;




-- -- 客户毛利率提成比例
-- select 
-- 	customer_code,customer_name,
-- 	
-- 	sum(profit)/abs(sum(sale_amt)) profit_rate,
-- 	sum(case when business_type_code in(1,4,5) then profit end)
-- 		/abs(sum(case when business_type_code in(1,4,5) then sale_amt end)) rp_profit_rate,
-- 	sum(case when business_type_code in(2,6) then profit end)
-- 		/abs(sum(case when business_type_code in(2,6) then sale_amt end)) fl_bbc_profit_rate
-- from 
-- (
-- select *,
-- row_number() over(partition by customer_code,source_bill_no order by happen_date desc)	as num
-- from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_1
-- where num=1
-- )a 

--人员的客户回款毛利率提成比例
drop table csx_analyse_tmp.tmp_tc_person_profit_rate_tc;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_rate_tc
as
select
	region_code,region_name,province_code,province_name,
	customer_code,customer_name,
	sales_id,
	work_no,
	sales_name,
	rp_sale_amt,
	rp_profit,
	rp_profit_rate,
	fl_bbc_sale_amt,
	fl_bbc_profit,
	fl_bbc_profit_rate,
	case when rp_profit_rate<0.08 then 0.002
		when rp_profit_rate>=0.08 and rp_profit_rate<0.12 then 0.005
		when rp_profit_rate>=0.12 and rp_profit_rate<0.16 then 0.007
		when rp_profit_rate>=0.16 and rp_profit_rate<0.2 then 0.009
		when rp_profit_rate>=0.2 and rp_profit_rate<0.25 then 0.013
		when rp_profit_rate>=0.25 then 0.015
		else 0.002 end as sales_rp_profit_rate_tc,
	case when fl_bbc_profit_rate<0.03 then 0.002
		when fl_bbc_profit_rate>=0.03 and fl_bbc_profit_rate<0.07 then 0.0035
		when fl_bbc_profit_rate>=0.07 and fl_bbc_profit_rate<0.1 then 0.0045
		when fl_bbc_profit_rate>=0.1 and fl_bbc_profit_rate<0.13 then 0.0065
		when fl_bbc_profit_rate>=0.13 and fl_bbc_profit_rate<0.17 then 0.0095
		when fl_bbc_profit_rate>=0.17 then 0.0115
		else 0.002 end as sales_fl_bbc_profit_rate_tc		
from 
(
select 
	region_code,region_name,province_code,province_name,
	customer_code,customer_name,
	sales_id,
	work_no,
	sales_name,
	sum(rp_sale_amt) rp_sale_amt,
	sum(rp_profit) rp_profit,
	sum(rp_profit)/abs(sum(rp_sale_amt)) rp_profit_rate,
	sum(fl_bbc_sale_amt) fl_bbc_sale_amt,
	sum(fl_bbc_profit) fl_bbc_profit,
	sum(fl_bbc_profit)/abs(sum(fl_bbc_sale_amt)) fl_bbc_profit_rate
from 
	(
		select region_code,region_name,province_code,province_name,
			customer_code,customer_name,
			sales_id,
			work_no,
			sales_name,
			case when business_type_code in(1,4,5) then sale_amt end as rp_sale_amt,
			case when business_type_code in(1,4,5) then profit end as rp_profit,
			case when business_type_code in(2,6) then sale_amt end as fl_bbc_sale_amt,
			case when business_type_code in(2,6) then profit end as fl_bbc_profit			
		from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_1
		where work_no<>''

		union all
		select region_code,region_name,province_code,province_name,
			customer_code,customer_name,
			rp_service_user_id as sales_id,
			rp_service_user_work_no as work_no,
			rp_service_user_name as sales_name,
			sale_amt as rp_sale_amt,
			profit as rp_profit,
			null as fl_bbc_sale_amt,
			null as fl_bbc_profit			
		from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_1
		where rp_service_user_work_no<>''
		and business_type_code in(1,4,5)
		
		union all
		select region_code,region_name,province_code,province_name,
			customer_code,customer_name,
			fl_service_user_id as sales_id,
			fl_service_user_work_no as work_no,
			fl_service_user_name as sales_name,
			null as rp_sale_amt,
			null as rp_profit,
			sale_amt as fl_bbc_sale_amt,
			profit as fl_bbc_profit
		from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_1
		where fl_service_user_work_no<>''
		and business_type_code in(2)
		
		union all
		select region_code,region_name,province_code,province_name,
			customer_code,customer_name,
			bbc_service_user_id as sales_id,
			bbc_service_user_work_no as work_no,
			bbc_service_user_name as sales_name,
			null as rp_sale_amt,
			null as rp_profit,
			sale_amt as fl_bbc_sale_amt,
			profit as fl_bbc_profit
		from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu_1
		where bbc_service_user_work_no<>''
		and business_type_code in(6)
	)a
group by region_code,region_name,province_code,province_name,
	customer_code,customer_name,
	sales_id,
	work_no,
	sales_name
)a;


drop table csx_analyse_tmp.tmp_tc_customer_order_detail;
create temporary table csx_analyse_tmp.tmp_tc_customer_order_detail
as
select *,
	
	case when business_type_code in(1,4,5) then pay_amt*if(source_sys='BEGIN' and sales_rp_profit_rate_tc is null,0.002,sales_rp_profit_rate_tc)*dff_rate*rp_sales_fp_rate*sales_target_rate_tc
		when business_type_code in(2) then pay_amt*if(source_sys='BEGIN' and sales_fl_bbc_profit_rate_tc is null,0.002,sales_fl_bbc_profit_rate_tc)*dff_rate*fl_sales_sale_fp_rate*sales_target_rate_tc 
		when business_type_code in(6) then pay_amt*if(source_sys='BEGIN' and sales_fl_bbc_profit_rate_tc is null,0.002,sales_fl_bbc_profit_rate_tc)*dff_rate*bbc_sales_sale_fp_rate*sales_target_rate_tc 
		end as tc_sales,
		
	case when business_type_code in(1,4,5) then pay_amt*if(source_sys='BEGIN' and rp_service_profit_rate_tc is null,0.002,rp_service_profit_rate_tc)*dff_rate*rp_service_user_fp_rate*rp_service_target_rate_tc
		end as tc_rp_service,		
		
	case when business_type_code in(2) then pay_amt*if(source_sys='BEGIN' and fl_service_profit_rate_tc is null,0.002,fl_service_profit_rate_tc)*dff_rate*fl_service_user_fp_rate*fl_service_target_rate_tc
		end as tc_fl_service,	

	case when business_type_code in(6) then pay_amt*if(source_sys='BEGIN' and bbc_service_profit_rate_tc is null,0.002,bbc_service_profit_rate_tc)*dff_rate*bbc_service_user_fp_rate*bbc_service_target_rate_tc
		end as tc_bbc_service
from 
(
select 
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
	a.bill_date, -- 结算日期
	a.overdue_date,	-- 逾期开始日期	
	a.paid_date,	-- 核销日期	
	a.dff,
	-- 回款时间系数
	a.dff_rate,
	a.order_amt,	-- 源单据对账金额
	a.pay_amt,	-- 核销金额
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
	a.fl_sales_sale_fp_rate,
	a.bbc_sales_sale_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,
	-- 人员的客户回款毛利率提成比例
	b1.rp_profit_rate as sales_rp_profit_rate,
	b1.fl_bbc_profit_rate as sales_fl_bbc_profit_rate,	
	b1.sales_rp_profit_rate_tc as sales_rp_profit_rate_tc,
	b1.sales_fl_bbc_profit_rate_tc as sales_fl_bbc_profit_rate_tc,
	
	b2.rp_profit_rate as rp_service_profit_rate,	
	b2.sales_rp_profit_rate_tc as rp_service_profit_rate_tc,
	
	b3.fl_bbc_profit_rate as fl_service_profit_rate,	
	b3.sales_fl_bbc_profit_rate_tc as fl_service_profit_rate_tc,
	
	b4.fl_bbc_profit_rate as bbc_service_profit_rate,	
	b4.sales_fl_bbc_profit_rate_tc as bbc_service_profit_rate_tc,	
	--目标毛利系数-销售员与客服经理
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
	d4.profit_target_rate_tc as bbc_service_target_rate_tc			
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu a 
--人员的客户回款毛利率提成比例
left join csx_analyse_tmp.tmp_tc_person_profit_rate_tc b1 on b1.work_no=a.work_no and b1.customer_code=a.customer_code
left join csx_analyse_tmp.tmp_tc_person_profit_rate_tc b2 on b2.work_no=a.rp_service_user_work_no and b2.customer_code=a.customer_code
left join csx_analyse_tmp.tmp_tc_person_profit_rate_tc b3 on b3.work_no=a.fl_service_user_work_no and b3.customer_code=a.customer_code
left join csx_analyse_tmp.tmp_tc_person_profit_rate_tc b4 on b4.work_no=a.bbc_service_user_work_no and b4.customer_code=a.customer_code
--目标毛利系数-销售员与客服经理
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d1 on d1.work_no=a.work_no
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d2 on d2.work_no=a.rp_service_user_work_no
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d3 on d3.work_no=a.fl_service_user_work_no
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d4 on d4.work_no=a.bbc_service_user_work_no
where b1.sales_name is not null 
or b2.sales_name is not null
or b3.sales_name is not null
or b4.sales_name is not null
)a;


drop table csx_analyse_tmp.tmp_tc_customer_detail;
create temporary table csx_analyse_tmp.tmp_tc_customer_detail
as
select 
	-- region_code,
	region_name,
	-- province_code,
	province_name,
	-- city_group_code,
	city_group_name,
	customer_code,	-- 客户编码
	customer_name,
	-- sales_id,
	work_no,
	sales_name,
	-- rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	-- fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,		
	-- bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	sum(pay_amt) pay_amt,	-- 核销金额
	sum(case when business_type_code in(1,4,5) then pay_amt end) rp_pay_amt,
	sum(case when business_type_code in(2) then pay_amt end) fl_pay_amt,
	sum(case when business_type_code in(6) then pay_amt end) bbc_pay_amt,
	
	-- 人员的客户回款毛利率提成比例
	sales_rp_profit_rate,
	sales_fl_bbc_profit_rate,	
	sales_rp_profit_rate_tc,
	sales_fl_bbc_profit_rate_tc,
	
	rp_service_profit_rate,	
	rp_service_profit_rate_tc,
	
	fl_service_profit_rate,	
	fl_service_profit_rate_tc,
	
	bbc_service_profit_rate,	
	bbc_service_profit_rate_tc,
	
	-- 提成分配系数
	rp_sales_fp_rate,
	fl_sales_sale_fp_rate,
	bbc_sales_sale_fp_rate,
	rp_service_user_fp_rate,
	fl_service_user_fp_rate,
	bbc_service_user_fp_rate,	

	--目标毛利系数-销售员与客服经理
	sales_profit_basic,
	sales_profit_finish,
	sales_target_rate,
	sales_target_rate_tc,
	
	rp_service_profit_basic,
	rp_service_profit_finish,
	rp_service_target_rate,
	rp_service_target_rate_tc,

	fl_service_profit_basic,
	fl_service_profit_finish,
	fl_service_target_rate,
	fl_service_target_rate_tc,

	bbc_service_profit_basic,
	bbc_service_profit_finish,
	bbc_service_target_rate,
	bbc_service_target_rate_tc,

	--当月提成金额
	sum(tc_sales) tc_sales,
	sum(tc_rp_service) tc_rp_service,			
	sum(tc_fl_service) tc_fl_service,	
	sum(tc_bbc_service) tc_bbc_service	
from csx_analyse_tmp.tmp_tc_customer_order_detail
where status=1
group by 
	-- region_code,
	region_name,
	-- province_code,
	province_name,
	-- city_group_code,
	city_group_name,
	customer_code,	-- 客户编码
	customer_name,
	-- sales_id,
	work_no,
	sales_name,
	-- rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	-- fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,		
	-- bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	-- 人员的客户回款毛利率提成比例
	sales_rp_profit_rate,
	sales_fl_bbc_profit_rate,	
	sales_rp_profit_rate_tc,
	sales_fl_bbc_profit_rate_tc,
	
	rp_service_profit_rate,	
	rp_service_profit_rate_tc,
	
	fl_service_profit_rate,	
	fl_service_profit_rate_tc,
	
	bbc_service_profit_rate,	
	bbc_service_profit_rate_tc,
	
	-- 提成分配系数
	rp_sales_fp_rate,
	fl_sales_sale_fp_rate,
	bbc_sales_sale_fp_rate,
	rp_service_user_fp_rate,
	fl_service_user_fp_rate,
	bbc_service_user_fp_rate,	

	--目标毛利系数-销售员与客服经理
	sales_profit_basic,
	sales_profit_finish,
	sales_target_rate,
	sales_target_rate_tc,
	
	rp_service_profit_basic,
	rp_service_profit_finish,
	rp_service_target_rate,
	rp_service_target_rate_tc,

	fl_service_profit_basic,
	fl_service_profit_finish,
	fl_service_target_rate,
	fl_service_target_rate_tc,

	bbc_service_profit_basic,
	bbc_service_profit_finish,
	bbc_service_target_rate,
	bbc_service_target_rate_tc;












	



--目标毛利系数-销售员与客服经理
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d on d.work_no=b.work_no


	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
	a.source_bill_no,	-- 来源单号
	a.customer_code,	-- 客户编码
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
	round(pow(0.95,if(datediff(a.paid_date, a.bill_date)<0,0,floor(datediff(a.paid_date, a.bill_date)/30))),2) dff_rate,
	a.order_amt,	-- 源单据对账金额
	a.pay_amt,	-- 核销金额
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
	-- 毛利提成系数
	b.rp_sales_fp_rate,
	b.fl_sales_sale_fp_rate,
	b.bbc_sales_sale_fp_rate,
	b.rp_service_user_fp_rate,
	b.fl_service_user_fp_rate,
	b.bbc_service_user_fp_rate	





csx_dws_sss_order_invoice_bill_settle_detail_di
csx_dws_sss_order_credit_invoice_bill_settle_detail_di

csx_dws_sss_customer_invoice_bill_settle_stat_di
csx_dws_sss_customer_credit_invoice_bill_settle_stat_di



-- 日配
sum(case when business_type_code in(1,4,5) then sale_amt else 0 end) as rp_sale_amt,




	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		-- 202208签呈，毛利核算中125533客户业务类型BBC改福利 5-8月
		sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_code in(6) and customer_code not in ('') then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in(2) or (customer_code in ('') and business_type_code in(6)) then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(case when inventory_dc_code <>'W0K4' then profit else 0 end) as profit, -- W0K4只计算销售额 不计算定价毛利额 每月
		sum(case when business_type_code in ('1','4','5') and inventory_dc_code <>'W0K4' then profit else 0 end) as rp_profit,
		sum(case when business_type_code in(6) and customer_code not in ('') and inventory_dc_code <>'W0K4' then profit else 0 end) as bbc_profit,
		sum(case when (business_type_code in(2) or (customer_code in ('') and business_type_code in(6))) and inventory_dc_code <>'W0K4' then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
		-- where sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			where channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649','840509'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
				-- 福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202210签呈 北京 129026 129000 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				or (business_type_code in ('4') and customer_code in ('126690','127923','129026','129000'))
				-- 202303签呈 上海 '130733','128865','130078' 每月纳入大客户提成计算 仅管家拿提成
				or customer_code in ('130733','128865','130078'))
			-- and performance_province_name in ('福建省')
			and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断
	group by customer_code,substr(sdt,1,6)	


--备注记录
-- 1、销售结算系统中有返利、但无调价与价格补救
select original_order_code,order_code,order_channel_detail_name,sale_amt
from csx_dws.csx_dws_sale_detail_di
where sdt>='20230601'
-- 下单渠道细分编码: 11-中台 12-小程序 13-红旗 21-云超 22-云创 23-bbc 24-永辉生活 25-客户返利 26-价格补救 27-客户调价
and order_channel_detail_code=27
limit 10;	

OC23053100233
split('ab-cd-ef','-')[0]		

BBC订单
regexp_replace(order_code, '([^0-9]+)', '')


--客户信控的账期
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
                account_period_code       payment_terms, --  账期类型
                account_period_name       payment_name,  --  账期名称
                account_period_value      payment_days,  --  帐期天数
                credit_limit,                            --  信控额度
                temp_credit_limit,                       --  临时额度
                temp_begin_time,                         --  临时额度起始时间
                temp_end_time,                            --  临时额度截止时间
                business_attribute_code,                 -- 信控业务属性编码
                business_attribute_name                  -- 信控业务属性名称
         from csx_dim.csx_dim_crm_customer_company_details
         where sdt = 'current' and status = 1
     ) hhh on aaa.customer_code = hhh.customer_no and aaa.company_code = hhh.company_code and
              aaa.credit_code = hhh.credit_code




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


