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

--###########################################################################################



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
drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill;
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
	if(a.source_sys='BEGIN',1,b.business_type_code) business_type_code,
	if(a.source_sys='BEGIN','日配业务',b.business_type_name) business_type_name,
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
		a.order_amt,	-- 源单据对账金额
		sum(b.pay_amt) pay_amt	-- 核销金额
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
		b.paid_date,
		a.order_amt
)a
-- 销售单业绩毛利
-- oc返利单的可能一个返利单对应多个原单号，original_order_code
left join
(
select order_code,operation_mode_code,
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
	-- 202303签呈 上海 '130733','128865','130078' 每月纳入大客户提成计算 仅管家拿提成
	or customer_code in ('130733','128865','130078','114872','124484','227054','228705','225582','123415'))
-- and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断
group by order_code,operation_mode_code,
case when business_type_code in(1,4,5) then 1 
	 when business_type_code in(6) and operation_mode_code=1 then 6.1
	 when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
	 else business_type_code end,

case when business_type_code in(1,4,5) then '日配业务'  
	 when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
	 when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
	 else business_type_name end,
if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)
)b on b.order_code=a.source_bill_no;


-- 结算单回款+BBC纯现金客户+价格补救单
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
	-- 202303签呈 上海 '130733','128865','130078' 每月纳入大客户提成计算 仅管家拿提成
	or customer_code in ('130733','128865','130078','114872','124484','227054','228705','225582','123415'))
group by sdt,customer_code,credit_code,company_code,original_order_code,order_code,
case when business_type_code in(1,4,5) then 1 
	 when business_type_code in(6) and operation_mode_code=1 then 6.1
	 when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 6.2
	 else business_type_code end,

case when business_type_code in(1,4,5) then '日配业务' 
	 when business_type_code in(6) and operation_mode_code=1 then 'BBC联营'
	 when business_type_code in(6) and (operation_mode_code=0 or operation_mode_code is null) then 'BBC自营'
	 else business_type_name end,
if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1)
)b on a.source_bill_no=b.original_order_code
)a 
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
		if(rp_sales_sale_fp_rate=0.7,0.6,if(rp_sales_sale_fp_rate=0.3,0.4,if(rp_sales_sale_fp_rate=0.2,0.3,if(rp_sales_sale_fp_rate=0.1,0.2,rp_sales_sale_fp_rate)))) as rp_sales_fp_rate,
		if(fl_sales_sale_fp_rate=0.7,0.6,if(fl_sales_sale_fp_rate=0.3,0.4,if(fl_sales_sale_fp_rate=0.2,0.3,if(fl_sales_sale_fp_rate=0.1,0.2,fl_sales_sale_fp_rate)))) as fl_sales_sale_fp_rate,
		if(bbc_sales_sale_fp_rate=0.7,0.6,if(bbc_sales_sale_fp_rate=0.3,0.4,if(bbc_sales_sale_fp_rate=0.2,0.3,if(bbc_sales_sale_fp_rate=0.1,0.2,bbc_sales_sale_fp_rate)))) as bbc_sales_sale_fp_rate,
		if(rp_service_user_sale_fp_rate=0.7,0.6,if(rp_service_user_sale_fp_rate=0.3,0.4,if(rp_service_user_sale_fp_rate=0.2,0.3,if(rp_service_user_sale_fp_rate=0.1,0.2,rp_service_user_sale_fp_rate)))) as rp_service_user_fp_rate,
		if(fl_service_user_sale_fp_rate=0.7,0.6,if(fl_service_user_sale_fp_rate=0.3,0.4,if(fl_service_user_sale_fp_rate=0.2,0.3,if(fl_service_user_sale_fp_rate=0.1,0.2,fl_service_user_sale_fp_rate)))) as fl_service_user_fp_rate,
		if(bbc_service_user_sale_fp_rate=0.7,0.6,if(bbc_service_user_sale_fp_rate=0.3,0.4,if(bbc_service_user_sale_fp_rate=0.2,0.3,if(bbc_service_user_sale_fp_rate=0.1,0.2,bbc_service_user_sale_fp_rate)))) as bbc_service_user_fp_rate	
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
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
			distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			-- 202302签呈 上海 130733 每月纳入大客户提成计算 仅管家拿提成
			case when channel_code='9' and customer_code not in ('130733','128865','130078','114872','124484','227054','228705','225582','123415') then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from csx_dim.csx_dim_crm_customer_info 
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and channel_code in('1','7','9')
		and customer_type_code=4
	)d on d.customer_code=a.customer_code
where d.ywdl_cust is null  and d.ng_cust is null and d.customer_code is not null
;


-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail;
-- create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail partition(smt)
select 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),
		source_bill_no,paid_date,customer_code) biz_id,
*,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 			
from csx_analyse_tmp.tmp_tc_cust_credit_bill_xianjin_bujiu
where sale_amt is not null 
or source_sys='BEGIN';	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初






-- 断约再下单客户清单：客户距离上次履约间隔3个月以上，再激活在一年以内
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_dy_jh_list;
-- create temporary table csx_analyse_tmp.tmp_tc_cust_dy_jh_list
-- as
-- select a.customer_code,a.customer_name,a.sale_month,a.rn,a.rn1	
-- from
-- 	(
-- 	select 
-- 		a.customer_code,b.customer_name,substr(a.sdt,1,6) as sale_month,
-- 		row_number()over(partition by a.customer_code order by a.sdt desc) as rn,
-- 		-- 间隔3个月以上次数超过3次，且最近一次再激活时间在4个月以内，认为是季度轮配客户，不记为断约再履约
-- 		row_number()over(partition by a.customer_code order by a.sdt asc) as rn1
-- 	from
-- 		(
-- 		select
-- 			-- performance_region_name,performance_province_name,performance_city_name,
-- 			customer_code,sdt,
-- 			-- to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))) as sdt_date,
-- 			-- to_date(from_unixtime(unix_timestamp(lead(sdt,1,null)over(partition by customer_code order by sdt desc),'yyyyMMdd'))) as lead_sdt_date,
-- 			datediff(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),to_date(from_unixtime(unix_timestamp(lead(sdt,1,null)over(partition by customer_code order by sdt desc),'yyyyMMdd')))) as diff_days
-- 		from 
-- 			(
-- 			select distinct customer_code,sdt
-- 			from csx_dws.csx_dws_sale_detail_di
-- 			where sdt>='20190101' -- 历史所有数据
-- 				and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')		
-- 				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
-- 				and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)	
-- 				and order_channel_code not in (4,6) -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
-- 			)a
-- 		) a 
-- 		join
-- 			(
-- 			select 
-- 				customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
-- 				performance_province_name
-- 			from csx_dim.csx_dim_crm_customer_info
-- 			where sdt = 'current'
-- 				and channel_code in('1','7','9')
-- 				and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
-- 			) b on b.customer_code=a.customer_code	
-- 	where a.diff_days>90
-- 	) a 
-- where rn=1 and (rn1<=3 or sale_month<regexp_replace(last_day(add_months('${sdt_yes_date}',-4)),'-',''))
-- -- 最近一次激活再履约在一年以内
-- and a.sale_month>=regexp_replace(last_day(add_months('${sdt_yes_date}',-12)),'-','');


-- 目标毛利系数-客户月度毛利
drop table if exists csx_analyse_tmp.tmp_tc_cust_profit_month;
create temporary table csx_analyse_tmp.tmp_tc_cust_profit_month
as
select 
	b.performance_region_code,b.performance_region_name,
	b.performance_province_code,b.performance_province_name,
	b.performance_city_code,b.performance_city_name,
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
	d.bbc_sale_amt,
	d.bbc_ly_sale_amt,
	d.bbc_zy_sale_amt,
	d.fl_sale_amt,
	-- 各类型定价毛利额
	-- 个人实际毛利额核算时福利及联营bbc业务按照1.2系数上浮
	d.profit,
	d.rp_profit,
	d.bbc_profit,
	d.bbc_ly_profit*1.2 as bbc_ly_profit,
	d.bbc_zy_profit,
	d.fl_profit*1.2 as fl_profit
from
-- 客户对应的销售员、客服经理	
	(  
	select *
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)a
left join  
	(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in (1,4,5) then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_code in(6) then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in(6) and operation_mode_code=1 then sale_amt else 0 end) as bbc_ly_sale_amt,
		sum(case when business_type_code in(6) and operation_mode_code=0 then sale_amt else 0 end) as bbc_zy_sale_amt,
		sum(case when business_type_code in(2) then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit,
		sum(case when business_type_code in (1,4,5) then profit else 0 end) as rp_profit,
		sum(case when business_type_code in(6) then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in(6) and operation_mode_code=1 then profit else 0 end) as bbc_ly_profit,
		sum(case when business_type_code in(6) and operation_mode_code=0 then profit else 0 end) as bbc_zy_profit,
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
	or customer_code in ('130733','128865','130078','114872','124484','227054','228705','225582','123415'))	
	and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断	
	group by customer_code,substr(sdt,1,6)	
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
	and customer_code !=''
	)b on b.customer_code=a.customer_no
-- 关联对应各月销售员
join		
	(  
	select *
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)c on c.customer_no=a.customer_no	
where b.ng_cust is null;
-- where b.ywdl_cust is null and b.ng_cust is null;


-- 毛利额汇总-销售员与客服经理：未处理多管家情况-顿号隔开
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_total
as
select 
	a.performance_region_code,a.performance_region_name,
	a.performance_province_code,a.performance_province_name,
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,	
	sum(a.sale_amt) as sale_amt, -- 客户总销售额
	sum(a.profit) as profit-- 客户总定价毛利额
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
)a
group by 
	a.performance_region_code,a.performance_region_name,
	a.performance_province_code,a.performance_province_name,
	a.smonth,
	a.sales_id,a.work_no,a.sales_name;
	

-- 毛利额汇总-销售员与客服经理：多管家-拆分到单人
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total_split;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_total_split
as
select
	a.performance_region_code,a.performance_region_name,
	a.performance_province_code,a.performance_province_name,
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
	performance_region_code,performance_region_name,
	performance_province_code,performance_province_name,
	smonth,
	sales_id,work_no,sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))=1

union all
select size(split(sales_id,'、')) as count_person,
	performance_region_code,performance_region_name,
	performance_province_code,performance_province_name,
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
	performance_region_code,performance_region_name,
	performance_province_code,performance_province_name,
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
	performance_region_code,performance_region_name,
	performance_province_code,performance_province_name,
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
	performance_region_code,performance_region_name,
	performance_province_code,performance_province_name,
	smonth,
	split(sales_id,'、')[3] sales_id,
	split(work_no,'、')[3] work_no,
	split(sales_name,'、')[3] sales_name,		
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1
)a 
left join csx_analyse.csx_analyse_tc_sales_service_profit_basic_mf d on d.work_no=a.work_no 
		and d.smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	left join 
	(
	select employee_code,employee_status,begin_date,
	case when begin_date>=concat(substr(regexp_replace(add_months('${sdt_yes_date}',-12),'-',''), 1, 6),01) then '是' else '否' end as begin_less_1year_flag
	from csx_dim.csx_dim_basic_employee
	where sdt='current'
	-- and employee_status=0
	and card_type='0'
	) e on a.work_no=e.employee_code		
where coalesce(sales_id,'')<>''
group by 
	a.performance_region_code,a.performance_region_name,
	a.performance_province_code,a.performance_province_name,
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	d.profit_basic,
	e.begin_date,
	e.begin_less_1year_flag	
;


-- 目标毛利系数-销售员与客服经理
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc;
create temporary table csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc
as
select *,
	-- case when coalesce(begin_less_1year_flag,'否')='是' and profit_target_rate<1 then profit_target_rate
	-- 	when coalesce(begin_less_1year_flag,'否')='是' and profit_target_rate>=1 then 1
	-- 	when coalesce(begin_less_1year_flag,'否')='否' and profit_target_rate<1 then 0
	-- 	when coalesce(begin_less_1year_flag,'否')='否' and profit_target_rate>=1 then 1
	-- 	else 1 end as profit_target_rate_tc	
	coalesce(if(profit_target_rate>=1,1,profit_target_rate),1) as profit_target_rate_tc			
from 
(
	select
		a.performance_region_code,a.performance_region_name,
		a.performance_province_code,a.performance_province_name,
		a.smonth,
		a.sales_id,a.work_no,a.sales_name,
		-- 多管家毛利额达成排序中的最大值，就高原则
		case when arr[3]=profit_target_rate_1 then begin_date_1
			 when arr[3]=profit_target_rate_2 then begin_date_2
			 when arr[3]=profit_target_rate_3 then begin_date_3
			 when arr[3]=profit_target_rate_4 then begin_date_4
			 end as begin_date,	
		case when arr[3]=profit_target_rate_1 then begin_less_1year_flag_1
			 when arr[3]=profit_target_rate_2 then begin_less_1year_flag_2
			 when arr[3]=profit_target_rate_3 then begin_less_1year_flag_3
			 when arr[3]=profit_target_rate_4 then begin_less_1year_flag_4
			 end as begin_less_1year_flag,	
		
		case when arr[3]=profit_target_rate_1 then profit_basic_1
			 when arr[3]=profit_target_rate_2 then profit_basic_2
			 when arr[3]=profit_target_rate_3 then profit_basic_3
			 when arr[3]=profit_target_rate_4 then profit_basic_4
			 end as profit_basic,
		case when arr[3]=profit_target_rate_1 then profit_1
			 when arr[3]=profit_target_rate_2 then profit_2
			 when arr[3]=profit_target_rate_3 then profit_3
			 when arr[3]=profit_target_rate_4 then profit_4
			 end as profit,
		case when arr[3]=profit_target_rate_1 then profit_target_rate_1
			 when arr[3]=profit_target_rate_2 then profit_target_rate_2
			 when arr[3]=profit_target_rate_3 then profit_target_rate_3
			 when arr[3]=profit_target_rate_4 then profit_target_rate_4
			 end as profit_target_rate
	from 
	(
	select 
		a.performance_region_code,a.performance_region_name,
		a.performance_province_code,a.performance_province_name,
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
)a;



-- 客户+结算月+回款时间系数：各业务类型毛利率提成比例
drop table if exists csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc;
create temporary table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc
as
select *,
	case when rp_profit_rate<0.08 then 0.002
		when rp_profit_rate>=0.08 and rp_profit_rate<0.12 then 0.005
		when rp_profit_rate>=0.12 and rp_profit_rate<0.16 then 0.007
		when rp_profit_rate>=0.16 and rp_profit_rate<0.2 then 0.009
		when rp_profit_rate>=0.2 and rp_profit_rate<0.25 then 0.013
		when rp_profit_rate>=0.25 then 0.015
		else 0.002 end as cust_rp_profit_rate_tc,

	case when bbc_zy_profit_rate<0.08 then 0.002
		when bbc_zy_profit_rate>=0.08 and bbc_zy_profit_rate<0.12 then 0.005
		when bbc_zy_profit_rate>=0.12 and bbc_zy_profit_rate<0.16 then 0.007
		when bbc_zy_profit_rate>=0.16 and bbc_zy_profit_rate<0.2 then 0.009
		when bbc_zy_profit_rate>=0.2 and bbc_zy_profit_rate<0.25 then 0.013
		when bbc_zy_profit_rate>=0.25 then 0.015
		else 0.002 end as cust_bbc_zy_profit_rate_tc,		
		
	case when bbc_ly_profit_rate<0.03 then 0.002
		when bbc_ly_profit_rate>=0.03 and bbc_ly_profit_rate<0.07 then 0.0035
		when bbc_ly_profit_rate>=0.07 and bbc_ly_profit_rate<0.1 then 0.0045
		when bbc_ly_profit_rate>=0.1 and bbc_ly_profit_rate<0.13 then 0.0065
		when bbc_ly_profit_rate>=0.13 and bbc_ly_profit_rate<0.17 then 0.0095
		when bbc_ly_profit_rate>=0.17 and bbc_ly_profit_rate<0.23 then 0.013
		when bbc_ly_profit_rate>=0.23 then 0.015
		else 0.002 end as cust_bbc_ly_profit_rate_tc,

	case when fl_profit_rate<0.03 then 0.002
		when fl_profit_rate>=0.03 and fl_profit_rate<0.07 then 0.0035
		when fl_profit_rate>=0.07 and fl_profit_rate<0.1 then 0.0045
		when fl_profit_rate>=0.1 and fl_profit_rate<0.13 then 0.0065
		when fl_profit_rate>=0.13 and fl_profit_rate<0.17 then 0.0095
		when fl_profit_rate>=0.17 and fl_profit_rate<0.23 then 0.013
		when fl_profit_rate>=0.23 then 0.015
		else 0.002 end as cust_fl_profit_rate_tc		
from 
(
select *,
	profit/abs(sale_amt) as profit_rate,
	rp_profit/abs(rp_sale_amt) as rp_profit_rate,
	bbc_profit/abs(bbc_sale_amt) as bbc_profit_rate,
	bbc_ly_profit/abs(bbc_ly_sale_amt) as bbc_ly_profit_rate,
	bbc_zy_profit/abs(bbc_zy_sale_amt) as bbc_zy_profit_rate,
	fl_profit/abs(fl_sale_amt) as fl_profit_rate
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
		substr(regexp_replace(bill_date,'-',''),1,6) as bill_month, -- 结算月
		dff_rate,
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
	from csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and status=1
	group by 	region_code,
		region_name,
		province_code,
		province_name,
		city_group_code,
		city_group_name,
		customer_code,	-- 客户编码
		customer_name,
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
		dff_rate
)a	
)a;	
	


-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail;
-- create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail partition(smt)
select 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.region_code,a.customer_code,a.bill_month,cast(a.dff_rate as string)) biz_id,
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
	a.fl_sales_sale_fp_rate,
	a.bbc_sales_sale_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
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
	
	((rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_sales_fp_rate)*coalesce(e.new_cust_rate,1)+
	(bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_sales_sale_fp_rate)+
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_sales_sale_fp_rate)+
	(fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_sales_sale_fp_rate))*d1.profit_target_rate_tc as tc_sales,
		
	rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_service_user_fp_rate*d2.profit_target_rate_tc as tc_rp_service,		
		
	fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_service_user_fp_rate*d3.profit_target_rate_tc as tc_fl_service,	

	((bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_service_user_fp_rate)+
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_service_user_fp_rate))*d4.profit_target_rate_tc as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	coalesce(e.new_cust_rate,1) new_cust_rate,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 		
	
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
)a
) e on a.customer_code=e.customer_code
;







-- 结果表
-- drop table if exists csx_analyse_tmp.tmp_tc_customer_detail;
-- create temporary table csx_analyse_tmp.tmp_tc_customer_detail
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_detail partition(smt)
select 
	concat_ws('-',smt_ct,region_code,province_code,city_group_code,customer_code) biz_id,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	customer_code,	-- 客户编码
	customer_name,
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

	-- 目标毛利系数-销售员与客服经理
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
	-- 当月提成金额
	sum(tc_sales) tc_sales,
	sum(tc_rp_service) tc_rp_service,			
	sum(tc_fl_service) tc_fl_service,	
	sum(tc_bbc_service) tc_bbc_service,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	smt_ct,
	new_cust_rate,
	smt -- 统计日期 
-- from csx_analyse_tmp.tmp_tc_customer_order_detail
-- where status=1
from csx_analyse.csx_analyse_fr_tc_customer_order_detail
where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
and status=1
group by concat_ws('-',smt_ct,region_code,province_code,city_group_code,customer_code),
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	customer_code,	-- 客户编码
	customer_name,
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

	-- 目标毛利系数-销售员与客服经理
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
	from_utc_timestamp(current_timestamp(),'GMT'),smt_ct,new_cust_rate,smt;












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


