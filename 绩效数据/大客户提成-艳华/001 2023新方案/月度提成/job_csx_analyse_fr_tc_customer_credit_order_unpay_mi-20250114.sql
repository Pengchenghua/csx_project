-- ******************************************************************** 
-- @功能描述：csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi
-- @创建者： 饶艳华 
-- @创建者日期：2023-11-03 14:29:56 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 


-- 调整am内存
SET
  tez.am.resource.memory.mb = 4096;
set hive.tez.container.size=8192;

-- 销售单业绩毛利
-- oc返利单的可能一个返利单对应多个原单号，original_order_code
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_sale_order;
-- create temporary table csx_analyse_tmp.tmp_tc_cust_sale_order
-- as
with tmp_tc_cust_sale_order as (
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
	    and shipper_code='YHCSX'
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	    and (business_type_code in('1','2','6')
		or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
		or (business_type_code in ('4') and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))
		-- or (business_type_code in ('4') and customer_code in ('235949','222853','131428','131466','131202','131208','129746','128435','230788',
		-- '112846','118357','115832','125795','125831','131462','114496','117322','131421','118395','114470','130024','130430','118644','131091',
		-- '217946','129955','130226','120115','226821','129870','129865','130269','126125','129674','129880','227563','129855','129860','130955',
		-- '127521','225541','232102','233354','234828','130844','223112','129854','125545','128705','125513','126001'))
		or (business_type_code in ('4') and customer_code in (
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and (category_second like '%纳入大客户提成计算：项目供应商%' or category_second like '%纳入大客户提成计算：前置仓%')
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
	    and shipper_code='YHCSX'
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
			and (category_second like '%纳入大客户提成计算：项目供应商%' or category_second like '%纳入大客户提成计算：前置仓%')
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
),





-- 注意：调价返利数据快照中订单号可能变化，8月OC23072500033-1，9月OC23072500033
-- 结算单中本月回款核销金额 限定本月核销单但是以认领单中的打款日期计算回款时间系数
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale;
-- create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale
tmp_tc_cust_credit_bill_nsale 
as
(select 
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
	and shipper_code='YHCSX'
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
				-- 	and shipper_code='YHCSX'
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
		and shipper_code='YHCSX'
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
	c.unpay_amt
),


-- 结算单中本月回款核销金额 限定本月核销单但是以认领单中的打款日期计算回款时间系数
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill;
-- create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill
tmp_tc_cust_credit_bill 
as
(
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
from tmp_tc_cust_credit_bill_nsale a
-- 销售单业绩毛利
left join tmp_tc_cust_sale_order b on b.order_code=a.source_bill_no
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
from tmp_tc_cust_credit_bill_nsale a
left join tmp_tc_cust_sale_order b on b.order_code=a.source_bill_no
left join tmp_tc_cust_sale_order c on c.order_code=a.source_bill_no_new 
where b.sale_amt is null
)


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
from tmp_tc_cust_credit_bill a 
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