--HR计算提成核销明细csx_analyse_customer_verification_detail_mf
-- ******************************************************************** 
-- @功能描述：用于HR计算提成核销明细
-- @创建者： 彭承华 
-- @创建者日期：2025-07-16 15:21:44 
-- @修改者日期：
-- @修改人：涉及表
-- @修改内容：job_csx_analyse_hr_fl_customer_commission_mf\job_csx_analyse_fr_tc_customer_credit_order_unpay_mi\job_csx_analyse_fr_hr_business_agent_mi
-- ******************************************************************** 

-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;
-- 关闭平台默认HDFS小文件合并，控制 Tez 引擎在执行 Hive 查询时是否进行文件合并。
SET hive.merge.tezfiles=false;


--  drop table  csx_analyse_tmp.csx_analyse_tmp_bill ;
--  create table csx_analyse_tmp.csx_analyse_tmp_bill as 
with tmp_bill_data as 
(
	select bbc_bill_flag,
		bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		sdt,
		source_bill_no,	-- 来源单号
		CASE WHEN source_sys='BBC' AND substr(source_bill_no,1,1) = 'S' 
          THEN regexp_replace(substr(source_bill_no,3), '^0+', '')
        -- 原有逻辑：以B开头且结尾是(A-E)字母的情况
        WHEN source_sys='BBC' AND substr(split(source_bill_no,'-')[0],1,1)='B' 
             AND substr(split(source_bill_no,'-')[0],-1,1) IN ('A','B','C','D','E')
          THEN substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
        -- 原有逻辑：以B开头但结尾不是(A-E)字母的情况
        WHEN source_sys='BBC' AND substr(split(source_bill_no,'-')[0],1,1)='B' 
             AND substr(split(source_bill_no,'-')[0],-1,1) NOT IN ('A','B','C','D','E')
          THEN substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
        -- 原有逻辑：以R或S开头的情况
        WHEN source_sys='BBC' AND substr(source_bill_no,1,1) IN ('R','S') 
          THEN regexp_replace(source_bill_no,'^[A-Za-z]+|[A-Za-z]+$','')
        -- 默认情况
        ELSE split(source_bill_no,'-')[0]
    END AS source_bill_no_new,	-- 来源单号_新
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
   -- ******************************************************
    -- 非项目制或日期差≤31天：bill_end_date + 1。
    -- 项目制且日期差>31天：
    -- 业务日 ≤ 项目结束日：
    -- 若reconciliation_period=1 → 下个月1号。
    -- 否则 → 安全构造日期（业务月 + 项目结束日），无效时返回下月1号。
    -- 业务日 > 项目结束日 → 下个月1号 + (项目结束日 - 1)天。
    -- *********************************************************
	CASE WHEN COALESCE(project_end_date, '') = ''   OR (COALESCE(project_end_date, '') <> '' AND DATEDIFF(TO_DATE(project_end_date), TO_DATE(happen_date)) <= 31)
        THEN DATE_ADD(TO_DATE(bill_end_date), 1)
        WHEN COALESCE(project_end_date, '') <> '' AND DATEDIFF(TO_DATE(project_end_date), TO_DATE(happen_date)) > 31
        THEN 
            CASE 
                WHEN DAY(TO_DATE(happen_date)) <= DAY(TO_DATE(project_end_date)) AND reconciliation_period = 1 
                THEN TRUNC(ADD_MONTHS(TO_DATE(happen_date), 1), 'MM')
                WHEN DAY(TO_DATE(happen_date)) <= DAY(TO_DATE(project_end_date))
                THEN 
                    -- 安全构造日期：业务发生月的年月 + 项目结束日期的日
                    COALESCE(
                        DATE_ADD(TO_DATE(CONCAT_WS('-',CAST(YEAR(TO_DATE(happen_date)) AS STRING),LPAD(CAST(MONTH(TO_DATE(happen_date)) AS STRING), 2, '0'),
                                LPAD(CAST(DAY(TO_DATE(project_end_date)) AS STRING), 2, '0'))), 
                            1
                        ),
                        -- 若构造的日期无效，则用下个月1号
                        ADD_MONTHS(TRUNC(TO_DATE(happen_date), 'MM'), 1)
                    )
                    
                ELSE 
                    -- 业务发生日期 > 项目结束日期的日：下个月第一天 + (项目结束日 - 1)天
                    DATE_ADD(
                        TRUNC(ADD_MONTHS(TO_DATE(happen_date), 1), 'MM'),
                        CAST(DAY(TO_DATE(project_end_date)) AS INT) - 1
                    )
            END
        
    ELSE 
        DATE_ADD(TO_DATE(bill_end_date), 1)
    END AS 		bill_date,
	overdue_date	-- 逾期开始日期	  
	from    csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	and date_format(happen_date,'yyyy-MM-dd')>='2022-06-01'
	and shipper_code='YHCSX'
),
 tmp_tc_cust_credit_bill_nsale 
as
(select 
	a.bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
	bbc_bill_flag,
	source_bill_no,	-- 来源单号
    source_bill_no_new,
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
	b.paid_date_new, -- 优化取打款日期、无打款日期取实际核销日期（大客户提成使用）
	a.order_amt,	-- 源单据对账金额
	c.unpay_amt,    -- 历史核销剩余金额
	c.pay_amt as history_pay_amt,
	sum(b.pay_amt) pay_amt_old,	-- 原核销金额
	-- 如果本月核销金额大于历史核销剩余金额则按比例折算
	sum(if(c.unpay_amt is null,b.pay_amt,
		if(c.unpay_amt=0,0,
		if(c.unpay_amt<>0 and b.pay_amt_bill<>0 and abs(b.pay_amt_bill/c.unpay_amt)>1,b.pay_amt/abs(b.pay_amt_bill/c.unpay_amt),b.pay_amt)
		))) pay_amt  -- 核销金额，这里考虑后历史核销后又红冲的情况 
from 
(
	select bbc_bill_flag,
		bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		sdt,
		source_bill_no,	-- 来源单号
		source_bill_no_new,	-- 来源单号_新

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
   -- ******************************************************
    -- 非项目制或日期差≤31天：bill_end_date + 1。
    -- 项目制且日期差>31天：
    -- 业务日 ≤ 项目结束日：
    -- 若reconciliation_period=1 → 下个月1号。
    -- 否则 → 安全构造日期（业务月 + 项目结束日），无效时返回下月1号。
    -- 业务日 > 项目结束日 → 下个月1号 + (项目结束日 - 1)天。
    -- *********************************************************
	CASE WHEN COALESCE(project_end_date, '') = ''   OR (COALESCE(project_end_date, '') <> '' AND DATEDIFF(TO_DATE(project_end_date), TO_DATE(happen_date)) <= 31)
        THEN DATE_ADD(TO_DATE(bill_end_date), 1)
        WHEN COALESCE(project_end_date, '') <> '' AND DATEDIFF(TO_DATE(project_end_date), TO_DATE(happen_date)) > 31
        THEN 
            CASE 
                WHEN DAY(TO_DATE(happen_date)) <= DAY(TO_DATE(project_end_date)) AND reconciliation_period = 1 
                THEN TRUNC(ADD_MONTHS(TO_DATE(happen_date), 1), 'MM')
                WHEN DAY(TO_DATE(happen_date)) <= DAY(TO_DATE(project_end_date))
                THEN 
                    -- 安全构造日期：业务发生月的年月 + 项目结束日期的日
                    COALESCE(
                        DATE_ADD(TO_DATE(CONCAT_WS('-',CAST(YEAR(TO_DATE(happen_date)) AS STRING),LPAD(CAST(MONTH(TO_DATE(happen_date)) AS STRING), 2, '0'),
                                LPAD(CAST(DAY(TO_DATE(project_end_date)) AS STRING), 2, '0'))), 
                            1
                        ),
                        -- 若构造的日期无效，则用下个月1号
                        ADD_MONTHS(TRUNC(TO_DATE(happen_date), 'MM'), 1)
                    )
                    
                ELSE 
                    -- 业务发生日期 > 项目结束日期的日：下个月第一天 + (项目结束日 - 1)天
                    DATE_ADD(
                        TRUNC(ADD_MONTHS(TO_DATE(happen_date), 1), 'MM'),
                        CAST(DAY(TO_DATE(project_end_date)) AS INT) - 1
                    )
            END
        
    ELSE 
        DATE_ADD(TO_DATE(bill_end_date), 1)
    END AS 		bill_date,
	overdue_date	-- 逾期开始日期	  
	from    tmp_bill_data
)a
join
(
	select a.close_bill_code,
		-- 用核销日期还是交易日期二选一
		coalesce(b.trade_date,a.paid_date) paid_date_new,
		a.paid_date,		
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
		where sdt>= regexp_replace(last_day(add_months('${sdt_yes_date}',-24)),'-','') -- 缩小时间 近两年数据
		and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		-- and (paid_amt<>0 or residue_amt<>0) -- 剔除补救单和对应原单
		and customer_code not like'G%'
		and shipper_code='YHCSX'
		group by claim_bill_code,substr(trade_time,1,10)
		)b on a.claim_bill_code=b.claim_bill_code
	group by a.close_bill_code,
	-- 用核销日期还是交易日期二选一
	coalesce(b.trade_date,a.paid_date),
	a.paid_date
)b on b.close_bill_code=a.source_bill_no_new
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
		where smt>= regexp_replace(substr(add_months('${sdt_yes_date}',-24),1,7),'-','')  -- 近两年快照
		and smt < regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','')
		-- 8月作为期初核销情况及以后每个计算月当月的核销，作为历史核销过的依据
		and (smt='202401'  
		        or (smt>'202401' and smt<=regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','') 
		        	and substr(sdt,1,6)=smt and substr(sdt,1,6)<regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','') 
		            )
		    )
		and date_format(happen_date,'yyyy-MM-dd')>='2024-01-01'
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
	a.source_bill_no_new,	-- 新的来源单号 ,		
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
	c.unpay_amt,
	paid_date_new,
	c.pay_amt,
	a.bbc_bill_flag
)
insert overwrite table csx_analyse.csx_analyse_customer_verification_detail_mf partition(smt)
select bill_type
        ,source_bill_no
        ,source_bill_no_new
        ,customer_code
        ,credit_code
        ,happen_date
        ,company_code
        ,source_sys
        ,reconciliation_period
        ,bill_date
        ,overdue_date
        ,paid_date
        ,paid_date_new
        ,order_amt
        ,unpay_amt
        ,history_pay_amt
        ,pay_amt_old
        ,pay_amt
        ,current_timestamp()
        ,substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) smonth
        ,sdt
        ,bbc_bill_flag
        ,substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) smt
 from tmp_tc_cust_credit_bill_nsale  a 
    -- where    paid_date>='2025-01-01'
;


