set hive.tez.container.size = 8192;
-- ******************************************************************** 
-- @功能描述：
-- @创建者： 饶艳华 
-- @创建者日期：2023-11-03 14:29:56 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
-- 调整am内存
SET tez.am.resource.memory.mb = 4096;
set hive.tez.container.size = 8192;
-- 销售单业绩毛利
-- oc返利单的可能一个返利单对应多个原单号，original_order_code
drop table if exists csx_analyse_tmp.tmp_tc_fl_cust_sale_order;
-- drop table if exists csx_analyse_tmp.tmp_tc_fl_cust_sale_order;
create table csx_analyse_tmp.tmp_tc_fl_cust_sale_order as 

with tmp_tc_fl_cust_sale_order as (
	select order_code,
		business_type_code,
		business_type_name,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit,
		sum(sale_amt_jiushui) as sale_amt_jiushui,
		sum(profit_jiushui) as profit_jiushui
	from (
			select order_code,
				-- operation_mode_code,
				case
					when business_type_code in(6)
					and operation_mode_code = 1 then 6.1
					when business_type_code in(6)
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 6.2
					when business_type_code = 2
					and operation_mode_code = 1 then 2.1
					when business_type_code = 2
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 2.2
					else business_type_code
				end business_type_code,
				case
					when business_type_code in(6)
					and operation_mode_code = 1 then 'BBC联营'
					when business_type_code in(6)
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 'BBC自营'
					when business_type_code = 2
					and operation_mode_code = 1 then '福利联营'
					when business_type_code = 2
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then '福利自营'
					else business_type_name
				end business_type_name,
				-- sum(profit) as profit,
				sum(
					case
						when goods_code not in ('8718', '8708', '8649', '840509') then sale_amt
					end
				) as sale_amt,
				sum(
					case
						when goods_code not in ('8718', '8708', '8649', '840509') then profit
					end
				) as profit,
				sum(
					case
						when goods_code in ('8718', '8708', '8649', '840509') then sale_amt
					end
				) as sale_amt_jiushui,
				sum(
					case
						when goods_code in ('8718', '8708', '8649', '840509') then profit
					end
				) as profit_jiushui
			from csx_dws.csx_dws_sale_detail_di
			where channel_code in('1', '7', '9')
				and shipper_code = 'YHCSX' -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
				and business_type_code in('2', '6')
				and order_channel_detail_code NOT IN ('24', '28')
				and sdt>='20240101'
			group by order_code,
				case
					when business_type_code in(6)
					and operation_mode_code = 1 then 6.1
					when business_type_code in(6)
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 6.2
					when business_type_code = 2
					and operation_mode_code = 1 then 2.1
					when business_type_code = 2
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 2.2
					else business_type_code
				end,
				case
					when business_type_code in(6)
					and operation_mode_code = 1 then 'BBC联营'
					when business_type_code in(6)
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 'BBC自营'
					when business_type_code = 2
					and operation_mode_code = 1 then '福利联营'
					when business_type_code = 2
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then '福利自营'
					else business_type_name
				end
			union all
			-- 价格补救需用原单
			select original_order_code order_code,
				case
					when business_type_code in(6)
					and operation_mode_code = 1 then 6.1
					when business_type_code in(6)
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 6.2
					when business_type_code = 2
					and operation_mode_code = 1 then 2.1
					when business_type_code = 2
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 2.2
					else business_type_code
				end business_type_code,
				case
					when business_type_code in(6)
					and operation_mode_code = 1 then 'BBC联营'
					when business_type_code in(6)
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 'BBC自营'
					when business_type_code = 2
					and operation_mode_code = 1 then '福利联营'
					when business_type_code = 2
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then '福利自营'
					else business_type_name
				end business_type_name,
				-- sum(sale_amt) as sale_amt,
				-- sum(profit) as profit,
				sum(
					case
						when goods_code not in ('8718', '8708', '8649', '840509') then sale_amt
					end
				) as sale_amt,
				sum(
					case
						when goods_code not in ('8718', '8708', '8649', '840509') then profit
					end
				) as profit,
				sum(
					case
						when goods_code in ('8718', '8708', '8649', '840509') then sale_amt
					end
				) as sale_amt_jiushui,
				sum(
					case
						when goods_code in ('8718', '8708', '8649', '840509') then profit
					end
				) as profit_jiushui
			from csx_dws.csx_dws_sale_detail_di
			where channel_code in('1', '7', '9')
				and shipper_code = 'YHCSX' -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
				and order_channel_code = 5 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
				and business_type_code in('2', '6')
				and order_channel_detail_code not in ('24', '28')
				and sdt>='20240101'
			group by original_order_code,
				case
					when business_type_code in(6)
					and operation_mode_code = 1 then 6.1
					when business_type_code in(6)
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 6.2
					when business_type_code = 2
					and operation_mode_code = 1 then 2.1
					when business_type_code = 2
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 2.2
					else business_type_code
				end,
				case
					when business_type_code in(6)
					and operation_mode_code = 1 then 'BBC联营'
					when business_type_code in(6)
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then 'BBC自营'
					when business_type_code = 2
					and operation_mode_code = 1 then '福利联营'
					when business_type_code = 2
					and (
						operation_mode_code = 0
						or operation_mode_code is null
					) then '福利自营'
					else business_type_name
				end
		) a
	group by order_code,
		business_type_code,
		business_type_name
),
-- 注意：调价返利数据快照中订单号可能变化，8月OC23072500033-1，9月OC23072500033
-- 结算单中本月回款核销金额 限定本月核销单但是以认领单中的打款日期计算回款时间系数
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale;
-- create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill_nsale
tmp_tc_cust_credit_bill_nsale as (
	select a.bill_type,
		-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		a.sdt,
		split(a.source_bill_no, '-') [0] source_bill_no,
		-- 来源单号
		case
			when source_sys = 'BBC'
			and substr(split(a.source_bill_no, '-') [0], 1, 1) = 'B'
			and substr(split(a.source_bill_no, '-') [0], -1, 1) in ('A', 'B', 'C', 'D', 'E') then substr(
				split(a.source_bill_no, '-') [0],
				2,
				length(split(a.source_bill_no, '-') [0]) -2
			)
			when source_sys = 'BBC'
			and substr(split(a.source_bill_no, '-') [0], 1, 1) = 'B'
			and substr(split(a.source_bill_no, '-') [0], -1, 1) not in ('A', 'B', 'C', 'D', 'E') then substr(
				split(a.source_bill_no, '-') [0],
				2,
				length(split(a.source_bill_no, '-') [0]) -1
			)
			else split(a.source_bill_no, '-') [0]
		end as source_bill_no_new,
		-- a.source_bill_no,	-- 来源单号
		-- case when source_sys='BBC' and substr(a.source_bill_no,1,1)='B' and substr(a.source_bill_no,-1,1) in ('A','B','C','D','E') then substr(a.source_bill_no,2,length(a.source_bill_no)-2)
		-- 	 when source_sys='BBC' and substr(a.source_bill_no,1,1)='B' and substr(a.source_bill_no,-1,1) not in ('A','B','C','D','E')then substr(a.source_bill_no,2,length(a.source_bill_no)-1)
		-- 	 else a.source_bill_no
		-- 	 end as source_bill_no_new,
		a.customer_code,
		-- 客户编码
		a.credit_code,
		-- 信控号
		a.happen_date,
		-- 发生时间		
		a.company_code,
		-- 签约公司编码
		-- a.account_period_code,	-- 账期编码
		-- a.account_period_name,	-- 账期名称
		-- a.account_period_value,	-- 账期值
		a.source_sys,
		-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
		a.reconciliation_period,
		-- 对账周期
		a.bill_date,
		-- 结算日期
		a.overdue_date,
		-- 逾期开始日期	
		b.paid_date,
		b.real_paid_date , -- 实际核销日期

		-- 核销日期
		a.order_amt,
		-- 源单据对账金额
		c.unpay_amt,
		-- 历史核销剩余金额
		sum(b.pay_amt) pay_amt_old,
		-- 核销金额
		-- 如果本月核销金额大于历史核销剩余金额则按比例折算
		sum(
			if(
				c.unpay_amt is null,
				b.pay_amt,
				if(
					c.unpay_amt = 0,
					0,
					if(
						c.unpay_amt <> 0
						and b.pay_amt_bill <> 0
						and abs(b.pay_amt_bill / c.unpay_amt) > 1,
						b.pay_amt / abs(b.pay_amt_bill / c.unpay_amt),
						b.pay_amt
					)
				)
			)
		) pay_amt -- 核销金额
	from (
			select bill_type,
				-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
				sdt,
				source_bill_no,
				-- 来源单号
				-- case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
				-- 	when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
				-- 	else split(source_bill_no,'-')[0]
				-- 	end as source_bill_no_new,		
				customer_code,
				-- 客户编码
				credit_code,
				-- 信控号
				happen_date,
				-- 发生时间
				order_amt,
				-- 源单据对账金额
				company_code,
				-- 签约公司编码
				residue_amt,
				-- 剩余预付款金额_预付款客户抵消订单金额后
				residue_amt_sss,
				-- 剩余预付款金额_原销售结算
				unpaid_amount,
				-- 未回款金额_抵消预付款后
				unpaid_amount_sss,
				-- 未回款金额_原销售结算
				bad_debt_amount,
				-- 坏账金额
				account_period_code,
				-- 账期编码
				account_period_name,
				-- 账期名称
				account_period_value,
				-- 账期值
				source_sys,
				-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
				close_bill_amount pay_amt,
				-- 核销金额
				reconciliation_period,
				-- 对账周期
				project_end_date,
				-- 项目制结束日期
				project_begin_date,
				-- 项目制开始日期
				bill_start_date,
				-- 账期周期开始时间
				bill_end_date,
				-- 账期周期结束时间
				-- date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1) bill_date, -- 结算日期
				-- 若不是项目制或者项目制且项目制结束日期距离业务发生日期小于等于31天，则常规结算日不变（账期周期结束时间+1）
				--  否则看项目制结束日期距离发生日期大于31天：①日期部分业务发生日期≤项目制结束日期，则业务发生月+项目制结束日期的日期；②期部分业务发生日期大于项目制结束日期，则业务发生次月+项目制结束日期的日期
				case
					when coalesce(project_end_date, '') = '' then date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1)
					when coalesce(project_end_date, '') <> '' then (
						case
							when datediff(
								date_format(project_end_date, 'yyyy-MM-dd'),
								date_format(happen_date, 'yyyy-MM-dd')
							) <= 31 then date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1)
							when substr(project_end_date, 9, 2) >= substr(happen_date, 9, 2)
							and reconciliation_period = 1 then trunc(
								add_months(date_format(happen_date, 'yyyy-MM-dd'), 1),
								'MM'
							)
							when substr(project_end_date, 9, 2) >= substr(happen_date, 9, 2) then date_add(
								date_format(
									concat(
										substr(happen_date, 1, 8),
										substr(project_end_date, 9, 2)
									),
									'yyyy-MM-dd'
								),
								1
							)
							else date_add(
								trunc(
									add_months(date_format(happen_date, 'yyyy-MM-dd'), 1),
									'MM'
								),
								cast(substr(project_end_date, 9, 2) as int)
							)
						end
					)
					else date_add(date_format(bill_end_date, 'yyyy-MM-dd'), 1)
				end as bill_date,
				-- 结算日期
				overdue_date -- 逾期开始日期	  
			from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di -- where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			where sdt = regexp_replace(add_months(date_sub(current_date, 1), 0), '-', '') -- and date_format(happen_date,'yyyy-MM-dd')>='2020-06-01'
				and date_format(happen_date, 'yyyy-MM-dd') >= '2024-01-01'
				and shipper_code = 'YHCSX' -- and customer_code='127307'
		) a
		join (
			select a.close_bill_code,
				-- 用核销日期还是交易日期二选一
				coalesce(b.trade_date, a.paid_date) paid_date,
				a.paid_date real_paid_date,		
				max(a.pay_amt_bill) pay_amt_bill,
				sum(a.pay_amt) pay_amt
			from (
					-- 核销流水明细表:本月核销金额
					select *,
						sum (pay_amt) over(partition by close_bill_code) as pay_amt_bill
					from (
							select close_bill_code,
								claim_bill_code,
								close_account_code,
								-- customer_code,credit_code,company_code,
								date_format(
									from_unixtime(unix_timestamp(sdt, 'yyyyMMdd')),
									'yyyy-MM-dd'
								) paid_date,
								-- sum (pay_amt) over(partition by close_bill_code) as pay_amt_bill,
								sum(pay_amt) pay_amt -- from csx_dwd.csx_dwd_sss_close_bill_account_record_di
								-- 单据核销流水明细月快照表
							from csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf -- 核销日期分区
							where smt = regexp_replace(
									substr(add_months('${sdt_yes_date}', -1), 1, 7),
									'-',
									''
								)
								and sdt >= regexp_replace(
									add_months(trunc('${sdt_yes_date}', 'MM'), -1),
									'-',
									''
								)
								and sdt <= regexp_replace(
									last_day(add_months('${sdt_yes_date}', -1)),
									'-',
									''
								)
								and date_format(happen_date, 'yyyy-MM-dd') >= '2024-01-01'
								and delete_flag = '0' -- 	and shipper_code='YHCSX'
							group by close_bill_code,
								claim_bill_code,
								close_account_code,
								date_format(
									from_unixtime(unix_timestamp(sdt, 'yyyyMMdd')),
									'yyyy-MM-dd'
								)
						) a
				) a -- 核销单中根据认领单找打款日期
				left join (
					select claim_bill_code,
						substr(trade_time, 1, 10) trade_date,
						count(1) aa
					from csx_dwd.csx_dwd_sss_money_back_di
					where sdt >= '20240101'
						and sdt <= regexp_replace(
							last_day(add_months('${sdt_yes_date}', -1)),
							'-',
							''
						) -- and (paid_amt<>0 or residue_amt<>0) -- 剔除补救单和对应原单
						and customer_code not like 'G%'
						and shipper_code = 'YHCSX'
					group by claim_bill_code,
						substr(trade_time, 1, 10)
				) b on a.claim_bill_code = b.claim_bill_code
			group by a.close_bill_code,
				-- 用核销日期还是交易日期二选一
				coalesce(b.trade_date, a.paid_date), -- 
				a.paid_date
		) b on b.close_bill_code = a.source_bill_no -- 核销月度快照表中202308后历史月快照中订单合计核销金额，被计算过的核销金额大于原单金额的，不再计算本月核销金额
		left join (
			select *
			from (
					select close_bill_code,
						max(bill_amt) bill_amt,
						sum(pay_amt) pay_amt,
						if(
							max(bill_amt) > 0,
							if(
								max(bill_amt) - sum(pay_amt) > 0,
								max(bill_amt) - sum(pay_amt),
								0
							),
							if(
								max(bill_amt) - sum(pay_amt) < 0,
								max(bill_amt) - sum(pay_amt),
								0
							)
						) unpay_amt -- 单据核销流水明细月快照表
					from csx_ads.csx_ads_sss_close_bill_account_record_snapshot_mf -- 202308及以后月快照中8月所有核销情况及以后每月快照中当月核销
					where smt >= '202308'
						and smt < regexp_replace(
							substr(add_months('${sdt_yes_date}', -1), 1, 7),
							'-',
							''
						) -- 8月作为期初核销情况及以后每个计算月当月的核销，作为历史核销过的依据
						and (
							smt = '202501'
							or (
								smt > '202501'
								and smt <= regexp_replace(
									substr(add_months('${sdt_yes_date}', -1), 1, 7),
									'-',
									''
								)
								and substr(sdt, 1, 6) = smt
								and substr(sdt, 1, 6) < regexp_replace(
									substr(add_months('${sdt_yes_date}', -1), 1, 7),
									'-',
									''
								)
							)
						)
						and date_format(happen_date, 'yyyy-MM-dd') >= '2024-01-01'
						and delete_flag = '0'
					group by close_bill_code
				) a -- where abs(bill_amt)<=abs(pay_amt)
		) c on c.close_bill_code = b.close_bill_code -- 本月新增订单或历史订单剩余未核销金额不为0
		-- where c.bill_amt is null or c.unpay_amt<>0
	group by a.bill_type,
		-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		a.sdt,
		a.source_bill_no,
		-- 来源单号
		case
			when source_sys = 'BBC'
			and substr(a.source_bill_no, 1, 1) = 'B'
			and substr(a.source_bill_no, -1, 1) in ('A', 'B', 'C', 'D', 'E') then substr(a.source_bill_no, 2, length(a.source_bill_no) -2)
			when source_sys = 'BBC'
			and substr(a.source_bill_no, 1, 1) = 'B'
			and substr(a.source_bill_no, -1, 1) not in ('A', 'B', 'C', 'D', 'E') then substr(a.source_bill_no, 2, length(a.source_bill_no) -1)
			else a.source_bill_no
		end,
		a.customer_code,
		-- 客户编码
		a.credit_code,
		-- 信控号
		a.happen_date,
		-- 发生时间		
		a.company_code,
		-- 签约公司编码
		a.source_sys,
		-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
		a.reconciliation_period,
		-- 对账周期
		a.bill_date,
		-- 结算日期
		a.overdue_date,
		-- 逾期开始日期	
		b.paid_date,
		real_paid_date,
		a.order_amt,
		c.unpay_amt
),
-- 结算单中本月回款核销金额 限定本月核销单但是以认领单中的打款日期计算回款时间系数
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_credit_bill;
-- create temporary table csx_analyse_tmp.tmp_tc_cust_credit_bill
tmp_tc_cust_credit_bill as (
	select distinct a.bill_type,
		-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		a.sdt,
		a.source_bill_no,
		-- 来源单号
		a.customer_code,
		-- 客户编码
		a.credit_code,
		-- 信控号
		a.happen_date,
		-- 发生时间		
		a.company_code,
		-- 签约公司编码
		-- a.account_period_code,	-- 账期编码
		-- a.account_period_name,	-- 账期名称
		-- a.account_period_value,	-- 账期值
		a.source_sys,
		-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
		a.reconciliation_period,
		-- 对账周期
		a.bill_date,
		-- 结算日期
		a.overdue_date,
		-- 逾期开始日期	
		a.paid_date,
		a.real_paid_date,
		-- 核销日期	
		a.order_amt,
		-- 源单据对账金额 
		a.unpay_amt,
		a.pay_amt_old,
		a.pay_amt,
		-- 核销金额
		if(a.source_sys = 'BEGIN', 1, b.business_type_code) business_type_code,
		if(a.source_sys = 'BEGIN', '日配业务', b.business_type_name) business_type_name,
		b.sale_amt,
		b.profit,
		b.sale_amt_jiushui,
		b.profit_jiushui
	from tmp_tc_cust_credit_bill_nsale a -- 销售单业绩毛利
		left join tmp_tc_fl_cust_sale_order b on b.order_code = a.source_bill_no
	where b.sale_amt is not null
	union all
	select distinct a.bill_type,
		-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		a.sdt,
		a.source_bill_no,
		-- 来源单号
		a.customer_code,
		-- 客户编码
		a.credit_code,
		-- 信控号
		a.happen_date,
		-- 发生时间		
		a.company_code,
		-- 签约公司编码
		-- a.account_period_code,	-- 账期编码
		-- a.account_period_name,	-- 账期名称
		-- a.account_period_value,	-- 账期值
		a.source_sys,
		-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
		a.reconciliation_period,
		-- 对账周期
		a.bill_date,
		-- 结算日期
		a.overdue_date,
		-- 逾期开始日期	
		a.paid_date,
		a.paid_date as real_paid_date,
		-- 核销日期	
		a.order_amt,
		-- 源单据对账金额 
		a.unpay_amt,
		a.pay_amt_old,
		a.pay_amt,
		-- 核销金额
		if(a.source_sys = 'BEGIN', 1, c.business_type_code) business_type_code,
		if(a.source_sys = 'BEGIN', '日配业务', c.business_type_name) business_type_name,
		c.sale_amt,
		c.profit,
		c.sale_amt_jiushui,
		c.profit_jiushui
	from tmp_tc_cust_credit_bill_nsale a
		left join tmp_tc_fl_cust_sale_order b on b.order_code = a.source_bill_no
		left join tmp_tc_fl_cust_sale_order c on c.order_code = a.source_bill_no_new
	where b.sale_amt is null
) -- insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_credit_order_unpay_mi partition(smt)
select concat_ws(
		'-',
		substr(
			regexp_replace(
				last_day(add_months('${sdt_yes_date}', -1)),
				'-',
				''
			),
			1,
			6
		),
		a.customer_code,
		a.credit_code,
		a.source_bill_no
	) biz_id,
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
	a.bill_type,
	-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	a.sdt,
	a.source_bill_no,
	-- 来源单号
	a.customer_code,
	-- 客户编码
	b.customer_name,
	a.credit_code,
	-- 信控号
	a.happen_date,
	-- 发生时间		
	a.company_code,
	-- 签约公司编码
	a.source_sys,
	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,
	-- 对账周期
	a.bill_date,
	-- 结算日期
	a.overdue_date,
	-- 逾期开始日期	
	-- 	case 
	-- 	when (regexp_replace(substr(a.paid_date,1,10),'-','') between c.date_star and c.date_end) and c.adjust_business_type='全业务' then c.paid_date_new
	-- 	when (regexp_replace(substr(a.paid_date,1,10),'-','') between c.date_star and c.date_end) and c.adjust_business_type='日配' and a.business_type_name like '日配%' then c.paid_date_new
	-- 	when (regexp_replace(substr(a.paid_date,1,10),'-','') between c.date_star and c.date_end) and c.adjust_business_type='BBC' and a.business_type_name like 'BBC%' then c.paid_date_new
	-- 	when (regexp_replace(substr(a.paid_date,1,10),'-','') between c.date_star and c.date_end) and c.adjust_business_type='福利' and a.business_type_name like '福利%' then c.paid_date_new
	-- 	else a.paid_date end as paid_date,	
	-- 核销日期	
	a.paid_date,
	real_paid_date,
	-- 核销日期	 
	a.order_amt,
	-- 源单据对账金额 
	a.unpay_amt,
	a.pay_amt_old,
	a.pay_amt,
	-- 核销金额
	a.business_type_code,
	a.business_type_name,
	-- 	a.status,  -- 是否有效 0.无效 1.有效
	a.sale_amt,
	a.profit,
	a.sale_amt_jiushui,
	a.profit_jiushui,
	from_utc_timestamp(current_timestamp(), 'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)),'-',''),1,6) as smt_ct,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)),'-',''),1,6) as smt -- 统计日期 	
from tmp_tc_cust_credit_bill a
	left join (
		select *
		from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
		where smt = regexp_replace(substr(add_months('${sdt_yes_date}', -1), 1, 7),'-','')
	) b on a.customer_code = b.customer_no;


	
-- 纯现金客户标记 月BBC销售金额大于0，月授信支付金额等于0
drop table if exists csx_analyse_tmp.csx_analyse_tmp_tc_fl_cust_chunxianjin;
create table csx_analyse_tmp.csx_analyse_tmp_tc_fl_cust_chunxianjin as
select a.customer_code,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)),'-',''),1,6) smonth,
	a.sale_amt,
	a.profit,
	if(
		a.bbc_sale_amt > 0
		and a.sale_amt = a.bbc_sale_amt
		and coalesce(a.credit_settle_amount, 0) = 0,
		'是',
		'否'
	) is_chunxianjin
from (
		select a.customer_code,
			a.smonth,
			sum(a.sale_amt) as sale_amt,
			sum(a.profit) as profit,
			sum(a.bbc_sale_amt) as bbc_sale_amt,
			sum(b.credit_settle_amount) as credit_settle_amount -- 授信结算金额
		from (
				select business_type_name,
					performance_province_name,
					customer_code,
					customer_name,
					original_order_code,
					substr(sdt, 1, 6) as smonth,
					sum(sale_amt) as sale_amt,
					sum(profit) as profit,
					sum(
						case
							when business_type_code in('1', '4', '5', '6') then sale_amt
							else 0
						end
					) as rp_bbc_sale_amt,
					sum(
						case
							when business_type_code in(6) then sale_amt
							else 0
						end
					) as bbc_sale_amt,
					sum(
						case
							when business_type_code in(2) then sale_amt
							else 0
						end
					) as fl_sale_amt
				from csx_dws.csx_dws_sale_detail_di
				where sdt >= regexp_replace(add_months(trunc('${sdt_yes_date}', 'MM'), -1),'-','')
					and sdt <= regexp_replace(last_day(add_months('${sdt_yes_date}', -1)),'-',	''	)
					 -- and channel_code in('1','7','9')
					and business_type_code in(2, 6)
					and order_channel_detail_code not in ('24', '28')
				group by business_type_name,
					performance_province_name,
					customer_code,
					customer_name,
					original_order_code,
					substr(sdt, 1, 6)
			) a
			left join (
				select order_code,
					sum(credit_settle_amt) credit_settle_amount
				from (
						select distinct order_code,
							bill_code,
							credit_settle_amt
						from csx_dwd.csx_dwd_bbc_wshop_bill_order_detail_di
						where sdt >= regexp_replace(add_months(trunc('${sdt_yes_date}', 'MM'), -5),	'-','')
					) tmp
				group by order_code
			) b on b.order_code = a.original_order_code
		group by a.customer_code,
			a.smonth
	) a
where a.bbc_sale_amt > 0
	and a.sale_amt = a.bbc_sale_amt
	and coalesce(a.credit_settle_amount, 0) = 0

;

-- select *
-- from csx_analyse_tmp.tmp_tc_fl_cust_credit_bill_xianjin_bujiu -- 结算单回款+BBC纯现金客户
drop table if exists csx_analyse_tmp.tmp_tc_fl_cust_credit_bill_xianjin_bujiu;
create table csx_analyse_tmp.tmp_tc_fl_cust_credit_bill_xianjin_bujiu 
	as with tmp_tc_cust_order_detail as -- 核销订单明细+纯现金核销
	(
		select bill_type,
			-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
			sdt,
			source_bill_no,
			-- 来源单号
			customer_code,
			-- 客户编码
			credit_code,
			-- 信控号
			happen_date,
			-- 发生时间		
			company_code,
			-- 签约公司编码
			source_sys,
			-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
			reconciliation_period,
			-- 对账周期
			date_format(bill_date, 'yyyy-MM-dd') bill_date,
			-- 结算日期
			overdue_date,
			-- 逾期开始日期	
			paid_date,
			real_paid_date,
			-- 核销日期	
			order_amt,
			-- 源单据对账金额
			unpay_amt,
			pay_amt,
			-- 核销金额
			cast(business_type_code as decimal(2, 1)) business_type_code,
			business_type_name,
			-- 	cast(status as int) status,  -- 是否有效 0.无效 1.有效
			sale_amt,
			profit,
			sale_amt_jiushui,
			profit_jiushui
		from csx_analyse_tmp.tmp_tc_fl_cust_sale_order -- where smt=regexp_replace(substr(add_months('${sdt_yes_date}',-1),1,7),'-','')
			-- 纯现金
		union all
		select null as bill_type,
			-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
			b.sdt,
			b.order_code as source_bill_no,
			-- 来源单号
			b.customer_code,
			-- 客户编码
			b.credit_code,
			-- 信控号
			date_format(from_unixtime(unix_timestamp(b.sdt, 'yyyyMMdd')),'yyyy-MM-dd') happen_date,
			-- 发生时间		
			b.company_code,
			-- 签约公司编码
			'纯现金' as source_sys,
			-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
			null reconciliation_period,
			-- 对账周期
			date_format(from_unixtime(unix_timestamp(b.sdt, 'yyyyMMdd')),'yyyy-MM-dd') bill_date,
			-- 结算日期
			date_format(from_unixtime(unix_timestamp(b.sdt, 'yyyyMMdd')),'yyyy-MM-dd') overdue_date,
			-- 逾期开始日期	
			date_format(from_unixtime(unix_timestamp(b.sdt, 'yyyyMMdd')),'yyyy-MM-dd') paid_date,
			date_format(from_unixtime(unix_timestamp(b.sdt, 'yyyyMMdd')),'yyyy-MM-dd') real_paid_date,
			-- 核销日期	
			b.sale_amt order_amt,
			-- 源单据对账金额
			null as unpay_amt,
			b.sale_amt pay_amt,
			-- 核销金额
			b.business_type_code,
			b.business_type_name,
			-- 	b.status,  -- 是否有效 0.无效 1.有效
			b.sale_amt,
			b.profit,
			b.sale_amt_jiushui,
			b.profit_jiushui
		from csx_analyse_tmp.csx_analyse_tmp_tc_fl_cust_chunxianjin a
			join (
				select company_code,
					case
						when business_type_code in(6)
						and operation_mode_code = 1 then 6.1
						when business_type_code in(6)
						and (
							operation_mode_code = 0
							or operation_mode_code is null
						) then 6.2
						when business_type_code = 2
						and operation_mode_code = 1 then 2.1
						when business_type_code = 2
						and (
							operation_mode_code = 0
							or operation_mode_code is null
						) then 2.2
					end business_type_code,
					case
						when business_type_code in(6)
						and operation_mode_code = 1 then 'BBC联营'
						when business_type_code in(6)
						and (
							operation_mode_code = 0
							or operation_mode_code is null
						) then 'BBC自营'
						when business_type_code = 2
						and operation_mode_code = 1 then '福利联营'
						when business_type_code = 2
						and (
							operation_mode_code = 0
							or operation_mode_code is null
						) then '福利自营'
					end business_type_name,
					performance_province_name,
					customer_code,
					credit_code,
					order_code,
					sdt,
					-- 		if(performance_province_name='福建省' and inventory_dc_name like '%V2DC%',0,1) status,  -- 是否有效 0.无效 1.有效
					-- sum(sale_amt) as sale_amt,
					-- sum(profit) as profit,
					sum(
						case
							when goods_code not in ('8718', '8708', '8649', '840509') then sale_amt
						end
					) as sale_amt,
					sum(
						case
							when goods_code not in ('8718', '8708', '8649', '840509') then profit
						end
					) as profit,
					sum(
						case
							when goods_code in ('8718', '8708', '8649', '840509') then sale_amt
						end
					) as sale_amt_jiushui,
					sum(
						case
							when goods_code in ('8718', '8708', '8649', '840509') then profit
						end
					) as profit_jiushui
				from csx_dws.csx_dws_sale_detail_di
				where sdt >= regexp_replace(
						add_months(trunc('${sdt_yes_date}', 'MM'), -1),
						'-',
						''
					)
					and sdt <= regexp_replace(
						last_day(add_months('${sdt_yes_date}', -1)),
						'-',
						''
					) -- and channel_code in('1','7','9')	
					and business_type_code in(2, 6)
					and order_channel_detail_code not in ('24', '28')
				group by company_code,
					case
						when business_type_code in(6)
						and operation_mode_code = 1 then 6.1
						when business_type_code in(6)
						and (
							operation_mode_code = 0
							or operation_mode_code is null
						) then 6.2
						when business_type_code = 2
						and operation_mode_code = 1 then 2.1
						when business_type_code = 2
						and (
							operation_mode_code = 0
							or operation_mode_code is null
						) then 2.2
					end,
					case
						when business_type_code in(6)
						and operation_mode_code = 1 then 'BBC联营'
						when business_type_code in(6)
						and (
							operation_mode_code = 0
							or operation_mode_code is null
						) then 'BBC自营'
						when business_type_code = 2
						and operation_mode_code = 1 then '福利联营'
						when business_type_code = 2
						and (
							operation_mode_code = 0
							or operation_mode_code is null
						) then '福利自营'
					end,
					performance_province_name,
					customer_code,
					credit_code,
					order_code,
					sdt
			) b on a.customer_code = b.customer_code
	),
	customer_company_details as (
		select customer_code customer_no,
			customer_name,
			company_code,
			company_name,
			credit_code,
			performance_city_code city_group_code,
			performance_city_name city_group_name,
			performance_province_code province_code,
			performance_province_name province_name,
			performance_region_code region_code,
			performance_region_name region_name,
			channel_code,
			--  渠道编码
			channel_name,
			--  渠道名称
			sales_user_id sales_id,
			--  销售员id
			sales_user_number work_no,
			--  销售员工号
			sales_user_name sales_name,
			--  销售员名称
			account_period_code,
			--  账期类型
			account_period_name,
			--  账期名称
			account_period_value,
			--  帐期天数
			credit_limit,
			--  信控额度
			temp_credit_limit,
			--  临时额度
			temp_begin_time,
			--  临时额度起始时间
			temp_end_time,
			--  临时额度截止时间
			business_attribute_code,
			-- 信控业务属性编码
			business_attribute_name -- 信控业务属性名称
		from csx_dim.csx_dim_crm_customer_company_details
		where sdt = 'current'
			and status = 1
	)
select a.bill_type,
	-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
	-- a.sdt,
	a.source_bill_no,
	-- 来源单号
	a.customer_code,
	-- 客户编码
	d.customer_name,
	a.credit_code,
	-- 信控号
	a.happen_date,
	-- 发生时间		
	a.company_code,
	-- 签约公司编码
	c.account_period_code,
	-- 账期编码
	c.account_period_name,
	-- 账期名称
	c.account_period_value,
	-- 账期值
	a.source_sys,
	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
	a.reconciliation_period,
	-- 对账周期
	a.bill_date,
	-- 结算日期
	a.overdue_date,
	-- 逾期开始日期	
	a.paid_date,
	a.real_padi_date,
	-- 核销日期	
	datediff(a.paid_date, a.bill_date) dff,
	case
		when datediff(a.paid_date, a.bill_date) <= 15 then 1.1
		when datediff(a.paid_date, a.bill_date) <= 31 then 1
		when datediff(a.paid_date, a.bill_date) <= 60 then 0.8
		when datediff(a.paid_date, a.bill_date) <= 90 then 0.6
		when datediff(a.paid_date, a.bill_date) <= 120 then 0.4
		when datediff(a.paid_date, a.bill_date) <= 150 then 0.2
		when datediff(a.paid_date, a.bill_date) > 150 then 0.1
	end dff_rate,
	if(
		a.sale_amt_jiushui /(a.sale_amt_jiushui + a.sale_amt) > 0,
		a.order_amt * (a.sale_amt /(a.sale_amt_jiushui + a.sale_amt)),
		a.order_amt
	) order_amt,
	-- 源单据对账金额
	if(
		a.sale_amt_jiushui /(a.sale_amt_jiushui + a.sale_amt) > 0,
		a.unpay_amt * (a.sale_amt /(a.sale_amt_jiushui + a.sale_amt)),
		a.unpay_amt
	) unpay_amt,
	-- 历史核销剩余金额
	if(
		a.sale_amt_jiushui /(a.sale_amt_jiushui + a.sale_amt) > 0,
		a.pay_amt * (a.sale_amt /(a.sale_amt_jiushui + a.sale_amt)),
		a.pay_amt
	) pay_amt,
	-- 核销金额
	a.business_type_code,
	a.business_type_name,
	a.sale_amt,
	a.profit,
	a.sale_amt_jiushui,
	a.profit_jiushui
from tmp_tc_cust_order_detail a 
-- 客户信控的账期
	left join customer_company_details c on a.customer_code = c.customer_no
	and a.company_code = c.company_code
	and a.credit_code = c.credit_code
	left join -- CRM客户信息取月最后一天
	(
		select customer_code,
			customer_name,
			sales_user_number,
			sales_user_name,
			performance_region_code,
			performance_region_name,
			performance_province_code,
			performance_province_name,
			performance_city_code,
			performance_city_name
		from csx_dim.csx_dim_crm_customer_info
		where sdt = regexp_replace(
				last_day(add_months('${sdt_yes_date}', -1)),
				'-',
				''
			)
			and channel_code in('1', '7', '9')
	) d on d.customer_code = a.customer_code
where d.customer_code is not null
 and  business_type_code in ('6.1','6.2','2.1','2.2')
 
 ;


-- 实际销售剔除茅台
drop table csx_analyse_tmp.csx_analyse_tmp_fl_sale_detail;
create table csx_analyse_tmp.csx_analyse_tmp_fl_sale_detail as
select customer_code,
	substr(sdt, 1, 6) as smonth,
	-- 各类型销售额
	sum(sale_amt) as sale_amt,
	sum(
		case
			when business_type_code in('6')
			and (
				operation_mode_code = 0
				or operation_mode_code is null
			) then sale_amt
			else 0
		end
	) as bbc_sale_amt_zy,
	sum(
		case
			when business_type_code in('6')
			and operation_mode_code = 1 then sale_amt
			else 0
		end
	) as bbc_sale_amt_ly,
	sum(
		case
			when business_type_code in('2')
			and (
				operation_mode_code = 0
				or operation_mode_code is null
			) then sale_amt
			else 0
		end
	) as fl_sale_amt_zy,
	sum(
		case
			when business_type_code in('2')
			and operation_mode_code = 1 then sale_amt
			else 0
		end
	) as fl_sale_amt_ly,
	-- 各类型定价毛利额
	sum(profit) as profit,
	-- sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
	sum(case when business_type_code in('6') and (operation_mode_code = 0 or operation_mode_code is null) then profit	else 0	end) as bbc_profit_zy,
	sum(case when business_type_code in('6') and operation_mode_code = 1 then profit else 0	end	) as bbc_profit_ly,
	sum(
		case
			when business_type_code in('2')
			and (
				operation_mode_code = 0
				or operation_mode_code is null
			) then profit
			else 0
		end
	) as fl_profit_zy,
	sum(
		case
			when business_type_code in('2')
			and operation_mode_code = 1 then profit
			else 0
		end
	) as fl_profit_ly
from csx_dws.csx_dws_sale_detail_di
where sdt >= '20240101'
	and sdt <= regexp_replace(
		last_day(add_months('${sdt_yes_date}', -1)),
		'-',
		''
	)
	and channel_code in('1', '7', '9') -- 		and operation_mode_code not in ('3','4')
	and goods_code not in ('8718', '8708', '8649', '840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
	and business_type_code in('2', '6')
	and order_channel_detail_code not in ('24', '28') -- and performance_province_name in ('福建省')
group by customer_code,
	substr(sdt, 1, 6)

;
-- 计算毛利率提成系数
drop table csx_analyse_tmp.csx_analyse_tmp_fl_sale_detail_01;
create table csx_analyse_tmp.csx_analyse_tmp_fl_sale_detail_01 as
select *,
	case
		when coalesce(bbc_profit_rate_zy,0) < 0.1 then 0
		when bbc_profit_rate_zy < 0.15 then 0.12
		when bbc_profit_rate_zy < 0.3 then 0.13
		when bbc_profit_rate_zy >= 0.3 then 0.15
		else 0
	end tc_bbc_profit_ratio_zy,
	case
		when coalesce(bbc_profit_rate_ly,0) < 0.05 then 0
		when bbc_profit_rate_ly < 0.11 then 0.12
		when bbc_profit_rate_ly < 0.20 then 0.13
		when bbc_profit_rate_ly >= 0.20 then 0.15
		else 0
	end tc_bbc_profit_ratio_ly,
	case
		when coalesce(fl_profit_rate_zy,0) < 0.06 then 0
		when fl_profit_rate_zy < 0.16 then 0.12
		when fl_profit_rate_zy < 0.31 then 0.13
		when fl_profit_rate_zy >= 0.31 then 0.15
		else 0
	end tc_fl_profit_ratio_zy,
	case
		when coalesce(fl_profit_rate_ly,0) < 0.03 then 0
		when fl_profit_rate_ly < 0.09 then 0.12
		when fl_profit_rate_ly < 0.18 then 0.13
		when fl_profit_rate_ly >= 0.18 then 0.15
		else 0
	end tc_fl_profit_ratio_ly,
	0.06 as fl_basic_profit_rate_zy,
	0.03 as fl_basic_profit_rate_ly,
	0.1 as bbc_basic_profit_rate_zy,
	0.05 as bbc_basic_profit_rate_ly
from (
		select customer_code,
			smonth,
			sale_amt,
			bbc_sale_amt_zy,
			bbc_sale_amt_ly,
			fl_sale_amt_zy,
			fl_sale_amt_ly,
			profit,
			bbc_profit_zy,
			bbc_profit_ly,
			fl_profit_zy,
			fl_profit_ly,
			profit / sale_amt as profit_rate,
			coalesce(fl_profit_zy / abs(fl_sale_amt_zy) ,0)   as fl_profit_rate_zy,
			coalesce(fl_profit_ly / abs(fl_sale_amt_ly) ,0)   as fl_profit_rate_ly,
			coalesce(bbc_profit_zy / abs(bbc_sale_amt_zy) ,0) as bbc_profit_rate_zy,
			coalesce(bbc_profit_ly / abs(bbc_sale_amt_ly) ,0) as bbc_profit_rate_ly
		from csx_analyse_tmp.csx_analyse_tmp_fl_sale_detail
	) a;


-- 客户提成明细

-- drop table csx_analyse_tmp.csx_analyse_tmp_fl_cust_credit_bill ;
create table csx_analyse_tmp.csx_analyse_tmp_fl_cust_credit_bill as 
with csx_analyse_tmp_fl_cust_credit_bill as 
(select bill_type,
	a.source_bill_no,
	a.customer_code,
	customer_name,
	a.credit_code,
	happen_date,
	a.company_code,
	e.account_period_code,
	e.account_period_name,
	e.account_period_value,
	source_sys,
	reconciliation_period,
	bill_date,
	overdue_date,
	paid_date,
	real_paid_date,
	dff,
	case when e.account_period_name like '预付%' then 1.1 else dff_rate end dff_rate,
	-- order_amt,
	unpay_amt,
	case when b.sale_amt is not null and a.business_type_name='BBC联营' then a.order_amt*b.sale_amt_bbc_ly_rate 
		 when b.sale_amt is not null and a.business_type_name='BBC自营' then a.order_amt*b.sale_amt_bbc_zy_rate 
	else a.order_amt end order_amt,	-- 源单据对账金额
	
	case when b.sale_amt is not null and a.business_type_name='BBC联营' then a.pay_amt*b.sale_amt_bbc_ly_rate 
		 when b.sale_amt is not null and a.business_type_name='BBC自营' then a.pay_amt*b.sale_amt_bbc_zy_rate 
	else a.pay_amt end pay_amt,	-- 核销金额	,
	business_type_code,
	business_type_name,
	a.sale_amt,
	a.profit,
	a.sale_amt_jiushui,
	a.profit_jiushui
from csx_analyse_tmp.tmp_tc_fl_cust_credit_bill_xianjin_bujiu a
left join 
	(
	select source_bill_no,
	sum(sale_amt) sale_amt,
	sum(case when business_type_name='BBC联营' then sale_amt end) sale_amt_bbc_ly,
	sum(case when business_type_name='BBC自营' then sale_amt end) sale_amt_bbc_zy,
	sum(case when business_type_name='BBC联营' then sale_amt end)/sum(sale_amt) sale_amt_bbc_ly_rate,
	sum(case when business_type_name='BBC自营' then sale_amt end)/sum(sale_amt) sale_amt_bbc_zy_rate
	from csx_analyse_tmp.tmp_tc_fl_cust_credit_bill_xianjin_bujiu
	where business_type_name like 'BBC%' 
	group by source_bill_no
	)b on a.source_bill_no=b.source_bill_no
 -- 系统中预付款信控号
  left join 
(
	select customer_code,
		credit_code,
		company_code,
		case when customer_code in ('191358','214457','254907','254936','255413','255466','255738','256016','256186','256479') then 'Z007'
			else	account_period_code end account_period_code,
		case when customer_code in ('191358','214457','254907','254936','255413','255466','255738','256016','256186','256479') then '预付货款' 
			else account_period_name end account_period_name,		-- 账期编码,账期名称
		account_period_value,		-- 账期值
		account_period_abbreviation_name,		-- 账期简称
		credit_limit,
		temp_credit_limit
		from csx_dim.csx_dim_crm_customer_company_details
		where sdt='current'
)e on a.credit_code=e.credit_code and a.company_code=e.company_code
)
  select
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    a.customer_code,
    -- 客户编码
    d.customer_name,
    credit_code,
    -- 信控号
    company_code,
    -- 签约公司编码
    account_period_code,
    -- 账期编码
    account_period_name,
    -- 账期名称
    sales_user_number,
    sales_user_name,
    substr(regexp_replace(bill_date, '-', ''), 1, 6) as bill_month,
    -- 结算月
    bill_date,
    -- 结算日期
    paid_date,
    real_paid_date,
    -- 核销日期（打款日期）
    substr(regexp_replace(paid_date, '-', ''), 1, 6) as paid_month,
    substr(regexp_replace(happen_date, '-', ''), 1, 6) as happen_month,
    -- 销售月
    -- 202308签呈 126275 将销售日期为6.15-8.15期间的BBC，结算日调整为8.16，且最高回款系数100%
    dff_rate,
    -- 回款时间系数
    sum(pay_amt) pay_amt,
    -- 核销金额
    sum( case when business_type_name like 'BBC%' then pay_amt else 0 end  ) as bbc_pay_amt,
    sum( case when business_type_name like '福利%' then pay_amt else 0 end ) as fl_pay_amt,
    sum( case when business_type_name = 'BBC联营' then pay_amt  else 0 end ) as bbc_ly_pay_amt,
    sum( case when business_type_name = 'BBC自营' then pay_amt  else 0 end ) as bbc_zy_pay_amt,
    sum( case when business_type_name = '福利自营' then pay_amt else 0  end ) as fl_zy_pay_amt,
    sum( case when business_type_name = '福利联营' then pay_amt else 0 end  ) as fl_ly_pay_amt,
    -- 各类型销售额
    sum(sale_amt) as sale_amt,
    sum(case when business_type_name like 'BBC%' then sale_amt else 0 end ) as bbc_sale_amt,
    sum(case when business_type_name like '福利%' then sale_amt else 0 end ) as fl_sale_amt,
    sum(case when business_type_name = 'BBC联营' then sale_amt else 0 end ) as bbc_ly_sale_amt,
    sum(case when business_type_name = 'BBC自营' then sale_amt else 0 end ) as bbc_zy_sale_amt,
    sum(case when business_type_name = '福利自营' then sale_amt else 0 end ) as fl_zy_sale_amt,
    sum(case when business_type_name = '福利联营' then sale_amt else 0 end ) as fl_ly_sale_amt,
    -- 各类型定价毛利额
    sum(profit) as profit,
    sum(case when business_type_name like 'BBC%' then profit else 0 end ) as bbc_profit,
    sum(case when business_type_name like '福利%' then profit else 0 end ) as fl_profit,
    sum(case when business_type_name = 'BBC联营' then profit  else 0 end ) as bbc_ly_profit,
    sum(case when business_type_name = 'BBC自营' then profit else 0  end ) as bbc_zy_profit,
    sum(case when business_type_name = '福利自营' then profit else 0 end ) as fl_zy_profit,
    sum(case when business_type_name = '福利联营' then profit else 0  end) as fl_ly_profit
  from
    csx_analyse_tmp_fl_cust_credit_bill a
    left join (
      select
        customer_code,
        customer_name,
        sales_user_number,
        sales_user_name,
        performance_region_code,
        performance_region_name,
        performance_province_code,
        performance_province_name,
        performance_city_code,
        performance_city_name
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = regexp_replace(
          last_day(add_months('${sdt_yes_date}', -1)),
          '-',
          ''
        )
        and channel_code in('1', '7', '9')
    ) d on d.customer_code = a.customer_code
	where a.business_type_code in ('6.1','6.2','2.1','2.2')
  group by
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    a.customer_code,
    -- 客户编码
    d.customer_name,
    credit_code,
    -- 信控号
    company_code,
    -- 签约公司编码
    account_period_code,
    -- 账期编码
    account_period_name,
    -- 账期名称
    sales_user_number,
    sales_user_name,
    substr(regexp_replace(bill_date, '-', ''), 1, 6),
    -- 结算月
    bill_date,
    paid_date,
    real_paid_date,
    -- if(paid_date<happen_date,'是','否'),
    substr(regexp_replace(happen_date, '-', ''), 1, 6),
    -- 销售月
    dff_rate -- 回款时间系数
; 

-- 创建临时表，表名为:福利客户提成表
create table if not exists csx_analyse_tmp.csx_analyse_tmp_hr_fl_customer_commission_mf as
--  客户提成 
select
  a.performance_region_code	,
  a.performance_region_name,
  a.performance_province_code	,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
--   a.credit_code,
--   a.company_code,
--   a.account_period_code,
  a.account_period_name,
  a.sales_user_number	,
  a.sales_user_name,
  a.bill_month,
  a.bill_date,
  a.paid_date,
  a.happen_month,
  a.dff_rate,
  a.pay_amt,
  a.bbc_pay_amt,
  a.fl_pay_amt,
  a.bbc_zy_pay_amt,
  a.bbc_ly_pay_amt,
  a.fl_zy_pay_amt	,
  a.fl_ly_pay_amt	,
  b.sale_amt as real_sale_amt,
  b.bbc_sale_amt_zy as real_bbc_sale_amt_zy,
  b.bbc_sale_amt_ly as real_bbc_sale_amt_ly,
  b.fl_sale_amt_zy as real_fl_sale_amt_zy,
  b.fl_sale_amt_ly as real_fl_sale_amt_ly,
  b.profit as real_profit,
  b.bbc_profit_zy as real_bbc_profit_zy,
  b.bbc_profit_ly as real_bbc_profit_ly,
  b.fl_profit_zy as real_fl_profit_zy,
  b.fl_profit_ly as real_fl_profit_ly,
  b.profit_rate,
  b.bbc_profit_rate_zy,
  b.bbc_profit_rate_ly,
  b.fl_profit_rate_zy,
  b.fl_profit_rate_ly,
  b.tc_bbc_profit_ratio_zy,
  b.tc_bbc_profit_ratio_ly,
  b.tc_fl_profit_ratio_zy,
  b.tc_fl_profit_ratio_ly,
  b.bbc_basic_profit_rate_zy,
  b.bbc_basic_profit_rate_ly,
  b.fl_basic_profit_rate_zy,
  b.fl_basic_profit_rate_ly,
  bbc_zy_pay_amt*(bbc_profit_rate_zy-bbc_basic_profit_rate_zy)*tc_bbc_profit_ratio_zy*dff_rate as bbc_zy_tc_amt,
  bbc_ly_pay_amt*(bbc_profit_rate_ly-bbc_basic_profit_rate_ly)*tc_bbc_profit_ratio_ly*dff_rate as bbc_ly_tc_amt,
  fl_zy_pay_amt* (fl_profit_rate_zy-fl_basic_profit_rate_zy)*tc_fl_profit_ratio_zy*dff_rate as fl_zy_tc_amt,
  fl_ly_pay_amt* (fl_profit_rate_ly-fl_basic_profit_rate_ly)*tc_fl_profit_ratio_ly*dff_rate as fl_ly_tc_amt,
  coalesce(bbc_zy_pay_amt*(bbc_profit_rate_zy-bbc_basic_profit_rate_zy)*tc_bbc_profit_ratio_zy*dff_rate,0)
  		+ coalesce(bbc_ly_pay_amt*(bbc_profit_rate_ly-bbc_basic_profit_rate_ly)*tc_bbc_profit_ratio_ly*dff_rate,0)
		+coalesce(fl_zy_pay_amt*(fl_profit_rate_zy-fl_basic_profit_rate_zy)*tc_fl_profit_ratio_zy*dff_rate ,0)
		+coalesce(fl_ly_pay_amt*(fl_profit_rate_ly-fl_basic_profit_rate_ly)*tc_fl_profit_ratio_ly*dff_rate ,0) as total_tc_amt
from
   csx_analyse_tmp.csx_analyse_tmp_fl_cust_credit_bill a
  left join 
  csx_analyse_tmp.csx_analyse_tmp_fl_sale_detail_01 b on a.customer_code = b.customer_code
 
  and a.happen_month = b.smonth
  where a.real_paid_date>='2025-04-01'
  and real_paid_date<='2025-04-30'
  ;
 
-- 当月发生的履约销售
with csx_analyse_tmp_fl_sale_detail as
(select performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  customer_code,
	substr(sdt, 1, 6) as smonth,
	-- 各类型销售额
	sum(sale_amt) as sale_amt,
	sum(
		case
			when business_type_code in('6')
			and (
				operation_mode_code = 0
				or operation_mode_code is null
			) then sale_amt
			else 0
		end
	) as bbc_sale_amt_zy,
	sum(
		case
			when business_type_code in('6')
			and operation_mode_code = 1 then sale_amt
			else 0
		end
	) as bbc_sale_amt_ly,
	sum(
		case
			when business_type_code in('2')
			and (
				operation_mode_code = 0
				or operation_mode_code is null
			) then sale_amt
			else 0
		end
	) as fl_sale_amt_zy,
	sum(
		case
			when business_type_code in('2')
			and operation_mode_code = 1 then sale_amt
			else 0
		end
	) as fl_sale_amt_ly,
	-- 各类型定价毛利额
	sum(profit) as profit,
	-- sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
	sum(case when business_type_code in('6') and (operation_mode_code = 0 or operation_mode_code is null) then profit	else 0	end) as bbc_profit_zy,
	sum(case when business_type_code in('6') and operation_mode_code = 1 then profit else 0	end	) as bbc_profit_ly,
	sum(
		case
			when business_type_code in('2')
			and (
				operation_mode_code = 0
				or operation_mode_code is null
			) then profit
			else 0
		end
	) as fl_profit_zy,
	sum(
		case
			when business_type_code in('2')
			and operation_mode_code = 1 then profit
			else 0
		end
	) as fl_profit_ly
from csx_dws.csx_dws_sale_detail_di
where sdt >= '20240101'
	and sdt <= regexp_replace(
		last_day(add_months('${sdt_yes_date}', -1)),
		'-',
		''
	)
	and channel_code in('1', '7', '9') -- 		and operation_mode_code not in ('3','4')
	and goods_code not in ('8718', '8708', '8649', '840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
	and business_type_code in('2', '6')
	and order_channel_detail_code not in ('24', '28') -- and performance_province_name in ('福建省')
group by performance_region_name,
  performance_province_name,
  performance_city_name,
  sales_user_number,
  sales_user_name,
  customer_code,
	substr(sdt, 1, 6)

) select
  a.smonth,
  a.performance_region_name,
  a.performance_province_name,
  a.performance_city_name,
   a.customer_code,
  b.customer_name,
  a.sales_user_number,
  a.sales_user_name,
 
  a.sale_amt,
  bbc_sale_amt_zy,
  bbc_sale_amt_ly,
  fl_sale_amt_zy,
  fl_sale_amt_ly,
  profit,
  bbc_profit_zy,
  bbc_profit_ly,
  fl_profit_zy,	
  fl_profit_ly,
    (profit)/(sale_amt) as real_profit_rate,
    (bbc_profit_zy)/(bbc_sale_amt_zy) as bbc_profit_rate_zy,
    (bbc_profit_ly)/(bbc_sale_amt_ly) as bbc_profit_rate_ly,
    (fl_profit_zy)/(fl_sale_amt_zy) as fl_profit_rate_zy,
    (fl_profit_ly)/(fl_sale_amt_ly) as fl_profit_rate_ly
from
  csx_analyse_tmp_fl_sale_detail a
  left join (
    select
      performance_region_name,
      performance_province_name,
      performance_city_name,
      sales_user_number,
      sales_user_name,
      customer_code,
      customer_name
    from
      csx_dim.csx_dim_crm_customer_info
    where
      sdt = 'current'
  ) b on a.customer_code=b.customer_code
where
  smonth = '202504'

; 
-- 当月业绩
with 
tmp_tc_detail as 
(
select a.paid_month,
  a.performance_region_code	,
  a.performance_region_name,
  a.performance_province_code	,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.sales_user_number	,
  a.sales_user_name,
   ( a.pay_amt  )pay_amt,
  ( a.bbc_pay_amt ) bbc_pay_amt,
  ( a.fl_pay_amt ) fl_pay_amt,
  ( a.bbc_ly_pay_amt) bbc_ly_pay_amt,
  ( a.bbc_zy_pay_amt) bbc_zy_pay_amt,
  ( a.fl_zy_pay_amt)fl_zy_pay_amt	,
  ( a.fl_ly_pay_amt)fl_ly_pay_amt,
  b.real_sale_amt,
  b.real_bbc_sale_amt_zy,
  b.real_bbc_sale_amt_ly,
  b.real_fl_sale_amt_zy,
  b.real_fl_sale_amt_ly,
  b.real_profit,
  b.real_bbc_profit_zy,
  b.real_bbc_profit_ly,
  b.real_fl_profit_zy,
  b.real_fl_profit_ly,
  b.profit_rate,
  b.fl_profit_rate_zy,
  b.fl_profit_rate_ly,
  b.bbc_profit_rate_zy,
  b.bbc_profit_rate_ly,
  b.tc_bbc_profit_ratio_zy,
  b.tc_bbc_profit_ratio_ly,
  b.tc_fl_profit_ratio_zy,
  b.tc_fl_profit_ratio_ly,
  b.fl_basic_profit_rate_zy,
  b.fl_basic_profit_rate_ly,
  b.bbc_basic_profit_rate_zy,
  b.bbc_basic_profit_rate_ly
 from 
(select a.paid_month,
  a.performance_region_code	,
  a.performance_region_name,
  a.performance_province_code	,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.sales_user_number	,
  a.sales_user_name,
  sum( a.pay_amt   )pay_amt,
  sum( a.bbc_pay_amt ) bbc_pay_amt,
  sum( a.fl_pay_amt ) fl_pay_amt,
  sum( a.bbc_ly_pay_amt) bbc_ly_pay_amt,
  sum( a.bbc_zy_pay_amt) bbc_zy_pay_amt,
  sum( a.fl_zy_pay_amt)fl_zy_pay_amt	,
  sum( a.fl_ly_pay_amt)fl_ly_pay_amt
 from  
   csx_analyse_tmp.csx_analyse_tmp_fl_cust_credit_bill  a 
  where a.real_paid_date>='2025-04-01'
  and real_paid_date<='2025-04-30'
  group by a.performance_region_code	,
  a.performance_region_name,
  a.performance_province_code	,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.sales_user_number	,
  a.sales_user_name,
  a.paid_month
  )a 
  left join 
 ( select 
   smonth happen_month,
  a.customer_code,
  a.sale_amt as real_sale_amt,
  a.bbc_sale_amt_zy as real_bbc_sale_amt_zy,
  a.bbc_sale_amt_ly as real_bbc_sale_amt_ly,
  a.fl_sale_amt_zy as real_fl_sale_amt_zy,
  a.fl_sale_amt_ly as real_fl_sale_amt_ly,
  a.profit as real_profit,
  a.bbc_profit_zy as real_bbc_profit_zy,
  a.bbc_profit_ly as real_bbc_profit_ly,
  a.fl_profit_zy as real_fl_profit_zy,
  a.fl_profit_ly as real_fl_profit_ly,
  a.profit_rate,
  a.fl_profit_rate_zy,
  a.fl_profit_rate_ly,
  a.bbc_profit_rate_zy,
  a.bbc_profit_rate_ly,
  a.tc_bbc_profit_ratio_zy,
  a.tc_bbc_profit_ratio_ly,
  a.tc_fl_profit_ratio_zy,
  a.tc_fl_profit_ratio_ly,
  a.fl_basic_profit_rate_zy,
  a.fl_basic_profit_rate_ly,
  a.bbc_basic_profit_rate_zy,
  a.bbc_basic_profit_rate_ly
from
      csx_analyse_tmp.csx_analyse_tmp_fl_sale_detail_01 a  
  where a.smonth='202504'
 ) b on  a.customer_code=b.customer_code
)
select paid_month,
-- performance_region_code,
performance_region_name,
-- performance_province_code,	
performance_province_name,	
-- performance_city_code,
performance_city_name,
customer_code,
customer_name,
sales_user_number,
sales_user_name,

sum(pay_amt) pay_amt,
sum(bbc_zy_pay_amt) bbc_zy_pay_amt,
sum(bbc_ly_pay_amt) bbc_ly_pay_amt,
sum(fl_zy_pay_amt) fl_zy_pay_amt	,
sum(fl_ly_pay_amt) fl_ly_pay_amt	,
sum(real_sale_amt) as real_sale_amt,
sum(real_bbc_sale_amt_zy) as real_bbc_sale_amt_zy,
sum(real_bbc_sale_amt_ly) as real_bbc_sale_amt_ly,
sum(real_fl_sale_amt_zy) as real_fl_sale_amt_zy,
sum(real_fl_sale_amt_ly) as real_fl_sale_amt_ly,
sum(real_profit) as real_profit,
sum(real_bbc_profit_zy)  as real_bbc_profit_zy,
sum(real_bbc_profit_ly)  as real_bbc_profit_ly,
sum(real_fl_profit_zy) as real_fl_profit_zy,
sum(real_fl_profit_ly) as real_fl_profit_ly,
sum(real_profit)/sum(real_sale_amt) as real_profit_rate,
sum(real_bbc_profit_zy)/sum(real_bbc_sale_amt_zy) as bbc_profit_rate_zy,
sum(real_bbc_profit_ly)/sum(real_bbc_sale_amt_ly) as bbc_profit_rate_ly,
sum(real_fl_profit_zy)/sum(real_fl_sale_amt_zy) as fl_profit_rate_zy,
sum(real_fl_profit_ly)/sum(real_fl_sale_amt_ly) as fl_profit_rate_ly
-- sum(bbc_zy_tc_amt ) bbc_zy_tc_amt,
-- sum(bbc_ly_tc_amt ) bbc_ly_tc_amt,
-- sum(fl_zy_tc_amt ) fl_zy_tc_amt,
-- sum(fl_ly_tc_amt ) fl_ly_tc_amt,
-- sum(coalesce(bbc_zy_tc_amt,0) + coalesce(bbc_ly_tc_amt,0) + coalesce(fl_zy_tc_amt,0) + coalesce(fl_ly_tc_amt ,0))  as total_tc_amt

from tmp_tc_detail a 
-- where customer_code='120794'
 group by performance_region_code,
        performance_region_name,
        performance_province_code,	
        performance_province_name,	
        performance_city_code,
        performance_city_name,
        sales_user_number,
        sales_user_name,
        paid_month,
        customer_code,
        customer_name
;


-- 销售员提成
 with tmp_tc_detail as 
(select
  a.performance_region_code	,
  a.performance_region_name,
  a.performance_province_code	,
  a.performance_province_name,
  a.performance_city_code,
  a.performance_city_name,
  a.customer_code,
  a.customer_name,
  a.credit_code,
  a.company_code,
  a.account_period_code,
  a.account_period_name,
  a.sales_user_number	,
  a.sales_user_name,
  a.bill_month,
  a.bill_date,
  a.paid_date,
  substr(regexp_replace(a.real_paid_date,'-',''),1,6) as real_paid_month,
  a.happen_month,
  a.dff_rate,
  a.pay_amt,
  a.bbc_pay_amt,
  a.fl_pay_amt,
  a.bbc_ly_pay_amt,
  a.bbc_zy_pay_amt,
  a.fl_zy_pay_amt	,
  a.fl_ly_pay_amt	,
  b.sale_amt as real_sale_amt,
  b.bbc_sale_amt_zy as real_bbc_sale_amt_zy,
  b.bbc_sale_amt_ly as real_bbc_sale_amt_ly,
  b.fl_sale_amt_zy as real_fl_sale_amt_zy,
  b.fl_sale_amt_ly as real_fl_sale_amt_ly,
  b.profit as real_profit,
  b.bbc_profit_zy as real_bbc_profit_zy,
  b.bbc_profit_ly as real_bbc_profit_ly,
  b.fl_profit_zy as real_fl_profit_zy,
  b.fl_profit_ly as real_fl_profit_ly,
  b.profit_rate,
  b.fl_profit_rate_zy,
  b.fl_profit_rate_ly,
  b.bbc_profit_rate_zy,
  b.bbc_profit_rate_ly,
  b.tc_bbc_profit_ratio_zy,
  b.tc_bbc_profit_ratio_ly,
  b.tc_fl_profit_ratio_zy,
  b.tc_fl_profit_ratio_ly,
  b.fl_basic_profit_rate_zy,
  b.fl_basic_profit_rate_ly,
  b.bbc_basic_profit_rate_zy,
  b.bbc_basic_profit_rate_ly,
--   pay_amt*(profit_rate-)
  bbc_ly_pay_amt*(bbc_profit_rate_ly-bbc_basic_profit_rate_ly)*tc_bbc_profit_ratio_ly*dff_rate as bbc_ly_tc_amt,
  bbc_zy_pay_amt*(bbc_profit_rate_zy-bbc_basic_profit_rate_zy)*tc_bbc_profit_ratio_zy*dff_rate as bbc_zy_tc_amt,
  fl_ly_pay_amt*(fl_profit_rate_ly-fl_basic_profit_rate_ly)*tc_fl_profit_ratio_ly*dff_rate as     fl_ly_tc_amt,
  fl_zy_pay_amt*(fl_profit_rate_zy-fl_basic_profit_rate_zy)*tc_fl_profit_ratio_zy*dff_rate as     fl_zy_tc_amt
from
   csx_analyse_tmp.csx_analyse_tmp_fl_cust_credit_bill  a
  left join 
  csx_analyse_tmp.csx_analyse_tmp_fl_sale_detail_01 b on a.customer_code = b.customer_code
  and a.happen_month = b.smonth
  where a.real_paid_date>='2025-04-01'
  and a.real_paid_date<='2025-04-30'
 )

select   happen_month,
-- performance_region_code,
performance_region_name,
-- performance_province_code,	
performance_province_name,	
-- performance_city_code,
performance_city_name,
sales_user_number,
sales_user_name,
sum(pay_amt) pay_amt,
sum(bbc_zy_pay_amt) bbc_zy_pay_amt,
sum(bbc_ly_pay_amt) bbc_ly_pay_amt,
sum(fl_zy_pay_amt) fl_zy_pay_amt	,
sum(fl_ly_pay_amt) fl_ly_pay_amt	,
sum(real_sale_amt) as real_sale_amt,
sum(real_bbc_sale_amt_zy) as real_bbc_sale_amt_zy,
sum(real_bbc_sale_amt_ly) as real_bbc_sale_amt_ly,
sum(real_fl_sale_amt_zy) as real_fl_sale_amt_zy,
sum(real_fl_sale_amt_ly) as real_fl_sale_amt_ly,
sum(real_profit) as real_profit,
sum(real_bbc_profit_zy)  as real_bbc_profit_zy,
sum(real_bbc_profit_ly)  as real_bbc_profit_ly,
sum(real_fl_profit_zy) as real_fl_profit_zy,
sum(real_fl_profit_ly) as real_fl_profit_ly,
sum(real_profit)/sum(real_sale_amt) as real_profit_rate,
sum(real_bbc_profit_zy)/sum(real_bbc_sale_amt_zy) as bbc_profit_rate_zy,
sum(real_bbc_profit_ly)/sum(real_bbc_sale_amt_ly) as bbc_profit_rate_ly,
sum(real_fl_profit_zy)/sum(real_fl_sale_amt_zy) as fl_profit_rate_zy,
sum(real_fl_profit_ly)/sum(real_fl_sale_amt_ly) as fl_profit_rate_ly,
sum(bbc_zy_tc_amt ) bbc_zy_tc_amt,
sum(bbc_ly_tc_amt ) bbc_ly_tc_amt,
sum(fl_zy_tc_amt ) fl_zy_tc_amt,
sum(fl_ly_tc_amt ) fl_ly_tc_amt,
sum(total_tc_amt)  as total_tc_amt

from (
select real_paid_month happen_month,
-- performance_region_code,
performance_region_name,
-- performance_province_code,	
performance_province_name,	
-- performance_city_code,
performance_city_name,
sales_user_number,
sales_user_name,
sum(pay_amt) pay_amt,
sum(bbc_zy_pay_amt) bbc_zy_pay_amt,
sum(bbc_ly_pay_amt) bbc_ly_pay_amt,
sum(fl_zy_pay_amt) fl_zy_pay_amt	,
sum(fl_ly_pay_amt) fl_ly_pay_amt	,
(real_sale_amt) as real_sale_amt,
(real_bbc_sale_amt_zy) as real_bbc_sale_amt_zy,
(real_bbc_sale_amt_ly) as real_bbc_sale_amt_ly,
(real_fl_sale_amt_zy) as real_fl_sale_amt_zy,
(real_fl_sale_amt_ly) as real_fl_sale_amt_ly,
(real_profit) as real_profit,
(real_bbc_profit_zy)  as real_bbc_profit_zy,
(real_bbc_profit_ly)  as real_bbc_profit_ly,
(real_fl_profit_zy) as real_fl_profit_zy,
(real_fl_profit_ly) as real_fl_profit_ly,
(real_profit)/(real_sale_amt) as real_profit_rate,
(real_bbc_profit_zy)/(real_bbc_sale_amt_zy) as bbc_profit_rate_zy,
(real_bbc_profit_ly)/(real_bbc_sale_amt_ly) as bbc_profit_rate_ly,
(real_fl_profit_zy)/(real_fl_sale_amt_zy) as fl_profit_rate_zy,
(real_fl_profit_ly)/(real_fl_sale_amt_ly) as fl_profit_rate_ly,
sum(bbc_zy_tc_amt ) bbc_zy_tc_amt,
sum(bbc_ly_tc_amt ) bbc_ly_tc_amt,
sum(fl_zy_tc_amt ) fl_zy_tc_amt,
sum(fl_ly_tc_amt ) fl_ly_tc_amt,
sum(coalesce(bbc_zy_tc_amt,0) + coalesce(bbc_ly_tc_amt,0) + coalesce(fl_zy_tc_amt,0) + coalesce(fl_ly_tc_amt ,0))  as total_tc_amt

from tmp_tc_detail a 
where real_paid_month='202504'
 group by performance_region_code,
        performance_region_name,
        performance_province_code,	
        performance_province_name,	
        performance_city_code,
        performance_city_name,
        sales_user_number,
        sales_user_name,
        real_paid_month,
        real_sale_amt,
		real_bbc_sale_amt_zy,
		real_bbc_sale_amt_ly,
		real_fl_sale_amt_zy,
		real_fl_sale_amt_ly,
		real_profit,
		real_bbc_profit_zy,
		real_bbc_profit_ly,
		real_fl_profit_zy,
		real_fl_profit_ly
		) a 
group by  happen_month,
-- performance_region_code,
performance_region_name,
-- performance_province_code,	
performance_province_name,	
-- performance_city_code,
performance_city_name,
sales_user_number,
sales_user_name

;


--授信额度
select * from   csx_analyse.csx_analyse_fr_bbc_wshop_user_credit_di
 where sdt='20250430' 
and (credit_balance is not null or 	count_person is not null)


-- 创建临时表，表名为:福利客户提成表
create table if not exists csx_analyse.csx_analyse_hr_fl_customer_commission_mf
(
s_month string comment '月份',
performance_region_code string comment '大区编码',
performance_region_name string comment '大区名称',
performance_province_code string comment '省区编码',
performance_province_name string comment '省区名称',
performance_city_code string comment '城市编码',
performance_city_name string comment '城市名称',
customer_code string comment '客户编码',
customer_name string comment '客户名称',
account_period_name string comment '账期',
sales_user_number string comment '销售员工号',
sales_user_name string comment '销售员',
bill_month string comment '结算月份',
bill_date string comment '结算日期',
paid_date string comment '过账日期',
happen_month string comment '业务发生月份',
dff_rate decimal(11,1) comment '回款系数',
pay_amt decimal(38,5) comment '回款金额',
bbc_pay_amt decimal(38,5) comment 'BBC回款金额',
fl_pay_amt decimal(38,5) comment '福利回款金额',
bbc_zy_pay_amt decimal(38,5) comment 'BBC联营回款金额',
bbc_ly_pay_amt decimal(38,5) comment 'BBC自营回款金额',
fl_zy_pay_amt decimal(38,5) comment '福利自营回款金额',
fl_ly_pay_amt decimal(38,5) comment '福利联营回款金额',
real_sale_amt decimal(30,6) comment '实际销售额',
real_bbc_sale_amt_zy decimal(30,6) comment '实际BBC自营销售额',
real_bbc_sale_amt_ly decimal(30,6) comment '实际BBC联营销售额',
real_fl_sale_amt_zy decimal(30,6) comment '实际福利自营销售额',
real_fl_sale_amt_ly decimal(30,6) comment '实际福利联营销售额',
real_profit decimal(30,6) comment '毛利额',
real_bbc_profit_zy decimal(30,6) comment 'BBC自营毛利额',
real_bbc_profit_ly decimal(30,6) comment 'BBC联营营毛利额',
real_fl_profit_zy decimal(30,6) comment '福利自营毛利额',
real_fl_profit_ly decimal(30,6) comment '福利联营毛利额',
profit_rate decimal(38,8) comment '毛利率',
bbc_profit_rate_zy decimal(38,8) comment 'BBC自营毛利率',
bbc_profit_rate_ly decimal(38,8) comment 'BBC联营营毛利率',
fl_profit_rate_zy decimal(38,8) comment '福利自营毛利率',
fl_profit_rate_ly decimal(38,8) comment '福利联营毛利率',
tc_bbc_profit_ratio_zy decimal(12,2) comment 'BBC自营提成系数',
tc_bbc_profit_ratio_ly decimal(12,2) comment 'BBC联营营提成系数',
tc_fl_profit_ratio_zy decimal(12,2) comment '福利自营提成系数',
tc_fl_profit_ratio_ly decimal(12,2) comment '福利联营提成系数',
bbc_basic_profit_rate_zy decimal(1,1) comment 'BBC自营基准毛利率',
bbc_basic_profit_rate_ly decimal(2,2) comment 'BBC联营营基准毛利率',
fl_basic_profit_rate_zy decimal(2,2) comment '福利自营基准毛利率',
fl_basic_profit_rate_ly decimal(2,2) comment '福利联营基准毛利率',
bbc_zy_tc_amt decimal(38,6) comment 'BBC自营提成额',
bbc_ly_tc_amt decimal(38,6) comment 'BBC联营营提成额',
fl_zy_tc_amt decimal(38,6) comment '福利自营提成额',
fl_ly_tc_amt decimal(38,6) comment '福利联营提成额',
total_tc_amt decimal(38,6) comment '总提成额',
update_time timestamp comment '更新时间'
)comment '福利客户提成表'
partitioned by (smt string)
stored as parquet;

