


-- 序号	大区	省区	客户编码	客户名称	合同签约主体	采购人	项目名称	合同签约日期	供货年限	
-- 开票日期	开票金额（元）	发票号码	备注

select -- a.*,b.*
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	coalesce(a.customer_no,b.customer_code) customer_code,
	a.customer_name,
	-- coalesce(a.company_code,b.company_code) company_code,		-- 公司代码
	a.company_code,
	a.company_name,		-- 公司名称
	a.wfqyztgsmc,	
	a.khbm,
	a.khmc,
	-- ywlx,		-- 业务类型
	-- ywlxmc,		-- 业务类型名称
	a.ywlx,
	a.thfs,		-- 拓户方式
	a.htbh,		-- 合同编号
	a.htqsrq,		-- 合同起始日期
	a.htzzrq,		-- 合同终止日期	
	b.sdt,
	b.invoice_no,
	b.company_code,		-- 公司代码		
	-- b.customer_code,		-- 客户编码
	b.total_amount,		-- 总金额
	b.offline_flag_code,		-- 是否线下开票 0 否 1 是
	b.invoice_remark		-- 发票的备注	
from 
(
select 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
	customer_no,customer_name,
	company_code,		-- 公司代码
	company_name,		-- 公司名称
	wfqyztgsdm,
	wfqyztgsmc,
	khbm,
	khmc,
	-- ywlx,		-- 业务类型
	-- ywlxmc,		-- 业务类型名称
	regexp_replace(regexp_replace(substr(htbh,3,4),'2',''),'0','') ywlx,
	thfs,		-- 拓户方式
	htbh,		-- 合同编号
	htqsrq,		-- 合同起始日期
	htzzrq,		-- 合同终止日期
	months_between(date_sub(to_date(from_unixtime(unix_timestamp(htzzrq,'yyyy-MM-dd'))),-1),
					to_date(from_unixtime(unix_timestamp(htqsrq,'yyyy-MM-dd')))) as ht_month,		-- 供货年限
	row_number() over(partition by customer_no order by htqsrq desc)	as num			
from csx_analyse.csx_analyse_report_weaver_contract_df
where sdt='${sdt_yes}'
and if_fanli='非返利'
-- and customer_no='105561'
)a 
full join 
(
-- 发票表
select sdt,invoice_no,
	company_code,		-- 公司代码		
	customer_code,		-- 客户编码
	total_amount,		-- 总金额
	offline_flag_code,		-- 是否线下开票 0 否 1 是
	regexp_replace(invoice_remark,'\\n|\\r|\\t','') invoice_remark		-- 发票的备注
	-- row_number() over(partition by invoice_no order by sdt desc)	as num1
from csx_dwd.csx_dwd_sss_invoice_di
where sdt>='20200101'
-- and invoice_no in('05502566','85686779')
and delete_flag='0'
and sync_status=1
)b on a.customer_no=b.customer_code -- and a.company_code=b.company_code
where a.num=1
-- and b.num1=1
;

-- 序号	大区	省区	客户编码	客户名称	合同签约主体	采购人	项目名称	合同签约日期	供货年限	
--回款日期	回款金额（元）	回单编号	备注
select -- a.*,b.*
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	coalesce(a.customer_no,b.customer_code) customer_code,
	coalesce(a.customer_name,b.customer_name) customer_name,
	-- coalesce(a.company_code,b.company_code) company_code,		-- 公司代码
	a.company_code,
	a.company_name,		-- 公司名称
	a.wfqyztgsmc,	
	a.khbm,
	a.khmc,
	-- ywlx,		-- 业务类型
	-- ywlxmc,		-- 业务类型名称
	a.ywlx,
	a.thfs,		-- 拓户方式
	a.htbh,		-- 合同编号
	a.htqsrq,		-- 合同起始日期
	a.htzzrq,		-- 合同终止日期	
	b.sdt,
	b.claim_bill_code,-- claim_employee_code,claim_employee_name,
	b.company_code,
	b.profit_center_code,  -- 利润中心、同一认领单有多条数据利润中心不一样 如01322122300477
	b.credit_code, -- 信控编号
	b.claim_amt,	-- 认领金额（含核销与未核销的，含补救单）
	b.summary_text,
	b.remark	
from 
(
select 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
	customer_no,customer_name,
	company_code,		-- 公司代码
	company_name,		-- 公司名称
	wfqyztgsdm,
	wfqyztgsmc,
	khbm,
	khmc,
	-- ywlx,		-- 业务类型
	-- ywlxmc,		-- 业务类型名称
	regexp_replace(regexp_replace(substr(htbh,3,4),'2',''),'0','') ywlx,
	thfs,		-- 拓户方式
	htbh,		-- 合同编号
	htqsrq,		-- 合同起始日期
	htzzrq,		-- 合同终止日期
	months_between(date_sub(to_date(from_unixtime(unix_timestamp(htzzrq,'yyyy-MM-dd'))),-1),
					to_date(from_unixtime(unix_timestamp(htqsrq,'yyyy-MM-dd')))) as ht_month,		-- 供货年限
	row_number() over(partition by customer_no order by htqsrq desc)	as num			
from csx_analyse.csx_analyse_report_weaver_contract_df
where sdt='${sdt_yes}'
and if_fanli='非返利'
-- and customer_no='105561'
)a 
--回款认领金额
full join
(
	select -- regexp_replace(substr(claim_time,1,10),'-','') claim_date,
		-- regexp_replace(substr(posting_time,1,10),'-','') post_date, -- =sdt
		sdt,
		claim_bill_code,-- claim_employee_code,claim_employee_name,
		customer_code,customer_name,company_code,
		profit_center_code,  -- 利润中心、同一认领单有多条数据利润中心不一样 如01322122300477
		credit_code, -- 信控编号
		claim_amt,	-- 认领金额（含核销与未核销的，含补救单）
		summary_text,
		regexp_replace(remark,'\\n|\\r|\\t','') remark
	from csx_dwd.csx_dwd_sss_money_back_di  --过账日期分区
	where sdt>='20200101'
	and sdt<='${sdt_yes}' 
	and delete_flag='0'
	and (paid_amt<>0 or residue_amt<>0) -- 剔除补救单和对应原单
)b on a.customer_no=b.customer_code -- and a.company_code=b.company_code
where a.num=1
-- and b.num1=1
;

-- 序号	大区	省区	客户编码	客户名称	合同签约主体	采购人	项目名称	合同签约日期	供货年限	合同金额（元）	
-- 销售金额（元）	最近销售日期	已开票金额	最近开票日期	发票数量	回单金额	最近回单日期	回单数量	备注

select -- a.*,b.*
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_no,
	a.customer_name,
	-- coalesce(a.company_code,b.company_code) company_code,		-- 公司代码
	a.company_code,
	a.company_name,		-- 公司名称
	a.wfqyztgsmc,	
	a.khbm,
	a.khmc,
	-- ywlx,		-- 业务类型
	-- ywlxmc,		-- 业务类型名称
	a.ywlx,
	a.thfs,		-- 拓户方式
	a.htbh,		-- 合同编号
	a.htqsrq,		-- 合同起始日期
	a.htzzrq,		-- 合同终止日期	
	a.htjey,		-- 合同金额(元)
	d.sale_total_amt,
	d.last_sale_date,
	c.total_amount,	-- 开票总金额
	c.kp_sdt_max,
	c.count_kp,
	b.claim_amt,	-- 认领金额（含核销与未核销的，含补救单）
	b.rl_sdt_max,
	b.count_rl		
from 
(
select 
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
	customer_no,customer_name,
	company_code,		-- 公司代码
	company_name,		-- 公司名称
	wfqyztgsdm,
	wfqyztgsmc,
	khbm,
	khmc,
	-- ywlx,		-- 业务类型
	-- ywlxmc,		-- 业务类型名称
	regexp_replace(regexp_replace(substr(htbh,3,4),'2',''),'0','') ywlx,
	thfs,		-- 拓户方式
	htbh,		-- 合同编号
	htqsrq,		-- 合同起始日期
	htzzrq,		-- 合同终止日期
	htjey,		-- 合同金额(元)
	months_between(date_sub(to_date(from_unixtime(unix_timestamp(htzzrq,'yyyy-MM-dd'))),-1),
					to_date(from_unixtime(unix_timestamp(htqsrq,'yyyy-MM-dd')))) as ht_month,		-- 供货年限
	row_number() over(partition by customer_no order by htqsrq desc)	as num			
from csx_analyse.csx_analyse_report_weaver_contract_df
where sdt='${sdt_yes}'
and if_fanli='非返利'
-- and customer_no='105561'
)a 
--回款认领金额  回单金额\最近回单日期\回单数量
left join
(
	select 
		customer_code,
		sum(claim_amt) claim_amt,	-- 认领金额（含核销与未核销的，含补救单）
		max(sdt) rl_sdt_max,
		count(claim_bill_code) count_rl
	from csx_dwd.csx_dwd_sss_money_back_di  --过账日期分区
	where sdt>='20200101'
	and sdt<='${sdt_yes}' 
	and delete_flag='0'
	and (paid_amt<>0 or residue_amt<>0) -- 剔除补救单和对应原单
	group by customer_code
)b on a.customer_no=b.customer_code -- and a.company_code=b.company_code
-- 发票表	最近开票日期	发票数量
left join 
(
select 
	customer_code,		-- 客户编码
	sum(total_amount) total_amount,	-- 总金额
	max(sdt) kp_sdt_max,
	count(invoice_no) count_kp	
from csx_dwd.csx_dwd_sss_invoice_di
where sdt>='20200101'
and delete_flag='0'
and sync_status=1
group by customer_code
)c on a.customer_no=c.customer_code 
--客户下单情况动态信息表
left join
(
  select customer_code,first_sale_date,last_sale_date,sale_active_days,sale_total_amt,
	-- 至今距离天数
	datediff(to_date(date_sub(current_date,1)),to_date(from_unixtime(unix_timestamp(last_sale_date,'yyyyMMdd')))) date_diff1	  
  from csx_dws.csx_dws_crm_customer_active_di
  where sdt = 'current'
)d on d.customer_code=a.customer_no 
where a.num=1
;











