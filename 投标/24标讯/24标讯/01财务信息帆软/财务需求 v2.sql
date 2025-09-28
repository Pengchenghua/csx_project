


-- 客户合同开票明细
insert overwrite table csx_analyse.csx_analyse_fr_customer_contract_invoice_detail_df partition(sdt)
select 
	concat_ws('-','${sdt_yes}',a.customer_no,a.htbh,b.invoice_no,b.invoice_code) as biz_id,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.customer_no customer_code,
	a.customer_name,
	c.first_category_code,     --  一级客户分类编码
	c.first_category_name,     --  一级客户分类名称
	c.second_category_code,     --  二级客户分类编码
	c.second_category_name,     --  二级客户分类名称
	c.third_category_code,     --  三级客户分类编码
	c.third_category_name,     --  三级客户分类名称
	c.contact_person,     --  联系人姓名
	c.contact_phone,     --  联系电话	
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
	a.djsqsj,		-- 单据申请时间
	a.htbh,		-- 合同编号
	a.htqsrq,		-- 合同起始日期
	a.htzzrq,		-- 合同终止日期	
	a.htjey,		-- 合同金额(元)
	b.invoice_date,
	b.invoice_no,		-- 发票号码	
	b.invoice_code,		-- 发票代码	
	b.company_code company_code_invoice,		-- 公司代码		
	-- b.customer_code,		-- 客户编码
	b.total_amount,		-- 总金额
	b.offline_flag,		-- 是否线下开票 0 否 1 是
	b.invoice_remark,		-- 发票的备注	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	'${sdt_yes}' as sdt_c, 
	cast('${sdt_yes}' as string) as sdt -- 统计日期 	
from 
(
select *
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
		regexp_replace(djsqsj,'-','') djsqsj,		-- 单据申请时间
		htbh,		-- 合同编号
		regexp_replace(htqsrq,'-','') htqsrq,		-- 合同起始日期
		regexp_replace(htzzrq,'-','') htzzrq,		-- 合同终止日期
		htjey,		-- 合同金额(元)
		months_between(date_sub(to_date(from_unixtime(unix_timestamp(htzzrq,'yyyy-MM-dd'))),-1),
						to_date(from_unixtime(unix_timestamp(htqsrq,'yyyy-MM-dd')))) as ht_month,		-- 供货年限
		row_number() over(partition by customer_no order by htqsrq desc)	as num			
	from csx_analyse.csx_analyse_report_weaver_contract_df
	where sdt='${sdt_yes}'
	and if_fanli='非返利'
	)a
	where a.num=1
)a 
left join 
(
-- 发票表
select sdt invoice_date,
	invoice_no,		-- 发票号码	
	invoice_code,		-- 发票代码	
	company_code,		-- 公司代码		
	customer_code,		-- 客户编码
	total_amount,		-- 总金额
	if(offline_flag_code=1,'是','否') as offline_flag,		-- 是否线下开票 0 否 1 是
	regexp_replace(invoice_remark,'\\n|\\r|\\t','') invoice_remark		-- 发票的备注
	-- row_number() over(partition by invoice_no order by sdt desc)	as num1
from csx_dwd.csx_dwd_sss_invoice_di
where sdt>='20200101'
-- and invoice_no in('05502566','85686779')
and delete_flag='0'
and sync_status=1
)b on a.customer_no=b.customer_code -- and a.company_code=b.company_code
left join 
(
select customer_id,
	customer_code,
	customer_name,     --  客户名称
	first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	third_category_code,     --  三级客户分类编码
	third_category_name,     --  三级客户分类名称

	contact_person,     --  联系人姓名
	contact_phone,     --  联系电话
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name     --  城市组名称(业绩划分)
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and customer_type_code=4
)c on c.customer_code=a.customer_no
-- and b.num1=1
;

	
-- 客户合同回款明细
insert overwrite table csx_analyse.csx_analyse_fr_customer_contract_back_detail_df partition(sdt)
select 
	concat_ws('-','${sdt_yes}',a.customer_no,a.htbh,b.claim_bill_code,b.profit_center_code) as biz_id,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.customer_no customer_code,
	a.customer_name,
	c.first_category_code,     --  一级客户分类编码
	c.first_category_name,     --  一级客户分类名称
	c.second_category_code,     --  二级客户分类编码
	c.second_category_name,     --  二级客户分类名称
	c.third_category_code,     --  三级客户分类编码
	c.third_category_name,     --  三级客户分类名称
	c.contact_person,     --  联系人姓名
	c.contact_phone,     --  联系电话	
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
	a.djsqsj,		-- 单据申请时间
	a.htbh,		-- 合同编号
	a.htqsrq,		-- 合同起始日期
	a.htzzrq,		-- 合同终止日期	
	a.htjey,		-- 合同金额(元)
	b.claim_date,
	b.claim_bill_code,-- claim_employee_code,claim_employee_name,
	b.company_code company_code_claim,
	b.profit_center_code,  -- 利润中心
	b.credit_code, -- 信控编号
	b.claim_amt,	-- 认领金额（含核销与未核销的，含补救单）
	b.summary_text,
	b.remark,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	'${sdt_yes}' as sdt_c, 
	'${sdt_yes}' as sdt -- 统计日期 		
from 
(
select *
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
		regexp_replace(djsqsj,'-','') djsqsj,		-- 单据申请时间
		htbh,		-- 合同编号
		regexp_replace(htqsrq,'-','') htqsrq,		-- 合同起始日期
		regexp_replace(htzzrq,'-','') htzzrq,		-- 合同终止日期
		htjey,		-- 合同金额(元)
		months_between(date_sub(to_date(from_unixtime(unix_timestamp(htzzrq,'yyyy-MM-dd'))),-1),
						to_date(from_unixtime(unix_timestamp(htqsrq,'yyyy-MM-dd')))) as ht_month,		-- 供货年限
		row_number() over(partition by customer_no order by htqsrq desc)	as num			
	from csx_analyse.csx_analyse_report_weaver_contract_df
	where sdt='${sdt_yes}'
	and if_fanli='非返利'
	)a
	where a.num=1
)a 
-- 回款认领金额
left join
(
	select -- regexp_replace(substr(claim_time,1,10),'-','') claim_date,
		-- regexp_replace(substr(posting_time,1,10),'-','') post_date, -- =sdt
		sdt claim_date,
		claim_bill_code,-- claim_employee_code,claim_employee_name,
		customer_code,customer_name,company_code,
		profit_center_code,  -- 利润中心、同一认领单有多条数据利润中心不一样 如01322122300477
		credit_code, -- 信控编号
		claim_amt,	-- 认领金额（含核销与未核销的，含补救单）
		summary_text,
		regexp_replace(remark,'\\n|\\r|\\t','') remark
	from csx_dwd.csx_dwd_sss_money_back_di  -- 过账日期分区
	where sdt>='20200101'
	and sdt<='${sdt_yes}' 
	and delete_flag='0'
	and (paid_amt<>0 or residue_amt<>0) -- 剔除补救单和对应原单
)b on a.customer_no=b.customer_code -- and a.company_code=b.company_code
left join 
(
select customer_id,
	customer_code,
	customer_name,     --  客户名称
	first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	third_category_code,     --  三级客户分类编码
	third_category_name,     --  三级客户分类名称
	contact_person,     --  联系人姓名
	contact_phone,     --  联系电话
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name     --  城市组名称(业绩划分)
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and customer_type_code=4
)c on c.customer_code=a.customer_no
;


-- 客户合同销售开票回款金额
insert overwrite table csx_analyse.csx_analyse_fr_customer_contract_sale_invoice_back_df partition(sdt)
select 
	concat_ws('-','${sdt_yes}',a.customer_no,a.htbh) as biz_id,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
	a.customer_no,
	a.customer_name,
	e.first_category_code,     --  一级客户分类编码
	e.first_category_name,     --  一级客户分类名称
	e.second_category_code,     --  二级客户分类编码
	e.second_category_name,     --  二级客户分类名称
	e.third_category_code,     --  三级客户分类编码
	e.third_category_name,     --  三级客户分类名称
	e.contact_person,     --  联系人姓名
	e.contact_phone,     --  联系电话	
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
	a.djsqsj,		-- 单据申请时间
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
	b.claim_sdt_max,
	b.count_claim,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	'${sdt_yes}' as sdt_c, 
	channel_code,
	channel_name,
	sales_user_number,
	sales_user_name,	
	'${sdt_yes}' as sdt -- 统计日期 		
from 
(
select *
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
		regexp_replace(djsqsj,'-','') djsqsj,		-- 单据申请时间
		htbh,		-- 合同编号
		regexp_replace(htqsrq,'-','') htqsrq,		-- 合同起始日期
		regexp_replace(htzzrq,'-','') htzzrq,		-- 合同终止日期
		htjey,		-- 合同金额(元)
		months_between(date_sub(to_date(from_unixtime(unix_timestamp(htzzrq,'yyyy-MM-dd'))),-1),
						to_date(from_unixtime(unix_timestamp(htqsrq,'yyyy-MM-dd')))) as ht_month,		-- 供货年限
		row_number() over(partition by customer_no order by htqsrq desc)	as num			
	from csx_analyse.csx_analyse_report_weaver_contract_df
	where sdt='${sdt_yes}'
	and if_fanli='非返利'
	)a
	where a.num=1
)a 
-- 回款认领金额  回单金额\最近回单日期\回单数量
left join
(
	select 
		customer_code,
		sum(claim_amt) claim_amt,	-- 认领金额（含核销与未核销的，含补救单）
		max(sdt) claim_sdt_max,
		count(distinct claim_bill_code) count_claim
	from csx_dwd.csx_dwd_sss_money_back_di  -- 过账日期分区
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
-- 客户下单情况动态信息表
left join
(
  select customer_code,first_sale_date,last_sale_date,sale_active_days,sale_total_amt,
	-- 至今距离天数
	datediff(to_date(date_sub(current_date,1)),to_date(from_unixtime(unix_timestamp(last_sale_date,'yyyyMMdd')))) date_diff1	  
  from csx_dws.csx_dws_crm_customer_active_di
  where sdt = 'current'
)d on d.customer_code=a.customer_no 
left join 
(
select customer_id,
	customer_code,
	customer_name,     --  客户名称
	first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	third_category_code,     --  三级客户分类编码
	third_category_name,     --  三级客户分类名称
	contact_person,     --  联系人姓名
	contact_phone,     --  联系电话
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	channel_code,
	channel_name,
	sales_user_number,
	sales_user_name	
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and customer_type_code=4
)e on e.customer_code=a.customer_no
;











