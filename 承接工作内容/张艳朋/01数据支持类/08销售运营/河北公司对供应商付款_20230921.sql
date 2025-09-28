   -- 扣款

select 
	*
from 
	(
	select 
		substr(sdt,1,4) syear,
        company_code,
        max(company_name) as company_name,
        supplier_code,
        max(supplier_name) as supplier_name,
        sum(business_amount) as sum_amont
	from 
		csx_dwd.csx_dwd_pss_statement_prepayment_operation_record_di
	where 
		business_type = 2 -- 业务类型 1：新增  2 扣减
		-- 操作类型：1新增预付款 2生成付款单 3取消付款 4供应商付款 5手工调整 6扣减预付款（退汇）7供应商付款（退汇）8手工调整(导入) 9同步SAP 10余额加减 11余额转换 12取消付款
		and operation_type in (4,6,8,10,11)
         -- 金额类型：1可用 2冻结 3不可用
         and amount_type<>3                                                         
         and sdt>='20220101'
	group by 
		substr(sdt,1,4),company_code,supplier_code
	) a 
where 
	company_name like'%河北%'
	
	
	
select 
	*
from 
	(
	select 
		substr(sdt,1,4) syear,
        company_code,
        max(company_name) as company_name,
        supplier_code,
        max(supplier_name) as supplier_name,
        sum(business_amount) as sum_amont
	from 
		csx_dwd.csx_dwd_pss_statement_prepayment_operation_record_di
	where 
		sdt>='20230101' and sdt<='20230331'
		and business_type = 2 -- 业务类型 1：新增  2 扣减
		-- 操作类型：1新增预付款 2生成付款单 3取消付款 4供应商付款 5手工调整 6扣减预付款（退汇）7供应商付款（退汇）8手工调整(导入) 9同步SAP 10余额加减 11余额转换 12取消付款
		and operation_type in (4,6,8,10,11)
         -- 金额类型：1可用 2冻结 3不可用
        and amount_type<>3                                                         
	group by 
		substr(sdt,1,4),company_code,supplier_code
	) a 
where 
	company_name like'%河北%'