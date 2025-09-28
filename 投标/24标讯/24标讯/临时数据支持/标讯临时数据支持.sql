-- 2115公司2020年至今客户的福利BBC开票情况 20231121
select 
	b.bloc_code,     --  集团编码
	b.bloc_name,     --  集团名称		
	a.company_code,
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	a.customer_code,
	b.customer_name,     --  客户名称
	b.first_category_name,     --  一级客户分类名称
	b.second_category_name,     --  二级客户分类名称
	invoice_amount,
	a.qt_invoice_amount,
	a.fl_invoice_amount,
	a.fl_invoice_amount_2020,
	a.fl_invoice_amount_2021,
	a.fl_invoice_amount_2022,
	a.fl_invoice_amount_2023,
	
	a.bbc_invoice_amount,
	a.bbc_invoice_amount_2020,
	a.bbc_invoice_amount_2021,
	a.bbc_invoice_amount_2022,
	a.bbc_invoice_amount_2023	
from
(	
select
	a.customer_code,
	a.company_code,
	sum(a.invoice_amount) invoice_amount,
	sum(case when b.business_type_code is null then a.invoice_amount end) qt_invoice_amount,
	sum(case when b.business_type_code='2' then a.invoice_amount end) fl_invoice_amount,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) fl_invoice_amount_2020,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) fl_invoice_amount_2021,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) fl_invoice_amount_2022,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) fl_invoice_amount_2023,
	
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') then a.invoice_amount end) bbc_invoice_amount,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) bbc_invoice_amount_2020,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) bbc_invoice_amount_2021,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) bbc_invoice_amount_2022,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) bbc_invoice_amount_2023	
from 
(
	select 
	case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
		 when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
		 else split(source_bill_no,'-')[0]
		 end as source_bill_no_new,
		bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		sdt,
		source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
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
		invoice_amount  	-- 开票金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	and date_format(happen_date,'yyyy-MM-dd')>='2020-06-01'
	and company_code='2115'
)a
left join 
(
	select 
	-- order_code,
	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
		 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
		 else split(order_code,'-')[0]
		 end as order_code_new,	
	business_type_code,
	business_type_name,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit
	from csx_dws.csx_dws_sale_detail_di
	where channel_code in('1','7','9')
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	and business_type_code in('2','6')
	group by 
	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
		 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
		 else split(order_code,'-')[0]
		 end,		
	business_type_code,
	business_type_name
)b on a.source_bill_no_new=b.order_code_new
group by a.customer_code,a.company_code
)a
left join 
(
	select  
		bloc_code,     --  集团编码
		bloc_name,     --  集团名称
		parent_id,customer_id,
		customer_code,
		customer_name,     --  客户名称
		first_category_name,     --  一级客户分类名称
		second_category_name,     --  二级客户分类名称
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)b on a.customer_code=b.customer_code;


-- 2115公司2020年至今客户的福利BBC开票情况 20231121
select 
	b.bloc_code,     --  集团编码
	b.bloc_name,     --  集团名称		
	a.company_code,
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	a.customer_code,
	b.customer_name,     --  客户名称
	b.first_category_name,     --  一级客户分类名称
	b.second_category_name,     --  二级客户分类名称
	invoice_amount,
	a.qt_invoice_amount,
	a.fl_invoice_amount,
	a.fl_invoice_amount_2020,
	a.fl_invoice_amount_2021,
	a.fl_invoice_amount_2022,
	a.fl_invoice_amount_2023,
	
	a.bbc_invoice_amount,
	a.bbc_invoice_amount_2020,
	a.bbc_invoice_amount_2021,
	a.bbc_invoice_amount_2022,
	a.bbc_invoice_amount_2023	
from
(	
select
	a.customer_code,
	a.company_code,
	sum(a.invoice_amount) invoice_amount,
	sum(case when b.business_type_code is null then a.invoice_amount end) qt_invoice_amount,
	sum(case when b.business_type_code='2' then a.invoice_amount end) fl_invoice_amount,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) fl_invoice_amount_2020,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) fl_invoice_amount_2021,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) fl_invoice_amount_2022,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) fl_invoice_amount_2023,
	
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') then a.invoice_amount end) bbc_invoice_amount,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) bbc_invoice_amount_2020,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) bbc_invoice_amount_2021,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) bbc_invoice_amount_2022,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) bbc_invoice_amount_2023	
from 
(
	select 
	case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
		 when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
		 else split(source_bill_no,'-')[0]
		 end as source_bill_no_new,
		bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		sdt,
		source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
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
		invoice_amount  	-- 开票金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	and date_format(happen_date,'yyyy-MM-dd')>='2020-06-01'
	and company_code='2115'
)a
left join 
(
	select 
	-- order_code,
	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
		 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
		 else split(order_code,'-')[0]
		 end as order_code_new,	
	business_type_code,
	business_type_name,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit
	from csx_dws.csx_dws_sale_detail_di
	where channel_code in('1','7','9')
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	and business_type_code in('2','6')
	group by 
	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
		 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
		 else split(order_code,'-')[0]
		 end,		
	business_type_code,
	business_type_name
)b on a.source_bill_no_new=b.order_code_new
group by a.customer_code,a.company_code
)a
left join 
(
	select  
		bloc_code,     --  集团编码
		bloc_name,     --  集团名称
		parent_id,customer_id,
		customer_code,
		customer_name,     --  客户名称
		first_category_name,     --  一级客户分类名称
		second_category_name,     --  二级客户分类名称
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)b on a.customer_code=b.customer_code;



-- 2115公司2020年至今客户的日配福利BBC开票情况 20231220
select 
	b.bloc_code,     --  集团编码
	b.bloc_name,     --  集团名称		
	a.company_code,
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	a.customer_code,
	b.customer_name,     --  客户名称
	b.first_category_name,     --  一级客户分类名称
	b.second_category_name,     --  二级客户分类名称
	invoice_amount,
	a.qt_invoice_amount,
	a.rp_invoice_amount,
	a.rp_invoice_amount_2020,
	a.rp_invoice_amount_2021,
	a.rp_invoice_amount_2022,
	a.rp_invoice_amount_2023,	
	
	a.fl_invoice_amount,
	a.fl_invoice_amount_2020,
	a.fl_invoice_amount_2021,
	a.fl_invoice_amount_2022,
	a.fl_invoice_amount_2023,
	
	a.bbc_invoice_amount,
	a.bbc_invoice_amount_2020,
	a.bbc_invoice_amount_2021,
	a.bbc_invoice_amount_2022,
	a.bbc_invoice_amount_2023	
from
(	
select
	a.customer_code,
	a.company_code,
	sum(a.invoice_amount) invoice_amount,
	sum(case when b.business_type_code is null then a.invoice_amount end) qt_invoice_amount,
	sum(case when b.business_type_code='1' then a.invoice_amount end) rp_invoice_amount,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) rp_invoice_amount_2020,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) rp_invoice_amount_2021,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) rp_invoice_amount_2022,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) rp_invoice_amount_2023,
	
	sum(case when b.business_type_code='2' then a.invoice_amount end) fl_invoice_amount,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) fl_invoice_amount_2020,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) fl_invoice_amount_2021,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) fl_invoice_amount_2022,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) fl_invoice_amount_2023,
	
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') then a.invoice_amount end) bbc_invoice_amount,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) bbc_invoice_amount_2020,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) bbc_invoice_amount_2021,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) bbc_invoice_amount_2022,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) bbc_invoice_amount_2023	
from 
(
	select 
	case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
		 when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
		 else split(source_bill_no,'-')[0]
		 end as source_bill_no_new,
		bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		sdt,
		source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
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
		invoice_amount  	-- 开票金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	and date_format(happen_date,'yyyy-MM-dd')>='2020-06-01'
	and company_code='2115'
)a
left join 
(
	select 
	-- order_code,
	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
		 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
		 else split(order_code,'-')[0]
		 end as order_code_new,	
	business_type_code,
	business_type_name,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit
	from csx_dws.csx_dws_sale_detail_di
	-- where channel_code in('1','7','9')
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	where business_type_code in('1','2','6')
	group by 
	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
		 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
		 else split(order_code,'-')[0]
		 end,		
	business_type_code,
	business_type_name
)b on a.source_bill_no_new=b.order_code_new
group by a.customer_code,a.company_code
)a
left join 
(
	select  
		bloc_code,     --  集团编码
		bloc_name,     --  集团名称
		parent_id,customer_id,
		customer_code,
		customer_name,     --  客户名称
		first_category_name,     --  一级客户分类名称
		second_category_name,     --  二级客户分类名称
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)b on a.customer_code=b.customer_code;


-- 2115公司2020年至今客户的日配福利BBC开票情况 202340102
select 
	b.bloc_code,     --  集团编码
	b.bloc_name,     --  集团名称		
	a.company_code,
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	a.customer_code,
	b.customer_name,     --  客户名称
	b.first_category_name,     --  一级客户分类名称
	b.second_category_name,     --  二级客户分类名称
	invoice_amount,
	a.qt_invoice_amount,
	a.rp_invoice_amount,
	a.rp_invoice_amount_2020,
	a.rp_invoice_amount_2021,
	a.rp_invoice_amount_2022,
	a.rp_invoice_amount_2023,
	a.rp_invoice_amount_2024,	
	
	a.fl_invoice_amount,
	a.fl_invoice_amount_2020,
	a.fl_invoice_amount_2021,
	a.fl_invoice_amount_2022,
	a.fl_invoice_amount_2023,
	a.fl_invoice_amount_2024,
	
	a.bbc_invoice_amount,
	a.bbc_invoice_amount_2020,
	a.bbc_invoice_amount_2021,
	a.bbc_invoice_amount_2022,
	a.bbc_invoice_amount_2023,
	a.bbc_invoice_amount_2024
from
(	
select
	a.customer_code,
	a.company_code,
	sum(a.invoice_amount) invoice_amount,
	sum(case when b.business_type_code is null then a.invoice_amount end) qt_invoice_amount,
	sum(case when b.business_type_code='1' then a.invoice_amount end) rp_invoice_amount,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) rp_invoice_amount_2020,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) rp_invoice_amount_2021,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) rp_invoice_amount_2022,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) rp_invoice_amount_2023,
	sum(case when b.business_type_code='1' and substr(a.happen_date,1,4)=2024 then a.invoice_amount end) rp_invoice_amount_2024,
	
	sum(case when b.business_type_code='2' then a.invoice_amount end) fl_invoice_amount,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) fl_invoice_amount_2020,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) fl_invoice_amount_2021,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) fl_invoice_amount_2022,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) fl_invoice_amount_2023,
	sum(case when b.business_type_code='2' and substr(a.happen_date,1,4)=2024 then a.invoice_amount end) fl_invoice_amount_2024,
	
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') then a.invoice_amount end) bbc_invoice_amount,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2020 then a.invoice_amount end) bbc_invoice_amount_2020,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2021 then a.invoice_amount end) bbc_invoice_amount_2021,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2022 then a.invoice_amount end) bbc_invoice_amount_2022,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2023 then a.invoice_amount end) bbc_invoice_amount_2023,
	sum(case when (b.business_type_code='6' or a.source_sys='BBC') and substr(a.happen_date,1,4)=2024 then a.invoice_amount end) bbc_invoice_amount_2024	
from 
(
	select 
	case when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-2)
		 when source_sys='BBC' and substr(split(source_bill_no,'-')[0],1,1)='B' and substr(split(source_bill_no,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(source_bill_no,'-')[0],2,length(split(source_bill_no,'-')[0])-1)
		 else split(source_bill_no,'-')[0]
		 end as source_bill_no_new,
		bill_type,  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单 -1 期初单
		sdt,
		source_sys,	-- 来源系统 MALL B端销售 BBC BBC端 BEGIN期初
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
		invoice_amount  	-- 开票金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	and date_format(happen_date,'yyyy-MM-dd')>='2020-06-01'
	and company_code='2115'
)a
left join 
(
	select 
	-- order_code,
	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
		 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
		 else split(order_code,'-')[0]
		 end as order_code_new,	
	business_type_code,
	business_type_name,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit
	from csx_dws.csx_dws_sale_detail_di
	-- where channel_code in('1','7','9')
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	where business_type_code in('1','2','6')
	group by 
	case when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) in ('A','B','C','D','E') then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-2)
		 when business_type_code='6' and substr(split(order_code,'-')[0],1,1)='B' and substr(split(order_code,'-')[0],-1,1) not in ('A','B','C','D','E')then substr(split(order_code,'-')[0],2,length(split(order_code,'-')[0])-1)
		 else split(order_code,'-')[0]
		 end,		
	business_type_code,
	business_type_name
)b on a.source_bill_no_new=b.order_code_new
group by a.customer_code,a.company_code
)a
left join 
(
	select  
		bloc_code,     --  集团编码
		bloc_name,     --  集团名称
		parent_id,customer_id,
		customer_code,
		customer_name,     --  客户名称
		first_category_name,     --  一级客户分类名称
		second_category_name,     --  二级客户分类名称
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)b on a.customer_code=b.customer_code;




-- 清单客户的各年销售与开票情况 202340124

drop table csx_analyse_tmp.tmp1; 
create table csx_analyse_tmp.tmp1 as 
select '227927' as customer_code,'139268' as credit_code
union all select '227857' as customer_code,'138959' as credit_code
union all select '226141' as customer_code,'139100' as credit_code
union all select '130867' as customer_code,'130867' as credit_code
union all select '130849' as customer_code,'130849' as credit_code
union all select '130571' as customer_code,'130571' as credit_code
union all select '130563' as customer_code,'130563' as credit_code
union all select '130449' as customer_code,'130449' as credit_code
union all select '128700' as customer_code,'128700' as credit_code
union all select '128082' as customer_code,'128082' as credit_code
union all select '127974' as customer_code,'127974' as credit_code
union all select '127322' as customer_code,'127322' as credit_code
union all select '127096' as customer_code,'127096' as credit_code
union all select '126889' as customer_code,'126889' as credit_code
union all select '126781' as customer_code,'126781' as credit_code
union all select '126586' as customer_code,'126586' as credit_code
union all select '126327' as customer_code,'126327' as credit_code
union all select '126147' as customer_code,'126147' as credit_code
union all select '125801' as customer_code,'125801' as credit_code
union all select '125629' as customer_code,'125629' as credit_code
union all select '125233' as customer_code,'125233' as credit_code
union all select '124597' as customer_code,'124597' as credit_code
union all select '124230' as customer_code,'124230' as credit_code
union all select '124211' as customer_code,'124211' as credit_code
union all select '122852' as customer_code,'122852' as credit_code
union all select '122781' as customer_code,'122781' as credit_code
union all select '122738' as customer_code,'122738' as credit_code
union all select '122620' as customer_code,'122620' as credit_code
union all select '121880' as customer_code,'121880' as credit_code
union all select '121008' as customer_code,'121008' as credit_code
union all select '120930' as customer_code,'120930' as credit_code
union all select '120755' as customer_code,'120755' as credit_code
union all select '120718' as customer_code,'120718' as credit_code
union all select '120681' as customer_code,'120681' as credit_code
union all select '120517' as customer_code,'120517' as credit_code
union all select '120490' as customer_code,'120490' as credit_code
union all select '120366' as customer_code,'120366' as credit_code
union all select '120220' as customer_code,'120220' as credit_code
union all select '120131' as customer_code,'120131' as credit_code
union all select '120101' as customer_code,'120101' as credit_code
union all select '120056' as customer_code,'120056' as credit_code
union all select '119980' as customer_code,'119980' as credit_code
union all select '119898' as customer_code,'119898' as credit_code
union all select '119642' as customer_code,'119642' as credit_code
union all select '119402' as customer_code,'119402' as credit_code
union all select '119377' as customer_code,'119377' as credit_code
union all select '119376' as customer_code,'119376' as credit_code
union all select '118975' as customer_code,'118975' as credit_code
union all select '118471' as customer_code,'118471' as credit_code
union all select '118466' as customer_code,'118466' as credit_code
union all select '118396' as customer_code,'118396' as credit_code
union all select '118314' as customer_code,'118314' as credit_code
union all select '118084' as customer_code,'118084' as credit_code
union all select '117628' as customer_code,'117628' as credit_code
union all select '117450' as customer_code,'117450' as credit_code
union all select '117448' as customer_code,'117448' as credit_code
union all select '117444' as customer_code,'117444' as credit_code
union all select '117424' as customer_code,'117424' as credit_code
union all select '117405' as customer_code,'117405' as credit_code
union all select '117384' as customer_code,'117384' as credit_code
union all select '117374' as customer_code,'117374' as credit_code
union all select '117349' as customer_code,'117349' as credit_code
union all select '117250' as customer_code,'117250' as credit_code
union all select '117226' as customer_code,'117226' as credit_code
union all select '117221' as customer_code,'117221' as credit_code
union all select '117208' as customer_code,'117208' as credit_code
union all select '117198' as customer_code,'117198' as credit_code
union all select '117197' as customer_code,'117197' as credit_code
union all select '117197' as customer_code,'135698' as credit_code
union all select '117193' as customer_code,'117193' as credit_code
union all select '117192' as customer_code,'117192' as credit_code
union all select '117133' as customer_code,'117133' as credit_code
union all select '117133' as customer_code,'135702' as credit_code
union all select '117132' as customer_code,'117132' as credit_code
union all select '117114' as customer_code,'117114' as credit_code
union all select '115703' as customer_code,'115703' as credit_code
union all select '115692' as customer_code,'115692' as credit_code
union all select '114864' as customer_code,'114864' as credit_code
union all select '114706' as customer_code,'114706' as credit_code
union all select '114693' as customer_code,'114693' as credit_code
union all select '114613' as customer_code,'114613' as credit_code
union all select '114548' as customer_code,'114548' as credit_code
union all select '114540' as customer_code,'114540' as credit_code
union all select '114540' as customer_code,'135355' as credit_code
union all select '113906' as customer_code,'113906' as credit_code
union all select '113870' as customer_code,'113870' as credit_code
union all select '113850' as customer_code,'113850' as credit_code
union all select '113837' as customer_code,'113837' as credit_code
union all select '113809' as customer_code,'113809' as credit_code
union all select '113779' as customer_code,'113779' as credit_code
union all select '113758' as customer_code,'113758' as credit_code
;


select 
	b.bloc_code,     --  集团编码
	b.bloc_name,     --  集团名称		
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	a.customer_code,
	b.customer_name,     --  客户名称
	a.credit_code,
	b.first_category_name,     --  一级客户分类名称
	b.second_category_name,     --  二级客户分类名称
	a.invoice_amount,
	a.invoice_amount_2019,
	a.invoice_amount_2020,
	a.invoice_amount_2021,
	a.invoice_amount_2022,
	a.invoice_amount_2023,
	a.invoice_amount_2024,	
	
	a.sale_amt,
	a.sale_amt_2019,
	a.sale_amt_2020,
	a.sale_amt_2021,
	a.sale_amt_2022,
	a.sale_amt_2023,
	a.sale_amt_2024	
from
(	
select
	a.customer_code,
	a.credit_code,
	sum(a.invoice_amount) invoice_amount,
	sum(case when a.syear=2019 then a.invoice_amount end) invoice_amount_2019,
	sum(case when a.syear=2020 then a.invoice_amount end) invoice_amount_2020,
	sum(case when a.syear=2021 then a.invoice_amount end) invoice_amount_2021,
	sum(case when a.syear=2022 then a.invoice_amount end) invoice_amount_2022,
	sum(case when a.syear=2023 then a.invoice_amount end) invoice_amount_2023,
	sum(case when a.syear=2024 then a.invoice_amount end) invoice_amount_2024,
	
	sum(a.sale_amt) sale_amt,
	sum(case when a.syear=2019 then a.sale_amt end) sale_amt_2019,
	sum(case when a.syear=2020 then a.sale_amt end) sale_amt_2020,
	sum(case when a.syear=2021 then a.sale_amt end) sale_amt_2021,
	sum(case when a.syear=2022 then a.sale_amt end) sale_amt_2022,
	sum(case when a.syear=2023 then a.sale_amt end) sale_amt_2023,
	sum(case when a.syear=2024 then a.sale_amt end) sale_amt_2024	
	
from 
(
	select 
	substr(happen_date,1,4) as syear,
	customer_code,
	credit_code,
	0 as sale_amt,
	0 as profit,
	sum(invoice_amount) as invoice_amount  	-- 开票金额
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','')
	-- and date_format(happen_date,'yyyy-MM-dd')>='2020-06-01'
	and credit_code in (select credit_code from csx_analyse_tmp.tmp1)
	group by 
	substr(happen_date,1,4),
	customer_code,
	credit_code
	
	union all
	select 
	substr(sdt,1,4) as syear,
	customer_code,
	credit_code,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit,
	0 as invoice_amount  	-- 开票金额
	from csx_dws.csx_dws_sale_detail_di
	-- where channel_code in('1','7','9')
	-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	-- 20230101后的销售数据才有信控号，历史只有客户号
	where (sdt>='20230101' and credit_code in (select credit_code from csx_analyse_tmp.tmp1))
	or (sdt<'20230101' and customer_code in (select distinct customer_code from csx_analyse_tmp.tmp1))
	group by 
	substr(sdt,1,4),
	customer_code,
	credit_code
)a
group by a.customer_code,a.credit_code
)a
left join 
(
	select  
		bloc_code,     --  集团编码
		bloc_name,     --  集团名称
		parent_id,customer_id,
		customer_code,
		customer_name,     --  客户名称
		first_category_name,     --  一级客户分类名称
		second_category_name,     --  二级客户分类名称
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)b on a.customer_code=b.customer_code;



-- 清单客户开票明细（中类） 20240206

select
performance_region_name,
performance_province_name,
performance_city_name,
bloc_code,
bloc_name,
customer_code,
customer_name,
-- first_category_code,
first_category_name,
-- second_category_code,
second_category_name,
-- third_category_code,
third_category_name,
concat(contact_person,'/',contact_phone) contact_person_phone,
company_code,
company_name,
wfqyztgsmc,
khmc,
ywlx,
thfs,
djsqsj,
htbh,
htqsrq,
htzzrq,
htjey/10000 htjey,
invoice_date,
invoice_no,
invoice_code,
total_amount,
offline_flag,
invoice_remark,
invoice_customer_name,
classify_large_name_group,
classify_middle_name_group,
classify_small_name_group,
purchase_name_group
from csx_analyse.csx_analyse_fr_customer_contract_invoice_detail_df
where sdt='20240205'
and customer_code in(
'227927','227857','226141','130867','130849','130571','130563','130449','128700','128082','127974','127322','127096',
'126889','126781','126586','126327','126147','125801','125629','125233','124597','124230','124211','122852','122781',
'122738','122620','121880','121008','120930','120755','120718','120681','120517','120490','120366','120220','120131',
'120101','120056','119980','119898','119642','119402','119377','119376','118975','118471','118466','118396','118314',
'118084','117628','117450','117448','117444','117424','117405','117384','117374','117349','117250','117226','117221',
'117208','117198','117197','117193','117192','117133','117132','117114','115703','115692','114864','114706','114693',
'114613','114548','114540','113906','113870','113850','113837','113809','113779','113758','127389','126407','121578',
'118746','122817'
);



-- 清单客户小类开票明细 20240207
drop table csx_analyse_tmp.tmp1; 
create table csx_analyse_tmp.tmp1 as 
select '227927' as customer_code,'139268' as credit_code
union all select '227857' as customer_code,'138959' as credit_code
union all select '226141' as customer_code,'139100' as credit_code
union all select '130867' as customer_code,'130867' as credit_code
union all select '130849' as customer_code,'130849' as credit_code
union all select '130571' as customer_code,'130571' as credit_code
union all select '130563' as customer_code,'130563' as credit_code
union all select '130449' as customer_code,'130449' as credit_code
union all select '128700' as customer_code,'128700' as credit_code
union all select '128082' as customer_code,'128082' as credit_code
union all select '127974' as customer_code,'127974' as credit_code
union all select '127322' as customer_code,'127322' as credit_code
union all select '127096' as customer_code,'127096' as credit_code
union all select '126889' as customer_code,'126889' as credit_code
union all select '126781' as customer_code,'126781' as credit_code
union all select '126586' as customer_code,'126586' as credit_code
union all select '126327' as customer_code,'126327' as credit_code
union all select '126147' as customer_code,'126147' as credit_code
union all select '125801' as customer_code,'125801' as credit_code
union all select '125629' as customer_code,'125629' as credit_code
union all select '125233' as customer_code,'125233' as credit_code
union all select '124597' as customer_code,'124597' as credit_code
union all select '124230' as customer_code,'124230' as credit_code
union all select '124211' as customer_code,'124211' as credit_code
union all select '122852' as customer_code,'122852' as credit_code
union all select '122781' as customer_code,'122781' as credit_code
union all select '122738' as customer_code,'122738' as credit_code
union all select '122620' as customer_code,'122620' as credit_code
union all select '121880' as customer_code,'121880' as credit_code
union all select '121008' as customer_code,'121008' as credit_code
union all select '120930' as customer_code,'120930' as credit_code
union all select '120755' as customer_code,'120755' as credit_code
union all select '120718' as customer_code,'120718' as credit_code
union all select '120681' as customer_code,'120681' as credit_code
union all select '120517' as customer_code,'120517' as credit_code
union all select '120490' as customer_code,'120490' as credit_code
union all select '120366' as customer_code,'120366' as credit_code
union all select '120220' as customer_code,'120220' as credit_code
union all select '120131' as customer_code,'120131' as credit_code
union all select '120101' as customer_code,'120101' as credit_code
union all select '120056' as customer_code,'120056' as credit_code
union all select '119980' as customer_code,'119980' as credit_code
union all select '119898' as customer_code,'119898' as credit_code
union all select '119642' as customer_code,'119642' as credit_code
union all select '119402' as customer_code,'119402' as credit_code
union all select '119377' as customer_code,'119377' as credit_code
union all select '119376' as customer_code,'119376' as credit_code
union all select '118975' as customer_code,'118975' as credit_code
union all select '118471' as customer_code,'118471' as credit_code
union all select '118466' as customer_code,'118466' as credit_code
union all select '118396' as customer_code,'118396' as credit_code
union all select '118314' as customer_code,'118314' as credit_code
union all select '118084' as customer_code,'118084' as credit_code
union all select '117628' as customer_code,'117628' as credit_code
union all select '117450' as customer_code,'117450' as credit_code
union all select '117448' as customer_code,'117448' as credit_code
union all select '117444' as customer_code,'117444' as credit_code
union all select '117424' as customer_code,'117424' as credit_code
union all select '117405' as customer_code,'117405' as credit_code
union all select '117384' as customer_code,'117384' as credit_code
union all select '117374' as customer_code,'117374' as credit_code
union all select '117349' as customer_code,'117349' as credit_code
union all select '117250' as customer_code,'117250' as credit_code
union all select '117226' as customer_code,'117226' as credit_code
union all select '117221' as customer_code,'117221' as credit_code
union all select '117208' as customer_code,'117208' as credit_code
union all select '117198' as customer_code,'117198' as credit_code
union all select '117197' as customer_code,'117197' as credit_code
union all select '117197' as customer_code,'135698' as credit_code
union all select '117193' as customer_code,'117193' as credit_code
union all select '117192' as customer_code,'117192' as credit_code
union all select '117133' as customer_code,'117133' as credit_code
union all select '117133' as customer_code,'135702' as credit_code
union all select '117132' as customer_code,'117132' as credit_code
union all select '117114' as customer_code,'117114' as credit_code
union all select '115703' as customer_code,'115703' as credit_code
union all select '115692' as customer_code,'115692' as credit_code
union all select '114864' as customer_code,'114864' as credit_code
union all select '114706' as customer_code,'114706' as credit_code
union all select '114693' as customer_code,'114693' as credit_code
union all select '114613' as customer_code,'114613' as credit_code
union all select '114548' as customer_code,'114548' as credit_code
union all select '114540' as customer_code,'114540' as credit_code
union all select '114540' as customer_code,'135355' as credit_code
union all select '113906' as customer_code,'113906' as credit_code
union all select '113870' as customer_code,'113870' as credit_code
union all select '113850' as customer_code,'113850' as credit_code
union all select '113837' as customer_code,'113837' as credit_code
union all select '113809' as customer_code,'113809' as credit_code
union all select '113779' as customer_code,'113779' as credit_code
union all select '113758' as customer_code,'113758' as credit_code

union all select '127389' as customer_code,'127389' as credit_code
union all select '126407' as customer_code,'126407' as credit_code
union all select '121578' as customer_code,'121578' as credit_code
union all select '126407' as customer_code,'140817' as credit_code
union all select '118746' as customer_code,'118746' as credit_code
union all select '122817' as customer_code,'122817' as credit_code
;


-- 客户合同开票明细
with apply_goods_group as
-- 发票商品明细
(
select d.order_code as order_no,d.total_amt as total_amount,
	e.purchase_group_code,e.purchase_group_name,    
	e.classify_large_code,e.classify_large_name, -- 管理大类
	e.classify_middle_code,e.classify_middle_name,-- 管理中类
	e.classify_small_code,e.classify_small_name-- 管理小类		
from 	
	(
		select *,
		--  id排名取最新一条
		row_number() over(partition by id order by update_time desc) as id_rank				
		-- from csx_ods.csx_ods_csx_b2b_sss_sss_kp_apply_goods_group_di
		-- where (sdt>='20200101' or sdt='19990101')
		from csx_dwd.csx_dwd_sss_kp_apply_goods_group_di
		where sdt>='20200101'
		and is_delete='0'
	)d
	left join 
	(
		select
		goods_code,
		goods_name,
		purchase_group_code,purchase_group_name,    
		classify_large_code,classify_large_name, -- 管理大类
		classify_middle_code,classify_middle_name,-- 管理中类
		classify_small_code,classify_small_name-- 管理小类
		from csx_dim.csx_dim_basic_goods
		where sdt = 'current'
	)e on d.goods_code=e.goods_code
where d.id_rank=1
),

-- 客户信息
customer_info as
(
select 
	bloc_code,     --  集团编码
	bloc_name,     --  集团名称
	customer_id,
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
),
-- 销售结算对账开票结算详情表_信控号维度
order_credit_invoice_bill as
(
	select 
		source_bill_no,  -- 来源单号
		customer_code,  -- 客户编码
		credit_code  -- 信控号		
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	where sdt='${sdt_yes}'
),
-- 发票表
invoice as
(
select sdt invoice_date,
	invoice_no,		-- 发票号码	
	order_code,		-- 订单编号
	invoice_code,		-- 发票代码	
	company_code,		-- 公司代码		
	customer_code,		-- 客户编码
	total_amount,		-- 总金额
	invoice_customer_name,		-- 客户开发票名称
	if(offline_flag_code=1,'是','否') as offline_flag,		-- 是否线下开票 0 否 1 是
	regexp_replace(invoice_remark,'\\n|\\r|\\t','') invoice_remark		-- 发票的备注
	-- row_number() over(partition by invoice_no order by sdt desc)	as num1
from csx_dwd.csx_dwd_sss_invoice_di
where sdt>='20200101'
-- and invoice_no in('05502566','85686779')
and delete_flag='0'
and sync_status=1
),

-- 发票商品-小类
apply_classify_small_name as
(
	select order_no,classify_middle_name,classify_small_name,sum(total_amount) total_amount
	from apply_goods_group
	group by order_no,classify_middle_name,classify_small_name
)

-- create table csx_analyse_tmp.tmp2 as 
-- 客户合同开票明细
select 
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	a.customer_code,
	c.customer_name,
	-- a.credit_code,
	c.first_category_name,     --  一级客户分类名称
	c.second_category_name,     --  二级客户分类名称
	c.third_category_name,     --  三级客户分类名称
	b.invoice_date,
	b.invoice_no,		-- 发票号码	
	b.order_code,		-- 订单编号
	b.invoice_code,		-- 发票代码	
	b.company_code,		-- 公司代码		
	-- b.customer_code,		-- 客户编码
	-- b.total_amount,		-- 总金额
	b.invoice_customer_name,		-- 客户开发票名称
	b.offline_flag,		-- 是否线下开票 0 否 1 是
	b.invoice_remark,		-- 发票的备注	
	d3.classify_middle_name,
	d3.classify_small_name,
	d3.total_amount	
from
(
select distinct customer_code
from csx_analyse_tmp.tmp1
)a
left join invoice b on a.customer_code=b.customer_code -- and a.company_code=b.company_code
left join customer_info c on c.customer_code=a.customer_code
left join apply_classify_small_name d3 on d3.order_no=b.order_code
;



-- 清单小类TOP客户20200101至今开票 20240208
drop table csx_analyse_tmp.classify_small_tmp1; 
create table csx_analyse_tmp.classify_small_tmp1 as 
select 'B010101' as classify_small_code,'粉类' as classify_small_name
union all select 'B010105' as classify_small_code,'食用菌类（干货）' as classify_small_name
union all select 'B010106' as classify_small_code,'海产类' as classify_small_name
union all select 'B010107' as classify_small_code,'米面制品' as classify_small_name
union all select 'B010108' as classify_small_code,'其他干货类' as classify_small_name
union all select 'B010201' as classify_small_code,'杂粮' as classify_small_name
union all select 'B010203' as classify_small_code,'粳米' as classify_small_name
union all select 'B010302' as classify_small_code,'再制蛋' as classify_small_name
union all select 'B020102' as classify_small_code,'核果' as classify_small_name
union all select 'B020103' as classify_small_code,'瓜果' as classify_small_name
union all select 'B020104' as classify_small_code,'柑橘' as classify_small_name
union all select 'B020202' as classify_small_code,'根茎类' as classify_small_name
union all select 'B020208' as classify_small_code,'调味类' as classify_small_name
union all select 'B020209' as classify_small_code,'其他蔬菜类' as classify_small_name
union all select 'B020210' as classify_small_code,'净菜' as classify_small_name
union all select 'B030101' as classify_small_code,'鸡类' as classify_small_name
union all select 'B030102' as classify_small_code,'鸭类' as classify_small_name
union all select 'B030106' as classify_small_code,'家禽半成品' as classify_small_name
union all select 'B030107' as classify_small_code,'其他禽畜类' as classify_small_name
union all select 'B030112' as classify_small_code,'冻鸡' as classify_small_name
union all select 'B030116' as classify_small_code,'其他冻品' as classify_small_name
union all select 'B030202' as classify_small_code,'冷鲜猪肉类' as classify_small_name
union all select 'B030207' as classify_small_code,'品牌猪肉' as classify_small_name
union all select 'B030210' as classify_small_code,'冻猪' as classify_small_name
union all select 'B030211' as classify_small_code,'冻猪副' as classify_small_name
union all select 'B030304' as classify_small_code,'冷冻水产' as classify_small_name
union all select 'B030601' as classify_small_code,'牛类' as classify_small_name
union all select 'B030603' as classify_small_code,'品牌牛' as classify_small_name
union all select 'B030604' as classify_small_code,'品牌羊' as classify_small_name
union all select 'B030605' as classify_small_code,'冻牛' as classify_small_name
union all select 'B030608' as classify_small_code,'冻羊副' as classify_small_name
union all select 'B060102' as classify_small_code,'煮食面/粉' as classify_small_name
union all select 'B010205' as classify_small_code,'进口米' as classify_small_name
union all select 'B020206' as classify_small_code,'豆类' as classify_small_name
union all select 'B030114' as classify_small_code,'冻鸭' as classify_small_name
union all select 'B030115' as classify_small_code,'冻鸭副' as classify_small_name
union all select 'B020203' as classify_small_code,'结球类' as classify_small_name
union all select 'B030109' as classify_small_code,'品牌鸭' as classify_small_name
union all select 'B030204' as classify_small_code,'冷鲜猪骨类' as classify_small_name
union all select 'B010202' as classify_small_code,'籼米' as classify_small_name
union all select 'B030607' as classify_small_code,'冻羊' as classify_small_name
union all select 'B060103' as classify_small_code,'方便米/面制品' as classify_small_name
union all select 'B010102' as classify_small_code,'豆制品' as classify_small_name
union all select 'B010103' as classify_small_code,'糖类' as classify_small_name
union all select 'B020204' as classify_small_code,'茄果类' as classify_small_name
union all select 'B030203' as classify_small_code,'热鲜猪骨类' as classify_small_name
union all select 'B030205' as classify_small_code,'热鲜猪副类' as classify_small_name
union all select 'B020207' as classify_small_code,'食用菌类' as classify_small_name
union all select 'B030301' as classify_small_code,'冰鲜' as classify_small_name
union all select 'B030602' as classify_small_code,'羊类' as classify_small_name
union all select 'B010104' as classify_small_code,'调味类（干货）' as classify_small_name
union all select 'B020106' as classify_small_code,'其他水果类' as classify_small_name
union all select 'B020201' as classify_small_code,'叶菜类' as classify_small_name
union all select 'B020211' as classify_small_code,'沙拉' as classify_small_name
union all select 'B030113' as classify_small_code,'冻鸡副' as classify_small_name
union all select 'B030201' as classify_small_code,'热鲜猪肉类' as classify_small_name
union all select 'B010204' as classify_small_code,'组合米' as classify_small_name
union all select 'B020101' as classify_small_code,'仁果' as classify_small_name
union all select 'B010301' as classify_small_code,'鲜蛋' as classify_small_name
union all select 'B030108' as classify_small_code,'品牌鸡' as classify_small_name
union all select 'B030606' as classify_small_code,'冻牛副' as classify_small_name
union all select 'B020105' as classify_small_code,'浆果' as classify_small_name
union all select 'B020205' as classify_small_code,'瓜果类' as classify_small_name
union all select 'B030206' as classify_small_code,'冷鲜猪副类' as classify_small_name
union all select 'B060101' as classify_small_code,'速食品' as classify_small_name
;


-- 客户清单小类开票明细
-- drop table csx_analyse_tmp.classify_small_tmp2;  
create table csx_analyse_tmp.classify_small_tmp2 as 
with apply_goods_group as
-- 发票商品明细
(
select d.order_code as order_no,d.total_amt as total_amount,
	e.purchase_group_code,e.purchase_group_name,    
	e.classify_large_code,e.classify_large_name, -- 管理大类
	e.classify_middle_code,e.classify_middle_name,-- 管理中类
	e.classify_small_code,e.classify_small_name-- 管理小类		
from 	
	(
		select *,
		--  id排名取最新一条
		row_number() over(partition by id order by update_time desc) as id_rank				
		-- from csx_ods.csx_ods_csx_b2b_sss_sss_kp_apply_goods_group_di
		-- where (sdt>='20200101' or sdt='19990101')
		from csx_dwd.csx_dwd_sss_kp_apply_goods_group_di
		where sdt>='20200101'
		and is_delete='0'
	)d
	left join 
	(
		select
		goods_code,
		goods_name,
		purchase_group_code,purchase_group_name,    
		classify_large_code,classify_large_name, -- 管理大类
		classify_middle_code,classify_middle_name,-- 管理中类
		classify_small_code,classify_small_name-- 管理小类
		from csx_dim.csx_dim_basic_goods
		where sdt = 'current'
	)e on d.goods_code=e.goods_code
where d.id_rank=1
),

-- 客户信息
customer_info as
(
select 
	bloc_code,     --  集团编码
	bloc_name,     --  集团名称
	customer_id,
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
),
-- 销售结算对账开票结算详情表_信控号维度
order_credit_invoice_bill as
(
	select 
		source_bill_no,  -- 来源单号
		customer_code,  -- 客户编码
		credit_code  -- 信控号		
	from csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di
	where sdt='${sdt_yes}'
),
-- 发票表
invoice as
(
select sdt invoice_date,
	invoice_no,		-- 发票号码	
	order_code,		-- 订单编号
	invoice_code,		-- 发票代码	
	company_code,		-- 公司代码		
	customer_code,		-- 客户编码
	total_amount,		-- 总金额
	invoice_customer_name,		-- 客户开发票名称
	if(offline_flag_code=1,'是','否') as offline_flag,		-- 是否线下开票 0 否 1 是
	regexp_replace(invoice_remark,'\\n|\\r|\\t','') invoice_remark		-- 发票的备注
	-- row_number() over(partition by invoice_no order by sdt desc)	as num1
from csx_dwd.csx_dwd_sss_invoice_di
where sdt>='20200101'
-- and invoice_no in('05502566','85686779')
and delete_flag='0'
and sync_status=1
and company_code='2115'
),

-- 发票商品-小类
apply_classify_small_name as
(
	select a.order_no,a.classify_middle_name,a.classify_small_name,sum(a.total_amount) total_amount
	from apply_goods_group a 
	join csx_analyse_tmp.classify_small_tmp1 e on e.classify_small_name=a.classify_small_name
	group by a.order_no,a.classify_middle_name,a.classify_small_name
)

-- create table csx_analyse_tmp.tmp2 as 
-- 客户目标小类开票明细
-- customer_apply_classify_small_name as
-- (
select *,
	sum(total_amount)over(partition by customer_code,syear) as total_amount_year,
	sum(total_amount)over(partition by customer_code,classify_small_name,syear) as classify_small_amt_year
from 
(
select 
	substr(b.invoice_date,1,4) syear,
	c.performance_region_name,
	c.performance_province_name,
	c.performance_city_name,
	b.customer_code,
	c.customer_name,
	-- a.credit_code,
	c.first_category_name,     --  一级客户分类名称
	c.second_category_name,     --  二级客户分类名称
	c.third_category_name,     --  三级客户分类名称
	b.invoice_date,
	b.invoice_no,		-- 发票号码	
	b.order_code,		-- 订单编号
	b.invoice_code,		-- 发票代码	
	b.company_code,		-- 公司代码		
	-- b.customer_code,		-- 客户编码
	-- b.total_amount,		-- 总金额
	b.invoice_customer_name,		-- 客户开发票名称
	b.offline_flag,		-- 是否线下开票 0 否 1 是
	b.invoice_remark,		-- 发票的备注	
	d.classify_middle_name,
	d.classify_small_name,
	d.total_amount	
from invoice b 
left join customer_info c on c.customer_code=b.customer_code
join apply_classify_small_name d on d.order_no=b.order_code
)a;

-- 客户清单小类开票明细-TOP筛选
-- "目标小类-客户年度总开票金额”>=50W,
-- "目标小类-客户年度该小类总开票金额">=1000元，
-- 单发票中目标小类金额>=200
select 
	a.syear,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	a.customer_name,
	a.first_category_name,     --  一级客户分类名称
	a.second_category_name,     --  二级客户分类名称
	a.third_category_name,     --  三级客户分类名称
	a.invoice_date,
	a.invoice_no,		-- 发票号码	
	a.order_code,		-- 订单编号
	a.invoice_code,		-- 发票代码	
	a.company_code,		-- 公司代码		
	regexp_replace(regexp_replace(a.invoice_customer_name,'\n',''),'\r','') as invoice_customer_name,		-- 客户开发票名称
	a.offline_flag,		-- 是否线下开票 0 否 1 是
	a.invoice_remark,		-- 发票的备注	
	a.classify_middle_name,
	a.classify_small_name,
	a.total_amount,
	a.total_amount_year,
	a.classify_small_amt_year,
	b.num
from csx_analyse_tmp.classify_small_tmp2 a 
left join 
(
	select *,
		row_number() over(partition by syear order by total_amount_year desc) as num
	from 
	(
		select distinct customer_code,syear,total_amount_year
		from csx_analyse_tmp.classify_small_tmp2
	)a
)b on a.customer_code=b.customer_code and a.syear=b.syear
where a.customer_code not like 'G%'
and a.total_amount_year>=500000
-- where num<=500
and a.classify_small_amt_year>=1000
and a.total_amount>=200
;





