
--福利BBC客户履约签约信息 20240516
select
	d.performance_region_name,  	-- 业绩大区名称
	d.performance_province_name,  	-- 业绩省区名称
	d.performance_city_name,  	-- 业绩城市名称
	-- a.business_type_code,
	a.business_type_name,
	a.customer_code,  	-- 客户编码
	d.customer_name,
	d.second_category_name,     --  二级客户分类名称
	a.sign_company_code,
	e.company_name,
	b.first_business_sale_date,
	b.last_business_sale_date,
	a.sale_amt,
	a.estimate_contract_amount
from
(
	select
		coalesce(a.business_type_code,c.business_type_code) as business_type_code,
		coalesce(a.business_type_name,c.business_type_name) as business_type_name,
		coalesce(a.customer_code,c.customer_code) as customer_code,  	-- 客户编码
		-- customer_name,  	-- 客户名称
		a.sign_company_code,  	-- 签约公司编码
		-- sign_company_name,  	-- 签约公司名称
		sale_amt,
		estimate_contract_amount
	from 
	(
		select 
			business_type_code,
			business_type_name,
			customer_code,  	-- 客户编码
			-- customer_name,  	-- 客户名称
			sign_company_code,  	-- 签约公司编码
			-- sign_company_name,  	-- 签约公司名称
			sum(sale_amt)/10000 as sale_amt
		from csx_dws.csx_dws_sale_detail_di
		where business_type_code in(2,6)
		group by 
		business_type_code,
		business_type_name,
		customer_code,  	-- 客户编码
		-- customer_name,  	-- 客户名称
		sign_company_code  	-- 签约公司编码
		-- sign_company_name  	-- 签约公司名称
	)a 
	full join 
	(
		select
			customer_code,
			-- business_type_code,
			if(business_attribute_code=2,2,6) as business_type_code,
			if(business_attribute_code=2,'福利业务','bbc') as business_type_name,
			-- company_code,  	-- 公司代码
			sum(estimate_contract_amount) estimate_contract_amount  	-- 预估合同签约金额
		from csx_dim.csx_dim_crm_business_info
		where sdt='current'
			-- and channel_code in('1','7','9')
			and business_attribute_code in (2,5) -- 商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
			and status=1  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
			and business_stage=5
		group by 
			customer_code,
			if(business_attribute_code=2,2,6),
			if(business_attribute_code=2,'福利业务','bbc')
			-- company_code  	-- 公司代码		
	)c on a.customer_code=c.customer_code and a.business_type_code=c.business_type_code 
)a 
left join 
(
select customer_code,business_type_code,
	first_business_sale_date,last_business_sale_date,
	-- 至今距离天数
	datediff(to_date(date_sub(current_date,1)),to_date(from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd')))) date_diff,
	sale_business_active_days,  -- 销售业务类型活跃天数(即历史至今有销售的日期)
	sale_business_total_amt/10000 sale_business_total_amt 	-- 销售业务类型总金额
from csx_dws.csx_dws_crm_customer_business_active_di
where sdt = 'current'
-- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
and business_type_code in(2,6)
)b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
left join 
(
select 
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	channel_code,
	channel_name,
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

	sales_user_number,
	sales_user_name	
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and customer_type_code=4
)d on a.customer_code=d.customer_code
left join 
(
select * from csx_dim.csx_dim_basic_company where sdt = 'current'
) e on a.sign_company_code=e.company_code
;


