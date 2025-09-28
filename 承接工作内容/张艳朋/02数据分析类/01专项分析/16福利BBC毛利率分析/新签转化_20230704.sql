
-- 商机签约转化
-- drop table csx_analyse_tmp.tmp_cust_business_conversion;
-- create table csx_analyse_tmp.tmp_cust_business_conversion
-- as
select a.*,
	if(a.business_sign_date>a.first_sale_date,'老客户','新客户') cust_flag,
	sum(case when a.business_sign_date<=b.sdt then b.sale_amt end)/10000 sale_amt_lj,	
	sum(case when a.business_sign_date<=b.sdt 
			and coalesce(a.business_sign_date_2,regexp_replace(current_date, '-', ''))>=b.sdt then b.sale_amt end)/10000 sale_amt,
		sum(case when a.business_sign_date<=b.sdt 
			and coalesce(a.business_sign_date_2,regexp_replace(current_date, '-', ''))>=b.sdt then b.profit end)/10000 profit,		
	count(distinct case when a.business_sign_date<=b.sdt 
			and coalesce(a.business_sign_date_2,regexp_replace(current_date, '-', ''))>=b.sdt then b.sdt end) count_sdt		
from
	(
	select a.business_sign_month,
		a.business_number,     --  商机号
		a.customer_id,     --  客户ID
		a.customer_code,     
		a.customer_name,     --  客户名称
		-- a.first_category_code,     --  一级客户分类编码
		a.first_category_name,     --  一级客户分类名称
		-- a.second_category_code,     --  二级客户分类编码
		a.second_category_name,     --  二级客户分类名称
		-- a.third_category_code,     --  三级客户分类编码
		a.third_category_name,     --  三级客户分类名称
		-- a.business_attribute_code,     --  商机属性编码
		a.business_attribute_name,     --  商机属性名称
		-- a.performance_region_code,     --  销售大区编码(业绩划分)
		a.performance_region_name,     --  销售大区名称(业绩划分)
		-- a.performance_province_code,     --  销售归属省区编码
		a.performance_province_name,     --  销售归属省区名称
		-- a.performance_city_code,     --  城市组编码(业绩划分)
		a.performance_city_name,     --  城市组名称(业绩划分)
		a.owner_user_number,     --  归属人工号
		a.owner_user_name,     --  归属人姓名  
		a.business_type_code,
		a.business_type_name,    -- 业务类型 
		-- a.customer_acquisition_type_code,
		coalesce(a.customer_acquisition_type_name,'非投标') as customer_acquisition_type_name,    --  获客方式  1投标 2非投标
		a.business_stage,     --  阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
		a.contract_cycle,     --  合同周期
		a.estimate_contract_amount,     --  预计合同签约金额
		a.gross_profit_rate,     --  预计毛利率
		-- a.expect_sign_date,     --  预计签约时间
		-- a.expect_execute_time,     --  预计履约时间
		a.business_sign_date,     --  业务类型签约时间
		a.first_business_sign_time,     --  首次业务类型签约时间
		-- a.create_time,
		a.num,b.business_sign_date_2,     --  业务类型签约时间
		c.first_business_sale_date,c.last_business_sale_date,c.date_diff,
		c.sale_business_active_days,  -- 销售业务类型活跃天数(即历史至今有销售的日期)
		c.sale_business_total_amt, 	-- 销售业务类型总金额	
		d.first_sale_date,
		d.last_sale_date,d.sale_active_days,d.sale_total_amt,d.date_diff1			
	from 
		(
		select regexp_replace(substr(business_sign_time,1,7),'-','') business_sign_month,
			business_number,     --  商机号
			customer_id,     --  客户ID
			customer_code,     
			customer_name,     --  客户名称
			first_category_code,     --  一级客户分类编码
			first_category_name,     --  一级客户分类名称
			second_category_code,     --  二级客户分类编码
			second_category_name,     --  二级客户分类名称
			third_category_code,     --  三级客户分类编码
			third_category_name,     --  三级客户分类名称
			business_attribute_code,     --  商机属性编码
			business_attribute_name,     --  商机属性名称
			performance_region_code,     --  销售大区编码(业绩划分)
			performance_region_name,     --  销售大区名称(业绩划分)
			performance_province_code,     --  销售归属省区编码
			performance_province_name,     --  销售归属省区名称
			performance_city_code,     --  城市组编码(业绩划分)
			performance_city_name,     --  城市组名称(业绩划分)
			-- sales_id,     --  主销售员Id
			owner_user_number,     --  归属人工号
			owner_user_name,     --  归属人姓名  
			business_type_code,business_type_name,    -- 业务类型 
			customer_acquisition_type_code,customer_acquisition_type_name,    --  获客方式  1投标 2非投标
			business_stage,     --  阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
			contract_cycle,     --  合同周期
			estimate_contract_amount,     --  预计合同签约金额
			gross_profit_rate,     --  预计毛利率
			regexp_replace(substr(expect_sign_time, 1, 10), '-', '') as expect_sign_date,     --  预计签约时间
			expect_execute_time,     --  预计履约时间
			regexp_replace(substr(business_sign_time,1,10),'-','') business_sign_date,     --  业务类型签约时间
			regexp_replace(substr(first_business_sign_time,1,10),'-','') first_business_sign_time,     --  首次业务类型签约时间
			create_time,
			sdt,
			row_number() over(partition by concat(customer_code,business_type_code) order by business_sign_time) num --商机顺序
		from csx_dim.csx_dim_crm_business_info
		where sdt='${sdt_yes}'
		and channel_code in('1','7','9')
		and business_type_code in('1','2','4','6')
		and status='1'  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
		and business_stage=5
		and regexp_replace(substr(business_sign_time,1,10),'-','')>='20220101'
		)a
	left join
		(
		select 
			business_number,     --  商机号
			customer_id,     --  客户ID
			customer_code,     
			customer_name,     --  客户名称
			business_type_code,business_type_name,    -- 业务类型 
			regexp_replace(substr(business_sign_time,1,10),'-','') business_sign_date_2,     --  业务类型签约时间
			create_time,
			sdt,
			row_number() over(partition by concat(customer_code,business_type_code) order by business_sign_time)-1 num_2 --商机顺序
		from csx_dim.csx_dim_crm_business_info
		where sdt='${sdt_yes}'
		and channel_code in('1','7','9')
		and business_type_code in('1','2','4','6')
		and status='1'  -- 是否有效 0.无效 1.有效 (status=0,'停止跟进')
		and business_stage=5
		and regexp_replace(substr(business_sign_time,1,10),'-','')>='20220101'
		)b on b.customer_code=a.customer_code and a.business_type_code=b.business_type_code	and a.num=b.num_2	
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
	and business_type_code in('1','2','4','6')
	)c on c.customer_code=a.customer_code and a.business_type_code=c.business_type_code
	--客户下单情况动态信息表
    left join
    (
      select customer_code,first_sale_date,last_sale_date,sale_active_days,sale_total_amt,
		-- 至今距离天数
		datediff(to_date(date_sub(current_date,1)),to_date(from_unixtime(unix_timestamp(last_sale_date,'yyyyMMdd')))) date_diff1	  
      from csx_dws.csx_dws_crm_customer_active_di
      where sdt = 'current'
    )d on d.customer_code=a.customer_code 
	)a
left join 
	(
	select 
		customer_code,sdt,business_type_code,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit,
		sum(profit)/abs(sum(sale_amt)) as profit_rate
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='20220101'
	and channel_code in('1','7','9')
	and business_type_code in('1','2','4','6')
	group by customer_code,sdt,business_type_code
	)b on a.customer_code=b.customer_code and a.business_type_code=b.business_type_code
group by a.business_sign_month,
	a.business_number,     --  商机号
	a.customer_id,     --  客户ID
	a.customer_code,     
	a.customer_name,     --  客户名称
	-- a.first_category_code,     --  一级客户分类编码
	a.first_category_name,     --  一级客户分类名称
	-- a.second_category_code,     --  二级客户分类编码
	a.second_category_name,     --  二级客户分类名称
	-- a.third_category_code,     --  三级客户分类编码
	a.third_category_name,     --  三级客户分类名称
	-- a.business_attribute_code,     --  商机属性编码
	a.business_attribute_name,     --  商机属性名称
	-- a.performance_region_code,     --  销售大区编码(业绩划分)
	a.performance_region_name,     --  销售大区名称(业绩划分)
	-- a.performance_province_code,     --  销售归属省区编码
	a.performance_province_name,     --  销售归属省区名称
	-- a.performance_city_code,     --  城市组编码(业绩划分)
	a.performance_city_name,     --  城市组名称(业绩划分)
	a.owner_user_number,     --  归属人工号
	a.owner_user_name,     --  归属人姓名  
	a.business_type_code,
	a.business_type_name,    -- 业务类型 
	-- a.customer_acquisition_type_code,
	a.customer_acquisition_type_name,    --  获客方式  1投标 2非投标
	a.business_stage,     --  阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5
	a.contract_cycle,     --  合同周期
	a.estimate_contract_amount,     --  预计合同签约金额
	a.gross_profit_rate,     --  预计毛利率
	-- a.expect_sign_date,     --  预计签约时间
	-- a.expect_execute_time,     --  预计履约时间
	a.business_sign_date,     --  业务类型签约时间
	a.first_business_sign_time,     --  首次业务类型签约时间
	-- a.create_time,
	a.num,a.business_sign_date_2,     --  业务类型签约时间
	a.first_business_sale_date,a.last_business_sale_date,a.date_diff,
	a.sale_business_active_days,  -- 销售业务类型活跃天数(即历史至今有销售的日期)
	a.sale_business_total_amt, 	-- 销售业务类型总金额	
	a.first_sale_date,
	a.last_sale_date,a.sale_active_days,a.sale_total_amt,a.date_diff1
;



select 
customer_code,customer_name,
performance_region_name,
performance_province_name,
business_type_name,
sum(sale_amt)/10000 as sale_amt,
sum(profit)/10000 as profit,
sum(profit)/abs(sum(sale_amt)) profit_rate
from csx_dws.csx_dws_sale_detail_di
where sdt>='20190101'
and customer_code in('126090','127818','125720','127389','126407')
group by 
customer_code,customer_name,
performance_region_name,
performance_province_name,
business_type_name;
