
-- 4、月度 战略销售员客户未税业绩
-- 更改为含税20241008

select 
	-- b.performance_region_code,
	b.performance_region_name,
	-- b.performance_province_code,
	b.performance_province_name,
	-- b.performance_city_code,
	b.performance_city_name,
	b.sales_user_number,b.sales_user_name,
	a.customer_code,b.customer_name,a.smonth,
	-- 销售额
	sum(sale_amt) as sale_amt, -- 客户总销售额
	sum(rp_sale_amt) as rp_sale_amt, -- 客户日配销售额		
	-- sum(sale_amt) as sale_amt, -- 客户总销售额
	-- sum(rp_sale_amt) as rp_sale_amt, -- 客户日配销售额
	sum(bbc_sale_amt) as bbc_sale_amt, -- 客户bbc销售额
	sum(fl_sale_amt) as fl_sale_amt, -- 客户福利销售额
	sum(rp_sale_amt)+sum(bbc_sale_amt) as rp_bbc_sale_amt,
	-- 定价毛利额
	sum(profit) as profit,-- 客户总定价毛利额
	sum(rp_profit) as rp_profit,-- 客户日配定价毛利额
	sum(bbc_profit) as bbc_profit,-- 客户bbc定价毛利额
	sum(fl_profit) as fl_profit,-- 客户福利定价毛利额
	sum(rp_profit)+sum(bbc_profit) as rp_bbc_profit
from 
	(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_code in('6') then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in('2') then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(case when inventory_dc_code <>'W0K4' then profit else 0 end) as profit, -- W0K4只计算销售额 不计算定价毛利额 每月
		sum(case when business_type_code in ('1','4','5') and inventory_dc_code <>'W0K4' then profit else 0 end) as rp_profit,
		sum(case when business_type_code in('6')  and inventory_dc_code <>'W0K4' then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in('2')  and inventory_dc_code <>'W0K4' then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and business_type_code in('1','2','6')
			and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断
	group by customer_code,substr(sdt,1,6)	
	)a
	join 
		(
		select 
			distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from csx_dim.csx_dim_crm_customer_info 
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and customer_code !=''
			-- 柳桢 80959192、张晓宾 81211833、刘纪欣 81166841、潘志芳	80001032 刘才明 81192322、高林芳 81221107
			-- and sales_user_number in('80959192','80001032','81176812','81192322')
			-- and sales_user_number in('81208941','81211833','80001032','80959192','81176812','81192322','81221107')
			-- and sales_user_number in('80001032','81211833','80959192','81166841','81192322','81221107')
			and sales_user_number in('81211833')
		)b on b.customer_code=a.customer_code	
where b.ywdl_cust is null 
and b.ng_cust is null
group by b.performance_region_code,b.performance_region_name,b.performance_province_code,b.performance_province_name,
b.performance_city_code,b.performance_city_name,b.sales_user_number,b.sales_user_name,
a.customer_code,b.customer_name,a.smonth
;	

	
-- 每月人力给定销售员清单
select 
	-- b.performance_region_code,
	b.performance_region_name,
	-- b.performance_province_code,
	b.performance_province_name,
	-- b.performance_city_code,
	b.performance_city_name,
	b.sales_user_number,b.sales_user_name,
	a.customer_code,b.customer_name,a.smonth,
	-- 销售额
	sum(sale_amt) as sale_amt, -- 客户总销售额
	sum(rp_sale_amt) as rp_sale_amt, -- 客户日配销售额		
	-- sum(sale_amt) as sale_amt, -- 客户总销售额
	-- sum(rp_sale_amt) as rp_sale_amt, -- 客户日配销售额
	sum(bbc_sale_amt) as bbc_sale_amt, -- 客户bbc销售额
	sum(fl_sale_amt) as fl_sale_amt, -- 客户福利销售额
	sum(rp_sale_amt)+sum(bbc_sale_amt) as rp_bbc_sale_amt,
	-- 定价毛利额
	sum(profit) as profit,-- 客户总定价毛利额
	sum(rp_profit) as rp_profit,-- 客户日配定价毛利额
	sum(bbc_profit) as bbc_profit,-- 客户bbc定价毛利额
	sum(fl_profit) as fl_profit,-- 客户福利定价毛利额
	sum(rp_profit)+sum(bbc_profit) as rp_bbc_profit
from 
	(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt_no_tax) as sale_amt,
		sum(case when business_type_code in ('1','4','5') then sale_amt_no_tax else 0 end) as rp_sale_amt,
		sum(case when business_type_code in('6') then sale_amt_no_tax else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in('2') then sale_amt_no_tax else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(case when inventory_dc_code <>'W0K4' then profit_no_tax else 0 end) as profit, -- W0K4只计算销售额 不计算定价毛利额 每月
		sum(case when business_type_code in ('1','4','5') and inventory_dc_code <>'W0K4' then profit_no_tax else 0 end) as rp_profit,
		sum(case when business_type_code in('6')  and inventory_dc_code <>'W0K4' then profit_no_tax else 0 end) as bbc_profit,
		sum(case when business_type_code in('2')  and inventory_dc_code <>'W0K4' then profit_no_tax else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
	where sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and business_type_code in('1','2','6')
			and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断
	group by customer_code,substr(sdt,1,6)	
	)a
	join 
		(
		select 
			distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from csx_dim.csx_dim_crm_customer_info 
		where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and customer_code !=''
			-- 柳桢 80959192、张晓宾 81211833、刘纪欣 81166841、潘志芳	80001032 刘才明 81192322、高林芳 81221107
			-- and sales_user_number in('80959192','80001032','81176812','81192322')
			-- and sales_user_number in('81208941','81211833','80001032','80959192','81176812','81192322','81221107')
			-- and sales_user_number in('80001032','81211833','80959192','81166841','81192322','81221107')
			and sales_user_number in('81211833')
		)b on b.customer_code=a.customer_code	
where b.ywdl_cust is null 
and b.ng_cust is null
group by b.performance_region_code,b.performance_region_name,b.performance_province_code,b.performance_province_name,
b.performance_city_code,b.performance_city_name,b.sales_user_number,b.sales_user_name,
a.customer_code,b.customer_name,a.smonth
;	


-- 更改为含税