-- 北京日配AB客户续签情况 20240117
-- 续签率=（23年任何一个月分级为AB级的客户 且泛微合同存在23年到期）的客户中23年大于到期合同日期有合同开始日期的
-- （2023年年度销售金额，毛利率，2023年履约月份，合同开始时间，合同到期时间
with sale as 
(select  
		 
		customer_code,
	    sum(sale_amt)sale_amt,
		sum(profit) profit,
		concat_ws(',',collect_set(smonth)) as sale_month,
		min(smonth) min_smonth,
		count(distinct smonth) sale_mon
	from(
	select  
		substr(sdt,1,6) smonth,
		customer_code,
		sum(sale_amt)sale_amt,
		sum(profit) profit
	from    csx_dws.csx_dws_sale_detail_di 
	where sdt >='20230101'
	and sdt <= '20231231'
	and business_type_code in ('1') 
	and performance_province_name='北京市'
	and inventory_dc_code !='WB26'
	group by substr(sdt,1,6)  ,
		customer_code
		) a 
		group by customer_code
)
select 
	d.performance_province_name,
	a.customer_code,
	d.customer_name,
	d.second_category_name,
	d.sales_user_number,
	d.sales_user_name,	
	b.htbh,		-- 合同编号
	b.htqsrq,		-- 合同起始日期
	b.htzzrq,		-- 合同终止日期
	b.htjey,		-- 合同金额(元)	
	c.htbh as htbh_x,		-- 合同编号
	c.htqsrq as htqsrq_x,		-- 合同起始日期
	c.htzzrq as htzzrq_x,		-- 合同终止日期
	c.htjey as htjey_x,		-- 合同金额(元)
	e.last_business_sale_date,	
	f.business_sign_date,
	f.contract_number,  -- 		合同编号
	f.contract_begin_date,  -- 	合同起始日期
	f.contract_end_date,  -- 	合同终止日期
	f.estimate_contract_amount,	
	if(c.htqsrq>b.htqsrq or f.contract_begin_date>b.htqsrq,'是','否') is_x,
	sale_amt,
    profit,
    profit/sale_amt profit_rate,
	sale_month,
	sale_mon
from 
-- 23年任一个月是AB级的客户
(
   select distinct customer_no as customer_code
   from csx_analyse.csx_analyse_report_sale_customer_level_mf 
   -- where (substr(month,1,4)='2023' or month='202212') 
   where substr(month,1,4)='2023' 
   and tag=1
   and customer_large_level in('A','B')
   and province_name='北京市'
 )a  
left join 
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
		row_number() over(partition by customer_no order by htzzrq asc)	as num			
	from csx_analyse.csx_analyse_report_weaver_contract_df
	where sdt=regexp_replace(date_sub(current_date,1),'-','')
	and if_fanli='非返利'
	and regexp_replace(regexp_replace(substr(htbh,3,4),'2',''),'0','')='日配'
	and substr(regexp_replace(htzzrq,'-',''),1,4)='2023' -- 合同终止日期为2023年
	)a
where a.num=1  
   )b on b.customer_no=a.customer_code
left join 
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
		row_number() over(partition by customer_no order by htqsrq desc)	as num			
	from csx_analyse.csx_analyse_report_weaver_contract_df
	where sdt=regexp_replace(date_sub(current_date,1),'-','')
	and if_fanli='非返利'
	and regexp_replace(regexp_replace(substr(htbh,3,4),'2',''),'0','')='日配'
	and substr(regexp_replace(htqsrq,'-',''),1,4)>='2023' -- 合同起始日期为2023年
	)a
where a.num=1    
   )c on c.customer_no=a.customer_code
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
		sales_user_name,
		if(customer_acquisition_type_code=1,'投标','非投标') as customer_acquisition_type	     -- 获客方式编码(1:投标,2:非投标)	
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
)d on a.customer_code=d.customer_code
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
and business_type_code in('1')
)e on e.customer_code=a.customer_code
-- 最新商机信息
left join
(
select * 
from 
    (select customer_code,
		regexp_replace(substr(business_sign_time,1,10),'-','') business_sign_date,
		contract_number,  -- 		合同编号
		regexp_replace(substr(contract_begin_date,1,10),'-','') contract_begin_date,  -- 	合同起始日期
		regexp_replace(substr(contract_end_date,1,10),'-','') contract_end_date,  -- 	合同终止日期
		estimate_contract_amount,
        row_number()over(partition by customer_code order by business_number desc) as ranks 
    from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
    and business_attribute_code in(1)  -- 日配
    and status=1 
    -- and sign_type_code=1 
    )a 
where a.ranks=1
)f on f.customer_code=a.customer_code
left join sale on  a.customer_code=sale.customer_code;