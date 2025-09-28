
-- 表1：202303-202403每月业绩客户明细
drop table if exists csx_analyse_tmp.tmp_cust_sale_month;
create  table csx_analyse_tmp.tmp_cust_sale_month
as
select 
	a.smonth,
	b.region_code, -- 客户大区编码
	b.region_name, -- 客户大区名称
	b.province_code, -- 客户省区编码
	b.province_name, -- 客户省区名称
	b.city_group_code, -- 客户城市编码
	b.city_group_name, -- 客户城市名称
	b.work_no_new, -- 销售员工号_new
	b.sales_name_new, -- 销售员_new
	b.rp_service_user_work_no_new, -- 日配_服务管家工号_new
	b.rp_service_user_name_new, -- 日配_服务管家_new
	b.fl_service_user_work_no_new, -- 福利_服务管家工号_new
	b.fl_service_user_name_new, -- 福利_服务管家_new
	b.bbc_service_user_work_no_new, -- bbc_服务管家工号_new
	b.bbc_service_user_name_new, -- bbc_服务管家_new	
	a.customer_code,
	b.customer_name, -- 客户名称
	-- 销售额
	a.sale_amt, -- 客户总销售额
	a.rp_sale_amt, -- 客户日配销售额		
	a.bbc_sale_amt, -- 客户bbc销售额
	a.bbc_sale_amt_zy, -- 客户bbc自营销售额
	a.bbc_sale_amt_ly, -- 客户bbc联营销售额
	a.fl_sale_amt, -- 客户福利销售额
	-- 定价毛利额
	a.profit,-- 客户总定价毛利额
	a.rp_profit,-- 客户日配定价毛利额
	a.bbc_profit,-- 客户bbc定价毛利额
	a.bbc_profit_zy, -- 客户bbc自营定价毛利额
	a.bbc_profit_ly, -- 客户bbc联营定价毛利额
	a.fl_profit-- 客户福利定价毛利额
from 
	(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_code in('6') then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then sale_amt else 0 end) as bbc_sale_amt_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then sale_amt else 0 end) as bbc_sale_amt_ly,
		sum(case when business_type_code in('2') then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit, 
		sum(case when business_type_code in ('1','4','5') then profit else 0 end) as rp_profit,
		sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then profit else 0 end) as bbc_profit_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then profit else 0 end) as bbc_profit_ly,		
		sum(case when business_type_code in('2') then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='20230301' and sdt<'20240401'
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
				-- 福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202210签呈 北京 129026 129000 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202306签呈 福建省'229290','175709','125092' 项目供应商 纳入大客户提成计算
				or (business_type_code in ('4') and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))			
				)
			-- and performance_province_name in ('福建省')
			and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断					
	group by 
		customer_code,substr(sdt,1,6)	
	)a
	left join
		(
		-- 客户对应销售员与管家
		select *,substr(sdt,1,6) as smonth
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		-- where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		where sdt>='20230301'
		and sdt=regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') -- 每月最后一天
		) b on b.customer_no=a.customer_code and a.smonth=b.smonth
;	
	
-- 表2：202303-202403每月业绩人员汇总

select 
flag,
smonth,
region_name,
province_name,
city_group_name,
work_no, -- 销售员工号
sales_name, -- 销售员
count(distinct customer_code) as count_cust,
sum(sale_amt) as sale_amt, -- 客户销售额
sum(profit) as profit-- 客户定价毛利额
from 
(
select 
	'销售员' as flag,
	smonth,
	region_name,
	province_name,
	city_group_name,
	customer_code,
	work_no_new as work_no, -- 销售员工号
	sales_name_new as sales_name, -- 销售员
	-- 销售额
	sum(sale_amt) as sale_amt, -- 客户销售额
	-- 定价毛利额
	sum(profit) as profit-- 客户定价毛利额
from csx_analyse_tmp.tmp_cust_sale_month
where work_no_new<>''
group by smonth,region_name,province_name,city_group_name,customer_code,work_no_new,sales_name_new

union all
select 
	'服务管家' as flag,
	smonth,
	region_name,
	province_name,
	city_group_name,
	customer_code,
	rp_service_user_work_no_new as work_no,
	rp_service_user_name_new as sales_name,
	-- 销售额
	sum(rp_sale_amt) as sale_amt, -- 客户销售额
	-- 定价毛利额
	sum(rp_profit) as profit-- 客户定价毛利额
from csx_analyse_tmp.tmp_cust_sale_month
where rp_service_user_work_no_new<>''
group by smonth,region_name,province_name,city_group_name,customer_code,rp_service_user_work_no_new,rp_service_user_name_new

union all
select 
	'服务管家' as flag,
	smonth,
	region_name,
	province_name,
	city_group_name,
	customer_code,
	fl_service_user_work_no_new as work_no,
	fl_service_user_name_new as sales_name,
	-- 销售额
	sum(fl_sale_amt) as sale_amt, -- 客户销售额
	-- 定价毛利额
	sum(fl_profit) as profit-- 客户定价毛利额
from csx_analyse_tmp.tmp_cust_sale_month
where fl_service_user_work_no_new<>''
group by smonth,region_name,province_name,city_group_name,customer_code,fl_service_user_work_no_new,fl_service_user_name_new

union all
select 
	'服务管家' as flag,
	smonth,
	region_name,
	province_name,
	city_group_name,
	customer_code,
	bbc_service_user_work_no_new as work_no,
	bbc_service_user_name_new as sales_name,
	-- 销售额
	sum(bbc_sale_amt) as sale_amt, -- 客户销售额
	-- 定价毛利额
	sum(bbc_profit) as profit-- 客户定价毛利额
from csx_analyse_tmp.tmp_cust_sale_month
where bbc_service_user_work_no_new<>''
group by smonth,region_name,province_name,city_group_name,customer_code,bbc_service_user_work_no_new,bbc_service_user_name_new
)a
group by 
flag,
smonth,
region_name,
province_name,
city_group_name,
work_no,
sales_name;


