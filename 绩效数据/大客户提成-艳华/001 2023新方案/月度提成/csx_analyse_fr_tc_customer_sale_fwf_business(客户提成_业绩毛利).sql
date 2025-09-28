-- 、2023年至今销售员与管家每月销售额毛利及系数后奖金包 20230920
-- 删除csx_analyse.csx_analyse_fr_tc_customer_sale


drop table csx_analyse_tmp.tmp_tc_customer_sales_m;
create temporary table csx_analyse_tmp.tmp_tc_customer_sales_m
as
select
concat_ws('-',a.smonth,a.customer_code) as biz_id,
	a.smonth,
	c.region_code as region_code_customer,
	c.region_name as region_name_customer,
	c.province_code as province_code_customer,
	c.province_name as province_name_customer,
	c.city_group_code as city_group_code_customer,
	c.city_group_name as city_group_name_customer,	
	
	a.customer_code,
	c.customer_name,
	c.sales_user_id,
	c.sales_user_number,
	c.sales_user_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	sale_amt,
	rp_sale_amt,
	bbc_sale_amt as bbc_sale_amt_zy,
	0 as bbc_sale_amt_ly,
	fl_sale_amt,
	profit,
	rp_profit,
	bbc_profit as bbc_profit_zy,
	0 as bbc_profit_ly,
	fl_profit,
	profit/abs(sale_amt) as prorate,
	rp_profit/abs(rp_sale_amt) as rp_prorate,
	bbc_profit/abs(bbc_sale_amt) as bbc_prorate_zy,
	bbc_profit/abs(bbc_sale_amt) as bbc_prorate_ly,
	fl_profit/abs(fl_sale_amt) as fl_prorate
from
	(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_code in('6') then sale_amt else 0 end) as bbc_sale_amt,
		-- sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then sale_amt else 0 end) as bbc_sale_amt_zy,
		-- sum(case when business_type_code in('6') and operation_mode_code=1 then sale_amt else 0 end) as bbc_sale_amt_ly,
		sum(case when business_type_code in('2') then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit, 
		sum(case when business_type_code in ('1','4','5') then profit else 0 end) as rp_profit,
		sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
		-- sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then profit else 0 end) as bbc_profit_zy,
		-- sum(case when business_type_code in('6') and operation_mode_code=1 then profit else 0 end) as bbc_profit_ly,		
		sum(case when business_type_code in('2') then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='202206' 
	and sdt<='202307' 
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
				-- 福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202210签呈 北京 129026 129000 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202306签呈 福建省'229290','175709','125092' 项目供应商 纳入大客户提成计算
				or (business_type_code in ('4') and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092')))
			-- and performance_province_name in ('福建省')
			and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断					
	group by 
		customer_code,substr(sdt,1,6)
	)a
	left join 
		(
		select smt,
			region_code,region_name,
			province_code,province_name,
			city_group_code,city_group_name,
			customer_no,customer_name,
			sales_id sales_user_id,
			work_no sales_user_number,
			sales_name sales_user_name,
			rp_service_user_id,
			rp_service_user_work_no,
			rp_service_user_name,
			fl_service_user_id,
			fl_service_user_work_no,
			fl_service_user_name,		
			bbc_service_user_id,
			bbc_service_user_work_no,
			bbc_service_user_name	
		from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
		where smt>='202206' 
		) c on c.customer_no=a.customer_code and a.smonth=c.smt;
		
		
drop table csx_analyse_tmp.tmp_tc_customer_sales_salary_m;
create temporary table csx_analyse_tmp.tmp_tc_customer_sales_salary_m
as
select 
concat_ws('-',smonth,customer_code) as biz_id,
	smonth,
	region_code_customer,
	region_name_customer,
	province_code_customer,
	province_name_customer,
	city_group_code_customer,
	city_group_name_customer,
	customer_code,
	customer_name,
	sales_user_id,
	sales_user_number,
	sales_user_name,
	rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,
	bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	sale_amt,
	rp_sale_amt,
	bbc_sale_amt as bbc_sale_amt_zy,
	0 as bbc_sale_amt_ly,
	fl_sale_amt,
	profit,
	rp_profit,
	bbc_profit as bbc_profit_zy,
	0 as bbc_profit_ly,
	fl_profit,
	prorate,
	rp_prorate,
	bbc_prorate as bbc_prorate_zy,
	bbc_prorate as bbc_prorate_ly,
	fl_prorate	
from csx_analyse.csx_analyse_fr_tc_customer_salary_detail_new 
where smt>='202206'
and (smt<>'202207' or (smt='202207' and customer_code<>'116561'))

union all
select 
concat_ws('-',smonth,customer_code) as biz_id,
	smonth,
	region_code_customer,
	region_name_customer,
	province_code_customer,
	province_name_customer,
	city_group_code_customer,
	city_group_name_customer,
	customer_code,
	customer_name,
	sales_user_id,
	sales_user_number,
	sales_user_name,
	rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,
	bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	sale_amt,
	rp_sale_amt,
	bbc_sale_amt as bbc_sale_amt_zy,
	0 as bbc_sale_amt_ly,
	fl_sale_amt,
	profit,
	rp_profit,
	bbc_profit as bbc_profit_zy,
	0 as bbc_profit_ly,
	fl_profit,
	prorate,
	rp_prorate,
	bbc_prorate as bbc_prorate_zy,
	bbc_prorate as bbc_prorate_ly,
	fl_prorate
from csx_analyse.csx_analyse_fr_tc_customer_salary_detail_new 
where smt='202207' and customer_code='116561' and bbc_profit=7.01;




		
		

insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business partition(smt)
select 
	a.biz_id,
	a.smonth,
	a.region_code_customer,
	a.region_name_customer,
	a.province_code_customer,
	a.province_name_customer,
	a.city_group_code_customer,
	a.city_group_name_customer,
	a.customer_code,
	a.customer_name,
	a.sales_user_id,
	a.sales_user_number,
	a.sales_user_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt_zy,
	a.bbc_sale_amt_ly,
	a.fl_sale_amt,
	a.profit,
	a.rp_profit,
	a.bbc_profit_zy,
	a.bbc_profit_ly,
	a.fl_profit,
	a.prorate,
	a.rp_prorate,
	a.bbc_prorate_zy,
	a.bbc_prorate_ly,
	a.fl_prorate,
	b.service_falg,b.service_fee,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	-- substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt,
	cast(a.smonth as string) as smt	
from 
(
select a.* from csx_analyse_tmp.tmp_tc_customer_sales_m a 
left join csx_analyse_tmp.tmp_tc_customer_sales_salary_m b on a.customer_code=b.customer_code and a.smonth=b.smonth
where b.smonth is null 
union all 
select * from csx_analyse_tmp.tmp_tc_customer_sales_salary_m
) a 

left join 
(
select province_name,customer_no,smt,service_fee,
case when category_convention like'%管家按服务费%' then '管家按服务费'
	when category_convention like'%服务费%' then '服务费'
	else '服务费' end as service_falg
from csx_analyse.csx_analyse_fr_tc_customer_special_rules_mi
where smt>='202206'
and smt<'202308'
and category_convention like'%服务费%'
)b on a.customer_code=b.customer_no and a.smonth=b.smt
;




-- 202308以后客户本月销售额、定价毛利额统计
drop table csx_analyse_tmp.tmp_tc_customer_sales_prorate_m;
create temporary table csx_analyse_tmp.tmp_tc_customer_sales_prorate_m
as
select 
	a.customer_code,a.smonth,
	-- 销售额
	a.sale_amt, -- 客户总销售额
	a.rp_sale_amt, -- 客户日配销售额
	a.bbc_sale_amt_zy, -- 客户bbc自营销售额
	a.bbc_sale_amt_ly, -- 客户bbc联营销售额
	a.fl_sale_amt, -- 客户福利销售额
	
	-- 定价毛利额
	(a.rp_profit-coalesce(a.rp_sale_amt*b.rp_rate,0))+
	(a.bbc_profit_zy-coalesce(a.bbc_sale_amt_zy*b.bbc_rate_zy,0))+
	(a.bbc_profit_ly-coalesce(a.bbc_sale_amt_ly*b.bbc_rate_ly,0))+
	(a.fl_profit-coalesce(a.fl_sale_amt*b.fl_rate,0)) as profit,-- 客户总定价毛利额
	
	a.rp_profit-coalesce(a.rp_sale_amt*b.rp_rate,0) as rp_profit,-- 客户日配定价毛利额
	a.bbc_profit_zy-coalesce(a.bbc_sale_amt_zy*b.bbc_rate_zy,0) as bbc_profit_zy,-- 客户bbc自营定价毛利额
	a.bbc_profit_ly-coalesce(a.bbc_sale_amt_ly*b.bbc_rate_ly,0) as bbc_profit_ly,-- 客户bbc联营定价毛利额
	a.fl_profit-coalesce(a.fl_sale_amt*b.fl_rate,0) as fl_profit,-- 客户福利定价毛利额
	
	-- 定价毛利率
	coalesce(((a.rp_profit-coalesce(a.rp_sale_amt*b.rp_rate,0))+
	(a.bbc_profit_zy-coalesce(a.bbc_sale_amt_zy*b.bbc_rate_zy,0))+
	(a.bbc_profit_ly-coalesce(a.bbc_sale_amt_ly*b.bbc_rate_ly,0))+
	(a.fl_profit-coalesce(a.fl_sale_amt*b.fl_rate,0)))/abs(sale_amt),0) as prorate, -- 客户总定价毛利率
	
	coalesce((a.rp_profit-coalesce(a.rp_sale_amt*b.rp_rate,0))/abs(a.rp_sale_amt),0) as rp_prorate, -- 客户日配定价毛利率
	coalesce((a.bbc_profit_zy-coalesce(a.bbc_sale_amt_zy*b.bbc_rate_zy,0))/abs(a.bbc_sale_amt_zy),0) as bbc_prorate_zy, -- 客户bbc自营定价毛利率
	coalesce((a.bbc_profit_ly-coalesce(a.bbc_sale_amt_ly*b.bbc_rate_ly,0))/abs(a.bbc_sale_amt_ly),0) as bbc_prorate_ly, -- 客户bbc联营定价毛利率
	coalesce((a.fl_profit-coalesce(a.fl_sale_amt*b.fl_rate,0))/abs(a.fl_sale_amt),0) as fl_prorate-- 客户福利定价毛利率
from 
(
select 
	customer_code,smonth,
	-- 销售额
	sum(sale_amt) as sale_amt, -- 客户总销售额
	sum(rp_sale_amt) as rp_sale_amt, -- 客户日配销售额		
	-- sum(bbc_sale_amt) as bbc_sale_amt, -- 客户bbc销售额
	sum(bbc_sale_amt_zy) as bbc_sale_amt_zy, -- 客户bbc自营销售额
	sum(bbc_sale_amt_ly) as bbc_sale_amt_ly, -- 客户bbc联营销售额
	sum(fl_sale_amt) as fl_sale_amt, -- 客户福利销售额
	-- 定价毛利额
	sum(profit) as profit,-- 客户总定价毛利额
	sum(rp_profit) as rp_profit,-- 客户日配定价毛利额
	-- sum(bbc_profit) as bbc_profit,-- 客户bbc定价毛利额
	sum(bbc_profit_zy) as bbc_profit_zy, -- 客户bbc自营定价毛利额
	sum(bbc_profit_ly) as bbc_profit_ly, -- 客户bbc联营定价毛利额
	sum(fl_profit) as fl_profit-- 客户福利定价毛利额


from 
	(
	select 
		customer_code,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in ('1','4','5') then sale_amt else 0 end) as rp_sale_amt,
		-- sum(case when business_type_code in('6') then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then sale_amt else 0 end) as bbc_sale_amt_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then sale_amt else 0 end) as bbc_sale_amt_ly,
		sum(case when business_type_code in('2') then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit, 
		sum(case when business_type_code in ('1','4','5') then profit else 0 end) as rp_profit,
		-- sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then profit else 0 end) as bbc_profit_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then profit else 0 end) as bbc_profit_ly,		
		sum(case when business_type_code in('2') then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di
	where 
		sdt>=regexp_replace(add_months(trunc('${sdt_yes_date}','MM'),-1),'-','') and sdt<=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
				-- 福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202210签呈 北京 129026 129000 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				-- 202306签呈 福建省'229290','175709','125092' 项目供应商 纳入大客户提成计算
				or (business_type_code in ('4') and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))
				-- 202310签呈 河北南京部分项目供应商客户纳入大客户提成计算
				or (business_type_code in ('4') and customer_code in ('235949','222853','131428','131466','131202','131208','129746','128435','230788',
				'112846','118357','115832','125795','125831','131462','114496','117322','131421','118395','114470','130024','130430','118644','131091',
				'217946','129955','130226','120115','226821','129870','129865','130269','126125','129674','129880','227563','129855','129860','130955',
				'127521','225541','232102','233354','234828','130844','223112','129854','125545','128705','125513','126001'))
				)
			-- and performance_province_name in ('福建省')
			and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断					
	group by 
		customer_code,substr(sdt,1,6)
		
		
	-- 扣减定价毛利额 '扣减毛利额'
	union all
	select customer_code,smt_date as smonth,
	0 as sale_amt, 
	0 as rp_sale_amt,
	0 as bbc_sale_amt_zy,
	0 as bbc_sale_amt_ly,
	0 as fl_sale_amt,
	0-adjust_amount as profit,
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_profit, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_profit_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_profit_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_profit
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减毛利额%'	
	
	-- 扣减销售额与毛利额 '扣减销售额与毛利额'
	union all
	select customer_code,smt_date as smonth,
	0-adjust_amount as sale_amt, 
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_sale_amt, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_sale_amt_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_sale_amt_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_sale_amt,	
	
	0-adjust_amount as profit,	
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_profit, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_profit_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_profit_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_profit
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减销售额与毛利额%'	

	-- 扣减销售额 '扣减销售额'
	union all
	select customer_code,smt_date as smonth,
	0-adjust_amount as sale_amt, 
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_sale_amt, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_sale_amt_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_sale_amt_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_sale_amt,	
	
	0 as profit,	
	0 as rp_profit, 
	0 as bbc_profit_zy,
	0 as bbc_profit_ly,
	0 as fl_profit
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减销售额'		
	)a
	group by customer_code,smonth
	)a
	left join
		(
		-- 固定扣点
		select smt_date,customer_code,category_second,
		sum(case when adjust_business_type='日配' then adjust_rate end) as rp_rate,
		sum(case when adjust_business_type='BBC自营' then adjust_rate end) as bbc_rate_zy,
		sum(case when adjust_business_type='BBC联营' then adjust_rate end) as bbc_rate_ly,
		sum(case when adjust_business_type='福利' then adjust_rate end) as fl_rate
		from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and category_second='固定扣点'
		group by smt_date,customer_code,category_second
		) b on b.customer_code=a.customer_code;	
	



insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business partition(smt)
select 
concat_ws('-',a.smonth,a.customer_code) as biz_id,
	a.smonth,
	b.performance_region_code as region_code_customer,
	b.performance_region_name as region_name_customer,
	b.performance_province_code as province_code_customer,
	b.performance_province_name as province_name_customer,
	b.performance_city_code as city_group_code_customer,
	b.performance_city_name as city_group_name_customer,	
	
	a.customer_code,
	c.customer_name,
	c.sales_user_id,
	c.sales_user_number,
	c.sales_user_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt_zy,
	a.bbc_sale_amt_ly,
	a.fl_sale_amt,
	a.profit,
	a.rp_profit,
	a.bbc_profit_zy,
	a.bbc_profit_ly,
	a.fl_profit,
	a.profit/abs(a.sale_amt) as prorate,
	a.rp_profit/abs(a.rp_sale_amt) as rp_prorate,
	a.bbc_profit_zy/abs(a.bbc_sale_amt_zy) as bbc_prorate_zy,
	a.bbc_profit_ly/abs(a.bbc_sale_amt_ly) as bbc_prorate_ly,
	a.fl_profit/abs(a.fl_sale_amt) as fl_prorate,
	d.category_second as service_falg,
	d.service_fee,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	-- substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt,
	cast(a.smonth as string) as smt	
from 
	(
	select
		a.customer_code,a.smonth,
		-- 销售额
		a.sale_amt, -- 客户总销售额
		a.rp_sale_amt, -- 客户日配销售额
		a.bbc_sale_amt_zy, -- 客户bbc自营销售额
		a.bbc_sale_amt_ly, -- 客户bbc联营销售额
		a.fl_sale_amt, -- 客户福利销售额
		if(a2.rp_rate is null,if(a1.rp_rate is null,a.rp_profit,if(a.rp_profit>a1.rp_rate*a.rp_sale_amt,a1.rp_rate*a.rp_sale_amt,a.rp_profit)),a2.rp_rate*a.rp_sale_amt)+
		if(a2.bbc_rate_zy is null,if(a1.bbc_rate_zy is null,a.bbc_profit_zy,if(a.bbc_profit_zy>a1.bbc_rate_zy*a.bbc_sale_amt_zy,a1.bbc_rate_zy*a.bbc_sale_amt_zy,a.bbc_profit_zy)),a2.bbc_rate_zy*a.bbc_sale_amt_zy)+
		if(a2.bbc_rate_ly is null,if(a1.bbc_rate_ly is null,a.bbc_profit_ly,if(a.bbc_profit_ly>a1.bbc_rate_ly*a.bbc_sale_amt_ly,a1.bbc_rate_ly*a.bbc_sale_amt_ly,a.bbc_profit_ly)),a2.bbc_rate_ly*a.bbc_sale_amt_ly)+
		if(a2.fl_rate is null,if(a1.fl_rate is null,a.fl_profit,if(a.fl_profit>a1.fl_rate*a.fl_sale_amt,a1.fl_rate*a.fl_sale_amt,a.fl_profit)),a2.fl_rate*a.fl_sale_amt) as profit,
			
		if(a2.rp_rate is null,if(a1.rp_rate is null,a.rp_profit,if(a.rp_profit>a1.rp_rate*a.rp_sale_amt,a1.rp_rate*a.rp_sale_amt,a.rp_profit)),a2.rp_rate*a.rp_sale_amt) as rp_profit,-- 客户日配定价毛利额
		if(a2.bbc_rate_zy is null,if(a1.bbc_rate_zy is null,a.bbc_profit_zy,if(a.bbc_profit_zy>a1.bbc_rate_zy*a.bbc_sale_amt_zy,a1.bbc_rate_zy*a.bbc_sale_amt_zy,a.bbc_profit_zy)),a2.bbc_rate_zy*a.bbc_sale_amt_zy) as bbc_profit_zy,-- 客户bbc自营定价毛利额
		if(a2.bbc_rate_ly is null,if(a1.bbc_rate_ly is null,a.bbc_profit_ly,if(a.bbc_profit_ly>a1.bbc_rate_ly*a.bbc_sale_amt_ly,a1.bbc_rate_ly*a.bbc_sale_amt_ly,a.bbc_profit_ly)),a2.bbc_rate_ly*a.bbc_sale_amt_ly) as bbc_profit_ly,-- 客户bbc联营定价毛利额
		if(a2.fl_rate is null,if(a1.fl_rate is null,a.fl_profit,if(a.fl_profit>a1.fl_rate*a.fl_sale_amt,a1.fl_rate*a.fl_sale_amt,a.fl_profit)),a2.fl_rate*a.fl_sale_amt) as fl_profit-- 客户福利定价毛利额
	
	from csx_analyse_tmp.tmp_tc_customer_sales_prorate_m a
	-- 关联签呈'最高毛利率'
	left join
		(
		select smt_date,customer_code,category_second,
			sum(case when adjust_business_type='日配' then adjust_rate end) as rp_rate,
			sum(case when adjust_business_type='BBC自营' then adjust_rate end) as bbc_rate_zy,
			sum(case when adjust_business_type='BBC联营' then adjust_rate end) as bbc_rate_ly,
			sum(case when adjust_business_type='福利' then adjust_rate end) as fl_rate
		from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and category_second like '%最高毛利率%'
		group by smt_date,customer_code,category_second		
		)a1 on a1.customer_code=a.customer_code
	-- 关联签呈'部分业务固定毛利率'
	left join		
		(  
		select smt_date,customer_code,category_second,
			sum(case when adjust_business_type='日配' then adjust_rate end) as rp_rate,
			sum(case when adjust_business_type='BBC自营' then adjust_rate end) as bbc_rate_zy,
			sum(case when adjust_business_type='BBC联营' then adjust_rate end) as bbc_rate_ly,
			sum(case when adjust_business_type='福利' then adjust_rate end) as fl_rate
		from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and category_second like '%部分业务固定毛利率%'
		group by smt_date,customer_code,category_second
		)a2 on a2.customer_code=a.customer_code		
	) a
	left join 
		(
		select 
			distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			-- 202303签呈 上海 '130733','128865','130078','114872','124484' 每月纳入大客户提成计算 仅管家拿提成
			-- case when channel_code='9' and customer_code not in ('106299','130733','128865','130078','114872','124484','227054','228705','225582','123415','113260') then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from csx_dim.csx_dim_crm_customer_info 
		where 
			sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
			and customer_type_code=4
		)b on b.customer_code=a.customer_code
	left join 
		(
		select
			region_code,region_name,
			province_code,province_name,
			city_group_code,city_group_name,
			customer_no,customer_name,
			sales_id sales_user_id,
			work_no sales_user_number,
			sales_name sales_user_name,
			rp_service_user_id,
			rp_service_user_work_no,
			rp_service_user_name,
			fl_service_user_id,
			fl_service_user_work_no,
			fl_service_user_name,		
			bbc_service_user_id,
			bbc_service_user_work_no,
			bbc_service_user_name
			
		-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
		from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
		where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
		) c on c.customer_no=a.customer_code		
	left join 
		(
	select customer_code,smt_date as smonth,category_second,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%服务费%'		
	)d on a.customer_code=d.customer_code
left join 
		(
	select customer_code,smt_date as smonth,category_second
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%剔除客户%'		
	)e on a.customer_code=e.customer_code
where e.category_second is null	
;









drop table if exists csx_analyse.csx_analyse_fr_tc_customer_sale_business;

--hive 客户提成_业绩毛利
drop table if exists csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business;
create table csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business(
`biz_id`	string	COMMENT	'业务主键',
`smonth`	string	COMMENT	'年月',
`region_code_customer`	string	COMMENT	'客户大区编码',
`region_name_customer`	string	COMMENT	'客户大区名称',
`province_code_customer`	string	COMMENT	'客户省份编码',
`province_name_customer`	string	COMMENT	'客户省份名称',
`city_group_code_customer`	string	COMMENT	'客户城市组编码',
`city_group_name_customer`	string	COMMENT	'客户城市组名称',
`customer_code`	string	COMMENT	'客户编码',
`customer_name`	string	COMMENT	'客户名称',
`sales_user_id`	string	COMMENT	'销售员id',
`sales_user_number`	string	COMMENT	'销售员工号',
`sales_user_name`	string	COMMENT	'销售员名称',
`rp_service_user_id`	string	COMMENT	'日配服务管家id',
`rp_service_user_work_no`	string	COMMENT	'日配服务管家工号',
`rp_service_user_name`	string	COMMENT	'日配服务管家名称',
`fl_service_user_id`	string	COMMENT	'福利服务管家id',
`fl_service_user_work_no`	string	COMMENT	'福利服务管家工号',
`fl_service_user_name`	string	COMMENT	'福利服务管家名称',
`bbc_service_user_id`	string	COMMENT	'BBC服务管家id',
`bbc_service_user_work_no`	string	COMMENT	'BBC服务管家工号',
`bbc_service_user_name`	string	COMMENT	'BBC服务管家名称',
`sale_amt`	decimal(20,6)	COMMENT	'销售额',
`rp_sale_amt`	decimal(20,6)	COMMENT	'日配销售额',
`bbc_sale_amt_zy`	decimal(20,6)	COMMENT	'BBC自营销售额',
`bbc_sale_amt_ly`	decimal(20,6)	COMMENT	'BBC联营销售额',
`fl_sale_amt`	decimal(20,6)	COMMENT	'福利销售额',
`profit`	decimal(20,6)	COMMENT	'定价毛利额',
`rp_profit`	decimal(20,6)	COMMENT	'日配定价毛利额',
`bbc_profit_zy`	decimal(20,6)	COMMENT	'BBC自营定价毛利额',
`bbc_profit_ly`	decimal(20,6)	COMMENT	'BBC联营定价毛利额',
`fl_profit`	decimal(20,6)	COMMENT	'福利定价毛利额',
`prorate`	decimal(20,6)	COMMENT	'定价毛利率',
`rp_prorate`	decimal(20,6)	COMMENT	'日配定价毛利率',
`bbc_prorate_zy`	decimal(20,6)	COMMENT	'BBC自营定价毛利率',
`bbc_prorate_ly`	decimal(20,6)	COMMENT	'BBC联营定价毛利率',
`fl_prorate`	decimal(20,6)	COMMENT	'福利定价毛利率',
`service_falg`	string	COMMENT	'服务费标记',
`service_fee`	decimal(20,6)	COMMENT	'服务费金额',
`update_time`	timestamp	COMMENT    '更新时间'
) COMMENT '客户提成_业绩毛利'
PARTITIONED BY (smt string COMMENT '日期分区')
;

-- truncate tc_customer_special_rules_2023;

-- drop table if exists tc_customer_special_rules_2023;
CREATE TABLE `tc_customer_special_rules_2023` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `biz_id` varchar(64) NOT NULL COMMENT '业务主键',
  `province_name` varchar(64) DEFAULT NULL COMMENT '省份名称',
  `customer_code` varchar(64) DEFAULT NULL COMMENT '客户编码',
  `smt_date` varchar(64) DEFAULT NULL COMMENT '年月',
  `qc_yearmonth` varchar(128) DEFAULT NULL COMMENT '签呈年月',
  `category_first` varchar(512) DEFAULT NULL COMMENT '一级类别',
  `category_second` varchar(512) DEFAULT NULL COMMENT '二级类别', 
  `adjust_business_type` varchar(512) DEFAULT NULL COMMENT '业务类型',  
  `category_person_rate` varchar(512) DEFAULT NULL COMMENT '类别_调整人员或系数',
  `category_profit_no` varchar(512) DEFAULT NULL COMMENT '类别_不参与综合毛利计算',
  `category_receivable` varchar(512) DEFAULT NULL COMMENT '类别_应收账款考核',
  `category_profit` varchar(512) DEFAULT NULL COMMENT '类别_扣减毛利',
  `category_profit_rate` varchar(512) DEFAULT NULL COMMENT '类别_固定扣点',
  `effective_period` varchar(128) DEFAULT NULL COMMENT '时间期限',
  `remark` varchar(800) DEFAULT NULL COMMENT '备注',
  `adjust_amount` decimal(20,6) DEFAULT NULL COMMENT '调整金额',
  `adjust_rate` decimal(20,6) DEFAULT NULL COMMENT '调整比例',
  `service_fee` decimal(20,6) DEFAULT NULL COMMENT '服务费金额',
  `back_amt_tc_rate` decimal(20,6) DEFAULT NULL COMMENT '回款提成比例',  
  `company_code` varchar(32) DEFAULT NULL COMMENT '公司代码',
  `credit_code` varchar(64) DEFAULT NULL COMMENT '信控号',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=44504 DEFAULT CHARSET=utf8mb4 COMMENT='单客特殊规则维护表';


CREATE  TABLE IF NOT EXISTS csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf( 
`biz_id` STRING  COMMENT '业务主键',
  `province_name` STRING  COMMENT '省份名称',
  `customer_code` STRING  COMMENT '客户编码',
  `smt_date` STRING  COMMENT '年月',
  `qc_yearmonth` STRING  COMMENT '签呈年月',
  `category_first` STRING  COMMENT '一级类别',
  `category_second` STRING  COMMENT '二级类别', 
  `adjust_business_type` STRING  COMMENT '业务类型',  
  `effective_period` STRING  COMMENT '时间期限',
  `remark` STRING  COMMENT '备注',
  `adjust_amount` STRING  COMMENT '调整金额',
  `adjust_rate` STRING  COMMENT '调整比例',
  `service_fee` STRING  COMMENT '服务费金额',
  `back_amt_tc_rate` STRING  COMMENT '回款提成比例',  
  `company_code` STRING  COMMENT '公司代码',
`credit_code` STRING  COMMENT '信控号' ) 
 COMMENT '单客特殊规则维护表-同步' 
PARTITIONED BY (smt string COMMENT '日期分区')
;

csx_analyse_tc_customer_special_rules_2023_mf
csx_analyse_tcqc_customer_special_rules_2023_mf



insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_special_rules_2023_mi partition(smt)
select
	a.biz_id,
	a.customer_code,b.customer_name,
	b.channel_code,b.channel_name,
	b.region_code,b.region_name,
	b.province_code,b.province_name,
	b.city_group_code,b.city_group_name,
	b.sales_id,
	b.work_no,
	b.sales_name,
	b.rp_service_user_id,
	b.rp_service_user_work_no,
	b.rp_service_user_name,
	b.fl_service_user_id,
	b.fl_service_user_work_no,
	b.fl_service_user_name,
	b.bbc_service_user_id,	
	b.bbc_service_user_work_no,
	b.bbc_service_user_name,
	a.smt_date,
	a.qc_yearmonth,
	a.category_first,
	a.category_second,
	a.adjust_business_type,
	a.effective_period,
	a.remark,
	a.adjust_amount,
	a.adjust_rate,
	a.service_fee,
	a.back_amt_tc_rate,
	a.company_code,
	a.credit_code,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(add_months('${sdt_current_date}',-1),'-',''), 1, 6) as smt -- 统计日期
from 
(
	select *
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_current_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_current_date}',-1)),'-',''),1,6)
)a
left join 
(
	select *
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi 
	where smt=substr(regexp_replace(add_months('${sdt_current_date}',-1),'-',''), 1, 6)
)b on a.customer_code=b.customer_no;



CREATE  TABLE IF NOT EXISTS csx_analyse.csx_analyse_fr_tc_customer_special_rules_2023_mi( 
`biz_id` STRING  COMMENT '业务主键',
`customer_no` STRING  COMMENT '客户编码',
`customer_name` STRING  COMMENT '客户名称',
`channel_code` STRING  COMMENT '渠道编码',
`channel_name` STRING  COMMENT '渠道名称',
`region_code` STRING  COMMENT '客户大区编码',
`region_name` STRING  COMMENT '客户大区名称',
`province_code` STRING  COMMENT '客户省区编码',
`province_name` STRING  COMMENT '客户省区名称',
`city_group_code` STRING  COMMENT '客户城市编码',
`city_group_name` STRING  COMMENT '客户城市名称',
`sales_id` STRING  COMMENT '主销售员id',
`work_no` STRING  COMMENT '销售员工号',
`sales_name` STRING  COMMENT '销售员',
`rp_service_user_id` STRING  COMMENT '日配_服务管家id',
`rp_service_user_work_no` STRING  COMMENT '日配_服务管家工号',
`rp_service_user_name` STRING  COMMENT '日配_服务管家',
`fl_service_user_id` STRING  COMMENT '福利_服务管家id',
`fl_service_user_work_no` STRING  COMMENT '福利_服务管家工号',
`fl_service_user_name` STRING  COMMENT '福利_服务管家',
`bbc_service_user_id` STRING  COMMENT 'bbc_服务管家id',
`bbc_service_user_work_no` STRING  COMMENT 'bbc_服务管家工号',
`bbc_service_user_name` STRING  COMMENT 'bbc_服务管家',
  `smt_date` STRING  COMMENT '年月',
  `qc_yearmonth` STRING  COMMENT '签呈年月',
  `category_first` STRING  COMMENT '一级类别',
  `category_second` STRING  COMMENT '二级类别', 
  `adjust_business_type` STRING  COMMENT '业务类型',  
  `effective_period` STRING  COMMENT '时间期限',
  `remark` STRING  COMMENT '备注',
  `adjust_amount` STRING  COMMENT '调整金额',
  `adjust_rate` STRING  COMMENT '调整比例',
  `service_fee` STRING  COMMENT '服务费金额',
  `back_amt_tc_rate` STRING  COMMENT '回款提成比例',  
  `company_code` STRING  COMMENT '公司代码',
`credit_code` STRING  COMMENT '信控号',
`update_time` TIMESTAMP  COMMENT '更新时间' ) 
 COMMENT '单客特殊规则_帆软报表' 
PARTITIONED BY (smt string COMMENT '日期分区');

