
--============================================================================================================
--P1-周环比

--本周五日期
set i_sdate_5 =regexp_replace(if(pmod(datediff(current_date,'1920-01-04'),7)=6,
								date_sub(current_date,1),
								date_sub(current_date,pmod(datediff(current_date,'1920-01-04'),7)+2)),'-','');
								
select
	province_name as key_name,
	if(sdt>=${hiveconf:i_sdate_5}-6,'本周','上周') as week_type,
	sum(sales_value) as sales_value, 
	sum(profit) as profit,
	coalesce(sum(profit)/sum(sales_value),0) as profit_rate
from 
	csx_dw.dws_sale_r_d_customer_sale
where 
	sdt between ${hiveconf:i_sdate_5}-13 and ${hiveconf:i_sdate_5}
	and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
	and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC 
	and province_name not like '平台%'
group by
	province_name,if(sdt>=${hiveconf:i_sdate_5}-6,'本周','上周')
	
union all

select
	b.region_name as key_name,
	a.week_type,				
	sum(a.sales_value) as sales_value, 
	sum(a.profit) as profit,
	coalesce(sum(a.profit)/sum(a.sales_value),0) as profit_rate
from	
	(	
	select
		province_code,
		province_name,
		if(sdt>=${hiveconf:i_sdate_5}-6,'本周','上周') as week_type,
		sum(sales_value) as sales_value, 
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between ${hiveconf:i_sdate_5}-13 and ${hiveconf:i_sdate_5}
		and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC 
		and province_name not like '平台%'
	group by
		province_code,province_name,if(sdt>=${hiveconf:i_sdate_5}-6,'本周','上周')
	) as a	
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) b on b.province_code=a.province_code
group by
	b.region_name,
	a.week_type	
;



--============================================================================================================
--P2-分课组周环比

--本周五日期
set i_sdate_5 =regexp_replace(if(pmod(datediff(current_date,'1920-01-04'),7)=6,
								date_sub(current_date,1),
								date_sub(current_date,pmod(datediff(current_date,'1920-01-04'),7)+2)),'-','');
								
select
	province_name,
	if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	if(sdt>=${hiveconf:i_sdate_5}-6,'本周','上周') as week_type,
	sum(sales_value) as sales_value, 
	sum(profit) as profit,
	coalesce(sum(profit)/sum(sales_value),0) as profit_rate
from 
	csx_dw.dws_sale_r_d_customer_sale
where 
	sdt between ${hiveconf:i_sdate_5}-13 and ${hiveconf:i_sdate_5}
	and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
	and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC 
	and province_name not like '平台%'
group by
	province_name,
	if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他'),
	if(sdt>=${hiveconf:i_sdate_5}-6,'本周','上周')
;	
	
	
	
	
	

--============================================================================================================
--分业态_月累计 上月同期

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_21 =regexp_replace(add_months(date_sub(current_date,1),-1),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');

				
select
	a.province_name, 
	(case when b.sales_belong_flag like '%云超%' then '云超'
		when b.sales_belong_flag like '%云创%' then '云创'
		when a.customer_name rlike '红旗|中百' then '关联方'
		when b.sales_belong_flag='' or b.sales_belong_flag is null then '外部'
		else '其他'
	end) as sales_belong_type,
	a.month_type,
	sum(a.sales_value) as sales_value,
	sum(a.profit) as profit,
	coalesce(sum(a.profit)/sum(a.sales_value),0) as profit_rate
from	
	(	
	select
		province_code,province_name,customer_no,customer_name,
		(case when sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} then '本月'
			when sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21} then '上月'
			else '其他'
		end) as month_type,
		sum(sales_value) as sales_value,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		(sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} or sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21})
		and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC
		and province_name not like '平台%'				
	group by
		province_code,province_name,customer_no,customer_name,					
		(case when sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} then '本月'
			when sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21} then '上月'
			else '其他'
		end)
	) as a 
	left join
		(
		select 
			shop_id,company_code,sales_belong_flag
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt = 'current'
		group by
			shop_id,company_code,sales_belong_flag
		) b on a.customer_no = concat('S', b.shop_id)
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) c on c.province_code=a.province_code
group by
	a.province_name, 
	(case when b.sales_belong_flag like '%云超%' then '云超'
		when b.sales_belong_flag like '%云创%' then '云创'
		when a.customer_name rlike '红旗|中百' then '关联方'
		when b.sales_belong_flag='' or b.sales_belong_flag is null then '外部'
		else '其他'
	end),
	a.month_type
;






--============================================================================================================
--课组_月累计 上月同期

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_21 =regexp_replace(add_months(date_sub(current_date,1),-1),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');

				

select
	province_name,
	if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	(case when sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} then '本月'
		when sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21} then '上月'
		else '其他'
	end) as month_type,
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	coalesce(sum(profit)/sum(sales_value),0) as profit_rate
from 
	csx_dw.dws_sale_r_d_customer_sale
where 
	(sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} or sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21})
	and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
	and dc_code not in ('W0R1','W0T6','W0E7','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') --数据不含代加工DC
	and province_name not like '平台%'				
group by
	province_name,
	if(department_code in ('U01','H03','H05','H01','H02','H04','104'),department_name,'其他'),--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	(case when sdt between ${hiveconf:i_sdate_12} and ${hiveconf:i_sdate_11} then '本月'
		when sdt between ${hiveconf:i_sdate_22} and ${hiveconf:i_sdate_21} then '上月'
		else '其他'
	end)
;

