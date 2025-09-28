
set hive.tez.container.size=8192;

-- 毛利额达成情况-销售员与客服经理：未处理多管家情况-顿号隔开
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total_all;
create table csx_analyse_tmp.tmp_tc_person_profit_total_all
as
select distinct
	a.smt,
	a.sales_id,a.work_no,a.sales_name,
	a.sales_profit_basic,  -- 销售员毛利额_基数
	a.sales_profit_finish,  -- 销售员毛利额_达成
	a.sales_target_rate,  -- 销售员毛利额_达成率
	a.sales_target_rate_tc  -- 销售员毛利额_达成系数
from 
(
	select 
		smt,customer_code,customer_name,
		sales_id,work_no,sales_name,
		sales_profit_basic,  -- 销售员毛利额_基数
		sales_profit_finish,  -- 销售员毛利额_达成
		sales_target_rate,  -- 销售员毛利额_达成率
		sales_target_rate_tc  -- 销售员毛利额_达成系数
	from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
	where work_no<>''
	and smt between '202308' and '202312'
	
	union all
	select 
		smt,customer_code,customer_name,
		rp_service_user_id as sales_id,
		rp_service_user_work_no as work_no,
		rp_service_user_name as sales_name,
		rp_service_profit_basic as sales_profit_basic,  -- 销售员毛利额_基数
		rp_service_profit_finish as sales_profit_finish,  -- 销售员毛利额_达成
		rp_service_target_rate as sales_target_rate,  -- 销售员毛利额_达成率
		rp_service_target_rate_tc as sales_target_rate_tc  -- 销售员毛利额_达成系数
	from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
	where rp_service_user_work_no<>''
	and smt between '202308' and '202312'
	
	union all
	select 
		smt,customer_code,customer_name,
		fl_service_user_id as sales_id,
		fl_service_user_work_no as work_no,
		fl_service_user_name as sales_name,
		fl_service_profit_basic as sales_profit_basic,  -- 销售员毛利额_基数
		fl_service_profit_finish as sales_profit_finish,  -- 销售员毛利额_达成
		fl_service_target_rate as sales_target_rate,  -- 销售员毛利额_达成率
		fl_service_target_rate_tc as sales_target_rate_tc  -- 销售员毛利额_达成系数
	from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
	where fl_service_user_work_no<>''
	and smt between '202308' and '202312'
	
	union all
	select 
		smt,customer_code,customer_name,
		bbc_service_user_id as sales_id,
		bbc_service_user_work_no as work_no,
		bbc_service_user_name as sales_name,
		bbc_service_profit_basic as sales_profit_basic,  -- 销售员毛利额_基数
		bbc_service_profit_finish as sales_profit_finish,  -- 销售员毛利额_达成
		bbc_service_target_rate as sales_target_rate,  -- 销售员毛利额_达成率
		bbc_service_target_rate_tc as sales_target_rate_tc  -- 销售员毛利额_达成系数
	from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
	where bbc_service_user_work_no<>''
	and smt between '202308' and '202312'
)a;
	

-- 创建人员信息表，获取销售员和客服经理的城市，因为存在一个业务员名下客户跨城市的情况
drop table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info_all;
create table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info_all
as
select distinct a.user_id,a.user_number,a.user_name,
	b.performance_city_code,
	b.performance_city_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_region_code,
	b.performance_region_name
from
	(
	select 	user_id,user_number,user_name,user_position,city_name,province_name
	from csx_dim.csx_dim_uc_user
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	-- and status = 0 
	-- and delete_flag = '0'
	) a
	left join -- 区域表
	( 
	select distinct
		city_code,city_name,
		province_code,province_name,
		performance_city_code,
		performance_city_name,
		performance_province_code,
		performance_province_name,
		performance_region_code,
		performance_region_name
	from csx_dim.csx_dim_sales_area_belong_mapping
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	) b on b.city_name=a.city_name and b.province_name=a.province_name;
	
	

-- 销售员与管家年度毛利达成情况
drop table csx_analyse_tmp.tmp_tc_person_profit_target_rate_all;
create table csx_analyse_tmp.tmp_tc_person_profit_target_rate_all
as
select
	-- region_name,   -- 大区名称
	-- province_name,   -- 省区名称
	-- city_group_name,   -- 城市名称
	sales_id,   -- 销售员id
	work_no,   -- 销售员工号
	-- sales_name,   -- 销售员
	begin_date,   -- 入职日期
	case when begin_date>=regexp_replace(trunc(add_months('${sdt_yes_date}',-12),'MM'),'-','') then '是' else '否' end as begin_less_1year_flag,   -- 入职是否小于1年
	-- begin_less_1year_flag,   -- 入职是否小于1年
	-- sale_amt,   -- 销售额
	sum(profit) as profit,   -- 毛利额
	sum(profit_basic) as profit_basic,   -- 毛利目标
	sum(profit)/sum(profit_basic) as profit_target_rate   -- 毛利目标达成系数	
from csx_analyse.csx_analyse_tc_person_profit_target_rate
where smt between '202308' and '202312'
group by 
	-- region_name,   -- 大区名称
	-- province_name,   -- 省区名称
	-- city_group_name,   -- 城市名称
	sales_id,   -- 销售员id
	work_no,   -- 销售员工号
	-- sales_name,   -- 销售员
	begin_date   -- 入职日期
;




	
-- 目标毛利系数-销售员与客服经理
-- 过度期内入职时间超过1年的个人基准毛利率低于100%的按照实际率核算; 湖北8月过渡期，9月生效；其他省区都是8-10月过渡期，11月生效
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_target_rate_all_tc_all;
create table csx_analyse_tmp.tmp_tc_person_profit_target_rate_all_tc_all
as
-- 202310签呈：销售员詹成宏81147160不考核毛利额达成率 每月
-- 202311签呈：81131450 张珠妹、81133021 黄升、80952742 王秀云、80010438 林秀丽 不考核毛利额达成率 每月

-- 202311签呈：81090419 葛立新   2023年8月1日从湖北调动至合肥，申请保护期一年，2023年8月1日至2024年7月31日
-- 202311签呈：81014012 张鲲     2023年11月毛利额系数100%
-- 202311签呈：81048704 关毓萱   2023年11月毛利额系数100%
-- 202311签呈：80892167 李烁岚   2023年11-12月毛利额系数100%
select *,
	case 
		when a.work_no in('81147160') and a.smt>='202310' then 1
		when a.work_no in('81131450','81133021','80952742','80010438') and a.smt>='202311' then 1
		when a.work_no in('81090419') and a.smt>='202311' and a.smt<='202407' then profit_target_rate
		when a.work_no in('81014012','81048704') and a.smt='202311' then 1
		when a.work_no in('80892167') and a.smt in('202311','202312') then 1
		when b.performance_province_name in('湖北省') and a.smt<='202308' then if(profit_target_rate<1,profit_target_rate,1)
		when b.performance_province_name not in('湖北省') and a.smt<='202310' then if(profit_target_rate<1,profit_target_rate,1)
	else (case 	
		when coalesce(begin_less_1year_flag,'否')='是' and profit_target_rate<1 then profit_target_rate
		when coalesce(begin_less_1year_flag,'否')='是' and profit_target_rate>=1 then 1
		when coalesce(begin_less_1year_flag,'否')='否' and profit_target_rate<1 then 0
		when coalesce(begin_less_1year_flag,'否')='否' and profit_target_rate>=1 then 1
		else 1 end) end as profit_target_rate_tc	
	-- coalesce(if(profit_target_rate>=1,1,profit_target_rate),1) as profit_target_rate_tc			
from 
(
	select
		a.smt,
		a.sales_id,a.work_no,a.sales_name,
		-- 多管家毛利额达成排序中的最大值，就高原则
		case when arr[3]=profit_target_rate_1 then begin_date_1
			 when arr[3]=profit_target_rate_2 then begin_date_2
			 when arr[3]=profit_target_rate_3 then begin_date_3
			 when arr[3]=profit_target_rate_4 then begin_date_4
			 when arr[3] is null then begin_date_1
			 end as begin_date,	
		case when arr[3]=profit_target_rate_1 then begin_less_1year_flag_1
			 when arr[3]=profit_target_rate_2 then begin_less_1year_flag_2
			 when arr[3]=profit_target_rate_3 then begin_less_1year_flag_3
			 when arr[3]=profit_target_rate_4 then begin_less_1year_flag_4
			 when arr[3] is null then begin_less_1year_flag_1
			 end as begin_less_1year_flag,	
		
		case when arr[3]=profit_target_rate_1 then profit_basic_1
			 when arr[3]=profit_target_rate_2 then profit_basic_2
			 when arr[3]=profit_target_rate_3 then profit_basic_3
			 when arr[3]=profit_target_rate_4 then profit_basic_4
			 -- 如果没有业绩毛利，则取第一项毛利目标
			 when arr[3] is null then profit_basic_1			 
			 end as profit_basic,
		case when arr[3]=profit_target_rate_1 then profit_1
			 when arr[3]=profit_target_rate_2 then profit_2
			 when arr[3]=profit_target_rate_3 then profit_3
			 when arr[3]=profit_target_rate_4 then profit_4
			 when arr[3] is null then 0
			 end as profit,
		case when arr[3]=profit_target_rate_1 then profit_target_rate_1
			 when arr[3]=profit_target_rate_2 then profit_target_rate_2
			 when arr[3]=profit_target_rate_3 then profit_target_rate_3
			 when arr[3]=profit_target_rate_4 then profit_target_rate_4
			 when arr[3] is null then 0
			 end as profit_target_rate
	from 
	(
	select 
		a.smt,
		a.sales_id,a.work_no,a.sales_name,
		-- a.sales_profit_basic,  -- 销售员毛利额_基数
		-- a.sales_profit_finish,  -- 销售员毛利额_达成
		-- a.sales_target_rate,  -- 销售员毛利额_达成率
		-- a.sales_target_rate_tc,  -- 销售员毛利额_达成系数		
		b1.profit as profit_1,
		b1.profit_basic as profit_basic_1,
		b1.profit_target_rate as profit_target_rate_1,
		b1.begin_date as begin_date_1,
		b1.begin_less_1year_flag as begin_less_1year_flag_1,
	
		b2.profit as profit_2,
		b2.profit_basic as profit_basic_2,
		b2.profit_target_rate as profit_target_rate_2,	
		b2.begin_date as begin_date_2,
		b2.begin_less_1year_flag as begin_less_1year_flag_2,
		
		b3.profit as profit_3,
		b3.profit_basic as profit_basic_3,
		b3.profit_target_rate as profit_target_rate_3,	
		b3.begin_date as begin_date_3,
		b3.begin_less_1year_flag as begin_less_1year_flag_3,
		
		b4.profit as profit_4,
		b4.profit_basic as profit_basic_4,
		b4.profit_target_rate as profit_target_rate_4,	
		b4.begin_date as begin_date_4,
		b4.begin_less_1year_flag as begin_less_1year_flag_4,	
		-- 多个管家的毛利额达成排序
		sort_array(array(b1.profit_target_rate,b2.profit_target_rate,b3.profit_target_rate,b4.profit_target_rate)) as arr
	from csx_analyse_tmp.tmp_tc_person_profit_total_all a 
	left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_all b1 on split(a.sales_id,'、')[0]=b1.sales_id 
	left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_all b2 on split(a.sales_id,'、')[1]=b2.sales_id 
	left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_all b3 on split(a.sales_id,'、')[2]=b3.sales_id 
	left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_all b4 on split(a.sales_id,'、')[3]=b4.sales_id
	)a
)a
left join csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info_all b on split(a.sales_id,'、')[0] =b.user_id;





drop table if exists csx_analyse_tmp.tmp_tc_customer_bill_month_dff_rate_detail_year_b;
create table csx_analyse_tmp.tmp_tc_customer_bill_month_dff_rate_detail_year_b
as
select 
	a.smt,
	a.region_name,
	a.province_name,
	a.city_group_name,
	a.customer_code,
	a.customer_name,
	a.work_no,
	a.sales_name,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_work_no,
	a.fl_service_user_name,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.happen_month,
	a.bill_date,
	a.paid_date,
	a.dff_rate,
	-- 回款金额
	a.pay_amt,
	a.rp_pay_amt,
	a.bbc_pay_amt,
	a.bbc_ly_pay_amt,
	a.bbc_zy_pay_amt,
	a.fl_pay_amt,
	-- 毛利率与毛利率提成比例
	a.profit_rate,
	a.rp_profit_rate,
	a.bbc_profit_rate,
	a.bbc_ly_profit_rate,
	a.bbc_zy_profit_rate,
	a.fl_profit_rate,
	a.cust_rp_profit_rate_tc,
	a.cust_bbc_ly_profit_rate_tc,
	a.cust_bbc_zy_profit_rate_tc,
	a.cust_fl_profit_rate_tc,
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_sale_fp_rate,
	a.bbc_sales_sale_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,
	a.new_cust_rate,
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,
	--原来提成
	a.tc_sales,
	a.tc_rp_service,
	a.tc_fl_service,
	a.tc_bbc_service,
	-- 服务费
	a.service_falg,
	a.service_fee,
	

	-- d1.begin_less_1year_flag as begin_less_1year_flag,
	d1.profit_basic as sales_profit_basic_new,
	d1.profit as sales_profit_new,
	d1.profit_target_rate as sales_profit_target_rate_new,
	d1.profit_target_rate_tc as sales_profit_target_rate_tc_new,

	d2.profit_basic as rp_service_profit_basic_new,
	d2.profit as rp_service_profit_new,
	d2.profit_target_rate as rp_service_profit_target_rate_new,
	d2.profit_target_rate_tc as rp_service_profit_target_rate_tc_new,

	d3.profit_basic as fl_service_profit_basic_new,
	d3.profit as fl_service_profit_new,
	d3.profit_target_rate as fl_service_profit_target_rate_new,
	d3.profit_target_rate_tc as fl_service_profit_target_rate_tc_new,

	d4.profit_basic as bbc_service_profit_basic_new,
	d4.profit as bbc_service_profit_new,
	d4.profit_target_rate as bbc_service_profit_target_rate_new,
	d4.profit_target_rate_tc as bbc_service_profit_target_rate_tc_new,
	
	
	-- (rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_sales_fp_rate)*coalesce(a.new_cust_rate,1)*coalesce(d1.profit_target_rate_tc,1) as tc_sales_rp,
	-- (bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_sales_sale_fp_rate)*coalesce(d1.profit_target_rate_tc,1) as tc_sales_bbc_zy,
	-- (bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_sales_sale_fp_rate)*coalesce(d1.profit_target_rate_tc,1) as tc_sales_bbc_ly,
	-- (fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_sales_sale_fp_rate)*coalesce(d1.profit_target_rate_tc,1) as tc_sales_fl,
	-- 
	-- ((rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_sales_fp_rate)*coalesce(a.new_cust_rate,1)+
	-- (bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_sales_sale_fp_rate)+
	-- (bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_sales_sale_fp_rate)+
	-- (fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_sales_sale_fp_rate))*coalesce(d1.profit_target_rate_tc,1) as tc_sales,
	-- 	
	-- rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_service_user_fp_rate*coalesce(d2.profit_target_rate_tc,1) as tc_rp_service,		
	-- 	
	-- fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_service_user_fp_rate*coalesce(d3.profit_target_rate_tc,1) as tc_fl_service,	
	-- 
	-- bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_service_user_fp_rate*coalesce(d4.profit_target_rate_tc,1) as tc_bbc_service_zy,
	-- bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_service_user_fp_rate*coalesce(d4.profit_target_rate_tc,1) as tc_bbc_service_ly,
	-- 
	-- ((bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_service_user_fp_rate)+
	-- (bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_service_user_fp_rate))*coalesce(d4.profit_target_rate_tc,1) as tc_bbc_service,

	
	-- 若系统账期为预付货款，则按原回款时间系数
	-- 若是预付款客户，打款日期小于上月1号则，按原回款时间系数但最高100%
	-- 若打款日期小于上月1号则提成为0，若为服务费则=当月回款额/当月销售额*服务费标准*回款系数
	if(d1.profit_target_rate_tc=1 and a.sales_target_rate_tc=0 and d1.begin_less_1year_flag='否' and (a.province_name='湖北省' or a.province_name<>'湖北省' and a.smt>='202311'),(
	if(a.paid_date<=last_day(add_months(date_format(from_unixtime(unix_timestamp(concat(a.smt,'01'),'yyyyMMdd')), 'yyyy-MM-dd'),-2)) and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.tc_sales,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			((rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_sales_fp_rate)*coalesce(a.new_cust_rate,1)+
				(bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_sales_sale_fp_rate)+
				(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_sales_sale_fp_rate)+
				(fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_sales_sale_fp_rate))*coalesce(d1.profit_target_rate_tc,1)
				))
	)*if(d.category_second like'%提成减半%',0.5,1)),a.tc_sales) as tc_sales_new,
	
	if(d2.profit_target_rate_tc=1 and a.rp_service_target_rate_tc=0 and d2.begin_less_1year_flag='否' and (a.province_name='湖北省' or a.province_name<>'湖北省' and a.smt>='202311'),(
	if(a.paid_date<=last_day(add_months(date_format(from_unixtime(unix_timestamp(concat(a.smt,'01'),'yyyyMMdd')), 'yyyy-MM-dd'),-2)) and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.tc_rp_service,
		if(d.category_second in('不算提成','管家不算提成'),0,
			rp_pay_amt*cust_rp_profit_rate_tc*dff_rate*rp_service_user_fp_rate*coalesce(d2.profit_target_rate_tc,1)
			))
	)*if(d.category_second like'%提成减半%',0.5,1)),a.tc_rp_service) as tc_rp_service_new,		

	if(d3.profit_target_rate_tc=1 and a.fl_service_target_rate_tc=0 and d3.begin_less_1year_flag='否' and (a.province_name='湖北省' or a.province_name<>'湖北省' and a.smt>='202311'),(
	if(a.paid_date<=last_day(add_months(date_format(from_unixtime(unix_timestamp(concat(a.smt,'01'),'yyyyMMdd')), 'yyyy-MM-dd'),-2)) and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.tc_fl_service,
		if(d.category_second in('不算提成','管家不算提成'),0,
			fl_pay_amt*cust_fl_profit_rate_tc*dff_rate*fl_service_user_fp_rate*coalesce(d3.profit_target_rate_tc,1)
			))
		)*if(d.category_second like'%提成减半%',0.5,1)),a.tc_fl_service) as tc_fl_service_new,	

	if(d4.profit_target_rate_tc=1 and a.bbc_service_target_rate_tc=0 and d4.begin_less_1year_flag='否' and (a.province_name='湖北省' or a.province_name<>'湖北省' and a.smt>='202311'),(
	if(a.paid_date<=last_day(add_months(date_format(from_unixtime(unix_timestamp(concat(a.smt,'01'),'yyyyMMdd')), 'yyyy-MM-dd'),-2)) and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.tc_bbc_service,
		if(d.category_second in('不算提成','管家不算提成'),0,
			((bbc_zy_pay_amt*cust_bbc_zy_profit_rate_tc*dff_rate*bbc_service_user_fp_rate)+
	(bbc_ly_pay_amt*cust_bbc_ly_profit_rate_tc*dff_rate*bbc_service_user_fp_rate))*coalesce(d4.profit_target_rate_tc,1)
	))
	)*if(d.category_second like'%提成减半%',0.5,1)),a.tc_bbc_service) as tc_bbc_service_new
	
		
from 
(
	select *
	from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
	where smt between '202308' and '202312'
) a
-- 系统账期预付款客户
--  -- 信控公司账期 
-- left join 
-- (
-- 	select customer_code,credit_code,company_code,
-- 		account_period_code,account_period_name,		-- 账期编码,账期名称
-- 		account_period_value,		-- 账期值
-- 		account_period_abbreviation_name,		-- 账期简称
-- 		credit_limit,temp_credit_limit
-- 		from csx_dim.csx_dim_crm_customer_company_details
-- 		where sdt='current'
-- )e1 on a.credit_code=e1.credit_code and a.company_code=e1.company_code 
 -- 信控公司账期  因底表中已无信控与公司，只能按客户关联
left join 
(
	select distinct customer_code,  -- credit_code,company_code,
		account_period_code,account_period_name		-- 账期编码,账期名称
		from csx_dim.csx_dim_crm_customer_company_details
		where sdt='current'
		and account_period_name='预付货款'
)e1 on a.customer_code=e1.customer_code 
-- 预付款客户
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date between '202308' and '202312'
	and category_second like'%预付款%'
	and adjust_business_type in('日配','全业务')
	)b1 on b1.customer_code=a.customer_code and b1.smt_date=a.smt
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date between '202308' and '202312'
	and category_second like'%预付款%'
	and adjust_business_type in('BBC','全业务')
	)b2 on b2.customer_code=a.customer_code and b2.smt_date=a.smt
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date between '202308' and '202312'
	and category_second like'%预付款%'
	and adjust_business_type in('福利','全业务')
	)b3 on b3.customer_code=a.customer_code and b3.smt_date=a.smt
left join
	(
	select smt_date,customer_code,
		concat(customer_code,effective_period,remark) as dd,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date between '202308' and '202312'
	and (category_second like'%不算提成%'
	-- or category_second like'%服务费%'
	or category_second like'%提成减半%')
	)d on d.customer_code=a.customer_code and d.smt_date=a.smt
-- 目标毛利系数-销售员与客服经理
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_all_tc_all d1 on d1.work_no=a.work_no and d1.smt=a.smt
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_all_tc_all d2 on d2.work_no=a.rp_service_user_work_no and d2.smt=a.smt
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_all_tc_all d3 on d3.work_no=a.fl_service_user_work_no and d3.smt=a.smt
left join csx_analyse_tmp.tmp_tc_person_profit_target_rate_all_tc_all d4 on d4.work_no=a.bbc_service_user_work_no and d4.smt=a.smt
;





select 
from csx_analyse_tmp.tmp_tc_customer_bill_month_dff_rate_detail_year_b





-- 查数 
select smt,province_name,
	sum(tc_sales) as tc_sales,
	sum(tc_rp_service) as tc_rp_service,
	sum(tc_fl_service) as tc_fl_service,
	sum(tc_bbc_service) as tc_bbc_service,
	
	sum(tc_sales_new) as tc_sales_new,
	sum(tc_rp_service_new) as tc_rp_service_new,
	sum(tc_fl_service_new) as tc_fl_service_new,
	sum(tc_bbc_service_new) as tc_bbc_service_new	
from csx_analyse_tmp.tmp_tc_customer_bill_month_dff_rate_detail_year_b
where smt='202308'
group by smt,province_name;




select 
from csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
where smt between '202308' and '202312'




-- 销售员与服务管家提成对比2023年8-12月补发
drop table if exists csx_analyse_tmp.tmp_tc_person_month_dff_b;
create table csx_analyse_tmp.tmp_tc_person_month_dff_b
as
select 
'销售员' as person_flag,
smt,
region_name,
province_name,
city_group_name,
work_no,
sales_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc,
sum(pay_amt) pay_amt,
-- sum(rp_pay_amt) rp_pay_amt,
-- sum(bbc_pay_amt) bbc_pay_amt,
-- sum(bbc_ly_pay_amt) bbc_ly_pay_amt,
-- sum(bbc_zy_pay_amt) bbc_zy_pay_amt,
-- sum(fl_pay_amt) fl_pay_amt,
sum(tc_sales) tc_sales,
sales_profit_basic_new,
sales_profit_new,
sales_profit_target_rate_new,
sales_profit_target_rate_tc_new,
sum(tc_sales_new) tc_sales_new,
sum(tc_sales_new)-sum(tc_sales) as tc_sales_dff_bu  -- 需补发提成
from csx_analyse_tmp.tmp_tc_customer_bill_month_dff_rate_detail_year_b
where smt between '202308' and '202312'
group by 
smt,
region_name,
province_name,
city_group_name,
work_no,
sales_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc,
sales_profit_basic_new,
sales_profit_new,
sales_profit_target_rate_new,
sales_profit_target_rate_tc_new

union all
select 
'服务管家' as person_flag,
smt,
region_name,
province_name,
city_group_name,
sales_user_work_no,
sales_user_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc,
sum(pay_amt) pay_amt,
sum(tc_sales) tc_sales,
sales_profit_basic_new,
sales_profit_new,
sales_profit_target_rate_new,
sales_profit_target_rate_tc_new,
sum(tc_sales_new) tc_sales_new,
sum(tc_sales_new)-sum(tc_sales) as tc_sales_dff_bu  -- 需补发提成
from 
(
select 
smt,
region_name,
province_name,
city_group_name,
rp_service_user_work_no as sales_user_work_no,
rp_service_user_name as sales_user_name,
rp_service_profit_basic as sales_profit_basic,
rp_service_profit_finish as sales_profit_finish,
rp_service_target_rate as sales_target_rate,
rp_service_target_rate_tc as sales_target_rate_tc,
rp_pay_amt as pay_amt,
tc_rp_service as tc_sales,
rp_service_profit_basic_new as sales_profit_basic_new,
rp_service_profit_new as sales_profit_new,
rp_service_profit_target_rate_new as sales_profit_target_rate_new,
rp_service_profit_target_rate_tc_new as sales_profit_target_rate_tc_new,
tc_rp_service_new as tc_sales_new
from csx_analyse_tmp.tmp_tc_customer_bill_month_dff_rate_detail_year_b
where smt between '202308' and '202312'

union all
select 
smt,
region_name,
province_name,
city_group_name,
fl_service_user_work_no as sales_user_work_no,
fl_service_user_name as sales_user_name,
fl_service_profit_basic as sales_profit_basic,
fl_service_profit_finish as sales_profit_finish,
fl_service_target_rate as sales_target_rate,
fl_service_target_rate_tc as sales_target_rate_tc,
fl_pay_amt as pay_amt,
tc_fl_service as tc_sales,
fl_service_profit_basic_new as sales_profit_basic_new,
fl_service_profit_new as sales_profit_new,
fl_service_profit_target_rate_new as sales_profit_target_rate_new,
fl_service_profit_target_rate_tc_new as sales_profit_target_rate_tc_new,
tc_fl_service_new as tc_sales_new
from csx_analyse_tmp.tmp_tc_customer_bill_month_dff_rate_detail_year_b
where smt between '202308' and '202312'

union all
select 
smt,
region_name,
province_name,
city_group_name,
bbc_service_user_work_no as sales_user_work_no,
bbc_service_user_name as sales_user_name,
bbc_service_profit_basic as sales_profit_basic,
bbc_service_profit_finish as sales_profit_finish,
bbc_service_target_rate as sales_target_rate,
bbc_service_target_rate_tc as sales_target_rate_tc,
bbc_pay_amt as pay_amt,
tc_bbc_service as tc_sales,
bbc_service_profit_basic_new as sales_profit_basic_new,
bbc_service_profit_new as sales_profit_new,
bbc_service_profit_target_rate_new as sales_profit_target_rate_new,
bbc_service_profit_target_rate_tc_new as sales_profit_target_rate_tc_new,
tc_bbc_service_new as tc_sales_new
from csx_analyse_tmp.tmp_tc_customer_bill_month_dff_rate_detail_year_b
where smt between '202308' and '202312'
)a
group by 
smt,
region_name,
province_name,
city_group_name,
sales_user_work_no,
sales_user_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc,
sales_profit_basic_new,
sales_profit_new,
sales_profit_target_rate_new,
sales_profit_target_rate_tc_new;




select 
person_flag,
smt,
region_name,
province_name,
city_group_name,
work_no,
sales_name,
sales_profit_basic,
sales_profit_finish,
sales_target_rate,
sales_target_rate_tc,
pay_amt,
tc_sales,
sales_profit_basic_new,
sales_profit_new,
sales_profit_target_rate_new,
sales_profit_target_rate_tc_new,
tc_sales_new,
tc_sales_dff_bu  -- 需补发提成
from csx_analyse_tmp.tmp_tc_person_month_dff_b













