-- ******************************************************************** 
-- @功能描述：
-- @创建者： 饶艳华 
-- @创建者日期：2023-06-16 23:52:56 
-- @修改者日期：
-- @修改人：
-- @修改内容：扣减回款金额调整关联关系，-- 从202406开始出现金额相同，需要组合 ：销售月_打款日期_回款金额
-- 20240814新增调整福利与BBC联营系数
-- 20241220 签呈新增限制最高回款系数
-- ******************************************************************** 



-- 调整am内存
SET
  tez.am.resource.memory.mb = 4096;
-- 调整container内存
SET
  hive.tez.container.size = 8192;
 
-- 目标毛利系数-客户月度毛利
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_profit_month;
-- create temporary table csx_analyse_tmp.tmp_tc_cust_profit_month as
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total;
create  table csx_analyse_tmp.tmp_tc_person_profit_total
as
with tmp_tc_cust_profit_month as 
(select 
	-- b.performance_region_code,b.performance_region_name,
	-- b.performance_province_code,b.performance_province_name,
	-- b.performance_city_code,b.performance_city_name,
	a.customer_no as customer_code,b.customer_name,
	a.smt as smonth,
	c.sales_id,c.work_no,c.sales_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,		
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	-- 各类型销售额
	d.sale_amt,
	d.rp_sale_amt,
	(d.bbc_sale_amt_ly+d.bbc_sale_amt_zy) bbc_sale_amt,
	d.bbc_sale_amt_ly as bbc_ly_sale_amt,
	d.bbc_sale_amt_zy as bbc_zy_sale_amt,
	d.fl_sale_amt,
	-- 各类型定价毛利额
	-- 个人实际毛利额核算时福利及联营bbc业务按照1.2系数上浮
	case when e.flag='1' then d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly*e.adjust_rate+d.fl_profit*e.adjust_rate	
		when a.province_code in ('11','901') then (d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly+d.fl_profit)
		when  a.city_group_code in ('7') then (d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly+d.fl_profit)
		else (d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly*1.2+d.fl_profit*1.2) 
	end as profit,
	d.rp_profit,
	case when e.flag='1' then (d.bbc_profit_ly*e.adjust_rate+d.bbc_profit_zy)
		when a.province_code in ('11','901') then d.bbc_profit_ly+d.bbc_profit_zy
		when  a.city_group_code in ('7') then d.bbc_profit_ly+d.bbc_profit_zy
		else (d.bbc_profit_ly*1.2+d.bbc_profit_zy)
	end as bbc_profit,
	case when e.flag='1' then d.bbc_profit_ly*e.adjust_rate
		when a.province_code in ('11','901') then d.bbc_profit_ly
		when  a.city_group_code in ('7') then	 d.bbc_profit_ly
		else (d.bbc_profit_ly*1.2)
	end  as bbc_ly_profit,
	d.bbc_profit_zy as bbc_zy_profit,
	case when e.flag='1' then d.fl_profit*e.adjust_rate
		when a.province_code in ('11','901') then d.fl_profit
		when  a.city_group_code in ('7') then d.fl_profit
		else (d.fl_profit*1.2)
	end  as fl_profit
from
-- 客户对应的销售员、客服经理	
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi
	-- where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)a
left join  
	(
	-- 本月销售额毛利额 毛利目标达成用签呈后的
	select *
	from csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	-- 算基准毛利额达成系数时，剔除特定福建监狱客户
	and	customer_code not in('105150','105156','105164','105165','105177','105181','105182','106423','106721','107404','119990')
	and customer_code not in(
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second like '%不参与人员基准毛利额达成计算%'	
		)	
	)d on a.customer_no=d.customer_code
left join 
	(
	select 
		distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		performance_region_code,
		performance_region_name,
		performance_province_code,
		performance_province_name,
		performance_city_code,
		performance_city_name,
		-- case when channel_code='9' then '业务代理' end as ywdl_cust,
		case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and customer_type_code=4
	and shipper_code='YHCSX'
	)b on b.customer_code=a.customer_no
left join 
	-- 判断是否调整福利与BBC联营按照系数调整
	(
	select customer_code,
		adjust_rate,
		'1' as flag -- 1代表调整福利与BBC联营
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整福利与BBC联营%'	
	) e on a.customer_no=e.customer_code
-- 关联对应各月销售员
join		
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)c on c.customer_no=a.customer_no	
where b.ng_cust is null
)

-- 毛利额汇总-销售员与客服经理：未处理多管家情况-顿号隔开

select 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	sum(a.sale_amt) as sale_amt, -- 客户总销售额
	sum(a.profit) as profit-- 客户总定价毛利额
from 
(
	select 
		smonth,customer_code,customer_name,
		sales_id,work_no,sales_name,
		sale_amt,profit
	from tmp_tc_cust_profit_month
	where work_no<>''
	union all
	select 
		smonth,customer_code,customer_name,
		rp_service_user_id as sales_id,
		rp_service_user_work_no as work_no,
		rp_service_user_name as sales_name,
		rp_sale_amt as sale_amt,
		rp_profit as profit
	from tmp_tc_cust_profit_month
	where rp_service_user_work_no<>''
	
	union all
	select 
		smonth,customer_code,customer_name,
		fl_service_user_id as sales_id,
		fl_service_user_work_no as work_no,
		fl_service_user_name as sales_name,
		fl_sale_amt as sale_amt,
		fl_profit as profit
	from tmp_tc_cust_profit_month
	where fl_service_user_work_no<>''
	
	union all
	select 
		smonth,customer_code,customer_name,
		bbc_service_user_id as sales_id,
		bbc_service_user_work_no as work_no,
		bbc_service_user_name as sales_name,
		bbc_sale_amt as sale_amt,
		bbc_profit as profit
	from tmp_tc_cust_profit_month
	where bbc_service_user_work_no<>''
)a
group by 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name
	
;
	

-- 毛利额汇总-销售员与客服经理：多管家-拆分到单人
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total_split;
create  table csx_analyse_tmp.tmp_tc_person_profit_total_split
as
select
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	sum(a.sale_amt/a.count_person) sale_amt, -- 客户总销售额
	sum(a.profit/a.count_person) profit,-- 客户总定价毛利额
	d.profit_basic,
	e.begin_date,
	e.begin_less_1year_flag,
	round(sum(a.profit/a.count_person)/d.profit_basic,6) as profit_target_rate		
from
(		
select size(split(sales_id,'、')) as count_person,
	smonth,
	sales_id,work_no,sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))=1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[0] sales_id,
	split(work_no,'、')[0] work_no,
	split(sales_name,'、')[0] sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[1] sales_id,
	split(work_no,'、')[1] work_no,
	split(sales_name,'、')[1] sales_name,		
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[2] sales_id,
	split(work_no,'、')[2] work_no,
	split(sales_name,'、')[2] sales_name,	
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

union all
select size(split(sales_id,'、')) as count_person,
	smonth,
	split(sales_id,'、')[3] sales_id,
	split(work_no,'、')[3] work_no,
	split(sales_name,'、')[3] sales_name,		
	sale_amt, -- 客户总销售额
	profit-- 客户总定价毛利额
from csx_analyse_tmp.tmp_tc_person_profit_total
where size(split(sales_id,'、'))>1

-- 手工调整：调整业务员基准毛利额
-- 202402签呈：124484 业务代理人毛利额归属到於佳作为考核其毛利额是否达标的标准：28052.88
-- 202402签呈：128865 业务代理人毛利额归属到於佳作为考核其毛利额是否达标的标准：67249.86
-- 202402签呈：128865 业务代理人毛利额归属到於佳作为考核其毛利额是否达标的标准：-220.5
-- 202402签呈：128336 业务代理人毛利额归属到刘斌作为考核其毛利额是否达标的标准：12588.17
-- 202403签呈：225582 202403月管家董娟毛利额达成考核：增加3166.11元
		
-- 计划删除 20250926
-- union all
-- select 1 as count_person,
-- 	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smonth,
-- 	'1000000211077' as  sales_id,
-- 	'80946479' as work_no,
-- 	'於佳' as sales_name,		
-- 	0 sale_amt, -- 客户总销售额
-- 	if(substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)='202402',95082.24,0) as profit-- 客户总定价毛利额
	
-- union all
-- select 1 as count_person,
-- 	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smonth,
-- 	'1000000575185' as  sales_id,
-- 	'81160930' as work_no,
-- 	'刘斌' as sales_name,		
-- 	0 sale_amt, -- 客户总销售额
-- 	if(substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)='202402',12588.17,0) as profit-- 客户总定价毛利额	

-- union all
-- select 1 as count_person,
-- 	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smonth,
-- 	'1000000425506' as  sales_id,
-- 	'80970038' as work_no,
-- 	'董娟' as sales_name,		
-- 	0 sale_amt, -- 客户总销售额
-- 	if(substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)='202403',3166.11,0) as profit-- 客户总定价毛利额		
)a 
left join 
-- 基准毛利率目标
	(	select *
		from csx_analyse.csx_analyse_tc_sales_service_profit_basic_mf
		where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
		and smt_c=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	)	d on d.work_no=a.work_no 
		and d.smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	left join 
	(
	select employee_code,employee_status,begin_date,
	case when begin_date>=regexp_replace(trunc(add_months('${sdt_yes_date}',-12),'MM'),'-','') then '是' else '否' end as begin_less_1year_flag
	from csx_dim.csx_dim_basic_employee
	where sdt='current'
	-- and employee_status=0
	and card_type='0'
	) e on a.work_no=e.employee_code		
where coalesce(sales_id,'')<>''
group by 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name,
	d.profit_basic,
	e.begin_date,
	e.begin_less_1year_flag	
;

-- 创建人员信息表，获取销售员和客服经理的城市，因为存在一个业务员名下客户跨城市的情况
drop table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info;
create  table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info
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
	and delete_flag = '0'
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
	
	
	
-- 目标毛利系数-销售员与客服经理
-- 过度期内入职时间超过1年的个人基准毛利率低于100%的按照实际率核算; 湖北8月过渡期，9月生效；其他省区都是8-10月过渡期，11月生效
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc;
create  table csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc
as
-- 手工调整：调整业务员基准毛利率达成
-- 202310签呈：销售员詹成宏81147160不考核毛利额达成率 每月
-- 202311签呈：81131450 张珠妹、81133021 黄升、80952742 王秀云、80010438 林秀丽 不考核毛利额达成率 每月

-- 202311签呈：81090419 葛立新   2023年8月1日从湖北调动至合肥，申请保护期一年，2023年8月1日至2024年7月31日
-- 202311签呈：81014012 张鲲     2023年11月毛利额系数100%
-- 202311签呈：81048704 关毓萱   2023年11月毛利额系数100%
-- 202311签呈：80892167 李烁岚   2023年11-12月毛利额系数100%

-- 202312签呈："服务管家冯子玲（81089289）于 2023 年 11 月从运营部转岗到服务管家团队，按实际达成发放员工提成 2023.11-2024.10"
-- 202312签呈：81022821 杨程销售岗不考核基准毛利率
-- 202312签呈：80880757 张标华销售岗不考核基准毛利率
-- 202312签呈：81190209 张敏销售岗不考核基准毛利率
-- 202312签呈：80897025 耿海娜销售岗不考核基准毛利率
-- 202312签呈：81214655 贾莉莉服务管家不考核基准毛利率
-- 202312签呈：81125195 常文棋服务管家不考核基准毛利率

-- 202402签呈：服务管家周凤云（工号：80894299）为省区应收对账专员转岗到服务管家团队人员，转岗时间为：2023年9月1日，过渡期间未达成毛利额指标的按照实际达成率进行提成核算，现申请予以执行，2024年9月1日之后如果未达成，按照方案执行
-- 202402签呈：服务管家陈燕红（工号：80981355），2023年9月1日转岗为销售员,自2023年9月1日至 2024年8月31日期间陈燕红个人基准毛利率达成低于 100%的按照实际达成结果作为达成系数计算其月度提成
-- 202402签呈：BP岗李芬娟（工号：80948173），2023年11月1日起转岗为服务管家,自2023年11月至 2024年10月个人基准毛利率达成低于 100%的按照实际达成系数计算其月度提成
-- 202402签呈：产品运营张楠林（工号：81225749），2024年1月1日起转岗为服务管家,自2024年1月至 2024年12月个人基准毛利率达成低于 100%的按照实际达成系数计算其月度提成

-- 202403签呈：服务管家徐科（工号：80716714）转岗时间为：2023年11月1日，过渡期（202311-202410），过渡期间未达成毛利额指标的按照实际达成率进行提成核算（最高100%）
-- 202403签呈：接单员郦凌琳（工号：81029025），2023年9月1日起转岗为服务管家；过渡期（202309-202408），过渡期间未达成毛利额指标的按照实际达成率进行提成核算（最高100%）
-- 202403签呈：每月处理 南京签呈刘连兵（工号：80980890）不考核毛利额系数，按100%计算，江苏盐城都按实际达成计算提成

-- 202404签呈：每月处理江苏南京：80880757	张标华	不考核毛利额达成系数，正常按100%计算
-- 202404签呈：每月处理江苏南京：81125195	常文棋	不考核毛利额达成系数，正常按100%计算
-- 202404签呈：每月处理江苏南京：81022821	杨程	不考核毛利额达成系数，正常按100%计算
-- 202404签呈：每月处理江苏南京：80897025	耿海娜	不考核毛利额达成系数，正常按100%计算
-- 202404签呈：每月处理江苏南京：80980890	刘连兵	不考核毛利额达成系数，正常按100%计算
-- 202404签呈：每月处理江苏南京：81214655	贾莉莉	不考核毛利额达成系数，正常按100%计算
-- 202404签呈：每月处理江苏南京：81245177	汤慧娴	不考核毛利额达成系数，正常按100%计算
-- 202506签呈： 81196948江西服务管家给与一年保护期，毛利额达成系数按实际计算提成，202506-202605本月开始生的

select *,
	case 
		when a.work_no in('81147160','81131450','81133021','80952742','80010438') then 1
		when a.work_no in('81022821','80880757','81190209','80897025','81214655','81125195') then 1

		when a.work_no in('81196948') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) between '202506' and '202605' then if(profit_target_rate<1,profit_target_rate,1)
		
		when a.work_no in('81245177') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)>='202404' then 1
		
		when b.performance_city_name in('江苏盐城') and (substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)<='202310' 
			or substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)>='202403') then if(profit_target_rate<1,profit_target_rate,1)
		when b.performance_province_name in('湖北省') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)<='202308' then if(profit_target_rate<1,profit_target_rate,1)
		when b.performance_province_name not in('湖北省') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)<='202310' then if(profit_target_rate<1,profit_target_rate,1)
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
		a.smonth,
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
		a.smonth,
		a.sales_id,a.work_no,a.sales_name,
		a.sale_amt,
		a.profit,		
		b1.sale_amt as sale_amt_1,
		b1.profit as profit_1,
		b1.profit_basic as profit_basic_1,
		b1.profit_target_rate as profit_target_rate_1,
		b1.begin_date as begin_date_1,
		b1.begin_less_1year_flag as begin_less_1year_flag_1,
	
		b2.sale_amt as sale_amt_2,
		b2.profit as profit_2,
		b2.profit_basic as profit_basic_2,
		b2.profit_target_rate as profit_target_rate_2,	
		b2.begin_date as begin_date_2,
		b2.begin_less_1year_flag as begin_less_1year_flag_2,
		
		b3.sale_amt as sale_amt_3,
		b3.profit as profit_3,
		b3.profit_basic as profit_basic_3,
		b3.profit_target_rate as profit_target_rate_3,	
		b3.begin_date as begin_date_3,
		b3.begin_less_1year_flag as begin_less_1year_flag_3,
		
		b4.sale_amt as sale_amt_4,
		b4.profit as profit_4,
		b4.profit_basic as profit_basic_4,
		b4.profit_target_rate as profit_target_rate_4,	
		b4.begin_date as begin_date_4,
		b4.begin_less_1year_flag as begin_less_1year_flag_4,	
		-- 多个管家的毛利额达成排序
		sort_array(array(b1.profit_target_rate,b2.profit_target_rate,b3.profit_target_rate,b4.profit_target_rate)) as arr
	from csx_analyse_tmp.tmp_tc_person_profit_total a 
	left join csx_analyse_tmp.tmp_tc_person_profit_total_split b1 on split(a.sales_id,'、')[0]=b1.sales_id
	left join csx_analyse_tmp.tmp_tc_person_profit_total_split b2 on split(a.sales_id,'、')[1]=b2.sales_id
	left join csx_analyse_tmp.tmp_tc_person_profit_total_split b3 on split(a.sales_id,'、')[2]=b3.sales_id
	left join csx_analyse_tmp.tmp_tc_person_profit_total_split b4 on split(a.sales_id,'、')[3]=b4.sales_id
	)a
)a
left join csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info b on split(a.sales_id,'、')[0] =b.user_id;




-- 销售月金额与毛利率
drop table if exists csx_analyse_tmp.tmp_tc_customer_sale_profit_ls;
create  table csx_analyse_tmp.tmp_tc_customer_sale_profit_ls
as
select 
customer_code,smonth,
	-- 销售额
	sale_amt, -- 客户总销售额
	rp_sale_amt, -- 客户日配销售额		
	-- sum(bbc_sale_amt) as bbc_sale_amt, -- 客户bbc销售额
	bbc_sale_amt_zy, -- 客户bbc自营销售额
	bbc_sale_amt_ly, -- 客户bbc联营销售额
	fl_sale_amt, -- 客户福利销售额
	-- 定价毛利额
	profit,-- 客户总定价毛利额
	rp_profit,-- 客户日配定价毛利额
	-- sum(bbc_profit) as bbc_profit,-- 客户bbc定价毛利额
	bbc_profit_zy, -- 客户bbc自营定价毛利额
	bbc_profit_ly, -- 客户bbc联营定价毛利额
	fl_profit,  -- 客户福利定价毛利额
	-- 定价毛利率
	profit/abs(sale_amt) as prorate,-- 客户总定价毛利率
	rp_profit/abs(rp_sale_amt) as rp_prorate,-- 客户日配定价毛利率
	bbc_profit_zy/abs(bbc_sale_amt_zy) as bbc_prorate_zy, -- 客户bbc自营定价毛利率
	bbc_profit_ly/abs(bbc_sale_amt_ly) as bbc_prorate_ly, -- 客户bbc联营定价毛利率
	fl_profit/abs(fl_sale_amt) as fl_prorate  -- 客户福利定价毛利率	
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
		sum(case when business_type_code in('2','10') then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit, 
		sum(case when business_type_code in ('1','4','5') then profit else 0 end) as rp_profit,
		-- sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,
		sum(case when business_type_code in('6') and (operation_mode_code=0 or operation_mode_code is null) then profit else 0 end) as bbc_profit_zy,
		sum(case when business_type_code in('6') and operation_mode_code=1 then profit else 0 end) as bbc_profit_ly,		
		sum(case when business_type_code in('2','10') then profit else 0 end) as fl_profit
	from csx_dws.csx_dws_sale_detail_di a
	where sdt>='20220601' 
	    and shipper_code='YHCSX'
		and sdt<regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
		and channel_code in('1','7','9')
		and order_channel_detail_code not in ('24','28')  -- 剔除永辉生活、永辉线上
		and goods_code not in ('8718','8708','8649','840509') -- 202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
		and ( (a.business_type_code in ('1','2','6','10')  and a.partner_type_code not in (1, 3))
			or (business_type_code in('2','5') and performance_province_name = '平台-B') -- 平台酒水
			-- 福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
			-- 福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
			-- 202210签呈 北京 129026 129000 城市服务商业务销售额*0.2% 不计算毛利提成 每月
			-- 202306签呈 福建省'229290','175709','125092' 项目供应商 纳入大客户提成计算
			or (partner_type_code in (1, 3)  and customer_code in ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))
			-- 202310签呈 河北南京部分项目供应商客户纳入大客户提成计算
			-- or (partner_type_code in (1, 3)  and customer_code in ('235949','222853','131428','131466','131202','131208','129746','128435','230788',
			-- '112846','118357','115832','125795','125831','131462','114496','117322','131421','118395','114470','130024','130430','118644','131091',
			-- '217946','129955','130226','120115','226821','129870','129865','130269','126125','129674','129880','227563','129855','129860','130955',
			-- '127521','225541','232102','233354','234828','130844','223112','129854','125545','128705','125513','126001'))
			or (partner_type_code in (1, 3)  and customer_code in (
				select customer_code
				from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
				where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and (category_second like '%纳入大客户提成计算：项目供应商%' or category_second like '%纳入大客户提成计算：前置仓%')
			))
			or customer_code in (
				select customer_code
				from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
				where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
				and category_second='纳入大客户提成计算'
			)				
			)		
		-- and performance_province_name in ('福建省')
		and (performance_province_name !='福建省' or (performance_province_name='福建省' and inventory_dc_name not like '%V2DC%')) -- 2.0 按仓库名称判断					
	group by customer_code,substr(sdt,1,6)
)a ;





-- 签呈处理：部分订单按实际回款时间系数计算（给预付款标签）
drop table if exists csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2;
create  table csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2
as
select a.*,
	case 
		 -- 按实际回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e.date_star and e.date_end) and e.category_second is not null 
		 and (substr(e.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e.adjust_business_type='全业务') then '是' 
		 -- 按实际回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f.date_star and f.date_end) and f.category_second is not null 
		 and (substr(f.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f.adjust_business_type='全业务') then '是' 

		 -- 指定回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e2.date_star and e2.date_end) and e2.category_second is not null 
		 and (substr(e2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e2.adjust_business_type='全业务') then '是' 
		 -- 指定回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f2.date_star and f2.date_end) and f2.category_second is not null 
		 and (substr(f2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f2.adjust_business_type='全业务') then '是' 		 
		 else '否' end yufu_flag,

	case 
		 -- 指定回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e2.date_star and e2.date_end) and e2.category_second is not null 
		 and (substr(e2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e2.adjust_business_type='全业务') then e2.hk_dff_rate 
		 -- 指定回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f2.date_star and f2.date_end) and f2.category_second is not null 
		 and (substr(f2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f2.adjust_business_type='全业务') then f2.hk_dff_rate 
		 -- 指定客户最高系数
		when ( (f3.adjust_business_type=a.business_type_name or f3.adjust_business_type='全业务' or f3.adjust_business_type=substr(a.business_type_name,1,2) )
		        and f3.adju_flag=1	and dff_rate>=1 ) then f3.hk_dff_rate
		 else dff_rate end dff_rate_new
from
(
	select *
	from csx_analyse.csx_analyse_fr_tc_customer_credit_order_detail
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and status=1
)a
-- 部分订单按实际回款时间系数
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按实际回款时间系数-发生日期%'		
	)e on a.customer_code=e.customer_code 
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按实际回款时间系数-打款日期%'		
	)f on a.customer_code=f.customer_code	
-- 部分订单按指定回款时间系数	
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按指定回款时间系数-发生日期%'		
	)e2 on a.customer_code=e2.customer_code 
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按指定回款时间系数-打款日期%'		
	)f2 on a.customer_code=f2.customer_code	
left join 
-- 限制回款最高系数
	(
	select customer_code,
		smt_date as smonth,
		category_first,
		category_second,
		adjust_business_type,
		date_star,
		date_end,
		'1' adju_flag,
		cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整回款最高系数%'		
	)f3 on a.customer_code=f3.customer_code	
;	
	

	

-- 客户+结算月+回款时间系数：各业务类型毛利率提成比例
drop table if exists csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc;
create  table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc
as
select a.*,

	coalesce(e.rp_rate,
	case when rp_profit_rate<0.08 then 0.002
		when rp_profit_rate>=0.08 and rp_profit_rate<0.12 then 0.005
		when rp_profit_rate>=0.12 and rp_profit_rate<0.16 then 0.007
		when rp_profit_rate>=0.16 and rp_profit_rate<0.2 then 0.009
		when rp_profit_rate>=0.2 and rp_profit_rate<0.25 then 0.013
		when rp_profit_rate>=0.25 then 0.015
		else 0.002 end) as cust_rp_profit_rate_tc,

	coalesce(e.bbc_rate_zy,
	case when bbc_zy_profit_rate<0.08 then 0.002
		when bbc_zy_profit_rate>=0.08 and bbc_zy_profit_rate<0.12 then 0.005
		when bbc_zy_profit_rate>=0.12 and bbc_zy_profit_rate<0.16 then 0.007
		when bbc_zy_profit_rate>=0.16 and bbc_zy_profit_rate<0.2 then 0.009
		when bbc_zy_profit_rate>=0.2 and bbc_zy_profit_rate<0.25 then 0.013
		when bbc_zy_profit_rate>=0.25 then 0.015
		else 0.002 end) as cust_bbc_zy_profit_rate_tc,		
		
	coalesce(e.bbc_rate_ly,
	case when bbc_ly_profit_rate<0.03 then 0.002
		when bbc_ly_profit_rate>=0.03 and bbc_ly_profit_rate<0.07 then 0.0035
		when bbc_ly_profit_rate>=0.07 and bbc_ly_profit_rate<0.1 then 0.0045
		when bbc_ly_profit_rate>=0.1 and bbc_ly_profit_rate<0.13 then 0.0065
		when bbc_ly_profit_rate>=0.13 and bbc_ly_profit_rate<0.17 then 0.0095
		when bbc_ly_profit_rate>=0.17 and bbc_ly_profit_rate<0.23 then 0.013
		when bbc_ly_profit_rate>=0.23 then 0.015
		else 0.002 end) as cust_bbc_ly_profit_rate_tc,

	coalesce(e.fl_rate,
	case when fl_profit_rate<0.03 then 0.002
		when fl_profit_rate>=0.03 and fl_profit_rate<0.07 then 0.0035
		when fl_profit_rate>=0.07 and fl_profit_rate<0.1 then 0.0045
		when fl_profit_rate>=0.1 and fl_profit_rate<0.13 then 0.0065
		when fl_profit_rate>=0.13 and fl_profit_rate<0.17 then 0.0095
		when fl_profit_rate>=0.17 and fl_profit_rate<0.23 then 0.013
		when fl_profit_rate>=0.23 then 0.015
		else 0.002 end) as cust_fl_profit_rate_tc		

from 
(
select a.*,
	-- profit/abs(sale_amt) as profit_rate,
	-- rp_profit/abs(rp_sale_amt) as rp_profit_rate,
	-- bbc_profit/abs(bbc_sale_amt) as bbc_profit_rate,
	-- bbc_ly_profit/abs(bbc_ly_sale_amt) as bbc_ly_profit_rate,
	-- bbc_zy_profit/abs(bbc_zy_sale_amt) as bbc_zy_profit_rate,
	-- fl_profit/abs(fl_sale_amt) as fl_profit_rate
	if(b.sale_amt is not null,b.prorate,b2.prorate) as profit_rate,
	if(b.sale_amt is not null,b.rp_prorate,b2.rp_prorate) as rp_profit_rate,
	if(b.sale_amt is not null,(b.bbc_profit_zy+b.bbc_profit_ly)/abs(b.bbc_sale_amt_zy+b.bbc_sale_amt_ly),
		(b2.bbc_profit_zy+b2.bbc_profit_ly)/abs(b2.bbc_sale_amt_zy+b2.bbc_sale_amt_ly))as bbc_profit_rate,
	if(b.sale_amt is not null,b.bbc_prorate_zy,b2.bbc_prorate_zy) as bbc_zy_profit_rate,
	if(b.sale_amt is not null,b.bbc_prorate_ly,b2.bbc_prorate_ly) as bbc_ly_profit_rate,	
	if(b.sale_amt is not null,b.fl_prorate,b2.fl_prorate) as fl_profit_rate,

	-- 历史月销售额
	if(b.sale_amt is not null,b.sale_amt,b2.sale_amt) as sale_amt_real,
	if(b.sale_amt is not null,b.rp_sale_amt,b2.rp_sale_amt) as rp_sale_amt_real,
	if(b.sale_amt is not null,b.bbc_sale_amt_zy,b2.bbc_sale_amt_zy) as bbc_sale_amt_zy_real,
	if(b.sale_amt is not null,b.bbc_sale_amt_ly,b2.bbc_sale_amt_ly) as bbc_sale_amt_ly_real,
	if(b.sale_amt is not null,b.fl_sale_amt,b2.fl_sale_amt) as fl_sale_amt_real,	
	
	-- 服务费
	-- b.service_falg,
	-- b.service_fee,
	
	b.service_falg as service_falg,
    b.service_fee as service_fee,
	-- 本月销售额毛利额
	c.sale_amt as by_sale_amt,
	c.rp_sale_amt as by_rp_sale_amt,
	c.bbc_sale_amt_zy as by_bbc_sale_amt_zy,
	c.bbc_sale_amt_ly as by_bbc_sale_amt_ly,
	c.fl_sale_amt as by_fl_sale_amt,
	c.profit as by_profit,
	c.rp_profit as by_rp_profit,
	c.bbc_profit_zy as by_bbc_profit_zy,
	c.bbc_profit_ly as by_bbc_profit_ly,
	c.fl_profit as by_fl_profit
	
from 
(
	select
		region_code,
		region_name,
		province_code,
		province_name,
		city_group_code,
		city_group_name,
		customer_code,	-- 客户编码
		customer_name,
		credit_code,	-- 信控号	
		company_code,	-- 签约公司编码
		account_period_code,	-- 账期编码
		account_period_name,	-- 账期名称		
		sales_id,
		work_no,
		sales_name,
		rp_service_user_id,
		rp_service_user_work_no,
		rp_service_user_name,
		fl_service_user_id,
		fl_service_user_work_no,
		fl_service_user_name,		
		bbc_service_user_id,
		bbc_service_user_work_no,
		bbc_service_user_name,
		-- 提成分配系数
		rp_sales_fp_rate,
		fl_sales_sale_fp_rate as fl_sales_fp_rate,
		bbc_sales_sale_fp_rate as bbc_sales_fp_rate,
		rp_service_user_fp_rate,
		fl_service_user_fp_rate,
		bbc_service_user_fp_rate,	
		substr(regexp_replace(bill_date,'-',''),1,6) as bill_month, -- 结算月
		bill_date,  -- 结算日期
		paid_date,  -- 核销日期（打款日期）
		yufu_flag,
		-- '否' as yufu_flag,
		-- if(paid_date<happen_date,'是','否') as yufu_flag,
		substr(regexp_replace(happen_date,'-',''),1,6) as happen_month, -- 销售月		
		-- 202308签呈 126275 将销售日期为6.15-8.15期间的BBC，结算日调整为8.16，且最高回款系数100%
		-- 20241220 签呈新增限制最高回款系数
		case when a.customer_code='126275' and dff_rate_new>1 then 1
			else dff_rate_new end as dff_rate,  -- 回款时间系数
		sum(pay_amt) pay_amt,	-- 核销金额
		sum(case when business_type_code in (1,4,5) then pay_amt else 0 end) as rp_pay_amt,
		sum(case when business_type_name like 'BBC%' then pay_amt else 0 end) as bbc_pay_amt,
		sum(case when business_type_name='BBC联营' then pay_amt else 0 end) as bbc_ly_pay_amt,
		sum(case when business_type_name='BBC自营' then pay_amt else 0 end) as bbc_zy_pay_amt,
		sum(case when business_type_code in(2,10) then pay_amt else 0 end) as fl_pay_amt,
		
		-- 各类型销售额
		sum(sale_amt) as sale_amt,
		sum(case when business_type_code in (1,4,5) then sale_amt else 0 end) as rp_sale_amt,
		sum(case when business_type_name like 'BBC%' then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_name='BBC联营' then sale_amt else 0 end) as bbc_ly_sale_amt,
		sum(case when business_type_name='BBC自营' then sale_amt else 0 end) as bbc_zy_sale_amt,
		sum(case when business_type_code in(2,10) then sale_amt else 0 end) as fl_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit,
		sum(case when business_type_code in (1,4,5) then profit else 0 end) as rp_profit,
		sum(case when business_type_name like 'BBC%' then profit else 0 end) as bbc_profit,
		sum(case when business_type_name='BBC联营' then profit else 0 end) as bbc_ly_profit,
		sum(case when business_type_name='BBC自营' then profit else 0 end) as bbc_zy_profit,
		sum(case when business_type_code in(2,10) then profit else 0 end) as fl_profit	
	from csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2 a 

	group by 	region_code,
		region_name,
		province_code,
		province_name,
		city_group_code,
		city_group_name,
		a.customer_code,	-- 客户编码
		customer_name,
		credit_code,	-- 信控号	
		company_code,	-- 签约公司编码
		account_period_code,	-- 账期编码
		account_period_name,	-- 账期名称			
		sales_id,
		work_no,
		sales_name,
		rp_service_user_id,
		rp_service_user_work_no,
		rp_service_user_name,
		fl_service_user_id,
		fl_service_user_work_no,
		fl_service_user_name,		
		bbc_service_user_id,
		bbc_service_user_work_no,
		bbc_service_user_name,
		-- 提成分配系数
		rp_sales_fp_rate,
		fl_sales_sale_fp_rate,
		bbc_sales_sale_fp_rate,
		rp_service_user_fp_rate,
		fl_service_user_fp_rate,
		bbc_service_user_fp_rate,
		substr(regexp_replace(bill_date,'-',''),1,6), -- 结算月
		bill_date,	
		paid_date,
		yufu_flag,
		-- if(paid_date<happen_date,'是','否'),
		substr(regexp_replace(happen_date,'-',''),1,6),  -- 销售月
		case when a.customer_code='126275' and dff_rate_new>1 then 1
			else dff_rate_new end   -- 回款时间系数
)a	
left join csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business b on a.customer_code=b.customer_code and a.happen_month=b.smonth
left join csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business c on a.customer_code=c.customer_code and c.smonth=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
-- 因某客户可能后期纳入大客户提成计算，无历史处理签呈后的毛利率，若没有历史月的毛利率 则取最新计算的历史月毛利率
left join csx_analyse_tmp.tmp_tc_customer_sale_profit_ls b2 on a.customer_code=b2.customer_code and a.happen_month=b2.smonth

)a	
left join
	(
	select smt_date,customer_code,category_second,
		max(case when adjust_business_type in('日配','全业务') then back_amt_tc_rate end) as rp_rate,
		max(case when adjust_business_type in('BBC','BBC自营','全业务') then back_amt_tc_rate end) as bbc_rate_zy,
		max(case when adjust_business_type in('BBC','BBC联营','全业务') then back_amt_tc_rate end) as bbc_rate_ly,
		max(case when adjust_business_type in('福利','全业务') then back_amt_tc_rate end) as fl_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%调整提成比例%'
	group by smt_date,customer_code,category_second
	) e on e.customer_code=a.customer_code
;	



-- 签呈处理：扣减回款金额
drop table if exists csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1;
create  table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1
as
select
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号	
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称		
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	a.bill_month, -- 结算月
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.yufu_flag,
	a.happen_month, -- 销售月		
	a.dff_rate,  -- 回款时间系数
	a.pay_amt+nvl(b.adjust_amt,0) as pay_amt,	-- 核销金额
	a.rp_pay_amt+nvl(b.rp_adjust_amt,0) as rp_pay_amt,
	a.bbc_pay_amt+nvl(b.bbc_adjust_amt_zy,0)+nvl(b.bbc_adjust_amt_ly,0) as bbc_pay_amt,
	a.bbc_ly_pay_amt+nvl(b.bbc_adjust_amt_ly,0) as bbc_ly_pay_amt,
	a.bbc_zy_pay_amt+nvl(b.bbc_adjust_amt_zy,0) as bbc_zy_pay_amt,
	a.fl_pay_amt+nvl(b.fl_adjust_amt,0) as fl_pay_amt,

	
	-- 各类型销售额
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt,
	a.bbc_ly_sale_amt,
	a.bbc_zy_sale_amt,
	a.fl_sale_amt,
	-- 各类型定价毛利额
	a.profit,
	a.rp_profit,
	a.bbc_profit,
	a.bbc_ly_profit,
	a.bbc_zy_profit,
	a.fl_profit,	

	a.profit_rate,
	a.rp_profit_rate,
	a.bbc_profit_rate,
	a.bbc_zy_profit_rate,
	a.bbc_ly_profit_rate,	
	a.fl_profit_rate,

	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,
	
	a.cust_rp_profit_rate_tc,
	a.cust_bbc_zy_profit_rate_tc,			
	a.cust_bbc_ly_profit_rate_tc,
	a.cust_fl_profit_rate_tc	
from 
(
	select *,
		-- 从202406开始出现金额相同，需要组合 ：销售月_打款日期_回款金 --202406之前：销售月_回款金额
	-- 销售月_回款金额
	concat(happen_month,'_',cast(pay_amt as decimal(26,2))) as happen_month_pay_amt,
	-- 销售月_打款日期_回款金额,当出现销售月与金额重复里，需要调整代码销售月_打款日期_回款金  202406 202406_202406_0000
	concat(happen_month,'_',regexp_replace(paid_date,'-',''),'_',cast(pay_amt as decimal(26,2))) as happen_paid_month_pay_amt
	from csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc
)a 
left join
(
  select customer_code,
	smt_date as smonth,
	split(remark,'：')[1] as happen_month_pay_amt,  --扣减回款金额：销售月_打款日期_回款金额
	0-adjust_amount as adjust_amt,
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_adjust_amt, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_adjust_amt_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_adjust_amt_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_adjust_amt
  from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减回款金额%'	
)b on a.customer_code=b.customer_code 
	-- 从202406开始出现金额相同，需要组合 ：销售月_打款日期_回款金额
	and  if(a.bill_month<'202406',a.happen_month_pay_amt,happen_paid_month_pay_amt)=b.happen_month_pay_amt 
    -- and  a.happen_month_pay_amt =b.happen_month_pay_amt 
;



DROP TABLE IF EXISTS csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0;

CREATE TABLE csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0
AS
WITH base_data AS (
    SELECT 
        a.*,
        IF(ABS(a.pay_amt) < ABS(a.sale_amt), a.pay_amt, a.sale_amt) AS pay_amt_use,
        d1.profit_basic AS sales_profit_basic,
        d1.profit AS sales_profit_finish,
        d1.profit_target_rate AS sales_target_rate,
        d1.profit_target_rate_tc AS sales_target_rate_tc,
        d2.profit_basic AS rp_service_profit_basic,
        d2.profit AS rp_service_profit_finish,
        d2.profit_target_rate AS rp_service_target_rate,
        d2.profit_target_rate_tc AS rp_service_target_rate_tc,
        d3.profit_basic AS fl_service_profit_basic,
        d3.profit AS fl_service_profit_finish,
        d3.profit_target_rate AS fl_service_target_rate,
        d3.profit_target_rate_tc AS fl_service_target_rate_tc,
        d4.profit_basic AS bbc_service_profit_basic,
        d4.profit AS bbc_service_profit_finish,
        d4.profit_target_rate AS bbc_service_target_rate,
        d4.profit_target_rate_tc AS bbc_service_target_rate_tc,
        CASE 
            WHEN f.category_second <> '' THEN f.adjust_rate 
            WHEN j.renew_flag = '1' AND a.happen_month >= '202504' and happen_month>=j.smt  THEN j.renew_cust_rate
            WHEN e.new_cust_flag = '1' THEN new_cust_rate
            ELSE 1
        END AS new_cust_rate
    FROM csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1 a 
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d1 ON d1.work_no = a.work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d2 ON d2.work_no = a.rp_service_user_work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d3 ON d3.work_no = a.fl_service_user_work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d4 ON d4.work_no = a.bbc_service_user_work_no
    LEFT JOIN (
        SELECT DISTINCT customer_code, 
            if(first_sale_month<'202504' ,1.2,1.0) AS new_cust_rate, 
            '1' new_cust_flag
        FROM (
            SELECT customer_code, substr(MIN(first_business_sale_date), 1, 6) first_sale_month
            FROM csx_dws.csx_dws_crm_customer_business_active_di
            WHERE sdt = 'current' 
              AND business_type_code = '1'
              AND shipper_code = 'YHCSX'
            GROUP BY customer_code
        ) a
        WHERE first_sale_month >= substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -12)), '-', ''), 1, 6)
          AND first_sale_month >= '202308'
    ) e ON a.customer_code = e.customer_code
    LEFT JOIN (
        SELECT smt_date, customer_code, category_second, adjust_rate
        FROM csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
        WHERE smt = substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)), '-', ''), 1, 6)
          AND smt_date = substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)), '-', ''), 1, 6)
          AND category_second LIKE '%调整日配新客系数%'
    ) f ON f.customer_code = a.customer_code
    LEFT JOIN (
    -- 老客续签0.8系数,按照终止日期取最早一条
    select * from (
        SELECT customer_code, 
            0.8 AS renew_cust_rate, 
            '1' AS renew_flag ,
            smt,
            row_number()over(partition by customer_code order by smt asc  ) rn
        FROM csx_analyse.csx_analyse_tc_renew_customer_df
        WHERE smt >= '202504'
          AND htzzrq >= last_day(add_months('${sdt_yes_date}', -1))
          AND row_rn = 1
        GROUP BY customer_code,
            smt
    ) a where rn=1
     ) j ON j.customer_code = a.customer_code
),
calculated_data AS (
    SELECT 
        base.*,
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_sales_fp_rate) * new_cust_rate * coalesce(sales_target_rate_tc, 1) AS tc_sales_rp,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_bbc_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_bbc_ly,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_fl,
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_service_user_fp_rate) * coalesce(rp_service_target_rate_tc, 1) AS tc_rp_service,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_service_user_fp_rate) * coalesce(fl_service_target_rate_tc, 1) AS tc_fl_service,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service_ly,
        ((bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) +
         (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate)) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service,
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_sales_fp_rate) * new_cust_rate AS original_tc_sales_rp,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_sales_fp_rate) AS original_tc_sales_bbc_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_sales_fp_rate) AS original_tc_sales_bbc_ly,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_sales_fp_rate) AS original_tc_sales_fl,
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_service_user_fp_rate) AS original_tc_rp_service,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_service_user_fp_rate) AS original_tc_fl_service,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) AS original_tc_bbc_service_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) AS original_tc_bbc_service_ly,
        ((bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) +
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate)) AS original_tc_bbc_service
    FROM base_data base
)
SELECT 
    *,
    (original_tc_sales_rp + original_tc_sales_bbc_zy + original_tc_sales_bbc_ly + original_tc_sales_fl) AS original_tc_sales,
    (tc_sales_rp + tc_sales_bbc_zy + tc_sales_bbc_ly + tc_sales_fl) AS tc_sales
FROM calculated_data
 ;
 



drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1;
create  table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1
as
select 
	-- concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.region_code,a.customer_code,a.bill_month,cast(a.dff_rate as string)) biz_id,
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号	
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称		
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month, -- 结算月
	cast(a.dff_rate as decimal(20,6)) dff_rate,		
	a.pay_amt,	-- 核销金额
	a.rp_pay_amt,
	a.bbc_pay_amt,
	a.bbc_ly_pay_amt,
	a.bbc_zy_pay_amt,
	a.fl_pay_amt,		
	
	-- 各类型销售额
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt,
	a.bbc_ly_sale_amt,
	a.bbc_zy_sale_amt,
	a.fl_sale_amt,
	-- 各类型定价毛利额
	a.profit,
	a.rp_profit,
	a.bbc_profit,
	a.bbc_ly_profit,
	a.bbc_zy_profit,
	a.fl_profit,
	a.profit_rate,
	a.rp_profit_rate,
	a.bbc_profit_rate,
	a.bbc_ly_profit_rate,
	a.bbc_zy_profit_rate,
	a.fl_profit_rate,
	
	coalesce(a.cust_rp_profit_rate_tc,0.002) as cust_rp_profit_rate_tc, 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
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
	
	-- 若系统账期为预付货款，则按原回款时间系数
	-- 若是预付款客户，打款日期小于上月1号则，按原回款时间系数但最高100%
	-- 若打款日期小于上月1号则提成为0，若为服务费则=当月回款额/当月销售额*服务费标准*回款系数
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,coalesce(a.tc_sales_rp,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.tc_sales_bbc_zy,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.tc_sales_bbc_ly,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,coalesce(a.tc_sales_fl,0))
				))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_sales,
	
	-- 取消打款
	-- if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_rp_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_rp_service,		


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_fl_service))
		)*if(d.category_second like'%提成减半%',0.5,1) as tc_fl_service,	


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee
		*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_bbc_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,	
	
	-- 增加字段，计算不考虑毛利目标达成情况的提成
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,coalesce(a.original_tc_sales_rp,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.original_tc_sales_bbc_zy,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.original_tc_sales_bbc_ly,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,coalesce(a.original_tc_sales_fl,0))
				))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_sales,
		
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_rp_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_rp_service,		


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_fl_service))
		)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_fl_service,	


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee
		*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_bbc_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_bbc_service,
	
	
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 		
	
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0 a
-- 系统账期预付款客户
 -- 信控公司账期 
left join 
(
	select customer_code,credit_code,company_code,
		account_period_code,account_period_name,		-- 账期编码,账期名称
		account_period_value,		-- 账期值
		account_period_abbreviation_name,		-- 账期简称
		credit_limit,temp_credit_limit
		from csx_dim.csx_dim_crm_customer_company_details
		where sdt='current'
		    and shipper_code='YHCSX'
)e1 on a.credit_code=e1.credit_code and a.company_code=e1.company_code
-- 预付款客户
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('日配','全业务')
	)b1 on b1.customer_code=a.customer_code
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('BBC','全业务')
	)b2 on b2.customer_code=a.customer_code
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('福利','全业务')
	)b3 on b3.customer_code=a.customer_code
-- 调整打款日期：按打款日期对应的实际回款时间系数计算	
left join 
		(
	select customer_code,smt_date as smonth,category_second,adjust_business_type,
	  date_star,date_end,
	  date_format(from_unixtime(unix_timestamp(paid_date_new,'yyyyMMdd')),'yyyy-MM-dd') as paid_date_new 
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整打款日期%'		
	)c on a.customer_code=c.customer_code and a.paid_date=c.paid_date_new
left join
	(
	select smt_date,customer_code,
		concat(customer_code,effective_period,remark) as dd,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and (category_second like'%不算提成%'
	-- or category_second like'%服务费%'
	or category_second like'%提成减半%')
	)d on d.customer_code=a.customer_code
;




-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail;
-- create temporary table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
-- as
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail partition(smt)
select 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.province_code,a.customer_code,
	a.bill_month,a.happen_month,a.bill_date,a.paid_date,cast(a.dff_rate as string)) biz_id,
	
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month, -- 结算月
	cast(a.dff_rate as decimal(20,6)) dff_rate,		
	sum(pay_amt) as pay_amt,	-- 核销金额
	sum(rp_pay_amt) as rp_pay_amt,
	sum(bbc_pay_amt) as bbc_pay_amt,
	sum(bbc_ly_pay_amt) as bbc_ly_pay_amt,
	sum(bbc_zy_pay_amt) as bbc_zy_pay_amt,
	sum(fl_pay_amt) as fl_pay_amt,		
	
	-- 各类型销售额
	sum(sale_amt) as sale_amt,
	sum(rp_sale_amt) as rp_sale_amt,
	sum(bbc_sale_amt) as bbc_sale_amt,
	sum(bbc_ly_sale_amt) as bbc_ly_sale_amt,
	sum(bbc_zy_sale_amt) as bbc_zy_sale_amt,
	sum(fl_sale_amt) as fl_sale_amt,
	-- 各类型定价毛利额
	sum(profit) as profit,
	sum(rp_profit) as rp_profit,
	sum(bbc_profit) as bbc_profit,
	sum(bbc_ly_profit) as bbc_ly_profit,
	sum(bbc_zy_profit) as bbc_zy_profit,
	sum(fl_profit) as fl_profit,
	
	profit_rate,
	rp_profit_rate,
	bbc_profit_rate,
	bbc_ly_profit_rate,
	bbc_zy_profit_rate,
	fl_profit_rate,
	
	coalesce(a.cust_rp_profit_rate_tc,0.002) as cust_rp_profit_rate_tc, 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
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
	
	sum(tc_sales) as tc_sales,
	sum(tc_rp_service) as tc_rp_service,		
	sum(tc_fl_service) as tc_fl_service,	
	sum(tc_bbc_service) as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,	

	-- 增加字段，计算不考虑毛利目标达成情况的提成
	sum(original_tc_sales) as original_tc_sales,
	sum(original_tc_rp_service) as original_tc_rp_service,		
	sum(original_tc_fl_service) as original_tc_fl_service,	
	sum(original_tc_bbc_service) as original_tc_bbc_service,	
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 	
	
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1 a
group by 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.province_code,a.customer_code,
	a.bill_month,a.happen_month,a.bill_date,a.paid_date,cast(a.dff_rate as string)),
	
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month,

	profit_rate,
	rp_profit_rate,
	bbc_profit_rate,
	bbc_ly_profit_rate,
	bbc_zy_profit_rate,
	fl_profit_rate,
	
	cast(a.dff_rate as decimal(20,6)),
	coalesce(a.cust_rp_profit_rate_tc,0.002), 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
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
	
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit;	
	

