-- 创建人员信息表，获取销售员和服务管家的城市，因为存在一个业务员名下客户跨城市的情况
drop table if exists csx_tmp.tc_person_info;
create table csx_tmp.tc_person_info
as
select 
	distinct a.id,a.user_number,a.name,b.city_group_code,b.city_group_name,b.province_code,b.province_name,b.region_code,b.region_name
from
	(
	select
		id,user_number,name,user_position,city_name,prov_name
	from
		csx_dw.dws_basic_w_a_user
	where
		sdt=regexp_replace(date_sub(current_date,1),'-','')
		and del_flag = '0'
	) a
	left join -- 区域表
		( 
		select distinct
			city_code,city_name,area_province_code,area_province_name,city_group_code,city_group_name,province_code,province_name,region_code,region_name
		from
			csx_dw.dws_sale_w_a_area_belong
		) b on b.city_name=a.city_name and b.area_province_name=a.prov_name
;

-- 月初 月末 年初
set month_start_day ='20220601';	
set month_end_day ='20220630';	
set year_start_day ='20220101';		


-- 签呈处理销售员服务管家关系
--drop table if exists csx_tmp.tc_customer_service_manager_info_new;
--create table csx_tmp.tc_customer_service_manager_info_new
--as  
--select 
--	distinct province_name,customer_id,customer_no,customer_name,
--	sales_id_new as sales_id,
--	work_no_new as work_no,
--	sales_name_new as sales_name,
--	rp_service_user_id_new as rp_service_user_id,
--	rp_service_user_work_no_new as rp_service_user_work_no,
--	rp_service_user_name_new as rp_service_user_name,
--	fl_service_user_id_new as fl_service_user_id,
--	fl_service_user_work_no_new as fl_service_user_work_no,
--	fl_service_user_name_new as fl_service_user_name,
--	bbc_service_user_id_new as bbc_service_user_id,
--	bbc_service_user_work_no_new as bbc_service_user_work_no,
--	bbc_service_user_name_new as bbc_service_user_name,
--	--分配比例
--	rp_sales_sale_rate as rp_sales_sale_fp_rate,                     --日配销售员_销售额分配比例
--	rp_sales_profit_rate as rp_sales_profit_fp_rate,                 --日配销售员_毛利分配比例
--	fl_sales_sale_rate as fl_sales_sale_fp_rate,                     --福利销售员_销售额分配比例
--	fl_sales_profit_rate as fl_sales_profit_fp_rate,                 --福利销售员_毛利分配比例
--	bbc_sales_sale_rate as bbc_sales_sale_fp_rate,                   --BBC销售员_销售额分配比例
--	bbc_sales_profit_rate as bbc_sales_profit_fp_rate,               --BBC销售员_毛利分配比例	
--	rp_service_user_sale_rate as rp_service_user_sale_fp_rate,       --日配服务管家_销售额分配比例
--	rp_service_user_profit_rate as rp_service_user_profit_fp_rate,    --日配服务管家_毛利分配比例
--	fl_service_user_sale_rate as fl_service_user_sale_fp_rate,       --福利服务管家_销售额分配比例
--	fl_service_user_profit_rate as fl_service_user_profit_fp_rate,   --福利服务管家_毛利分配比例
--	bbc_service_user_sale_rate as bbc_service_user_sale_fp_rate,     --BBC服务管家_销售额分配比例
--	bbc_service_user_profit_rate as bbc_service_user_profit_fp_rate,  --BBC服务管家_毛利分配比例
--	is_sale,is_overdue
--from 
--	--csx_dw.report_crm_w_a_customer_service_manager_info_new
--	csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
--where 
--	--sdt=${hiveconf:month_end_day}
--	month='202204'
--	--and (is_sale='是' or is_overdue='是')
--	and customer_no not in ('X000000'
--	)
--	--union all

-- 签呈处理销售员服务管家关系
drop table if exists csx_tmp.tc_customer_service_manager_info_new;
create table csx_tmp.tc_customer_service_manager_info_new
as  
select 
	distinct customer_id,customer_no,customer_name,region_code,region_name,province_code,province_name,city_group_code,city_group_name,
	--更改销售员 每月
	case when customer_no in ('123311','124033','118376','124524') then '1000000567740' else sales_id_new end as sales_id,
	case when customer_no in ('123311','124033','118376','124524') then '81103065' else work_no_new end as work_no,
	case when customer_no in ('123311','124033','118376','124524') then '王世超' else sales_name_new end as sales_name,
	--
	case when customer_no in ('')then null 
		--每月
		when customer_no in ('124524') then null
		
		--when customer_no in ('123311') then '1000000211352;1000000570922' --陈素彩 马培培
		--when customer_no in ('124033') then '1000000211352' --陈素彩
		--when customer_no in ('118376') then '1000000567737;1000000570922' --武艳 马培培
		--when customer_no in ('124524') then '1000000567576;1000000571567' --彭倩倩 郭凯利
		
		--when customer_no in ('104036','106000','110575','112088','113443','113576','113588') then '1000000569847' -- 洪少灵
		--when customer_no in ('103830') then '1000000566052' --林志高
		--when customer_no in ('120418') then '1000000556663' --王能海
		--when customer_no in ('105638','105947','105975') then '1000000557101' --庄丽明
		else rp_service_user_id_new end as rp_service_user_id,
	case when customer_no in ('')then null
		--每月
		when customer_no in ('124524') then null
		
		--when customer_no in ('123311') then '81020509;81134553' --陈素彩 马培培
		--when customer_no in ('124033') then '81020509' --陈素彩
		--when customer_no in ('118376') then '81103328;81134553' --武艳 马培培
		--when customer_no in ('124524') then '81103326;81138104' --彭倩倩 郭凯利
		
		--when customer_no in ('104036','106000','110575','112088','113443','113576','113588') then '81129344' -- 洪少灵
		--when customer_no in ('103830') then '81094607' --林志高
		--when customer_no in ('120418') then '81004682' --王能海
		--when customer_no in ('105638','105947','105975') then '81003172' --庄丽明				
		else rp_service_user_work_no_new end as rp_service_user_work_no,		
	case when customer_no in ('')then null
		--每月
		when customer_no in ('124524') then null
				
		--when customer_no in ('123311') then '陈素彩;马培培' --陈素彩 马培培
		--when customer_no in ('124033') then '陈素彩' --陈素彩
		--when customer_no in ('118376') then '武艳;马培培' --武艳 马培培
		--when customer_no in ('124524') then '彭倩倩;郭凯利' --彭倩倩 郭凯利
		
		--when customer_no in ('104036','106000','110575','112088','113443','113576','113588') then '洪少灵' -- 洪少灵
		--when customer_no in ('103830') then '林志高' --林志高
		--when customer_no in ('120418') then '王能海' --王能海
		--when customer_no in ('105638','105947','105975') then '庄丽明' --庄丽明				
		else rp_service_user_name_new end as rp_service_user_name,
	--当月
	case when customer_no in ('') then '1000000560749'
		--每月
		when customer_no in ('124524') then null
		
		when customer_no in ('') then '1000000569181'	else fl_service_user_id_new end as fl_service_user_id,
	case when customer_no in ('') then '81034648'
		--每月
		when customer_no in ('124524') then null
			
		when customer_no in ('') then '81122116' else fl_service_user_work_no_new end as fl_service_user_work_no,
	case when customer_no in ('') then '李紫珊' 
		--每月
		when customer_no in ('124524') then null
		
		when customer_no in ('') then '陈滨滨' else fl_service_user_name_new end as fl_service_user_name,
	
	case when customer_no in ('') then '1000000569181' else bbc_service_user_id_new end as bbc_service_user_id,	
	case when customer_no in ('') then '81122116' else bbc_service_user_work_no_new end as bbc_service_user_work_no,
	case when customer_no in ('') then '陈滨滨' else bbc_service_user_name_new end as bbc_service_user_name,	
	--公司开发客户 按管家式提成 每月
	case when customer_no in ('117762','124179') then 0.3 
		--河南省 每月
		when customer_no in ('123311','124033','118376') then 0.9	
		when customer_no in ('124524') then 1	
		
		--202205月签呈 当月
		when customer_no in ('') then 1
		when customer_no in ('') then 0.7	
		else rp_sales_sale_rate end as rp_sales_sale_fp_rate,
	--公司开发客户 按管家式提成 每月
	case when customer_no in ('117762','124179') then 0.5 
		--河南省 每月
		when customer_no in ('123311','124033','118376') then 0.8
		when customer_no in ('124524') then 1	
		
		--202205月签呈 当月
		when customer_no in ('') then 1
		when customer_no in ('') then 0.5		
		else rp_sales_profit_rate end as rp_sales_profit_fp_rate,
		
	case when customer_no in ('') then 0.9 else fl_sales_sale_rate end as fl_sales_sale_fp_rate,
	case when customer_no in ('') then 0.8 else fl_sales_profit_rate end as fl_sales_profit_fp_rate,
	
	case when customer_no in ('') then 0.9 else bbc_sales_sale_rate end as bbc_sales_sale_fp_rate,
	case when customer_no in ('') then 0.8 else bbc_sales_profit_rate end as bbc_sales_profit_fp_rate,
	-- 销售员转岗前开发 按销售员算 每月
	case when customer_no in ('116639','117304','117605','118824','120985','122636','124408','126034','126295','124110','124139','124146','125285','125716','126637') then 1 
		--河南省 每月
		when customer_no in ('123311','124033','118376','124524') then 0.1
		--当月
		--when customer_no in ('104036','106000','110575','112088','113443','113576','113588','103830','120418','105638','105947','105975') then 0.3	
		else rp_service_user_sale_rate end as rp_service_user_sale_fp_rate,
	-- 销售员转岗前开发 按销售员算 每月
	case when customer_no in ('116639','117304','117605','118824','120985','122636','124408','126034','126295','124110','124139','124146','125285','125716','126637') then 1
		--河南省 每月
		when customer_no in ('123311','124033','118376','124524') then 0.2
		--当月
		--when customer_no in ('104036','106000','110575','112088','113443','113576','113588','103830','120418','105638','105947','105975') then 0.5	
		else rp_service_user_profit_rate end as rp_service_user_profit_fp_rate,
	--当月
	case when customer_no in ('') then 1 else fl_service_user_sale_rate end as fl_service_user_sale_fp_rate,
	case when customer_no in ('') then 1 else fl_service_user_profit_rate end as fl_service_user_profit_fp_rate,
	--当月
	case when customer_no in ('') then 1 else bbc_service_user_sale_rate end as bbc_service_user_sale_fp_rate,     
	case when customer_no in ('') then 1 else bbc_service_user_profit_rate end as bbc_service_user_profit_fp_rate,
	is_sale,is_overdue
from 
	csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
where 
	month=substr(${hiveconf:month_end_day},1,6)
; --5


--客户应收金额、逾期金额
drop table if exists csx_tmp.tc_cust_overdue_0;
create table csx_tmp.tc_cust_overdue_0
as
select
	a.sdt,
	--a.customer_code as customer_no,
	a.customer_no,
	a.customer_name,a.company_code,a.company_name,a.channel_code,a.channel_name,a.payment_terms,a.payment_days,a.payment_name,a.receivable_amount,a.overdue_amount,a.max_overdue_day,
	a.overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	a.overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
from	
	(
	select
		sdt,
		--customer_code as customer_no,
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from
		--csx_dw.dws_sss_r_d_customer_settle_detail
		csx_dw.dws_sss_r_a_customer_company_accounts
	where
		sdt=${hiveconf:month_end_day}
		--202201月签呈，剔除逾期，2022年1-4月
		--and customer_no not in ('119131','107404')
		--202202月签呈，剔除逾期，每月
		and customer_no not in ('104192','123395','117927','119925')
		--202202月签呈，客户已转代理人，剔除逾期，每月
		and customer_no not in ('122221','123086')
		--202202月签呈，剔除逾期，每月
		and customer_no not in ('116661','103369','104817','105601','104381','105304','105714','118794','120595','120830','122837','116932','119209','119022','119214','119257',
		'113425','112129')
		--202202月签呈，剔除逾期，当月
		--and customer_no not in ('116727','113439','105502','114576','114269','116721','119965','114945','120618','125613','124379','125247','125256','124025','124667','124621',
		--'124370','125469','123599','124782')
		--202202月签呈，公司BBC客户，不算提成，每月
		and customer_no not in ('123623')
		--202202月签呈，客户地采产品较多，不算提成，当月
		--and customer_no not in ('102866')
		--202202月签呈，每月
		and customer_no not in ('106626','112779','113122','124976','119977','102754')
		--202202月签呈，剔除逾期，每月
		and customer_no not in ('106526','120317','119906','108127','105838','105806')
		--202202月签呈，2022年1-6月
		and customer_no not in ('104086','116313','105249','106721','115204')
		--202202月签呈，2022年1-12月
		and customer_no not in ('106721','105182','105181','118103')
		--202203月签 剔除逾期 每月
		and customer_no not in ('120314','120426','111120','105235','105557')
		--202203月签呈，剔除逾期 每月
		and customer_no not in ('120314','120426','104192','123395','117927','115393','119925','122495','115393','105696')
		--202203月签呈，剔除逾期 当月
		--and customer_no not in ('125837','123009','123162','123776','124250','124486','120417','125004','124667','124370','123599','125469','124379')
		--202203月签呈 不算提成和逾期 每月 
		and customer_no not in ('104192','123395','117927','126154')
		--202203月签呈 不算提成和逾期 当月 
		--and customer_no not in ('123859')
		--202203月签呈 剔除逾期 3-5月
		--and customer_no not in ('123096','123100','123068','123106','122987','108744')
		--202203月签呈 安徽省 剔除逾期 当月
		--and customer_no not in ('125686','125029','125028','124584','116727','105502','114576','114269','119965','120618')
		--202204月签呈 剔除逾期 当月
		--and customer_no not in ('121625','123244','124473','124217','125686','125029','124584','124784','125017','125028','108102','124068','120376')
		--202204月签呈 剔除逾期 每月
		and customer_no not in ('104596','121244','121248','121259','121274','121286','121305')
		--202204月签呈 剔除逾期 每月
		and customer_no not in ('112718','124486','120417')	
		--202204月签呈 不算提成和逾期 每月 
		and customer_no not in ('120459','121206','102524','111204','109377')	
		--202204月签呈 不算提成和逾期 当月 
		--and customer_no not in ('119729','124658')
		--202204月签呈 剔除逾期 期间 202204-202206
		and customer_no not in ('125064')	
		--202205月签呈 剔除逾期 每月
		and customer_no not in ('105265','124811','123244','124473','124217','120102','122968')	
		--202205月签呈 剔除逾期 当月
		--and customer_no not in ('121625','125686','125029','124584','124784','125017','125028','108102','124068','127008','104901','119443','113477','119297','106124','118326',
		--'112954','106456','109474','126275','120295','114872','116052','126031','109092','105656')	
		--202205月签呈 剔除逾期 期间 202205-202207
		and customer_no not in ('122985')	
		--202205月签呈 剔除逾期 当月
		--and customer_no not in ('103772')	
		--202205月签呈 剔除逾期 期间 202205-202206
		and customer_no not in ('106721','105181','107404','122599','106330','106298','106309','106308','106306','106320','122155','120836','117108','106299','106321','106326',
		'106307','106325','106283','124041','106284','106301','116668','116718','118836','119442','122901','123827','115803')
		--202205月签呈 剔除逾期 期间 202205-202301
		and customer_no not in ('102754')	
		--202205月签呈 剔除逾期 当月
		--and customer_no not in ('116099','107852','122703')	
		--202205月签呈 剔除逾期 当月
		--and customer_no not in ('108599','113980','109788','106585','105242','123599','125469','114494','123516')			
	) a 
	--剔除业务代理与内购客户
	join		
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day} 
			and (channel_code in('1','7','8'))  ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
			and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
		)b on b.customer_no=a.customer_no  
	--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 
		(
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:month_start_day} 
			and sdt<=${hiveconf:month_end_day} 
			and business_type_code in('3','4')
			-- 不剔除城市服务商2.0，按大客户提成方案计算
			and customer_no not in('123244'
				)
		)e on e.customer_no=a.customer_no
where
	e.customer_no is null
; 

--逾期客户信息
drop table csx_tmp.tc_cust_overdue_info;
create table csx_tmp.tc_cust_overdue_info
as
select
	a.channel_code,
	a.channel_name,	-- 渠道
	d.province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	d.work_no,	-- 销售员工号
	d.sales_name,	-- 销售员
	d.rp_service_user_work_no,d.rp_service_user_name,
	a.payment_terms,	-- 账期编码
	a.payment_days,	-- 帐期天数
	a.payment_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称,
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end as receivable_amount,	-- 应收金额
	case when a.overdue_amount>=0 and a.receivable_amount>0 then a.overdue_amount else 0 end as overdue_amount,	-- 逾期金额
	case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end as overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end as overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
		else coalesce(case when overdue_coefficient_numerator>=0 and a.receivable_amount>0 then overdue_coefficient_numerator else 0 end, 0)
		/(case when overdue_coefficient_denominator>=0 and a.receivable_amount>0 then overdue_coefficient_denominator else 0 end) end, 6),0) as over_rate -- 逾期系数		
from
	(
	select
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from 
		csx_tmp.tc_cust_overdue_0  
	where 
		channel_name = '大客户' 
		and sdt = ${hiveconf:month_end_day} 	
	)a
	--关联客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct province_name,customer_no,rp_service_user_work_no,rp_service_user_name,	  
			work_no,sales_name
		from 
			csx_tmp.tc_customer_service_manager_info_new
		)d on d.customer_no=a.customer_no	  
where 
	a.receivable_amount>0 or a.receivable_amount is null
; 

-- 查询结果集
--计算逾期系数
insert overwrite directory '/tmp/zhangyanpeng/yuqi_dakehu' row format delimited fields terminated by '\t'	
select * from csx_tmp.tc_cust_overdue_info
;

--客户逾期系数
drop table csx_tmp.tc_cust_over_rate; --13
create table csx_tmp.tc_cust_over_rate
as 
select 
	channel_name,	-- 渠道
	customer_no,	-- 客户编码
	customer_name,	-- 客户名称,
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数 
from 
	csx_tmp.tc_cust_overdue_0 a 
where 
	channel_name = '大客户' 
	and sdt = ${hiveconf:month_end_day} 
group by 
	channel_name,customer_no,customer_name

;

--销售员逾期系数
drop table csx_tmp.tc_salesname_over_rate;
create table csx_tmp.tc_salesname_over_rate
as
select 
	a.channel_name,	-- 渠道
	d.work_no,	-- 销售员工号
	d.sales_name,	-- 销售员
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数 		
from
	(
	select
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from 
		csx_tmp.tc_cust_overdue_0  
	where 
		channel_name = '大客户' 
		and sdt = ${hiveconf:month_end_day} 	
	)a
	--关联客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_no,work_no,sales_name
		from 
			csx_tmp.tc_customer_service_manager_info_new
		)d on d.customer_no=a.customer_no	 
group by 
	a.channel_name,	-- 渠道
	d.work_no,	-- 销售员工号
	d.sales_name	-- 销售员
;
				

--服务管家逾期率
drop table csx_tmp.tc_service_user_over_rate;
create table csx_tmp.tc_service_user_over_rate
as
select 
	d.service_user_work_no,
	d.service_user_name,
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
	sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 
		then overdue_coefficient_numerator else 0 end) as overdue_coefficient_numerator,	-- 逾期金额*逾期天数
	sum(case when overdue_coefficient_denominator>=0 and receivable_amount>0 
		then overdue_coefficient_denominator else 0 end) overdue_coefficient_denominator,	-- 应收金额*帐期天数	
	coalesce(round(
		case when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
		else coalesce(sum(case when overdue_coefficient_numerator>=0 and receivable_amount>0 then overdue_coefficient_numerator else 0 end), 0)
		/(sum(case when overdue_coefficient_denominator>=0  and receivable_amount>0 then overdue_coefficient_denominator else 0 end)) end, 6),0) as over_rate -- 逾期系数 		
from
	(
	select
		channel_name,
		customer_no,
		customer_name,receivable_amount,overdue_amount,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from 
		csx_tmp.tc_cust_over_rate  
	)a
	--关联客户对应销售员与服务管家
	right join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name
		from 
			(
			select
				customer_no,rp_service_user_work_no as service_user_work_no,rp_service_user_name as service_user_name
			from
				csx_tmp.tc_customer_service_manager_info_new
			where
				rp_service_user_work_no is not null
			union all
			select
				customer_no,fl_service_user_work_no as service_user_work_no,fl_service_user_name as service_user_name
			from
				csx_tmp.tc_customer_service_manager_info_new
			where
				fl_service_user_work_no is not null
			union all
			select
				customer_no,bbc_service_user_work_no as service_user_work_no,bbc_service_user_name as service_user_name
			from
				csx_tmp.tc_customer_service_manager_info_new
			where
				bbc_service_user_work_no is not null
			) a 
		)d on d.customer_no=a.customer_no	  
group by 
	d.service_user_work_no,
	d.service_user_name
;

--大宗供应链的逾期系数
insert overwrite directory '/tmp/zhangyanpeng/yuqi_dazong' row format delimited fields terminated by '\t'
select 
	a.channel_name,	-- 渠道
	b.sales_province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	a.payment_terms,	-- 账期编码
	a.payment_days,	-- 帐期天数
	a.payment_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	a.receivable_amount,	-- 应收金额
	a.overdue_amount,	-- 逾期金额
	overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	if(overdue_coefficient_numerator/overdue_coefficient_denominator<0,0,overdue_coefficient_numerator/overdue_coefficient_denominator) as over_rate -- 逾期系数			    
from
	(
	select
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母		
	from 
		csx_tmp.tc_cust_overdue_0  
	where 
		(channel_name like '大宗%' or channel_name like '%供应链%')
		and sdt =${hiveconf:month_end_day} 
	)a
	join		 
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day} 
			and channel_code in('4','5','6') ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
		)b on b.customer_no=a.customer_no 
where
	(a.receivable_amount>0 or a.receivable_amount is null)
;

--=============================================================================================================================================================================

--查询城市服务商2.0客户,按库存DC

--select distinct inventory_dc_code from csx_ods.source_csms_w_a_yszx_town_service_provider_config; -- W0AW、W0BY、W0K7、W0L4；W0AW、W0K7

--select 
--	a.*,c.work_no,c.sales_name
--from 
--	(
--	select 
--		province_name,customer_no,customer_name,business_type_name,dc_code,
--		sum(sales_value)sales_value
--	from 
--		csx_dw.dws_sale_r_d_detail
--	where 
--		sdt>='20220201'
--		and sdt<='20220228'
--		and channel_code in('1','7','9')
--	group by 
--		province_name,customer_no,customer_name,business_type_name,dc_code
--	)a 
--	join 
--		(
--		select 
--			distinct customer_no
--		from 
--			csx_dw.dws_sale_r_d_detail
--		where 
--			sdt>='20220201'
--			and sdt<='20220228'
--			and channel_code in('1','7','9')
--			and dc_code in('W0AW','W0K7')
--		) b on b.customer_no=a.customer_no
--	left join 
--		(
--		select 
--			distinct customer_no,customer_name,work_no,sales_name,sales_province_name
--		from 
--			csx_dw.dws_crm_w_a_customer 
--		where 
--			sdt='20220228'
--		)c on c.customer_no=a.customer_no;


--安徽省按照大客户计算的客户

--select 
--	a.customer_no
--from 
--	(
--	select 
--		province_name,customer_no,customer_name,business_type_name,
--		sum(sales_value)sales_value
--	from 
--		csx_dw.dws_sale_r_d_detail
--	where 
--		sdt>='20220401'
--		and sdt<='20220430'
--		and channel_code in('1','7','9')
--		and business_type_code in ('4')
--	group by 
--		province_name,customer_no,customer_name,business_type_name
--	)a 
--	--淮南名单：81107924 陈治强  81034712 董冬燕，除了淮南都按大客户
--	join 
--		(
--		select 
--			customer_no,customer_name,work_no,sales_name,sales_province_name
--		from 
--			csx_dw.dws_crm_w_a_customer 
--		where 
--			sdt='20220430'
--			and sales_province_name='安徽省'
--			and work_no not in ('81107924','81034712')
--		)c on c.customer_no=a.customer_no
--group by 
--	a.customer_no
--;


