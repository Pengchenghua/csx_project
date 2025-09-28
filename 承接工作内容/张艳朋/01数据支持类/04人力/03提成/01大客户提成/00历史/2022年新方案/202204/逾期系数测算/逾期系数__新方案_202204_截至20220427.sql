-- 创建人员信息表，获取销售员和服务管家的城市，因为存在一个业务员名下客户跨城市的情况
drop table csx_tmp.tc_person_info;
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
set month_start_day ='20220401';	
set month_end_day ='20220427';	
set year_start_day ='20220101';		


-- 签呈处理销售员服务管家关系
drop table if exists csx_tmp.tc_customer_service_manager_info_new;
create table csx_tmp.tc_customer_service_manager_info_new
as  
select 
	distinct province_name,customer_id,customer_no,customer_name,
	sales_id_new as sales_id,
	work_no_new as work_no,
	sales_name_new as sales_name,
	rp_service_user_id_new as rp_service_user_id,
	rp_service_user_work_no_new as rp_service_user_work_no,
	rp_service_user_name_new as rp_service_user_name,
	fl_service_user_id_new as fl_service_user_id,
	fl_service_user_work_no_new as fl_service_user_work_no,
	fl_service_user_name_new as fl_service_user_name,
	bbc_service_user_id_new as bbc_service_user_id,
	bbc_service_user_work_no_new as bbc_service_user_work_no,
	bbc_service_user_name_new as bbc_service_user_name,
	--分配比例
	rp_sales_sale_rate as rp_sales_sale_fp_rate,                     --日配销售员_销售额分配比例
	rp_sales_profit_rate as rp_sales_profit_fp_rate,                 --日配销售员_毛利分配比例
	fl_sales_sale_rate as fl_sales_sale_fp_rate,                     --福利销售员_销售额分配比例
	fl_sales_profit_rate as fl_sales_profit_fp_rate,                 --福利销售员_毛利分配比例
	bbc_sales_sale_rate as bbc_sales_sale_fp_rate,                   --BBC销售员_销售额分配比例
	bbc_sales_profit_rate as bbc_sales_profit_fp_rate,               --BBC销售员_毛利分配比例	
	rp_service_user_sale_rate as rp_service_user_sale_fp_rate,       --日配服务管家_销售额分配比例
	rp_service_user_profit_rate as rp_service_user_profit_fp_rate,    --日配服务管家_毛利分配比例
	fl_service_user_sale_rate as fl_service_user_sale_fp_rate,       --福利服务管家_销售额分配比例
	fl_service_user_profit_rate as fl_service_user_profit_fp_rate,   --福利服务管家_毛利分配比例
	bbc_service_user_sale_rate as bbc_service_user_sale_fp_rate,     --BBC服务管家_销售额分配比例
	bbc_service_user_profit_rate as bbc_service_user_profit_fp_rate,  --BBC服务管家_毛利分配比例
	is_sale,is_overdue
from 
	csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
where 
	month='202204'
	and customer_no not in ('X000000'
	)
; --5


--客户应收金额、逾期金额
drop table csx_tmp.tc_cust_overdue_0;
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
		and customer_no not in ('119131','107404')
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
		and customer_no not in ('123096','123100','123068','123106','122987','108744')
		--202203月签呈 安徽省 剔除逾期 当月
		--and customer_no not in ('125686','125029','125028','124584','116727','105502','114576','114269','119965','120618')
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
			and customer_no not in('104698','112875','120939','121039','121625','124473','124498','125161','125898'
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
	e.province_name,
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
	left join
		(
		select
			distinct user_number,name,province_name
		from
			csx_tmp.tc_person_info
		) e on e.user_number=d.work_no
group by 
	e.province_name,
	a.channel_name,	-- 渠道
	d.work_no,	-- 销售员工号
	d.sales_name	-- 销售员
;
				

--服务管家逾期率
drop table csx_tmp.tc_service_user_over_rate;
create table csx_tmp.tc_service_user_over_rate
as
select 
	e.province_name,
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
	left join
		(
		select
			distinct user_number,name,province_name
		from
			csx_tmp.tc_person_info
		) e on e.user_number=split(d.service_user_work_no,';')[0]
group by 
	e.province_name,
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
--		sdt>='20220301'
--		and sdt<='20220331'
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
--			sdt='20220331'
--			and sales_province_name='安徽省'
--			and work_no not in ('81107924','81034712')
--		)c on c.customer_no=a.customer_no
--group by 
--	a.customer_no
--;


