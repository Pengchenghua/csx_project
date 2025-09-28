-- 确认需对哪些销售员补充收入组
set month_start_day ='20220901';	
set month_end_day ='20220930';
set last_month_end_day='20220831';


select 
	b.flag,b.user_work_no,b.user_name,c.income_type,sum(sales_value) sales_value,sum(profit) profit
from
	(
	select 
		province_code,province_name,customer_no,substr(sdt,1,6) smonth,sum(sales_value) sales_value,sum(profit) profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:month_start_day}
		and sdt<=${hiveconf:month_end_day}
		and channel_code in('1','7')
	group by 
		province_code,province_name,customer_no,substr(sdt,1,6)
	)a	
	left join 
	(
	select 
		'销售员' flag,
		customer_no,
		work_no_new as user_work_no,
		sales_name_new as user_name,
		sales_id_new as user_id
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt=${hiveconf:month_end_day}
	and sales_id_new is not null
	union all
	select 
		'日配服务管家' flag,
		customer_no,
		rp_service_user_work_no_new as user_work_no,
		rp_service_user_name_new as user_name,
		rp_service_user_id_new as user_id
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt=${hiveconf:month_end_day}
	and rp_service_user_id_new is not null
	union all
	select 
		'福利服务管家' flag,
		customer_no,
		fl_service_user_work_no_new as user_work_no,
		fl_service_user_name_new as user_name,
		fl_service_user_id_new as user_id
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt=${hiveconf:month_end_day}
	and fl_service_user_id_new is not null
	union all	
	select 
		'BBC服务管家' flag,
		customer_no,
		bbc_service_user_work_no_new as user_work_no,
		bbc_service_user_name_new as user_name,
		bbc_service_user_id_new as user_id
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt=${hiveconf:month_end_day}
	and bbc_service_user_id_new is not null
	) b on b.customer_no=a.customer_no
	left join 
	(
	select distinct work_no,income_type 
	from csx_tmp.sales_income_info_new where sdt=${hiveconf:last_month_end_day}
	) c on c.work_no=b.user_work_no
where 
	c.income_type is null and b.user_name not rlike 'B|C' 
group by 
	b.flag,b.user_work_no,b.user_name,c.income_type;

--=============================================================================================================================================================================
-- 补充收入组并校验
load data inpath '/tmp/raoyanhua/sales_income_info_new_202205.csv' overwrite into table csx_tmp.sales_income_info_new partition (sdt=${hiveconf:month_end_day});
select * from csx_tmp.sales_income_info_new where sdt=${hiveconf:month_end_day};

--=============================================================================================================================================================================
--确认需对哪些服务管家补充收入组

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;



-- 设置日期
--set current_day=regexp_replace(date_sub(current_date, 1), '-', '');
--set month_start_day ='20220801';	
--set month_end_day ='20220831';
--set last_month_end_day='20220731';	
--set year_start_day ='20220101';	
--set year_start_month ='202201';	
--set created_time =from_utc_timestamp(current_timestamp(),'GMT');	------当前时间
--set current_month =substr(${hiveconf:month_end_day},1,6);

set current_day=regexp_replace(date_sub(current_date, 1), '-', '');
set month_start_day ='20220901';	
set month_end_day ='20220930';
set last_month_end_day='20220831';	
set year_start_day ='20220101';	
set year_start_month ='202201';	
set created_time =from_utc_timestamp(current_timestamp(),'GMT');	------当前时间
set current_month =substr(${hiveconf:month_end_day},1,6);


-- 创建人员信息表，获取销售员和服务管家的城市，因为存在一个业务员名下客户跨城市的情况
drop table if exists csx_tmp.tc_r_person_info;
create table csx_tmp.tc_r_person_info
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


--9月对应关系
-- 签呈处理销售员服务管家关系
drop table if exists csx_tmp.tc_r_customer_service_manager_info_new;
create table csx_tmp.tc_r_customer_service_manager_info_new
as
select 
	distinct a.customer_id,a.customer_no,a.customer_name,a.region_code,a.region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,
	sales_id_new as sales_id,
	work_no_new as work_no,
	sales_name_new as sales_name,
	rp_service_user_id_new as rp_service_user_id,
	rp_service_user_work_no_new as rp_service_user_work_no,		
	rp_service_user_name_new as rp_service_user_name,
	case when customer_no in ('') then '1000000560749'
		-- 9月签呈更改服务管家 当月
		when customer_no in ('121082','121867') then '1000000569181'
		when customer_no in ('115205') then '1000000565238'
		else fl_service_user_id_new end as fl_service_user_id,
	case when customer_no in ('') then '81034648'
		-- 9月签呈更改服务管家 当月
		when customer_no in ('121082','121867') then '81122116'
		when customer_no in ('115205') then '81088296'
		else fl_service_user_work_no_new end as fl_service_user_work_no,
	case when customer_no in ('') then '李紫珊' 
		-- 9月签呈更改服务管家 当月
		when customer_no in ('121082','121867') then '陈滨滨'
		when customer_no in ('115205') then '陈惠燕'
		else fl_service_user_name_new end as fl_service_user_name,	

	bbc_service_user_id_new as bbc_service_user_id,	
	bbc_service_user_work_no_new as bbc_service_user_work_no,
	bbc_service_user_name_new as bbc_service_user_name,	
	-- 202209签呈 '126188','125643','127312','127336' 销售员按高级服务管家的提成系数 每月
	case when customer_no in ('126188','125643','127312','127336') then 0.3 
		when a.province_name='福建省' or sales_id_new ='' or sales_id_new is null then 0.7			
		else rp_sales_sale_rate end as rp_sales_sale_fp_rate,
	
	case when customer_no in ('126188','125643','127312','127336') then 0.5
		when a.province_name='福建省' or sales_id_new ='' or sales_id_new is null then 0.5	
		else rp_sales_profit_rate end as rp_sales_profit_fp_rate,
		
	case when customer_no in ('126188','125643','127312','127336') then 0.3 
		 when a.province_name='福建省' or sales_id_new ='' or sales_id_new is null then 0.7	
		 else fl_sales_sale_rate end as fl_sales_sale_fp_rate,
	case when customer_no in ('126188','125643','127312','127336') then 0.5
		 when a.province_name='福建省' or sales_id_new ='' or sales_id_new is null then 0.5	
		 else fl_sales_profit_rate end as fl_sales_profit_fp_rate,
	
	case when customer_no in ('126188','125643','127312','127336') then 0.3 
		 when a.province_name='福建省' or sales_id_new ='' or sales_id_new is null then 0.7			 
		 else bbc_sales_sale_rate end as bbc_sales_sale_fp_rate,
	case when customer_no in ('126188','125643','127312','127336') then 0.5
		 when a.province_name='福建省' or sales_id_new ='' or sales_id_new is null then 0.5	
		 else bbc_sales_profit_rate end as bbc_sales_profit_fp_rate,
	-- 202209签呈 124524 日配管家按高级服务管家的提成系数 每月
	case when customer_no in ('124524') then 0.3 else b1.level_sale_rate end as rp_service_user_sale_fp_rate,
	case when customer_no in ('124524') then 0.5 else b1.level_profit_rate end as rp_service_user_profit_fp_rate,	
	case when customer_no in ('') then 0.1 else b2.level_sale_rate end as fl_service_user_sale_fp_rate,
	case when customer_no in ('') then 0.2 else b2.level_profit_rate end as fl_service_user_profit_fp_rate,
	case when customer_no in ('') then 0.1 else b3.level_sale_rate end as bbc_service_user_sale_fp_rate,     
	case when customer_no in ('') then 0.2 else b3.level_profit_rate end as bbc_service_user_profit_fp_rate

from 
(
select *
from csx_tmp.report_crm_w_a_customer_service_manager_info_business
where sdt=${hiveconf:month_end_day}
)a	
left join (select * from csx_tmp.crm_r_m_service_level where sdt='20220831') b1 on a.rp_service_user_work_no_new=b1.service_user_work_no	
left join (select * from csx_tmp.crm_r_m_service_level where sdt='20220831') b2 on a.fl_service_user_work_no_new=b2.service_user_work_no
left join (select * from csx_tmp.crm_r_m_service_level where sdt='20220831') b3 on a.bbc_service_user_work_no_new=b3.service_user_work_no
; 




		
-- 销售员年度累计销额提成比例
drop table csx_tmp.tc_r_sales_rate_ytd; --5
create table csx_tmp.tc_r_sales_rate_ytd
as
select 
	sales_id,work_no,sales_name,income_type,sales_sales_value_ytd,sales_rp_bbc_sales_value_ytd,sales_fl_sales_value_ytd,
	case when 
			((sales_rp_bbc_sales_value_ytd<=10000000 and income_type in('Q1','Q2','Q3')) 
			or (sales_rp_bbc_sales_value_ytd>10000000 and sales_rp_bbc_sales_value_ytd<=20000000 and income_type in('Q2','Q3'))
			or (sales_rp_bbc_sales_value_ytd>20000000 and sales_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q3'))) then 0.002
		when ((sales_rp_bbc_sales_value_ytd>10000000 and sales_rp_bbc_sales_value_ytd<=20000000 and income_type in('Q1'))
			or (sales_rp_bbc_sales_value_ytd>20000000 and sales_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q2'))
			or (sales_rp_bbc_sales_value_ytd>30000000 and sales_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q3'))) then 0.0025
		when ((sales_rp_bbc_sales_value_ytd>20000000 and sales_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q1'))
			or (sales_rp_bbc_sales_value_ytd>30000000 and sales_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q2'))
			or (sales_rp_bbc_sales_value_ytd>40000000 and income_type in('Q3'))) then 0.003
		when ((sales_rp_bbc_sales_value_ytd>30000000 and sales_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q1'))
			or (sales_rp_bbc_sales_value_ytd>40000000 and income_type in('Q2'))) then 0.0035
		when (sales_rp_bbc_sales_value_ytd>40000000 and income_type in('Q1')) then 0.004			
		else 0.002 end sales_rp_bbc_sales_value_tc_rate,
	0.002 as sales_fl_sales_value_tc_rate
from 
	(
	select 
		b.sales_id,b.work_no,b.sales_name,coalesce(c.income_type,'Q1') as income_type,
		sum(a.sales_value) as sales_sales_value_ytd,
		sum(a.rp_bbc_sales_value) as sales_rp_bbc_sales_value_ytd,
		sum(a.fl_sales_value) as sales_fl_sales_value_ytd
	from 
		(
		select 
			customer_no,regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') as sdt_last,
			sum(sales_value) as sales_value,
			sum(case when business_type_code in('1','4','5','6') then sales_value else 0 end) as rp_bbc_sales_value,
			sum(case when business_type_code in('2') then sales_value else 0 end) as fl_sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:year_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and province_name = '平台-B') --平台酒水
			--安徽省城市服务商2.0，按大客户提成方案计算，当月 
				--福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				--福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				or (business_type_code in ('4') and customer_no in
				('120939','125719','124193','113829','124351','121337','120768'
				)))
			--and province_name in ('福建省')
			and (province_name !='福建省' or (province_name='福建省' and dc_name not like '%V2DC%')) --2.0 按仓库名称判断
			--202202月签呈，该客户已转代理人，不算提成，每月
			and customer_no not in ('122221','123086')
			--202202月签呈，公司BBC客户，不算提成，每月
			and customer_no not in ('123623')
			--202202月签呈，客户地采产品较多，不算提成，当月
			--and customer_no not in ('102866')
			--202203月签呈 不算提成和逾期 每月 
			and customer_no not in ('104192','123395','117927','126154')
			--202203月签呈 不算提成和逾期 当月 
			--and customer_no not in ('123859')
			--202204月签呈 不算提成和逾期 每月 
			and customer_no not in ('120459','121206')	
			--202205月签呈 不算提成 每月 
			and customer_no not in ('109435','111255')	
			--202207月签呈 不算提成 202207-202208 
			and customer_no not in ('116902','117705','122054','125137')
			--202207月签呈 不算提成 当月 
			--and customer_no not in ('126544','118007','126428')	
			-- 202207月签呈 代理人客户 不算提成 每月
			and customer_no not in ('127649')
			--202209月签呈 不算提成 当月 
			and customer_no not in ('129525')	
		group by 
			customer_no,regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
		)a 
		left join   --CRM客户信息取每月最后一天
			(
			select 
				sdt,customer_id,customer_no,customer_name,sales_id,work_no,sales_name,
				case when channel_code='9' then '业务代理' end as ywdl_cust,
				case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust	
			from 
				csx_dw.dws_crm_w_a_customer 
			where 
				sdt>=${hiveconf:year_start_day}
				and sdt<=${hiveconf:month_end_day}
				and customer_no !=''
				and sdt=regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') --每月最后一天
			)b on b.customer_no=a.customer_no and b.sdt=a.sdt_last 	
		left join 
			(
			select 
				distinct work_no,income_type 
			from 
				csx_tmp.sales_income_info_new 
			where 
				sdt=${hiveconf:month_end_day}
			) c on c.work_no=b.work_no   --上月最后1日			
	where 
		b.ywdl_cust is null -- 剔除业务代理和内购 or b.customer_no in ('118689','116957','116629'))
		and b.ng_cust is null
	group by 
		b.sales_id,b.work_no,b.sales_name,coalesce(c.income_type,'Q1')
	)a
where
	work_no !=''
;


-- 服务管家年度累计销额提成比例
drop table csx_tmp.tc_r_service_rate_ytd_0;
create table csx_tmp.tc_r_service_rate_ytd_0
as
	select 
		a.customer_no,a.smonth,
		d.rp_service_user_id_new,
		d.rp_service_user_work_no_new,
		d.rp_service_user_name_new,
		d.bbc_service_user_id_new,
		d.bbc_service_user_work_no_new,
		d.bbc_service_user_name_new,	
		a.sales_value,a.rp_bbc_sales_value,a.rp_sales_value,a.bbc_sales_value	
	from 
		(
		select 
			customer_no,substr(sdt,1,6) smonth,
			regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') as sdt_last,
			sum(sales_value) as sales_value,
			sum(case when business_type_code in('1','4','5','6') then sales_value else 0 end) as rp_bbc_sales_value,
			sum(case when business_type_code in('1','4','5') then sales_value else 0 end) as rp_sales_value,
			sum(case when business_type_code in('6') then sales_value else 0 end) as bbc_sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:year_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and province_name = '平台-B') --平台酒水
			--安徽省城市服务商2.0，按大客户提成方案计算，当月 
				--福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				--福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				or (business_type_code in ('4') and customer_no in
				('120939','125719','124193','113829','124351','121337','120768'
				)))
			--and province_name in ('福建省')
			and (province_name !='福建省' or (province_name='福建省' and dc_name not like '%V2DC%')) --2.0 按仓库名称判断
			--202202月签呈，该客户已转代理人，不算提成，每月
			and customer_no not in ('122221','123086')
			--202202月签呈，公司BBC客户，不算提成，每月
			and customer_no not in ('123623')
			--202202月签呈，客户地采产品较多，不算提成，当月
			--and customer_no not in ('102866')
			--202203月签呈 不算提成和逾期 每月 
			and customer_no not in ('104192','123395','117927','126154')
			--202203月签呈 不算提成和逾期 当月 
			--and customer_no not in ('123859')
			--202204月签呈 不算提成和逾期 每月 
			and customer_no not in ('120459','121206')	
			--202205月签呈 不算提成 每月 
			and customer_no not in ('109435','111255')	
			--202207月签呈 不算提成 202207-202208 
			and customer_no not in ('116902','117705','122054','125137')
			--202207月签呈 不算提成 当月 
			--and customer_no not in ('126544','118007','126428')	
			-- 202207月签呈 代理人客户 不算提成 每月
			and customer_no not in ('127649')	
			--202209月签呈 不算提成 当月 
			and customer_no not in ('129525')			
		group by 
			customer_no,substr(sdt,1,6),regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
		)a 	
		left join   --CRM客户信息取每月最后一天
			(
			select 
				sdt,customer_id,customer_no,customer_name,sales_id,work_no,sales_name,
				case when channel_code='9' then '业务代理' end as ywdl_cust,
				case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust	
			from 
				csx_dw.dws_crm_w_a_customer 
			where 
				sdt>=${hiveconf:year_start_day}
				and sdt<=${hiveconf:month_end_day}
				and customer_no !=''
				and sdt=regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') --每月最后一天
			)b on b.customer_no=a.customer_no and b.sdt=a.sdt_last 			
		--关联客户对应服务管家
		join		
			(  
				select
					sdt,customer_no,rp_service_user_id_new,
					rp_service_user_work_no_new,
					rp_service_user_name_new,
					bbc_service_user_id_new,
					bbc_service_user_work_no_new,
					bbc_service_user_name_new					
				from csx_tmp.report_crm_w_a_customer_service_manager_info_business
				where sdt>=${hiveconf:year_start_day} and sdt<${hiveconf:month_end_day}
				and (rp_service_user_work_no_new is not null
				or bbc_service_user_work_no_new is not null)
				and sdt=regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','') --每月最后一天
				union all
				select
					${hiveconf:month_end_day} as sdt,customer_no,
					rp_service_user_id as rp_service_user_id_new,
					rp_service_user_work_no as rp_service_user_work_no_new,
					rp_service_user_name as rp_service_user_name_new,
					bbc_service_user_id as bbc_service_user_id_new,
					bbc_service_user_work_no as bbc_service_user_work_no_new,
					bbc_service_user_name as bbc_service_user_name_new					
				from csx_tmp.tc_r_customer_service_manager_info_new
				where  (rp_service_user_work_no is not null
				or bbc_service_user_work_no is not null)
			)d on d.customer_no=a.customer_no and d.sdt=a.sdt_last	
	where 
		b.ywdl_cust is null -- 剔除业务代理和内购 or b.customer_no in ('118689','116957','116629'))
		and b.ng_cust is null
;		
		

-- 服务管家年度累计销额提成比例
drop table csx_tmp.tc_r_service_rate_ytd; --5
create table csx_tmp.tc_r_service_rate_ytd
as
select 
	service_user_id,service_user_work_no,service_user_name,income_type,service_rp_bbc_sales_value_ytd,service_fl_sales_value_ytd,
	case when 
			((service_rp_bbc_sales_value_ytd<=10000000 and income_type in('Q1','Q2','Q3')) 
			or (service_rp_bbc_sales_value_ytd>10000000 and service_rp_bbc_sales_value_ytd<=20000000 and income_type in('Q2','Q3'))
			or (service_rp_bbc_sales_value_ytd>20000000 and service_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q3'))) then 0.002
		when ((service_rp_bbc_sales_value_ytd>10000000 and service_rp_bbc_sales_value_ytd<=20000000 and income_type in('Q1'))
			or (service_rp_bbc_sales_value_ytd>20000000 and service_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q2'))
			or (service_rp_bbc_sales_value_ytd>30000000 and service_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q3'))) then 0.0025
		when ((service_rp_bbc_sales_value_ytd>20000000 and service_rp_bbc_sales_value_ytd<=30000000 and income_type in('Q1'))
			or (service_rp_bbc_sales_value_ytd>30000000 and service_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q2'))
			or (service_rp_bbc_sales_value_ytd>40000000 and income_type in('Q3'))) then 0.003
		when ((service_rp_bbc_sales_value_ytd>30000000 and service_rp_bbc_sales_value_ytd<=40000000 and income_type in('Q1'))
			or (service_rp_bbc_sales_value_ytd>40000000 and income_type in('Q2'))) then 0.0035
		when (service_rp_bbc_sales_value_ytd>40000000 and income_type in('Q1')) then 0.004			
		else 0.002 end service_rp_bbc_sales_value_tc_rate,
	0.002 as service_fl_sales_value_tc_rate
from 
	(
	select 
		a.service_user_id,a.service_user_work_no,a.service_user_name,
		coalesce(c.income_type,'Q1') as income_type,
		sum(a.rp_bbc_sales_value) as service_rp_bbc_sales_value_ytd,
		null as service_fl_sales_value_ytd
	from 
		(
		select
			customer_no,smonth,rp_sales_value as rp_bbc_sales_value,
			rp_service_user_id_new as service_user_id,
			rp_service_user_work_no_new as service_user_work_no,
			rp_service_user_name_new as service_user_name
		from csx_tmp.tc_r_service_rate_ytd_0
		where rp_service_user_id_new is not null
		union all 
		select
			customer_no,smonth,bbc_sales_value as rp_bbc_sales_value,
			bbc_service_user_id_new as service_user_id,
			bbc_service_user_work_no_new as service_user_work_no,
			bbc_service_user_name_new as service_user_name
		from csx_tmp.tc_r_service_rate_ytd_0
		where bbc_service_user_id_new is not null		
		)a
	left join 
		(
		select 
			distinct work_no,income_type 
		from 
			csx_tmp.sales_income_info_new 
		where 
			sdt=${hiveconf:month_end_day}
		) c on c.work_no=a.service_user_work_no   --上月最后1日	split(a.service_user_work_no,';')[0] 新平台分号改了	
	group by 
		a.service_user_id,a.service_user_work_no,a.service_user_name,coalesce(c.income_type,'Q1')	
	)a 
where
	service_user_work_no !=''	
;--6


-- 客户本月销售额、定价毛利额统计
drop table csx_tmp.tc_r_sales_value_profit_00;--7
create table csx_tmp.tc_r_sales_value_profit_00
as
select 
	b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,a.customer_no,b.customer_name,a.smonth,
	-- 销售额
	--202208签呈，128133 128180 128163 127336 日配扣减销售额 当月
	--202209签呈，调整客户日配销售额与毛利额 114387 117816  当月
	sum(if(a.customer_no='114387',sales_value-87249.28,
		if(a.customer_no='117816',sales_value-26289.88,sales_value))) as sales_value, -- 客户总销售额
	sum(if(a.customer_no='114387',rp_sales_value-87249.28,
		if(a.customer_no='117816',rp_sales_value-26289.88,rp_sales_value))) as rp_sales_value, -- 客户日配销售额		
	--sum(sales_value) as sales_value, -- 客户总销售额
	--sum(rp_sales_value) as rp_sales_value, -- 客户日配销售额
	sum(bbc_sales_value) as bbc_sales_value, -- 客户bbc销售额
	sum(fl_sales_value) as fl_sales_value, -- 客户福利销售额
	sum(rp_sales_value)+sum(bbc_sales_value) as rp_bbc_sales_value,
	-- 定价毛利额
	--202209签呈，调整客户日配销售额与毛利额 114387 117816  当月
	sum(if(a.customer_no='114387',profit-87249.28,
		if(a.customer_no='117816',profit-26289.88,profit))) as profit,-- 客户总定价毛利额
	sum(if(a.customer_no='114387',rp_profit-87249.28,
		if(a.customer_no='117816',rp_profit-26289.88,rp_profit))) as rp_profit,-- 客户日配定价毛利额
	sum(bbc_profit) as bbc_profit,-- 客户bbc定价毛利额
	sum(fl_profit) as fl_profit,-- 客户福利定价毛利额
	sum(rp_profit)+sum(bbc_profit) as rp_bbc_profit
from 
	(
	select 
		customer_no,substr(sdt,1,6) as smonth,
		-- 各类型销售额
		sum(sales_value) as sales_value,
		--202208签呈，毛利核算中125533客户业务类型BBC改福利 5-8月
		sum(case when business_type_code in ('1','4','5') then sales_value else 0 end) as rp_sales_value,
		sum(case when business_type_code in('6') and customer_no not in ('125533') then sales_value else 0 end) as bbc_sales_value,
		sum(case when business_type_code in('2') or (customer_no in ('125533') and business_type_code in('6')) then sales_value else 0 end) as fl_sales_value,
		-- 各类型定价毛利额
		--202208签呈，毛利核算中125533客户业务类型BBC改福利 5-8月
		sum(case when dc_code <>'W0K4' then profit else 0 end) as profit, --W0K4只计算销售额 不计算定价毛利额 每月
		sum(case when business_type_code in ('1','4','5') and dc_code <>'W0K4' then profit else 0 end) as rp_profit,
		sum(case when business_type_code in('6') and customer_no not in ('125533') and dc_code <>'W0K4' then profit else 0 end) as bbc_profit,
		sum(case when (business_type_code in('2') or (customer_no in ('125533') and business_type_code in('6'))) and dc_code <>'W0K4' then profit else 0 end) as fl_profit
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:month_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
			and (business_type_code in('1','2','6')
				or (business_type_code in('2','5') and province_name = '平台-B') --平台酒水
				--安徽省城市服务商2.0，按大客户提成方案计算，当月 
				--福建省'127923'为个人开发客户 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				--福建省'126690' 城市服务商业务销售额*0.2% 不计算毛利提成 每月
				or (business_type_code in ('4') and customer_no in
				('120939','125719','124193','113829','124351','121337','120768'
				)))
			--and province_name in ('福建省')
			and (province_name !='福建省' or (province_name='福建省' and dc_name not like '%V2DC%')) --2.0 按仓库名称判断
			--202202月签呈，该客户已转代理人，不算提成，每月
			and customer_no not in ('122221','123086')
			--202202月签呈，不算提成，当月
			--and customer_no not in ('125613','124379','125247','125256','124025','124667','124621','124370','125469','123599','124782')
			--202202月签呈，公司BBC客户，不算提成，每月
			and customer_no not in ('123623')
			--202202月签呈，客户地采产品较多，不算提成，当月
			--and customer_no not in ('102866')
			--202202月签呈，剔除直送客户，当月
			--and customer_no not in ('114834','111832','123685','120723','124367','124387','124416','119760','121425','121841','123553')	
			--202203月签呈 不算提成和逾期 每月 
			and customer_no not in ('104192','123395','117927','126154')
			--202203月签呈 不算提成和逾期 当月 
			--and customer_no not in ('123859')
			--202204月签呈 不算提成和逾期 每月 
			and customer_no not in ('120459','121206')	
			--202205月签呈 不算提成 每月 
			and customer_no not in ('109435','111255')
			--202206月签呈 剔除销售额及毛利额 当月
			--and customer_no not in ('125111','127156')
			--202207月签呈 不算提成 202207-202208 
			and customer_no not in ('116902','117705','122054','125137')
			--202207月签呈 不算提成 当月 
			--and customer_no not in ('126544','118007','126428')	
			-- 202207月签呈 代理人客户 不算提成 每月
			and customer_no not in ('127649')
			--202209月签呈 不算提成 当月 
			and customer_no not in ('129525')			
	group by 
		customer_no,substr(sdt,1,6)
		
		
	--扣减定价毛利额
	--重庆市
	--union all select '105569' as customer_no,'202206' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	---10000.00 as profit,-10000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	--四川省 每月
	union all select '124403' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-6300.00 as profit,-6300.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '108835' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-3500.00 as profit,-3500.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
		
	--河南省 扣减1万元 202207-202209
	union all select '127969' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-10000.00 as profit,-10000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--202209签呈 福建省 每月
	union all select '123540' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-7000.00 as profit,-7000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '120589' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
		-12800.00 as profit,-12800.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	union all select '115971' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-8000.00 as profit,-8000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--202209签呈 安徽省 当月
union all select '114853' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1166.69 as profit,0.00 as rp_profit, -1166.69 as bbc_profit, 0.00 as fl_profit
union all select '124046' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-7951.73 as profit,0.00 as rp_profit, -7951.73 as bbc_profit, 0.00 as fl_profit
union all select '119892' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5600.00 as profit,0.00 as rp_profit, -5600.00 as bbc_profit, 0.00 as fl_profit
union all select '107411' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-6669.00 as profit,-6669.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '121233' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-9842.16 as profit,-9842.16 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '115883' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-7581.00 as profit,-7581.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120428' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-6269.55 as profit,-6269.55 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '119539' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1526.13 as profit,-1526.13 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120430' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1725.80 as profit,-1725.80 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '109460' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2245.73 as profit,-2245.73 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '121039' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-521.15 as profit,-521.15 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128086' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-999.56 as profit,-999.56 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '107890' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-730.56 as profit,-730.56 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124338' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-7784.93 as profit,-7784.93 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128847' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-728.13 as profit,0.00 as rp_profit, -728.13 as bbc_profit, 0.00 as fl_profit
union all select '128843' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-29.65 as profit,0.00 as rp_profit, -29.65 as bbc_profit, 0.00 as fl_profit
union all select '128854' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-171.33 as profit,0.00 as rp_profit, -171.33 as bbc_profit, 0.00 as fl_profit
union all select '128616' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1571.47 as profit,-1571.47 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '125686' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2274.78 as profit,-2274.78 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '121967' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2636.38 as profit,-2636.38 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129140' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1171.74 as profit,-1171.74 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128924' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-831.37 as profit,-831.37 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129123' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-744.98 as profit,-744.98 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129533' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-633.00 as profit,-633.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129133' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-423.00 as profit,-423.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129146' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3627.03 as profit,-3627.03 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129131' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3309.95 as profit,-3309.95 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129141' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2672.43 as profit,-2672.43 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129132' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2317.76 as profit,-2317.76 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129136' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2189.27 as profit,-2189.27 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124175' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2059.35 as profit,-2059.35 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127806' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2044.89 as profit,-2044.89 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127810' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1980.20 as profit,-1980.20 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127717' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1546.46 as profit,-1546.46 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129145' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1415.30 as profit,-1415.30 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127781' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1396.30 as profit,-1396.30 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129092' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1379.90 as profit,-1379.90 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '105870' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1363.64 as profit,-1363.64 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '106600' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-750.00 as profit,-750.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--202209签呈 北京市 当月
union all select '129731' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-83.00 as profit,0.00 as rp_profit, -83.00 as bbc_profit, 0.00 as fl_profit
union all select '129581' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-7000.00 as profit,-7000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129235' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-865.00 as profit,-865.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '129098' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5000.00 as profit,-5000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128789' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2330.03 as profit,0.00 as rp_profit, -2330.03 as bbc_profit, 0.00 as fl_profit
union all select '128441' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-684.37 as profit,-684.37 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128359' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-33000.00 as profit,-33000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128177' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-393.54 as profit,-393.54 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128201' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1859.90 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -1859.90 as fl_profit
union all select '128258' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-285.24 as profit,-285.24 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128194' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5738.04 as profit,-5738.04 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128210' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-10295.83 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -10295.83 as fl_profit
union all select '127769' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1859.90 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -1859.90 as fl_profit
union all select '127634' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-6023.28 as profit,-5738.04 as rp_profit, 0.00 as bbc_profit, -285.24 as fl_profit
union all select '126379' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-43.70 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -43.70 as fl_profit
union all select '125592' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-16251.95 as profit,0.00 as rp_profit, -16251.95 as bbc_profit, 0.00 as fl_profit
union all select '125004' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-22329.34 as profit,0.00 as rp_profit, -22329.34 as bbc_profit, 0.00 as fl_profit
union all select '124960' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-398.94 as profit,0.00 as rp_profit, -398.94 as bbc_profit, 0.00 as fl_profit
union all select '124939' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-113.31 as profit,0.00 as rp_profit, -113.31 as bbc_profit, 0.00 as fl_profit
union all select '124934' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-30.03 as profit,0.00 as rp_profit, -30.03 as bbc_profit, 0.00 as fl_profit
union all select '124947' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-67.28 as profit,0.00 as rp_profit, -67.28 as bbc_profit, 0.00 as fl_profit
union all select '124933' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-10.02 as profit,0.00 as rp_profit, -10.02 as bbc_profit, 0.00 as fl_profit
union all select '124929' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-171.16 as profit,0.00 as rp_profit, -171.16 as bbc_profit, 0.00 as fl_profit
union all select '124911' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-115.41 as profit,0.00 as rp_profit, -115.41 as bbc_profit, 0.00 as fl_profit
union all select '124837' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-577.18 as profit,0.00 as rp_profit, -577.18 as bbc_profit, 0.00 as fl_profit
union all select '124316' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-280.66 as profit,-280.66 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '123920' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-935.84 as profit,-935.84 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '123915' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-148.66 as profit,-148.66 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '123869' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-472.69 as profit,-472.69 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '123870' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-962.14 as profit,-962.14 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '122990' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-41669.49 as profit,-41669.49 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '122352' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-23881.04 as profit,-12076.27 as rp_profit, -11804.77 as bbc_profit, 0.00 as fl_profit
union all select '122204' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-18599.71 as profit,-18599.71 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '121927' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5000.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -5000.00 as fl_profit
union all select '121919' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5000.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -5000.00 as fl_profit
union all select '121900' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4939.20 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -4939.20 as fl_profit
union all select '121874' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-19670.71 as profit,0.00 as rp_profit, -19670.71 as bbc_profit, 0.00 as fl_profit
union all select '121862' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-6512.85 as profit,0.00 as rp_profit, -6512.85 as bbc_profit, 0.00 as fl_profit
union all select '121569' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5000.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -5000.00 as fl_profit
union all select '121201' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-10237.78 as profit,0.00 as rp_profit, -10237.78 as bbc_profit, 0.00 as fl_profit
union all select '120993' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-8615.93 as profit,0.00 as rp_profit, -8615.93 as bbc_profit, 0.00 as fl_profit
union all select '120975' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-388.00 as profit,-1.00 as rp_profit, 0.00 as bbc_profit, -387.00 as fl_profit
union all select '120953' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3975.62 as profit,-3975.62 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120952' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-67.47 as profit,-67.47 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120946' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-192.96 as profit,-192.96 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120892' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-407.37 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -407.37 as fl_profit
union all select '120900' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1629.27 as profit,-1629.27 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120794' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-341.85 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -341.85 as fl_profit
union all select '120805' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-90.35 as profit,-90.35 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120771' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4043.00 as profit,-67.00 as rp_profit, 0.00 as bbc_profit, -3976.00 as fl_profit
union all select '120239' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2829.86 as profit,0.00 as rp_profit, -2829.86 as bbc_profit, 0.00 as fl_profit
union all select '120301' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5297.12 as profit,-5297.12 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120054' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2049.68 as profit,-2049.68 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120042' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3242.04 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -3242.04 as fl_profit
union all select '119997' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-582.34 as profit,-582.34 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '119973' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-193.00 as profit,-193.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '119824' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2036.00 as profit,-1629.00 as rp_profit, 0.00 as bbc_profit, -407.00 as fl_profit
union all select '119811' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1837.73 as profit,-1837.73 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '119780' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-46380.93 as profit,-46380.93 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '119320' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-432.00 as profit,-90.00 as rp_profit, 0.00 as bbc_profit, -342.00 as fl_profit
union all select '119019' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4286.54 as profit,-4286.54 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '118724' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3033.00 as profit,-3033.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '118428' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2830.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -2830.00 as fl_profit
union all select '118318' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5297.00 as profit,-5297.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '118306' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2050.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -2050.00 as fl_profit
union all select '115303' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3242.00 as profit,-3242.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '114494' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-38753.99 as profit,-38753.99 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '113609' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3636.92 as profit,-3636.92 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '111608' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-8000.00 as profit,-8000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '111214' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-774.27 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -774.27 as fl_profit
union all select '109195' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-760.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -760.00 as fl_profit
union all select '104617' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-582.00 as profit,-582.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '103145' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4823.00 as profit,-4823.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '103207' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5930.00 as profit,-5930.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '103155' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1845.00 as profit,-7.00 as rp_profit, 0.00 as bbc_profit, -1838.00 as fl_profit
union all select '105687' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2670.32 as profit,-2670.32 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '105696' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-9173.63 as profit,-9173.63 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '105695' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3058.48 as profit,-3058.48 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '105686' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2097.57 as profit,-2097.57 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--202209签呈 江苏苏州 当月
union all select '112365' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2067.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -2067.00 as fl_profit
union all select '116659' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-966.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -966.00 as fl_profit
union all select '119819' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-342.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -342.00 as fl_profit
union all select '113090' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3000.00 as profit,-3000.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '123561' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-51968.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -51968.00 as fl_profit
union all select '121104' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	3375.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, 3375.00 as fl_profit
	
	--202209签呈 上海松江 当月
union all select '104901' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-11792.35 as profit,-11792.35 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '105381' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-9221.64 as profit,-9221.64 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '107059' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4834.32 as profit,-4834.32 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '114872' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2652.25 as profit,-2652.25 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117755' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2957.23 as profit,-2957.23 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '118288' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-11710.07 as profit,-11710.07 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '112920' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2858.06 as profit,-2858.06 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117007' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3797.88 as profit,-3797.88 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120467' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4758.90 as profit,-4758.90 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--202209签呈 重庆市 当月
union all select '121061' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-104047.39 as profit,-104047.39 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '122129' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2713.60 as profit,-2713.60 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '121626' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-81841.20 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -81841.20 as fl_profit
union all select '121606' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-620.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -620.00 as fl_profit
union all select '129295' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-624.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -624.00 as fl_profit
union all select '115315' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2807.79 as profit,-2807.79 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '122606' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2609.27 as profit,-2609.27 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '116101' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2580.48 as profit,-2580.48 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128180' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2214.50 as profit,-2214.50 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '125572' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-30559.31 as profit,-30559.31 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128779' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1036.83 as profit,-1036.83 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '112177' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-9039.95 as profit,-9039.95 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '115554' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-7137.56 as profit,-7137.56 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '118206' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4774.46 as profit,-4774.46 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '104965' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1694.21 as profit,-1694.21 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '112813' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2256.39 as profit,-2256.39 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117753' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1392.54 as profit,-1392.54 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117727' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1796.76 as profit,-1796.76 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117728' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1472.43 as profit,-1472.43 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117729' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1618.90 as profit,-1618.90 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117748' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1370.42 as profit,-1370.42 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117773' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-541.90 as profit,-541.90 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117776' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1465.96 as profit,-1465.96 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117782' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1153.00 as profit,-1153.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117790' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2253.02 as profit,-2253.02 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117791' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-941.46 as profit,-941.46 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117795' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1002.07 as profit,-1002.07 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117800' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-708.22 as profit,-708.22 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117805' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2865.72 as profit,-2865.72 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124044' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1828.30 as profit,-1828.30 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124004' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1270.27 as profit,-1270.27 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124577' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-8473.35 as profit,-8473.35 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124638' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5671.84 as profit,-5671.84 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124650' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-7754.97 as profit,-7754.97 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124606' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-6189.64 as profit,-6189.64 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128645' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5745.43 as profit,-5745.43 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128473' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2161.23 as profit,-2161.23 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128491' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2318.78 as profit,-2318.78 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128179' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2314.52 as profit,-2314.52 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128464' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-798.09 as profit,-798.09 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128486' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4290.47 as profit,-4290.47 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128492' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3024.99 as profit,-3024.99 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127682' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4773.60 as profit,-4773.60 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127684' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3120.08 as profit,-3120.08 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127692' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2556.19 as profit,-2556.19 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127693' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4601.50 as profit,-4601.50 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127701' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-7215.33 as profit,-7215.33 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128667' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3147.64 as profit,-3147.64 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128668' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4028.77 as profit,-4028.77 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128695' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3797.70 as profit,-3797.70 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128696' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3557.38 as profit,-3557.38 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128697' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3493.46 as profit,-3493.46 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '117920' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1844.51 as profit,-1844.51 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '125434' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1796.16 as profit,-1796.16 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124505' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4050.58 as profit,-4050.58 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124353' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-6710.31 as profit,-6710.31 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '124474' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2671.86 as profit,-2671.86 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127333' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5893.74 as profit,-5893.74 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '125311' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2419.52 as profit,-2419.52 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '112160' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-16742.44 as profit,-12380.51 as rp_profit, -4361.93 as bbc_profit, 0.00 as fl_profit
union all select '121340' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-5940.91 as profit,0.00 as rp_profit, -5940.91 as bbc_profit, 0.00 as fl_profit
union all select '119659' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1838.93 as profit,-1838.93 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120105' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-271.58 as profit,-271.58 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120623' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-6491.31 as profit,-6491.31 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120497' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-4253.32 as profit,0.00 as rp_profit, -4253.32 as bbc_profit, 0.00 as fl_profit
union all select '122247' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1420.87 as profit,-1420.87 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120976' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-689.56 as profit,-689.56 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '125679' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-695.60 as profit,-695.60 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '118738' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1813.26 as profit,-1813.26 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '125745' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-766.51 as profit,-766.51 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '119042' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-569.54 as profit,-569.54 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127275' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1307.93 as profit,-1307.93 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128608' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3309.22 as profit,-3309.22 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '110866' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1309.22 as profit,-1309.22 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '123084' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1592.01 as profit,0.00 as rp_profit, -1592.01 as bbc_profit, 0.00 as fl_profit
union all select '107099' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-6426.86 as profit,-6426.86 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '122103' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-2437.39 as profit,-2437.39 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127165' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-873.87 as profit,-873.87 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128202' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1465.64 as profit,-1465.64 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '118212' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3185.82 as profit,0.00 as rp_profit, -3185.82 as bbc_profit, 0.00 as fl_profit
union all select '117721' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-692.07 as profit,-692.07 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '114287' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3158.60 as profit,-3158.60 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '112803' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3483.41 as profit,-3483.41 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '113423' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-3096.53 as profit,-3096.53 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '120924' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-27539.90 as profit,-27539.90 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '127668' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1083.59 as profit,0.00 as rp_profit, -1083.59 as bbc_profit, 0.00 as fl_profit
union all select '129291' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-487.65 as profit,-487.65 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '128950' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1362.85 as profit,-1362.85 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--202209签呈 福建省 当月
union all select '105975' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1875.37 as profit,-1875.37 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '125861' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-337.50 as profit,-337.50 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '105947' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-725.00 as profit,-725.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '106000' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1887.50 as profit,-1887.50 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '113443' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-375.00 as profit,-375.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '113576' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-674.99 as profit,-674.99 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '113569' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-235.00 as profit,-235.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '126109' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-756.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -756.00 as fl_profit
union all select '119688' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1265.00 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -1265.00 as fl_profit
union all select '129143' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-1475.70 as profit,0.00 as rp_profit, 0.00 as bbc_profit, -1475.70 as fl_profit
union all select '129075' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-500.00 as profit,-500.00 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
union all select '114099' as customer_no,'202209' as smonth,0 as sales_value, 0 as rp_sales_value,0 as bbc_sales_value,0 as fl_sales_value,
	-562.50 as profit,-562.50 as rp_profit, 0.00 as bbc_profit, 0.00 as fl_profit
	
	--202209签呈 XX省 当月	
	)a
	left join 
		(
		select 
			distinct customer_id,customer_no,customer_name,work_no,sales_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day}
			and customer_no !=''
		)b on b.customer_no=a.customer_no
where 
	b.ywdl_cust is null -- or b.customer_no in ('118689','116957','116629'))
	and b.ng_cust is null
group by 
	b.sales_region_code,b.sales_region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,a.customer_no,b.customer_name,a.smonth
;--8

-- 客户定价毛利额扣点、退货金额统计
drop table csx_tmp.tc_r_sales_value_profit_01;--9
create table csx_tmp.tc_r_sales_value_profit_01
as
select 	
	a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_no,a.customer_name,a.smonth,
	c.customer_id,c.sales_id,c.work_no,c.sales_name,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	c.rp_sales_sale_fp_rate,
	c.rp_sales_profit_fp_rate,
	c.fl_sales_sale_fp_rate,
	c.fl_sales_profit_fp_rate,
	c.bbc_sales_sale_fp_rate,
	c.bbc_sales_profit_fp_rate,
	c.rp_service_user_sale_fp_rate,
	
	c.rp_service_user_profit_fp_rate,
	c.fl_service_user_sale_fp_rate,
	c.fl_service_user_profit_fp_rate,
	c.bbc_service_user_sale_fp_rate,
	c.bbc_service_user_profit_fp_rate,
	-- 销售额
	sales_value, -- 客户总销售额
	rp_sales_value, -- 客户日配销售额
	bbc_sales_value, -- 客户bbc销售额
	fl_sales_value, -- 客户福利销售额
	rp_bbc_sales_value, -- 客户日配&bbc销售额
	-- 定价毛利额
	(rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))+
	(bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0))+
	(fl_profit-coalesce(fl_sales_value*fl_kd_rate,0)) as profit,-- 客户总定价毛利额
	
	rp_profit-coalesce(rp_sales_value*rp_kd_rate,0) as rp_profit,-- 客户日配定价毛利额
	bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0) as bbc_profit,-- 客户bbc定价毛利额
	fl_profit-coalesce(fl_sales_value*fl_kd_rate,0) as fl_profit,-- 客户福利定价毛利额
	
	(rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))+
	(bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0)) as rp_bbc_profit,
	--定价毛利率
	coalesce(((rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))+
	(bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0))+
	(fl_profit-coalesce(fl_sales_value*fl_kd_rate,0)))/abs(sales_value),0) as prorate, -- 客户总定价毛利率
	
	coalesce((rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))/abs(rp_sales_value),0) as rp_prorate, -- 客户日配定价毛利率
	coalesce((bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0))/abs(bbc_sales_value),0) as bbc_prorate, -- 客户bbc定价毛利率
	coalesce((fl_profit-coalesce(fl_sales_value*fl_kd_rate,0))/abs(fl_sales_value),0) as fl_prorate,-- 客户福利定价毛利率
	coalesce(((rp_profit-coalesce(rp_sales_value*rp_kd_rate,0))+
	(bbc_profit-coalesce(bbc_sales_value*bbc_kd_rate,0)))/abs(rp_bbc_sales_value),0) as rp_bbc_prorate,
	coalesce(d.refund_sales_value,0) as refund_sales_value,
	coalesce(d.refund_rp_sales_value,0) as refund_rp_sales_value,
	coalesce(d.refund_bbc_sales_value,0) as refund_bbc_sales_value,
	coalesce(d.refund_rp_bbc_sales_value,0) as refund_rp_bbc_sales_value,
	coalesce(d.refund_fl_sales_value,0) as refund_fl_sales_value,
	coalesce(d.w0k4_sales_value,0) as w0k4_sales_value,
	coalesce(d.w0k4_rp_sales_value,0) as w0k4_rp_sales_value,
	coalesce(d.w0k4_bbc_sales_value,0) as w0k4_bbc_sales_value,
	coalesce(d.w0k4_rp_bbc_sales_value,0) as w0k4_rp_bbc_sales_value,
	coalesce(d.w0k4_fl_sales_value,0) as w0k4_fl_sales_value	
from
	csx_tmp.tc_r_sales_value_profit_00 a 
	left join
		(
		--202201月签呈，重庆市 每月
		select 'X000000' as customer_no, 0.00 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--union all select '105186' as customer_no, 0.01 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--union all select '118206' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		


		--202209月签呈，福建省 每月
		union all select '102734' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '102784' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '102901' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113263' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '115366' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select 'PF0649' as customer_no, 0.090 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '114038' as customer_no, 0.000 as rp_kd_rate, 0.048 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113088' as customer_no, 0.040 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '102924' as customer_no, 0.000 as rp_kd_rate, 0.030 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '102524' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '105703' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '105750' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '106698' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '112553' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113678' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113679' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113746' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113760' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113805' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '115602' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '115656' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '117244' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '104281' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '107398' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '115826' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '119213' as customer_no, 0.000 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '108589' as customer_no, 0.000 as rp_kd_rate, 0.040 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118653' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118654' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118682' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118705' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118730' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118934' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118961' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '119185' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '119172' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '120666' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118689' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '116355' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '121054' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.020 as fl_kd_rate
		union all select '105150' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '105156' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '105177' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '105181' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '105182' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '106423' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '106721' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '107404' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '105164' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '105165' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '119990' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '106805' as customer_no, 0.100 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '123252' as customer_no, 0.000 as rp_kd_rate, 0.080 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '106587' as customer_no, 0.000 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.050 as fl_kd_rate
		union all select '108557' as customer_no, 0.000 as rp_kd_rate, 0.020 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '116131' as customer_no, 0.000 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.060 as fl_kd_rate
		union all select '103782' as customer_no, 0.000 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.060 as fl_kd_rate
		union all select '116707' as customer_no, 0.000 as rp_kd_rate, 0.030 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '116561' as customer_no, 0.000 as rp_kd_rate, 0.080 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '116947' as customer_no, 0.010 as rp_kd_rate, 0.010 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '111207' as customer_no, 0.010 as rp_kd_rate, 0.010 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '125534' as customer_no, 0.010 as rp_kd_rate, 0.010 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '115537' as customer_no, 0.010 as rp_kd_rate, 0.010 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '126331' as customer_no, 0.000 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.053 as fl_kd_rate
		union all select '125840' as customer_no, 0.040 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select 'PF0500' as customer_no, 0.063 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '125179' as customer_no, 0.005 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '124830' as customer_no, 0.005 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '124278' as customer_no, 0.005 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117671' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '119235' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.010 as fl_kd_rate
		union all select '127442' as customer_no, 0.000 as rp_kd_rate, 0.050 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '128158' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '106493' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '124250' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '120250' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '124006' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117884' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '128357' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '128751' as customer_no, 0.020 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '125288' as customer_no, 0.005 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select 'PF0365' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '102995' as customer_no, 0.010 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '104955' as customer_no, 0.280 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '104970' as customer_no, 0.280 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '125333' as customer_no, 0.130 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '129134' as customer_no, 0.130 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '129165' as customer_no, 0.130 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113772' as customer_no, 0.130 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '129160' as customer_no, 0.130 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '129149' as customer_no, 0.130 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '121357' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '101653' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select 'PF1205' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '120994' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '129531' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '129547' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '129630' as customer_no, 0.042 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117676' as customer_no, 0.130 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '116231' as customer_no, 0.000 as rp_kd_rate, 0.050 as bbc_kd_rate, 0.000 as fl_kd_rate
		


		--202201月签呈，河北省，每月
		union all select '112285' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '112024' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '123035' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--202201月签呈，河北省，当月
		--union all select '122551' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		--202202月签呈，四川省，每月
		union all select '116401' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '104493' as customer_no, 0.06 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118041' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118208' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '117217' as customer_no, 0.11 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119354' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '122347' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '122860' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '123706' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '124061' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '123755' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '115191' as customer_no, 0.12 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118564' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '122628' as customer_no, 0.10 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118770' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '120567' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '118299' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '113974' as customer_no, 0.03 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '125201' as customer_no, 0.08 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '125191' as customer_no, 0.02 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '125344' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126137' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126981' as customer_no, 0.04 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126660' as customer_no, 0.028 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '119865' as customer_no, 0.028 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '126691' as customer_no, 0.028 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '121309' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '127030' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '127856' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '127811' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '120175' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '106923' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113454' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '128912' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '128999' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118312' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '128674' as customer_no, 0.060 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '128915' as customer_no, 0.060 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '128743' as customer_no, 0.060 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '118057' as customer_no, 0.060 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate

		--浙江省 每月
		union all select '108905' as customer_no, 0.05 as rp_kd_rate, 0.00 as bbc_kd_rate, 0.00 as fl_kd_rate
		union all select '111834' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		--贵州省 每月 
		union all select '113873' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113918' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113935' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113936' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '113940' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117137' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117142' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '117143' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '115479' as customer_no, 0.050 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '123999' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '115341' as customer_no, 0.030 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '125298' as customer_no, 0.080 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate
		union all select '125678' as customer_no, 0.080 as rp_kd_rate, 0.000 as bbc_kd_rate, 0.000 as fl_kd_rate


		) b on b.customer_no=a.customer_no
	--关联销售员、服务管家
	left join		
		(  
		select 
			*
		from 
			csx_tmp.tc_r_customer_service_manager_info_new
		)c on c.customer_no=a.customer_no
	--退货金额统计、W0K4仓销售金额统计
	left join
		(	
		select 
			customer_no,substr(sdt,1,6) as smonth,
			-- 各类型退货金额
			sum(case when return_flag='X' then sales_value else 0 end) as refund_sales_value, 
			sum(case when return_flag='X' and business_type_code in('1','4') then sales_value else 0 end) as refund_rp_sales_value,
			sum(case when return_flag='X' and business_type_code in('6') then sales_value else 0 end) as refund_bbc_sales_value,
			sum(case when return_flag='X' and business_type_code in('1','6','4') then sales_value else 0 end) as refund_rp_bbc_sales_value,
			sum(case when return_flag='X' and business_type_code in('2') then sales_value else 0 end) as refund_fl_sales_value,
			-- W0K4仓销售金额
			sum(case when dc_code='W0K4' then sales_value else 0 end) as w0k4_sales_value, 
			sum(case when dc_code='W0K4' and business_type_code in('1','4') then sales_value else 0 end) as w0k4_rp_sales_value,
			sum(case when dc_code='W0K4' and business_type_code in('6') then sales_value else 0 end) as w0k4_bbc_sales_value,
			sum(case when dc_code='W0K4' and business_type_code in('1','6','4') then sales_value else 0 end) as w0k4_rp_bbc_sales_value,
			sum(case when dc_code='W0K4' and business_type_code in('2') then sales_value else 0 end) as w0k4_fl_sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>=${hiveconf:month_start_day} and sdt<=${hiveconf:month_end_day}
			and channel_code in('1','7','9')
			and (return_flag='X' or dc_code='W0K4')
		group by 
			customer_no,substr(sdt,1,6)
		)d on d.customer_no=a.customer_no and d.smonth=a.smonth		
;--10



			


-- 销售员本月定价毛利率，计算销售员定价毛利额提成比例
drop table csx_tmp.tc_r_sales_profit_tc_rate;--11
create table csx_tmp.tc_r_sales_profit_tc_rate
as
select
	work_no,sales_name,sales_sales_value,sales_rp_bbc_sales_value,sales_fl_sales_value,sales_profit,sales_rp_bbc_profit,
	sales_fl_profit,sales_prorate,sales_rp_bbc_prorate,sales_fl_prorate,
	sales_rp_prorate,sales_bbc_prorate,
	-- 日配&bbc定价毛利额提成比例
		case when sales_rp_bbc_prorate<0.08 then 0
			when sales_rp_bbc_prorate>=0.08 and sales_rp_bbc_prorate<0.12 then 0.03
			when sales_rp_bbc_prorate>=0.12 and sales_rp_bbc_prorate<0.15 then 0.033
			when sales_rp_bbc_prorate>=0.15 and sales_rp_bbc_prorate<0.18 then 0.035
			when sales_rp_bbc_prorate>=0.18 and sales_rp_bbc_prorate<0.2 then 0.04
			when sales_rp_bbc_prorate>=0.2 then 0.05
			else 0 end as sales_rp_bbc_profit_tc_rate,
	-- 福利定价毛利额提成比例
		case when sales_fl_prorate<0.03 then 0
			when sales_fl_prorate>=0.03 and sales_fl_prorate<0.05 then 0.02
			when sales_fl_prorate>=0.05 and sales_fl_prorate<0.08 then 0.025
			when sales_fl_prorate>=0.08 and sales_fl_prorate<0.1 then 0.03
			when sales_fl_prorate>=0.1 and sales_fl_prorate<0.15 then 0.04
			when sales_fl_prorate>=0.15 then 0.05
			else 0 end as sales_fl_profit_tc_rate	
from
	(
	select 	
		work_no,sales_name,
		-- 销售额
		sum(sales_value-w0k4_sales_value) as sales_sales_value, -- 总销售额
		sum(rp_sales_value-w0k4_rp_sales_value) as sales_rp_sales_value, -- 日配销售额
		sum(bbc_sales_value-w0k4_bbc_sales_value) as sales_bbc_sales_value, -- bbc销售额
		sum(rp_bbc_sales_value-w0k4_rp_bbc_sales_value) as sales_rp_bbc_sales_value, -- 日配&bbc销售额
		sum(fl_sales_value-w0k4_fl_sales_value) as sales_fl_sales_value, -- 福利销售额
		-- 定价毛利额
		sum(profit) as sales_profit,-- 总定价毛利额
		sum(rp_profit) as sales_rp_profit,-- 日配定价毛利额
		sum(bbc_profit) as sales_bbc_profit,--bbc定价毛利额
		sum(rp_bbc_profit) as sales_rp_bbc_profit,-- 日配&bbc定价毛利额
		sum(fl_profit) as sales_fl_profit,-- 福利定价毛利额
		--定价毛利率
		sum(profit)/abs(sum(sales_value-w0k4_sales_value)) as sales_prorate, -- 总定价毛利率
		sum(rp_profit)/abs(sum(rp_sales_value-w0k4_rp_sales_value)) as sales_rp_prorate,
		sum(bbc_profit)/abs(sum(bbc_sales_value-w0k4_bbc_sales_value)) as sales_bbc_prorate,
		sum(rp_bbc_profit)/abs(sum(rp_bbc_sales_value-w0k4_rp_bbc_sales_value)) as sales_rp_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
		sum(fl_profit)/abs(sum(fl_sales_value-w0k4_fl_sales_value)) as sales_fl_prorate -- 销售员本月福利定价毛利率
	from
		csx_tmp.tc_r_sales_value_profit_01
	where
		--202202月签呈，安徽省剔除以下客户后再算销售员的定价毛利率，服务费，当月
		--customer_no not in ('104402','104885','121452','107415','113731','113857','122147','122159','122167','122185','122186','122188','125274')
		--202202月签呈，剔除直送客户后再算销售员定价毛利率，当月
		--and customer_no not in ('114834','111832','123685','120723','124367','124387','124416','119760','121425','121841','123553')
		--202202月签呈，剔除武警客户，当月
		--and customer_no not in ('120595','124584','124784','125029','125028','125017','116932','120830','122837')
		--202203月签呈 直送客户 不计算销售员整体毛利率 每月
		customer_no not in ('124652')
		--202203月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','121452','107415','113731','113857','122147','122159','122167','122185','122186','122188','126406','113439')
		--202203月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('123685','116566','125360','123122','111832','111691','120723','115221','112906','118509','117817','121472','121994','124356',
		--	'124367','124387','124401','124416','119760','125767')
		--202204月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','123287','107415','121452','113857','122147','122167','113439','125770','126503','126571','126644','126406')
		--202204月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('114834','116566','111832','120723','112906','118509','117817','121472','121994','124356','124367','124387','124401','124416','119760','121841','125767')
		--202205月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','127396','107415','121452','113857','122147','122167','122159','122185','122186','122188')	
		--202205月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('116566','120723','112906','118509','117817','121472','121994','124356','124367','124387','124401','124416','119760','121841','115221','126084')
		--202205月签呈 河南省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126069','126113')
		--202205月签呈 河南省 剔除直送客户后再算销售员定价毛利率 每月
		and customer_no not in ('126259','127312','127336')	
		--202206月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','115286','127396','121452','107415','113857','122147','122159','122167','122185','122186','122188','125770','126644','127747','127754',
		--'127543','127592','127698','127728','127729','127739','127743','127745','127746','127750','127751','127753','127755','127760','127766','127767','127775','127553',
		--'127737','127756')	
		--202206月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126935','126084','128005','116566','120333','120723','112906','118509','122264','117817','121994','124356','124367','124387','124401','124416',
		--'119760','121841','125767','127262','114859','115681','115941','120872','120879','121287','125722','120768','121384','124351','127954')
		--202206月签呈 河南省 剔除直送客户后再算销售员定价毛利率 每月
		and customer_no not in ('127043','126069','126113')	
		--202207月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','115286','127396','121452','107415','113857','122147','122159','122167','122185','122186','122188','125770','127747','127754','127698',
		--'127728','127746','127750','127753','127760','127766','127775')	
		--202207月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126935','126084','128005','104114','116566','112207','123459','120333','120723','112906','116959','118509','117817','121472','128069','128074',
		--'128087','128105','121994','124356','124367','124387','124401','124416','120939','119760','121841','112410','113829','114859','115681','115941','120872','120879',
		--'121287','121337','124558','125719','125722','120768','121020','121384','124193','124351','127954')
		--202208月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','107415','113857','125770','127543','127592','127698','127728','127729',
		--'127739','127743','127745','127750','127751','127755','127553','127737','127756')
		--202208月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126935','126124','127955','128005','128434','104114','116566','112207','120333','128580','128710',
		--'120723','127862','112906','117817','128074','128087','128105','124356','124367','124401','124416',
		--'128006','119760','121841','124169','128274','128457','128474','128487','128493','128494','128500',
		--'128505','112410','114859','115941','120872','120879','124558','125719','125722','126032')
		--202208月签呈 河南省 核算销售员综合毛利率需剔除按服务费的客户 每月
		and customer_no not in ('128244')
		--202208签呈 北京市  只核算销售额，不核算毛利额，不参与综合毛利 当月
		--and customer_no not in ('123029','123622','123702','124103','124109','124121','124138','124274','124576','125885','125967','128201','128314')
		--202208签呈 福建省  只核算销售额，不核算毛利额，不参与综合毛利 202205-202208
		--and customer_no not in ('107404','105177','105156','105181','106423','106721','105150','105182','119990')
		--202209月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		and customer_no not in ('104885','107415','121452','122167','122185','122186','122188','127543','127592',
		'127698','127728','127729','127739','127743','127745','127750','127751','127755','127553','127737','127756')
		--202209月签呈 河南省 核算销售员综合毛利率需剔除按服务费的客户 每月
		and customer_no not in ('126113','127043','128244','126069')
		--202209签呈 北京市  直送客户，单独核算，服务管家100%提成系数 当月
		and customer_no not in ('118996','123029','123622','124103','124109','124121','124138','124576','125885')
		--202209月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		and customer_no not in ('126935','129405','129410','126084','126124','104114','124674','116566','112207','128580',
		'128710','120723','117817','128069','128074','128087','121994','124356','124367','124401',
		'124416','125920','128006','129370','129519','119760','121841','124169','128274','128457',
		'128474','128487','128493','128494','128500','128505','112410','114859','115681','120872',
		'120879','121287','121337','124558','125719','125722')
	group by work_no,sales_name
	) a 
where
	work_no !=''
;--12





-- 服务管家本月定价毛利率，计算服务管家定价毛利额提成比例
drop table csx_tmp.tc_r_service_profit_tc_rate;--11
create table csx_tmp.tc_r_service_profit_tc_rate
as
select
	service_user_work_no,service_user_name,service_rp_bbc_sales_value,service_fl_sales_value,service_rp_bbc_profit,
	service_fl_profit,service_rp_bbc_prorate,service_fl_prorate,
	-- 日配&bbc定价毛利额提成比例
	case when service_rp_bbc_prorate<0.08 then 0
		when service_rp_bbc_prorate>=0.08 and service_rp_bbc_prorate<0.12 then 0.03
		when service_rp_bbc_prorate>=0.12 and service_rp_bbc_prorate<0.15 then 0.033
		when service_rp_bbc_prorate>=0.15 and service_rp_bbc_prorate<0.18 then 0.035
		when service_rp_bbc_prorate>=0.18 and service_rp_bbc_prorate<0.2 then 0.04
		when service_rp_bbc_prorate>=0.2 then 0.05
		else 0 end  as service_rp_bbc_profit_tc_rate,
	-- 福利定价毛利额提成比例
	case when service_fl_prorate<0.03 then 0
		when service_fl_prorate>=0.03 and service_fl_prorate<0.05 then 0.02
		when service_fl_prorate>=0.05 and service_fl_prorate<0.08 then 0.025
		when service_fl_prorate>=0.08 and service_fl_prorate<0.1 then 0.03
		when service_fl_prorate>=0.1 and service_fl_prorate<0.15 then 0.04
		when service_fl_prorate>=0.15 then 0.05
		else 0 end as service_fl_profit_tc_rate	
from
	(
	select 
		service_user_work_no,service_user_name,
		-- 销售额
		sum(rp_bbc_sales_value-w0k4_rp_bbc_sales_value) as service_rp_bbc_sales_value, -- 日配&bbc销售额
		sum(fl_sales_value-w0k4_fl_sales_value) as service_fl_sales_value, -- 福利销售额
		-- 定价毛利额
		sum(rp_bbc_profit) as service_rp_bbc_profit,-- 日配&bbc定价毛利额
		sum(fl_profit) as service_fl_profit,-- 福利定价毛利额
		--定价毛利率
		sum(rp_bbc_profit)/abs(sum(rp_bbc_sales_value-w0k4_rp_bbc_sales_value)) as service_rp_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
		sum(fl_profit)/abs(sum(fl_sales_value-w0k4_fl_sales_value)) as service_fl_prorate -- 销售员本月福利定价毛利率
	from
	(
		select *
		from 
		(		
			select
				customer_no,smonth,
				rp_sales_value as rp_bbc_sales_value,w0k4_rp_sales_value as w0k4_rp_bbc_sales_value,
				rp_profit as rp_bbc_profit,
				null as fl_sales_value,null as w0k4_fl_sales_value,
				null as fl_profit,
				rp_service_user_id as service_user_id,
				rp_service_user_work_no as service_user_work_no,
				rp_service_user_name as service_user_name
			from csx_tmp.tc_r_sales_value_profit_01
			where rp_service_user_id is not null
			union all 
			select
				customer_no,smonth,
				bbc_sales_value as rp_bbc_sales_value,w0k4_bbc_sales_value as w0k4_rp_bbc_sales_value,
				bbc_profit as rp_bbc_profit,
				null as fl_sales_value,null as w0k4_fl_sales_value,
				null as fl_profit,
				bbc_service_user_id as service_user_id,
				bbc_service_user_work_no as service_user_work_no,
				bbc_service_user_name as service_user_name
			from csx_tmp.tc_r_sales_value_profit_01
			where bbc_service_user_id is not null
			union all 
			select
				customer_no,smonth,
				null as rp_bbc_sales_value,null as w0k4_rp_bbc_sales_value,
				null as rp_bbc_profit,
				fl_sales_value as fl_sales_value,w0k4_fl_sales_value as w0k4_fl_sales_value,
				fl_profit as fl_profit,
				fl_service_user_id as service_user_id,
				fl_service_user_work_no as service_user_work_no,
				fl_service_user_name as service_user_name
			from csx_tmp.tc_r_sales_value_profit_01
			where fl_service_user_id is not null	
		)a
		where
		--202202月签呈，安徽省剔除以下客户后再算销售员的定价毛利率，服务费，当月
		--customer_no not in ('104402','104885','121452','107415','113731','113857','122147','122159','122167','122185','122186','122188','125274')
		--202202月签呈，剔除直送客户后再算销售员定价毛利率，当月
		--and customer_no not in ('114834','111832','123685','120723','124367','124387','124416','119760','121425','121841','123553')
		--202202月签呈，剔除武警客户，当月
		--and customer_no not in ('120595','124584','124784','125029','125028','125017','116932','120830','122837')
		--202203月签呈 直送客户 不计算销售员整体毛利率 每月
		customer_no not in ('124652')
		--202203月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','121452','107415','113731','113857','122147','122159','122167','122185','122186','122188','126406','113439')
		--202203月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('123685','116566','125360','123122','111832','111691','120723','115221','112906','118509','117817','121472','121994','124356',
		--	'124367','124387','124401','124416','119760','125767')
		--202204月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','123287','107415','121452','113857','122147','122167','113439','125770','126503','126571','126644','126406')
		--202204月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('114834','116566','111832','120723','112906','118509','117817','121472','121994','124356','124367','124387','124401','124416','119760','121841','125767')
		--202205月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','127396','107415','121452','113857','122147','122167','122159','122185','122186','122188')	
		--202205月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('116566','120723','112906','118509','117817','121472','121994','124356','124367','124387','124401','124416','119760','121841','115221','126084')
		--202205月签呈 河南省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126069','126113')
		--202205月签呈 河南省 剔除直送客户后再算销售员定价毛利率 每月
		and customer_no not in ('126259','127312','127336')	
		--202206月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','115286','127396','121452','107415','113857','122147','122159','122167','122185','122186','122188','125770','126644','127747','127754',
		--'127543','127592','127698','127728','127729','127739','127743','127745','127746','127750','127751','127753','127755','127760','127766','127767','127775','127553',
		--'127737','127756')	
		--202206月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126935','126084','128005','116566','120333','120723','112906','118509','122264','117817','121994','124356','124367','124387','124401','124416',
		--'119760','121841','125767','127262','114859','115681','115941','120872','120879','121287','125722','120768','121384','124351','127954')
		--202206月签呈 河南省 剔除直送客户后再算销售员定价毛利率 每月
		and customer_no not in ('127043','126069','126113')	
		--202207月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','115286','127396','121452','107415','113857','122147','122159','122167','122185','122186','122188','125770','127747','127754','127698',
		--'127728','127746','127750','127753','127760','127766','127775')	
		--202207月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126935','126084','128005','104114','116566','112207','123459','120333','120723','112906','116959','118509','117817','121472','128069','128074',
		--'128087','128105','121994','124356','124367','124387','124401','124416','120939','119760','121841','112410','113829','114859','115681','115941','120872','120879',
		--'121287','121337','124558','125719','125722','120768','121020','121384','124193','124351','127954')
		--202208月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		--and customer_no not in ('104885','107415','113857','125770','127543','127592','127698','127728','127729',
		--'127739','127743','127745','127750','127751','127755','127553','127737','127756')
		--202208月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		--and customer_no not in ('126935','126124','127955','128005','128434','104114','116566','112207','120333','128580','128710',
		--'120723','127862','112906','117817','128074','128087','128105','124356','124367','124401','124416',
		--'128006','119760','121841','124169','128274','128457','128474','128487','128493','128494','128500',
		--'128505','112410','114859','115941','120872','120879','124558','125719','125722','126032')
		--202208月签呈 河南省 核算销售员综合毛利率需剔除按服务费的客户 每月
		and customer_no not in ('128244')	
		--202208签呈 北京市  只核算销售额，不核算毛利额，不参与综合毛利 当月
		--and customer_no not in ('123029','123622','123702','124103','124109','124121','124138','124274','124576','125885','125967','128201','128314')
		--202208签呈 福建省  只核算销售额，不核算毛利额，不参与综合毛利 202205-202208
		--and customer_no not in ('107404','105177','105156','105181','106423','106721','105150','105182','119990')
		--202209月签呈 安徽省 核算销售员综合毛利率需剔除按服务费的客户 当月
		and customer_no not in ('104885','107415','121452','122167','122185','122186','122188','127543','127592',
		'127698','127728','127729','127739','127743','127745','127750','127751','127755','127553','127737','127756')
		--202209月签呈 河南省 核算销售员综合毛利率需剔除按服务费的客户 每月
		and customer_no not in ('126113','127043','128244','126069')
		--202209签呈 北京市  直送客户，单独核算，服务管家100%提成系数 当月
		and customer_no not in ('118996','123029','123622','124103','124109','124121','124138','124576','125885')
		--202209月签呈 安徽省 剔除直送客户后再算销售员定价毛利率 当月
		and customer_no not in ('126935','129405','129410','126084','126124','104114','124674','116566','112207','128580',
		'128710','120723','117817','128069','128074','128087','121994','124356','124367','124401',
		'124416','125920','128006','129370','129519','119760','121841','124169','128274','128457',
		'128474','128487','128493','128494','128500','128505','112410','114859','115681','120872',
		'120879','121287','121337','124558','125719','125722')		
	)a
	group by service_user_work_no,service_user_name
)a	
where service_user_work_no !=''
;--12







--客户业绩毛利、销售员奖金包、服务管家奖金包 ：未乘分配系数
drop table csx_tmp.tc_r_cust_salary_wubili;--13
create table csx_tmp.tc_r_cust_salary_wubili
as
select 
	a.smonth,a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_id,a.customer_no,a.customer_name,
	a.sales_id,a.work_no,a.sales_name,
	a.rp_service_user_id,a.rp_service_user_work_no,a.rp_service_user_name,
	a.fl_service_user_id,a.fl_service_user_work_no,a.fl_service_user_name,
	a.bbc_service_user_id,a.bbc_service_user_work_no,a.bbc_service_user_name,
	a.sales_value,a.rp_sales_value,a.bbc_sales_value,a.fl_sales_value,a.rp_bbc_sales_value,
	a.profit,a.rp_profit,a.bbc_profit,a.fl_profit,a.rp_bbc_profit,
	a.prorate,a.rp_prorate,a.bbc_prorate,a.fl_prorate,a.rp_bbc_prorate,
	--奖金包-销售员	
	coalesce(e.sales_prorate,0) as sales_prorate,
	coalesce(e.sales_rp_prorate,0) as sales_rp_prorate,
	coalesce(e.sales_bbc_prorate,0) as sales_bbc_prorate,
	coalesce(e.sales_rp_bbc_prorate,0) as sales_rp_bbc_prorate, -- 销售员本月日配&bbc定价毛利率
	coalesce(e.sales_fl_prorate,0) as sales_fl_prorate, -- 销售员本月福利定价毛利率
	--奖金包_日配业务 
	--202209月签呈 安徽直送客户 126935 等 默认毛利额提成比例为2% 当月
	--202209签呈 北京市  118996 等直送客户，单独核算，服务管家100%提成系数 当月
	--'126259' 毛利单独核算 每月 
	--'127923' 销售额*0.2% 不计算毛利提成 每月
	--'126690' 销售额*0.2% 不计算毛利提成 每月
	--'127043' 销售额*0.2% 不计算毛利提成 每月
	a.rp_sales_value*if(a.customer_no in ('127923','126690','127043'),0.002,coalesce(f.sales_rp_bbc_sales_value_tc_rate,0.002)) as sales_salary0_rp_sales_value, -- 奖金包_日配销售额
	a.rp_profit*(case 
	when a.customer_no in ('126935','129405','129410','126084','126124','104114','124674','116566','112207','128580',
		'128710','120723','117817','128069','128074','128087','121994','124356','124367','124401',
		'124416','125920','128006','129370','129519','119760','121841','124169','128274','128457',
		'128474','128487','128493','128494','128500','128505','112410','114859','115681','120872',
		'120879','121287','121337','124558','125719','125722'
		,'118996','123029','123622','124103','124109','124121','124138','124576','125885') then 0.02
		--'127923' 销售额*0.2% 不计算毛利提成 每月
		--'126690' 销售额*0.2% 不计算毛利提成 每月
		when a.customer_no in ('127923','126690','127043') then 0
		when a.customer_no in ('126259') then 0.03
		else coalesce(e.sales_rp_bbc_profit_tc_rate,0.03) end) as sales_salary0_rp_profit, --奖金包_日配定价毛利额
	--奖金包_福利业务
	a.fl_sales_value*coalesce(f.sales_fl_sales_value_tc_rate,0.002) as sales_salary0_fl_sales_value, --奖金包_福利销售额
	a.fl_profit*if(a.customer_no in ('126935','129405','129410','126084','126124','104114','124674','116566','112207','128580',
		'128710','120723','117817','128069','128074','128087','121994','124356','124367','124401',
		'124416','125920','128006','129370','129519','119760','121841','124169','128274','128457',
		'128474','128487','128493','128494','128500','128505','112410','114859','115681','120872',
		'120879','121287','121337','124558','125719','125722'
		,'118996','123029','123622','124103','124109','124121','124138','124576','125885'),0.02,
	coalesce(e.sales_fl_profit_tc_rate,0.02)) as sales_salary0_fl_profit, --奖金包_福利定价毛利额
	--奖金包_BBC业务
	a.bbc_sales_value*coalesce(f.sales_rp_bbc_sales_value_tc_rate,0.002) as sales_salary0_bbc_sales_value, -- 奖金包_bbc销售额
	a.bbc_profit*if(a.customer_no in ('126935','129405','129410','126084','126124','104114','124674','116566','112207','128580',
		'128710','120723','117817','128069','128074','128087','121994','124356','124367','124401',
		'124416','125920','128006','129370','129519','119760','121841','124169','128274','128457',
		'128474','128487','128493','128494','128500','128505','112410','114859','115681','120872',
		'120879','121287','121337','124558','125719','125722'
		,'118996','123029','123622','124103','124109','124121','124138','124576','125885'),0.02,
	coalesce(e.sales_rp_bbc_profit_tc_rate,0.03)) as sales_salary0_bbc_profit, --奖金包_bbc定价毛利额	
	
	
	--奖金包-服务管家	
	coalesce(d1.service_rp_bbc_prorate,0) as rpservice_rp_bbc_prorate,
	coalesce(d2.service_fl_prorate,0) as flservice_fl_prorate, 	
	coalesce(d3.service_rp_bbc_prorate,0) as bbcservice_rp_bbc_prorate,
	--奖金包_日配业务 
	--202209月签呈安徽直送客户 默认毛利额提成比例为2% 当月
	--202209签呈 北京市  118996 等 直送客户，单独核算，服务管家100%提成系数 当月
	--'126259' 毛利单独核算 每月 
	--'127923' 销售额*0.2% 不计算毛利提成 每月
	--'126690' 销售额*0.2% 不计算毛利提成 每月
	--'127043' 销售额*0.2% 不计算毛利提成 每月
	a.rp_sales_value*if(a.customer_no in ('127923','126690','127043'),0,coalesce(g1.service_rp_bbc_sales_value_tc_rate,0.002)) as service_salary0_rp_sales_value, -- 奖金包_日配销售额
	a.rp_profit*(case 
	when a.customer_no in ('126935','129405','129410','126084','126124','104114','124674','116566','112207','128580',
		'128710','120723','117817','128069','128074','128087','121994','124356','124367','124401',
		'124416','125920','128006','129370','129519','119760','121841','124169','128274','128457',
		'128474','128487','128493','128494','128500','128505','112410','114859','115681','120872',
		'120879','121287','121337','124558','125719','125722'
		,'118996','123029','123622','124103','124109','124121','124138','124576','125885') then 0.02
		--'127923' 销售额*0.2% 不计算毛利提成 每月
		--'126690' 销售额*0.2% 不计算毛利提成 每月
		when a.customer_no in ('127923','126690','127043') then 0
		when a.customer_no in ('126259') then 0.03
		else coalesce(d1.service_rp_bbc_profit_tc_rate,0.03) end) as service_salary0_rp_profit, --奖金包_日配定价毛利额
	--奖金包_福利业务
	a.fl_sales_value*0.002 as service_salary0_fl_sales_value, --奖金包_福利销售额
	a.fl_profit*if(a.customer_no in ('126935','129405','129410','126084','126124','104114','124674','116566','112207','128580',
		'128710','120723','117817','128069','128074','128087','121994','124356','124367','124401',
		'124416','125920','128006','129370','129519','119760','121841','124169','128274','128457',
		'128474','128487','128493','128494','128500','128505','112410','114859','115681','120872',
		'120879','121287','121337','124558','125719','125722'
		,'118996','123029','123622','124103','124109','124121','124138','124576','125885'),0.02,
	coalesce(d2.service_fl_profit_tc_rate,0.02)) as service_salary0_fl_profit, --奖金包_福利定价毛利额
	--奖金包_BBC业务
	a.bbc_sales_value*coalesce(g3.service_rp_bbc_sales_value_tc_rate,0.002) as service_salary0_bbc_sales_value, -- 奖金包_bbc销售额
	a.bbc_profit*if(a.customer_no in ('126935','129405','129410','126084','126124','104114','124674','116566','112207','128580',
		'128710','120723','117817','128069','128074','128087','121994','124356','124367','124401',
		'124416','125920','128006','129370','129519','119760','121841','124169','128274','128457',
		'128474','128487','128493','128494','128500','128505','112410','114859','115681','120872',
		'120879','121287','121337','124558','125719','125722'
		,'118996','123029','123622','124103','124109','124121','124138','124576','125885'),0.02,
	coalesce(d3.service_rp_bbc_profit_tc_rate,0.03)) as service_salary0_bbc_profit, --奖金包_bbc定价毛利额	

	a.rp_sales_sale_fp_rate,a.rp_sales_profit_fp_rate,a.fl_sales_sale_fp_rate,a.fl_sales_profit_fp_rate,a.bbc_sales_sale_fp_rate,a.bbc_sales_profit_fp_rate,
	a.rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
	a.fl_service_user_sale_fp_rate,a.fl_service_user_profit_fp_rate,
	a.bbc_service_user_sale_fp_rate,a.bbc_service_user_profit_fp_rate,
	
	f.sales_sales_value_ytd,-- 销售员年度累计销售额
	f.sales_rp_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
	f.sales_fl_sales_value_ytd, -- 销售员年度累计福利销售额
	g1.service_rp_bbc_sales_value_ytd as rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
	null as flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
	g3.service_rp_bbc_sales_value_ytd as bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额
	coalesce(f.sales_rp_bbc_sales_value_tc_rate,0.002) as sales_rp_bbc_sales_value_tc_rate,
	coalesce(f.sales_fl_sales_value_tc_rate,0.002) as sales_fl_sales_value_tc_rate,
	coalesce(e.sales_rp_bbc_profit_tc_rate,0.03) as sales_rp_bbc_profit_tc_rate,
	coalesce(e.sales_fl_profit_tc_rate,0.02) as sales_fl_profit_tc_rate,	
	g1.service_rp_bbc_sales_value_tc_rate as rpservice_rp_bbc_sales_value_tc_rate,
	g3.service_rp_bbc_sales_value_tc_rate as bbcservice_rp_bbc_sales_value_tc_rate,
	d1.service_rp_bbc_profit_tc_rate as rpservice_rp_bbc_profit_tc_rate,
	d2.service_fl_profit_tc_rate as flservice_fl_profit_tc_rate,
	d3.service_rp_bbc_profit_tc_rate as bbcservice_rp_bbc_profit_tc_rate,
	
	refund_sales_value,refund_rp_sales_value,refund_bbc_sales_value,
	refund_rp_bbc_sales_value,refund_fl_sales_value
from  
	(
	select 
		sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,customer_no,customer_name,smonth,customer_id,
		sales_id,work_no,sales_name,rp_service_user_id,rp_service_user_work_no,rp_service_user_name,
		fl_service_user_id,fl_service_user_work_no,fl_service_user_name,bbc_service_user_id,bbc_service_user_work_no,bbc_service_user_name,rp_sales_sale_fp_rate,rp_sales_profit_fp_rate,
		fl_sales_sale_fp_rate,fl_sales_profit_fp_rate,bbc_sales_sale_fp_rate,bbc_sales_profit_fp_rate,
		rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
		fl_service_user_sale_fp_rate,fl_service_user_profit_fp_rate,
		bbc_service_user_sale_fp_rate,bbc_service_user_profit_fp_rate,
		sales_value,rp_sales_value,bbc_sales_value,fl_sales_value,
		rp_bbc_sales_value,profit,rp_profit,bbc_profit,fl_profit,rp_bbc_profit,prorate,rp_prorate,bbc_prorate,fl_prorate,rp_bbc_prorate,refund_sales_value,
		refund_rp_sales_value,refund_bbc_sales_value,refund_rp_bbc_sales_value,refund_fl_sales_value
	from 
		csx_tmp.tc_r_sales_value_profit_01
	)a
	left join csx_tmp.tc_r_service_profit_tc_rate d1 on d1.service_user_name=a.rp_service_user_name and coalesce(d1.service_user_work_no,0)=coalesce(a.rp_service_user_work_no,0)
	left join csx_tmp.tc_r_service_profit_tc_rate d2 on d2.service_user_name=a.fl_service_user_name and coalesce(d2.service_user_work_no,0)=coalesce(a.fl_service_user_work_no,0)
	left join csx_tmp.tc_r_service_profit_tc_rate d3 on d3.service_user_name=a.bbc_service_user_name and coalesce(d3.service_user_work_no,0)=coalesce(a.bbc_service_user_work_no,0)
	left join csx_tmp.tc_r_sales_profit_tc_rate e on e.work_no=a.work_no and e.sales_name=a.sales_name
	left join csx_tmp.tc_r_sales_rate_ytd f on f.work_no=a.work_no and f.sales_name=a.sales_name
	left join csx_tmp.tc_r_service_rate_ytd g1 on g1.service_user_work_no=a.rp_service_user_work_no and g1.service_user_name=a.rp_service_user_name
	left join csx_tmp.tc_r_service_rate_ytd g3 on g3.service_user_work_no=a.bbc_service_user_work_no and g3.service_user_name=a.bbc_service_user_name
;--14


--客户业绩毛利、销售员奖金包、服务管家奖金包 ：乘分配系数
drop table csx_tmp.tc_r_new_cust_salary_bili; --15
create table csx_tmp.tc_r_new_cust_salary_bili
as
select
	a.smonth,a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_id,a.customer_no,a.customer_name,
	a.sales_id,a.work_no,a.sales_name,a.rp_service_user_id,a.rp_service_user_work_no,a.rp_service_user_name,
	a.fl_service_user_id,a.fl_service_user_work_no,a.fl_service_user_name,a.bbc_service_user_id,a.bbc_service_user_work_no,a.bbc_service_user_name,
	a.sales_value,a.rp_sales_value,a.bbc_sales_value,a.fl_sales_value,a.rp_bbc_sales_value,
	a.profit,a.rp_profit,a.bbc_profit,a.fl_profit,a.rp_bbc_profit,a.prorate,a.rp_prorate,a.bbc_prorate,a.fl_prorate,a.rp_bbc_prorate,
	a.sales_prorate,a.sales_rp_prorate,a.sales_bbc_prorate,a.sales_rp_bbc_prorate,a.sales_fl_prorate,
	a.rpservice_rp_bbc_prorate,a.flservice_fl_prorate,a.bbcservice_rp_bbc_prorate,
	--奖金包-销售员：日配、福利、BBC
	a.sales_salary0_rp_sales_value,a.sales_salary0_rp_profit,
	a.sales_salary0_fl_sales_value,a.sales_salary0_fl_profit,
	a.sales_salary0_bbc_sales_value,a.sales_salary0_bbc_profit, 
	--奖金包-服务管家：日配、福利、BBC
	a.service_salary0_rp_sales_value,a.service_salary0_rp_profit,
	a.service_salary0_fl_sales_value,a.service_salary0_fl_profit,
	a.service_salary0_bbc_sales_value,a.service_salary0_bbc_profit, 	
	a.rp_sales_sale_fp_rate,a.rp_sales_profit_fp_rate,a.fl_sales_sale_fp_rate,a.fl_sales_profit_fp_rate,a.bbc_sales_sale_fp_rate,a.bbc_sales_profit_fp_rate,
	a.rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
	a.fl_service_user_sale_fp_rate,a.fl_service_user_profit_fp_rate,
	a.bbc_service_user_sale_fp_rate,a.bbc_service_user_profit_fp_rate,

	--销售员各业务提成
	a.salary_rp_sales_value_sales,a.salary_rp_profit_sales,a.salary_fl_sales_value_sales,a.salary_fl_profit_sales,a.salary_bbc_sales_value_sales,a.salary_bbc_profit_sales,
	--各业务服务管家提成
	a.salary_rp_sales_value_service,a.salary_rp_profit_service,a.salary_fl_sales_value_service,a.salary_fl_profit_service,a.salary_bbc_sales_value_service,a.salary_bbc_profit_service,	
	a.sales_sales_value_ytd,a.sales_rp_bbc_sales_value_ytd,a.sales_fl_sales_value_ytd,
	a.rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
	a.flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
	a.bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额	
	a.sales_rp_bbc_sales_value_tc_rate,
	a.sales_fl_sales_value_tc_rate,
	a.sales_rp_bbc_profit_tc_rate,
	a.sales_fl_profit_tc_rate,	
	a.rpservice_rp_bbc_sales_value_tc_rate,
	a.bbcservice_rp_bbc_sales_value_tc_rate,	
	a.rpservice_rp_bbc_profit_tc_rate,
	a.flservice_fl_profit_tc_rate,
	a.bbcservice_rp_bbc_profit_tc_rate,	
	--202207月签呈 河南省 服务管家固定服务费 销售员无提成也无服务费 每月
	--202209月签呈 河南省 销售员固定服务费 '126113','127043','128244','126069' 每月
	if(fixed_service_fee is not null,fixed_service_fee,if(service_fee_2 is not null,service_fee_2,if(a.service_fee is not null,a.service_fee,
		coalesce(salary_rp_sales_value_sales,0)+coalesce(salary_rp_profit_sales,0)+
		coalesce(salary_fl_sales_value_sales,0)+coalesce(salary_fl_profit_sales,0)+
		coalesce(salary_bbc_sales_value_sales,0)+coalesce(salary_bbc_profit_sales,0)
		))) as salary_sales,
	--202208月签呈 河南服务管家固定100元服务费 每月
	if(a.customer_no in ('128244'),100,if(fixed_service_fee is not null,fixed_service_fee,if(a.service_fee_2 is not null,service_fee_2,if(a.service_fee is not null,service_fee,salary_rp_sales_value_service+salary_rp_profit_service)))) as salary_rp_service,
	if(fixed_service_fee is not null,fixed_service_fee,if(a.service_fee_2 is not null,service_fee_2,if(a.service_fee is not null,service_fee,salary_fl_sales_value_service+salary_fl_profit_service))) as salary_fl_service,
	if(fixed_service_fee is not null,fixed_service_fee,if(a.service_fee_2 is not null,service_fee_2,if(a.service_fee is not null,service_fee,salary_bbc_sales_value_service+salary_bbc_profit_service))) as salary_bbc_service,
	refund_sales_value,refund_rp_sales_value,refund_bbc_sales_value,refund_rp_bbc_sales_value,refund_fl_sales_value	
from
	(
	select 
		a.smonth,a.sales_region_code,a.sales_region_name,a.province_code,a.province_name,a.city_group_code,a.city_group_name,a.customer_id,a.customer_no,a.customer_name,
		a.sales_id,a.work_no,a.sales_name,a.rp_service_user_id,a.rp_service_user_work_no,a.rp_service_user_name,
		a.fl_service_user_id,a.fl_service_user_work_no,a.fl_service_user_name,a.bbc_service_user_id,a.bbc_service_user_work_no,a.bbc_service_user_name,	
		a.sales_value,a.rp_sales_value,a.bbc_sales_value,a.fl_sales_value,a.rp_bbc_sales_value,
		a.profit,a.rp_profit,a.bbc_profit,a.fl_profit,a.rp_bbc_profit,a.prorate,a.rp_prorate,a.bbc_prorate,a.fl_prorate,a.rp_bbc_prorate,
		a.sales_prorate,a.sales_rp_prorate,a.sales_bbc_prorate,a.sales_rp_bbc_prorate,a.sales_fl_prorate,
		a.rpservice_rp_bbc_prorate,a.flservice_fl_prorate,a.bbcservice_rp_bbc_prorate,
		--a.salary_total_sales_value,a.salary_total_profit,
		--奖金包-销售员：日配、福利、BBC
		a.sales_salary0_rp_sales_value,a.sales_salary0_rp_profit,
		a.sales_salary0_fl_sales_value,a.sales_salary0_fl_profit,
		a.sales_salary0_bbc_sales_value,a.sales_salary0_bbc_profit, 
		--奖金包-服务管家：日配、福利、BBC
		a.service_salary0_rp_sales_value,a.service_salary0_rp_profit,
		a.service_salary0_fl_sales_value,a.service_salary0_fl_profit,
		a.service_salary0_bbc_sales_value,a.service_salary0_bbc_profit,
		
		--奖金包提成比例
		a.rp_sales_sale_fp_rate,a.rp_sales_profit_fp_rate,a.fl_sales_sale_fp_rate,a.fl_sales_profit_fp_rate,a.bbc_sales_sale_fp_rate,a.bbc_sales_profit_fp_rate,
		a.rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
		a.fl_service_user_sale_fp_rate,a.fl_service_user_profit_fp_rate,
		a.bbc_service_user_sale_fp_rate,a.bbc_service_user_profit_fp_rate,	
		
		--提成_日配业务_销售员
		--'123311','124033','118376' 只核算销售额提成 不核算毛利额提成 每月
		--'124524','127312','127336' 减半核算销售额提成及毛利额提成 每月
		--'124652' 只核算销售额提成 不核算毛利额提成 每月
		--'126069','126113' 只核算销售额提成 不核算毛利额提成 每月
		--202208签呈 '123029','123622','123702','124103','124109','124121','124138','124274','124576','125885','125967','128201','128314' 只核算销售额，不核算毛利额，不参与综合毛利 当月
		--202208签呈  '107404','105177','105156','105181','106423','106721','105150','105182','119990' 只核算销售额，不核算毛利额，不参与综合毛利 202205-202208
		a.sales_salary0_rp_sales_value*a.rp_sales_sale_fp_rate*if(a.customer_no in ('124524','127312','127336'),0.5,1) as salary_rp_sales_value_sales,
		a.sales_salary0_rp_profit*a.rp_sales_profit_fp_rate*
			if(a.customer_no in (''
			),0,
			if(a.customer_no in ('123311','124033','118376','124652','126113','127043','128244','126069'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1))) as salary_rp_profit_sales,		
				
		--提成_福利业务_销售员
		a.sales_salary0_fl_sales_value*a.fl_sales_sale_fp_rate*if(a.customer_no in ('124524','127312','127336'),0.5,1) as salary_fl_sales_value_sales,
		a.sales_salary0_fl_profit*a.fl_sales_profit_fp_rate*
			if(a.customer_no in (''
			),0,
			if(a.customer_no in ('123311','124033','118376','124652','126113','127043','128244','126069'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1))) as salary_fl_profit_sales,
		
		--提成_BBC业务_销售员
		a.sales_salary0_bbc_sales_value*a.bbc_sales_sale_fp_rate*if(a.customer_no in ('124524','127312','127336'),0.5,1) as salary_bbc_sales_value_sales,
		a.sales_salary0_bbc_profit*a.bbc_sales_profit_fp_rate*
			if(a.customer_no in (''
			),0,
			if(a.customer_no in ('123311','124033','118376','124652','126113','127043','128244','126069'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1))) as salary_bbc_profit_sales,		
		
		--提成_日配业务_服务管家
		a.service_salary0_rp_sales_value*coalesce(a.rp_service_user_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as salary_rp_sales_value_service,
		a.service_salary0_rp_profit*coalesce(a.rp_service_user_profit_fp_rate,0)*
			if(a.customer_no in (''
			),0,
			if(a.customer_no in ('123311','124033','118376','124652','126113','127043','128244','126069'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1))) as salary_rp_profit_service,	

		--提成_福利业务_服务管家
		a.service_salary0_fl_sales_value*coalesce(a.fl_service_user_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as salary_fl_sales_value_service,
		a.service_salary0_fl_profit*coalesce(a.fl_service_user_profit_fp_rate,0)*
			if(a.customer_no in (''
			),0,
			if(a.customer_no in ('123311','124033','118376','124652','126113','127043','128244','126069'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1))) as salary_fl_profit_service,

		--提成_BBC业务_服务管家
		a.service_salary0_bbc_sales_value*coalesce(a.bbc_service_user_sale_fp_rate,0)*if(a.customer_no in ('124524','127312','127336'),0.5,1) as salary_bbc_sales_value_service,
		a.service_salary0_bbc_profit*coalesce(a.bbc_service_user_profit_fp_rate,0)*
			if(a.customer_no in (''
			),0,
			if(a.customer_no in ('123311','124033','118376','124652','126113','127043','128244','126069'),0,if(a.customer_no in ('124524','127312','127336'),0.5,1))) as salary_bbc_profit_service,			
		
		a.sales_sales_value_ytd,-- 销售员年度累计销售额
		a.sales_rp_bbc_sales_value_ytd,-- 销售员年度累计日配&BBC销售额
		a.sales_fl_sales_value_ytd, -- 销售员年度累计福利销售额
		a.rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
		a.flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
		a.bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额	
		a.sales_rp_bbc_sales_value_tc_rate,
		a.sales_fl_sales_value_tc_rate,
		a.sales_rp_bbc_profit_tc_rate,
		a.sales_fl_profit_tc_rate,	
		a.rpservice_rp_bbc_sales_value_tc_rate,
		a.bbcservice_rp_bbc_sales_value_tc_rate,		
		a.rpservice_rp_bbc_profit_tc_rate,
		a.flservice_fl_profit_tc_rate,
		a.bbcservice_rp_bbc_profit_tc_rate,		
		b.service_fee,
		if(c.service_fee_2 is not null,
			case when a.customer_no in('120595') then service_fee_2 --固定毛利率 服务费不变
				when a.customer_no in('122837') then service_fee_2 --*0.8 --销售员武警客户整体毛利率
				when a.customer_no in('124584','124784') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				when a.customer_no in('125017','125028') then service_fee_2 --+500 --*0.8 --销售员：陈静 武警客户整体毛利率
				--202208月签呈，安徽省  当月 '125017','116932','120830','125028' 只按服务费，取消毛利率约束
				--when a.customer_no in('125017','116932','120830','125028') then service_fee_2
				when a.customer_no in('125029') then service_fee_2
				when prorate<0.1 then service_fee_2*0.8
				when prorate>=0.1 and prorate<0.12 then service_fee_2
				when prorate>=0.12 and prorate<0.15 then service_fee_2+500.00
				when prorate>=0.15 then service_fee_2+1000.00 else null 
			end,null) as service_fee_2,
		d.fixed_service_fee,	
		refund_sales_value,refund_rp_sales_value,refund_bbc_sales_value,
		refund_rp_bbc_sales_value,refund_fl_sales_value
	from csx_tmp.tc_r_cust_salary_wubili a 
	--202209月签呈，安徽省，服务费，当月
	left join
		(
			select '999999x' as customer_no,0.00 as service_fee
			union all  select '104885' as customer_no,1000.00 as service_fee
			union all  select '107415' as customer_no,800.00 as service_fee
			union all  select '121452' as customer_no,300.00 as service_fee
			union all  select '122167' as customer_no,100.00 as service_fee
			union all  select '122185' as customer_no,100.00 as service_fee
			union all  select '122186' as customer_no,100.00 as service_fee
			union all  select '122188' as customer_no,100.00 as service_fee
			union all  select '127543' as customer_no,100.00 as service_fee
			union all  select '127592' as customer_no,100.00 as service_fee
			union all  select '127698' as customer_no,100.00 as service_fee
			union all  select '127728' as customer_no,100.00 as service_fee
			union all  select '127729' as customer_no,100.00 as service_fee
			union all  select '127739' as customer_no,100.00 as service_fee
			union all  select '127743' as customer_no,100.00 as service_fee
			union all  select '127745' as customer_no,100.00 as service_fee
			union all  select '127750' as customer_no,100.00 as service_fee
			union all  select '127751' as customer_no,100.00 as service_fee
			union all  select '127755' as customer_no,100.00 as service_fee
			union all  select '127553' as customer_no,100.00 as service_fee
			union all  select '127737' as customer_no,100.00 as service_fee
			union all  select '127756' as customer_no,100.00 as service_fee

		) b on b.customer_no=a.customer_no
	--202202月签呈 安徽省 武警客户 服务费*折扣 每月
	left join
		(
			select '999999x' as customer_no,0.00 as service_fee_2
			union all  select '120595' as customer_no,200.00 as service_fee_2
			union all  select '124584' as customer_no,750.00 as service_fee_2
			union all  select '124784' as customer_no,750.00 as service_fee_2
			union all  select '125028' as customer_no,1000.00 as service_fee_2
			union all  select '116932' as customer_no,1000.00 as service_fee_2
			union all  select '120830' as customer_no,500.00 as service_fee_2
			union all  select '122837' as customer_no,500.00 as service_fee_2
			union all  select '125029' as customer_no,500.00 as service_fee_2
			union all  select '125017' as customer_no,500.00 as service_fee_2
		) c on c.customer_no=a.customer_no
	--固定服务费 每月 河南省
	left join
		( 
			select '999999x' as customer_no,0.00 as fixed_service_fee
			union all  select '126259' as customer_no,200.00 as fixed_service_fee
			union all  select '123311' as customer_no,500.00 as fixed_service_fee
			--union all  select '127312' as customer_no,100.00 as fixed_service_fee
			--union all  select '127336' as customer_no,100.00 as fixed_service_fee
			union all  select '126069' as customer_no,100.00 as fixed_service_fee
			union all  select '126113' as customer_no,100.00 as fixed_service_fee
			union all  select '127043' as customer_no,100.00 as fixed_service_fee
			union all  select '128102' as customer_no,100.00 as fixed_service_fee
			union all  select '128244' as customer_no,100.00 as fixed_service_fee
		) d on d.customer_no=a.customer_no		
	) a 
;--16


--客户业绩毛利、销售员奖金包、服务管家乘分配系数后奖金包 ：人员所在省区
drop table csx_tmp.tc_r_new_cust_salary_info_province; --17
create table csx_tmp.tc_r_new_cust_salary_info_province
as
select
	a.smonth,
	--客户地区信息
	a.sales_region_code as region_code_customer,a.sales_region_name as region_name_customer,
	a.province_code as province_code_customer,a.province_name as province_name_customer,
	a.city_group_code as city_group_code_customer,a.city_group_name as city_group_name_customer,
	--销售员地区信息
	coalesce(c.region_code,'') as region_code_sales,
	coalesce(c.region_name,'') as region_name_sales,
	coalesce(c.province_code,'') as province_code_sales,
	coalesce(c.province_name,'') as province_name_sales,
	coalesce(c.city_group_code,'') as city_group_code_sales,
	coalesce(c.city_group_name,'') as city_group_name_sales,
	--日配服务管家地区信息
	coalesce(d.region_code,'') as region_code_rp_service,
	coalesce(d.region_name,'') as region_name_rp_service,
	coalesce(d.province_code,'') as province_code_rp_service,
	coalesce(d.province_name,'') as province_name_rp_service,
	coalesce(d.city_group_code,'') as city_group_code_rp_service,
	coalesce(d.city_group_name,'') as city_group_name_rp_service,
	--福利服务管家地区信息
	coalesce(d2.region_code,'') as region_code_fl_service,
	coalesce(d2.region_name,'') as region_name_fl_service,
	coalesce(d2.province_code,'') as province_code_fl_service,
	coalesce(d2.province_name,'') as province_name_fl_service,
	coalesce(d2.city_group_code,'') as city_group_code_fl_service,
	coalesce(d2.city_group_name,'') as city_group_name_fl_service,
	--BBC服务管家地区信息
	coalesce(d3.region_code,'') as region_code_bbc_service,
	coalesce(d3.region_name,'') as region_name_bbc_service,
	coalesce(d3.province_code,'') as province_code_bbc_service,
	coalesce(d3.province_name,'') as province_name_bbc_service,
	coalesce(d3.city_group_code,'') as city_group_code_bbc_service,
	coalesce(d3.city_group_name,'') as city_group_name_bbc_service,
	a.customer_id,a.customer_no,a.customer_name,
	coalesce(a.sales_id,'') as sales_id,
	coalesce(a.work_no,'') as work_no,
	coalesce(a.sales_name,'') as sales_name,
	coalesce(a.rp_service_user_id,'') as rp_service_user_id,
	coalesce(a.rp_service_user_work_no,'') as rp_service_user_work_no,
	coalesce(a.rp_service_user_name,'') as rp_service_user_name,
	coalesce(a.fl_service_user_id,'') as fl_service_user_id,
	coalesce(a.fl_service_user_work_no,'') as fl_service_user_work_no,
	coalesce(a.fl_service_user_name,'') as fl_service_user_name,
	coalesce(a.bbc_service_user_id,'') as bbc_service_user_id,
	coalesce(a.bbc_service_user_work_no,'') as bbc_service_user_work_no,
	coalesce(a.bbc_service_user_name,'') as bbc_service_user_name,
	a.sales_value,a.rp_sales_value,a.bbc_sales_value,a.fl_sales_value,a.rp_bbc_sales_value,
	a.profit,a.rp_profit,a.bbc_profit,a.fl_profit,a.rp_bbc_profit,a.prorate,a.rp_prorate,a.bbc_prorate,a.fl_prorate,a.rp_bbc_prorate,
	a.sales_prorate,a.sales_rp_prorate,a.sales_bbc_prorate,a.sales_rp_bbc_prorate,a.sales_fl_prorate,
	a.rpservice_rp_bbc_prorate,a.flservice_fl_prorate,a.bbcservice_rp_bbc_prorate,
	--奖金包-销售员：日配、福利、BBC
	a.sales_salary0_rp_sales_value,a.sales_salary0_rp_profit,
	a.sales_salary0_fl_sales_value,a.sales_salary0_fl_profit,
	a.sales_salary0_bbc_sales_value,a.sales_salary0_bbc_profit, 
	--奖金包-服务管家：日配、福利、BBC
	a.service_salary0_rp_sales_value,a.service_salary0_rp_profit,
	a.service_salary0_fl_sales_value,a.service_salary0_fl_profit,
	a.service_salary0_bbc_sales_value,a.service_salary0_bbc_profit, 	
	a.rp_sales_sale_fp_rate,a.rp_sales_profit_fp_rate,a.fl_sales_sale_fp_rate,a.fl_sales_profit_fp_rate,a.bbc_sales_sale_fp_rate,a.bbc_sales_profit_fp_rate,
	a.rp_service_user_sale_fp_rate,rp_service_user_profit_fp_rate,
	a.fl_service_user_sale_fp_rate,a.fl_service_user_profit_fp_rate,
	a.bbc_service_user_sale_fp_rate,a.bbc_service_user_profit_fp_rate,
	a.sales_sales_value_ytd,a.sales_rp_bbc_sales_value_ytd,a.sales_fl_sales_value_ytd,
	a.rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
	a.flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
	a.bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额	
	a.sales_rp_bbc_sales_value_tc_rate,
	a.sales_fl_sales_value_tc_rate,
	a.sales_rp_bbc_profit_tc_rate,
	a.sales_fl_profit_tc_rate,	
	a.rpservice_rp_bbc_sales_value_tc_rate,
	a.bbcservice_rp_bbc_sales_value_tc_rate,	
	a.rpservice_rp_bbc_profit_tc_rate,
	a.flservice_fl_profit_tc_rate,
	a.bbcservice_rp_bbc_profit_tc_rate,	
	refund_sales_value,refund_rp_sales_value,refund_bbc_sales_value,refund_rp_bbc_sales_value,refund_fl_sales_value,
	--销售员提成
	a.salary_rp_sales_value_sales,
	a.salary_rp_profit_sales,
	a.salary_fl_sales_value_sales,
	a.salary_fl_profit_sales,
	a.salary_bbc_sales_value_sales,
	a.salary_bbc_profit_sales,
	a.salary_sales,
	--日配服务管家提成
	if(a.rp_service_user_id<>'',a.salary_rp_sales_value_service,0) as salary_rp_sales_value_service,
	if(a.rp_service_user_id<>'',a.salary_rp_profit_service,0) as salary_rp_profit_service,
	if(a.rp_service_user_id<>'',a.salary_rp_service,0) as salary_rp_service,
	--福利服务管家提成
	if(a.fl_service_user_id<>'',a.salary_fl_sales_value_service,0) as salary_fl_sales_value_service,
	if(a.fl_service_user_id<>'',a.salary_fl_profit_service,0) as salary_fl_profit_service,
	if(a.fl_service_user_id<>'',a.salary_fl_service,0) as salary_fl_service,
	--bbc服务管家提成
	if(a.bbc_service_user_id<>'',a.salary_bbc_sales_value_service,0) as salary_bbc_sales_value_service,
	if(a.bbc_service_user_id<>'',a.salary_bbc_profit_service,0) as salary_bbc_profit_service,
	if(a.bbc_service_user_id<>'',a.salary_bbc_service,0) as salary_bbc_service	
from
	csx_tmp.tc_r_new_cust_salary_bili a 
	--left join(select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:month_end_day}) b on b.customer_no=a.customer_no
	left join csx_tmp.tc_r_person_info c on c.id=a.sales_id
	left join csx_tmp.tc_r_person_info d on d.user_number=split(a.rp_service_user_work_no,';')[0]
	left join csx_tmp.tc_r_person_info d2 on d2.user_number=split(a.fl_service_user_work_no,';')[0]
	left join csx_tmp.tc_r_person_info d3 on d3.user_number=split(a.bbc_service_user_work_no,';')[0]
;--18


--客户提成_奖金包宽表
insert overwrite table csx_tmp.report_sss_r_m_crm_customer_salary_detail partition(smt)
select
	concat_ws('-',${hiveconf:current_month},customer_no) as biz_id,
	smonth,
	region_code_customer,
	region_name_customer,
	province_code_customer,
	province_name_customer,
	city_group_code_customer,
	city_group_name_customer,
	region_code_sales,
	region_name_sales,
	province_code_sales,
	province_name_sales,
	city_group_code_sales,
	city_group_name_sales,
	region_code_rp_service,
	region_name_rp_service,
	province_code_rp_service,
	province_name_rp_service,
	city_group_code_rp_service,
	city_group_name_rp_service,
	region_code_fl_service,
	region_name_fl_service,
	province_code_fl_service,
	province_name_fl_service,
	city_group_code_fl_service,
	city_group_name_fl_service,
	region_code_bbc_service,
	region_name_bbc_service,
	province_code_bbc_service,
	province_name_bbc_service,
	city_group_code_bbc_service,
	city_group_name_bbc_service,
	customer_id,
	customer_no,
	customer_name,
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
	sales_value,
	rp_sales_value,
	bbc_sales_value,
	fl_sales_value,
	rp_bbc_sales_value,
	profit,
	rp_profit,
	bbc_profit,
	fl_profit,
	rp_bbc_profit,
	prorate,
	rp_prorate,
	bbc_prorate,
	fl_prorate,
	rp_bbc_prorate,
	sales_prorate,
	sales_rp_prorate,
	sales_bbc_prorate,
	sales_rp_bbc_prorate,
	sales_fl_prorate,
	rpservice_rp_bbc_prorate,
	flservice_fl_prorate,
	bbcservice_rp_bbc_prorate,	
	sales_salary0_rp_sales_value,
	sales_salary0_rp_profit,
	sales_salary0_fl_sales_value,
	sales_salary0_fl_profit,
	sales_salary0_bbc_sales_value,
	sales_salary0_bbc_profit,
	service_salary0_rp_sales_value,
	service_salary0_rp_profit,
	service_salary0_fl_sales_value,
	service_salary0_fl_profit,
	service_salary0_bbc_sales_value,
	service_salary0_bbc_profit,
	rp_sales_sale_fp_rate,
	rp_sales_profit_fp_rate,
	fl_sales_sale_fp_rate,
	fl_sales_profit_fp_rate,
	bbc_sales_sale_fp_rate,
	bbc_sales_profit_fp_rate,
	rp_service_user_sale_fp_rate,
	rp_service_user_profit_fp_rate,
	fl_service_user_sale_fp_rate,
	fl_service_user_profit_fp_rate,
	bbc_service_user_sale_fp_rate,
	bbc_service_user_profit_fp_rate,
	sales_sales_value_ytd,
	sales_rp_bbc_sales_value_ytd,
	sales_fl_sales_value_ytd,
	rpservice_rp_bbc_sales_value_ytd,
	flservice_fl_sales_value_ytd,
	bbcservice_rp_bbc_sales_value_ytd,
	sales_rp_bbc_sales_value_tc_rate,
	sales_fl_sales_value_tc_rate,
	sales_rp_bbc_profit_tc_rate,
	sales_fl_profit_tc_rate,	
	rpservice_rp_bbc_sales_value_tc_rate,
	bbcservice_rp_bbc_sales_value_tc_rate,	
	rpservice_rp_bbc_profit_tc_rate,
	flservice_fl_profit_tc_rate,
	bbcservice_rp_bbc_profit_tc_rate,	
	refund_sales_value,
	refund_rp_sales_value,
	refund_bbc_sales_value,
	refund_rp_bbc_sales_value,
	refund_fl_sales_value,
	salary_rp_sales_value_sales,
	salary_rp_profit_sales,
	salary_fl_sales_value_sales,
	salary_fl_profit_sales,
	salary_bbc_sales_value_sales,
	salary_bbc_profit_sales,
	salary_sales,
	salary_rp_sales_value_service,
	salary_rp_profit_service,
	salary_rp_service,
	salary_fl_sales_value_service,
	salary_fl_profit_service,
	salary_fl_service,
	salary_bbc_sales_value_service,
	salary_bbc_profit_service,
	salary_bbc_service,
	${hiveconf:created_time} update_time,
	${hiveconf:current_month} as smt -- 统计日期 
from csx_tmp.tc_r_new_cust_salary_info_province;



--本月回款核销订单明细 订单号、日期、年月、金额、逾期日期、逾期天数、提成比例
drop table csx_tmp.tc_r_source_bill_paid; --15
create table csx_tmp.tc_r_source_bill_paid
as
select 
    regexp_replace(substr(a.happen_date,1,7),'-','') smonth,
	a.source_bill_no,	--来源单号
    a.customer_code,	--客户编码
    a.happen_date,	--发生时间
    a.source_statement_amount,	--源单据对账金额
    a.company_code,	--签约公司编码
    a.residual_amount,	--剩余预付款金额_预付款客户抵消订单金额后
    a.residual_amount_sss,	--剩余预付款金额_原销售结算
    a.unpaid_amount,	--未回款金额_抵消预付款后
    a.unpaid_amount_sss,	--未回款金额_原销售结算
    a.bad_debt_amount,	--坏账金额
    a.account_period_code,	--账期编码
    a.account_period_name,	--账期名称
    a.account_period_val,	--账期值
    a.source_sys,	--来源系统 MALL B端销售 BBC BBC端 BEGIN期初
    a.payment_amount paid_amount,	--当前核销金额
    a.statement_start_date,	--账期周期开始时间
    a.statement_end_date,	--账期周期结束时间
	a.overdue_date_new,	--逾期开始日期
	coalesce(b.payment_amount,0) payment_amount,	--本月核销金额
	--c.payment_amount_ls0,	--本月前历史 核销金额
	--d.unpaid_amount_ls,	--本月前历史 未回款金额
	--d.payment_amount_ls,	--本月前历史 核销金额
	--兜底：当月订单 历史核销金额为0
	case when regexp_replace(substr(a.happen_date,1,7),'-','')=substr(${hiveconf:month_end_day},1,6) then a.source_statement_amount else d.unpaid_amount_ls end unpaid_amount_ls,	--本月前历史 未回款金额,
	case when regexp_replace(substr(a.happen_date,1,7),'-','')=substr(${hiveconf:month_end_day},1,6) then 0 else d.payment_amount_ls end payment_amount_ls,	--本月前历史 核销金额
	--case when account_period_code='Z007' 
	--       then if(c.unpaid_amount is null,a.source_statement_amount-a.unpaid_amount,
	--	            if(c.unpaid_amount>a.unpaid_amount,c.unpaid_amount-a.unpaid_amount,0))
	--	 else b.payment_amount end payment_amount,
	coalesce(b.payment_amount_100,0) payment_amount_100,
	coalesce(b.payment_amount_80,0) payment_amount_80,
	coalesce(b.payment_amount_60,0) payment_amount_60,
	coalesce(b.payment_amount_50,0) payment_amount_50,
	coalesce(b.payment_amount_10,0) payment_amount_10
from 
(
	select sdt,
		source_bill_no,	--来源单号
		customer_code,	--客户编码
		happen_date,	--发生时间
		source_statement_amount,	--源单据对账金额
		company_code,	--签约公司编码
		residual_amount,	--剩余预付款金额_预付款客户抵消订单金额后
		residual_amount_sss,	--剩余预付款金额_原销售结算
		unpaid_amount,	--未回款金额_抵消预付款后
		unpaid_amount_sss,	--未回款金额_原销售结算
		bad_debt_amount,	--坏账金额
		account_period_code,	--账期编码
		account_period_name,	--账期名称
		account_period_val,	--账期值
		source_sys,	--来源系统 MALL B端销售 BBC BBC端 BEGIN期初
		payment_amount,	--核销金额
		statement_start_date,	--账期周期开始时间
		statement_end_date,	--账期周期结束时间
		overdue_date_new	--逾期开始日期	 
	from csx_dw.dws_sss_r_d_order_kp_settle_detail
	where sdt=${hiveconf:month_end_day}
	and date_format(happen_date,'yyyy-MM-dd')>='2022-03-01'
	--and customer_code='127307'
)a
left join
(	
	--核销流水明细表:本月核销金额
	select close_bill_no,
		sum(payment_amount) payment_amount,
		sum(case when rate=1 then payment_amount end) payment_amount_100,
		sum(case when rate=0.8 then payment_amount end) payment_amount_80,
		sum(case when rate=0.6 then payment_amount end) payment_amount_60,
		sum(case when rate=0.5 then payment_amount end) payment_amount_50,
		sum(case when rate=0.1 then payment_amount end) payment_amount_10
	from 
	(
		select   
			b.customer_code,b.company_code,b.close_bill_no,b.paid_date,
			b.payment_amount,	--核销金额
			--逾期天数
			datediff(b.paid_date,a.overdue_date_new) over_days,
			--发放比例 客户公司账期若为Z007 预付货款，则回款的发放比例均为100%
			case when c.payment_terms='Z007' then 1
				when datediff(b.paid_date ,a.overdue_date_new)<=0 then 1
				when datediff(b.paid_date ,a.overdue_date_new)<=30 then 0.8
				when datediff(b.paid_date ,a.overdue_date_new)<=60 then 0.6
				when datediff(b.paid_date ,a.overdue_date_new)<=90 then 0.5
				when datediff(b.paid_date ,a.overdue_date_new)>90 then 0.1
			end rate
		from 
			(
				select customer_code,company_code,close_bill_no,date_format(paid_time,'yyyy-MM-dd') paid_date,
				sum(payment_amount) payment_amount
				from
					csx_dw.dwd_sss_r_d_close_bill_account_record_20200908
				--where (regexp_replace(substr(happen_date,1,10),'-','')<=${hiveconf:current_day} or happen_date='' or happen_date is NULL) 
				where regexp_replace(substr(paid_time,1,10),'-','') >=${hiveconf:month_start_day}
				and regexp_replace(substr(paid_time,1,10),'-','') <=${hiveconf:month_end_day}
				and date_format(happen_date,'yyyy-MM-dd')>='2022-03-01'
				and is_deleted ='0'
				--and money_back_id<>'0' --回款关联ID为0是微信支付、-1是退货系统核销
				group by customer_code,company_code,close_bill_no,date_format(paid_time,'yyyy-MM-dd')
			)b 
			left join
			(
				select source_bill_no,overdue_date_new	--逾期开始日期	
				from csx_dw.dws_sss_r_d_order_kp_settle_detail
				where sdt=${hiveconf:month_end_day}
				and date_format(happen_date,'yyyy-MM-dd')>='2022-03-01'
			)a on b.close_bill_no=a.source_bill_no
			--客户账期类型、账期、信控 若Z007 预付货款，则回款的发放比例均为100%
			left join
			(
				select customer_no customer_code,company_code,
				 payment_terms,payment_name,payment_days,payment_short_name,credit_limit,temp_credit_limit
				from csx_dw.dws_crm_w_a_customer_company 
				where sdt='current'
			)c on c.customer_code=b.customer_code and c.company_code=b.company_code	
	)a 
	group by close_bill_no
)b on b.close_bill_no=a.source_bill_no  
left join  
(
--本月前历史核销金额
	select sdt,
		source_bill_no,	--来源单号
		customer_code,	--客户编码
		happen_date,	--发生时间
		source_statement_amount,	--源单据对账金额
		company_code,	--签约公司编码
		residual_amount,	--剩余预付款金额_预付款客户抵消订单金额后
		residual_amount_sss,	--剩余预付款金额_原销售结算
		unpaid_amount,	--未回款金额_抵消预付款后
		unpaid_amount_sss unpaid_amount_ls,	--未回款金额_原销售结算
		bad_debt_amount,	--坏账金额
		payment_amount payment_amount_ls	--核销金额	 
	from csx_dw.dws_sss_r_d_order_kp_settle_detail
	where sdt=${hiveconf:last_month_end_day}
	and date_format(happen_date,'yyyy-MM-dd')>='2022-05-01'
)d on d.source_bill_no=a.source_bill_no  
; 

--纯现金客户标记 月BBC销售金额大于0，月授信支付金额等于0
drop table if exists csx_tmp.tmp_cust_chunxianjin;
create table csx_tmp.tmp_cust_chunxianjin
as
select *,bbc_sales_value-credit_settle_amount as xianjin_sales_value,
	if(bbc_sales_value>0 and sales_value=bbc_sales_value and coalesce(credit_settle_amount,0)=0,'是','否') is_chunxianjin
from 
(
	select a.customer_no,a.smonth,
		sum(a.sales_value) as sales_value,
		sum(a.bbc_sales_value) as bbc_sales_value,
		sum(b.credit_settle_amount) as credit_settle_amount -- 授信结算金额
		--if(sum(a.bbc_sales_value)>0 and coalesce(sum(b.credit_settle_amount),0)=0,'是','否') is_chunxianjin
	from
	(
		select 
			business_type_name,province_name,customer_no,customer_name,order_no,substr(sdt,1,6) as smonth,
			sum(sales_value) as sales_value,
			sum(case when business_type_code in('1','4','5','6') then sales_value else 0 end) as rp_bbc_sales_value,
			sum(case when business_type_code in('6') then sales_value else 0 end) as bbc_sales_value,
			sum(case when business_type_code in('2') then sales_value else 0 end) as fl_sales_value
		from csx_dw.dws_sale_r_d_detail
		where sdt>=${hiveconf:month_start_day} and sdt<=${hiveconf:month_end_day}
		--where sdt>='20220301'
		and channel_code in('1','7','9')	
		group by business_type_name,province_name,customer_no,customer_name,order_no,substr(sdt,1,6)
	)a 
	left join csx_ods.source_bbc_r_a_wshop_bill_order b on b.order_code=a.order_no
	group by a.customer_no,a.smonth
)a 
where bbc_sales_value>0
--and coalesce(credit_settle_amount,0)=0
;	



--客户当月转移前一个离职销售员
drop table csx_tmp.tmp_cust_sales_last1;
create table csx_tmp.tmp_cust_sales_last1
as
select a.*,b.emp_status
from 
(
select 
	a.region_name,a.province_name,a.customer_no,a.customer_name,
	b.sales_id_last,b.work_no_last,b.sales_name_last,
	a.sales_id_new,a.work_no_new,a.sales_name_new,b.max_sdt,
	row_number() over(partition by a.customer_no order by b.max_sdt desc) rno1
	--row_number() over(partition by a.customer_no order by a.min_sdt asc) rno2
from 
	(
	select region_name,province_name,
		customer_no,
		customer_name,
		sales_id_new,
		work_no_new,
		sales_name_new,
		user_position_new,
		rp_service_user_work_no_new,
		rp_service_user_name_new,
		rp_service_user_id_new,
		rp_service_user_position_new,
		fl_service_user_work_no_new,
		fl_service_user_name_new,
		fl_service_user_id_new,
		fl_service_user_position_new,
		bbc_service_user_work_no_new,
		bbc_service_user_name_new,
		bbc_service_user_id_new,
		bbc_service_user_position_new
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt=${hiveconf:month_end_day}
	and  customer_no<>''
	)a
left join 
	(
	select 
		customer_no,
		sales_id_new as sales_id_last,
		work_no_new as work_no_last,
		sales_name_new as sales_name_last,
		min(sdt) min_sdt,
		max(sdt) max_sdt
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt>=${hiveconf:month_start_day}
	and sdt<=${hiveconf:month_end_day}
	group by customer_no,sales_id_new,work_no_new,sales_name_new	
	)b on b.customer_no=a.customer_no	
)a 
join 
(
select employee_code jobno,emp_status
from csx_dw.dws_basic_w_a_employee_org_m
where sdt=regexp_replace(date_sub(current_date,1),'-','')
and emp_status='leave'
)b on a.work_no_last=b.jobno
where a.work_no_last<>a.work_no_new
and a.rno1=2;



--客户当月转移前一个离职服务管家
drop table csx_tmp.tmp_cust_service_last1;
create table csx_tmp.tmp_cust_service_last1
as
select a.*,b.emp_status
from 
(
select 
	a.flag,a.region_name,a.province_name,a.customer_no,a.customer_name,
	b.service_user_id_last,b.service_user_work_no_last,b.service_user_name_last,
	a.service_user_id_new,a.service_user_work_no_new,a.service_user_name_new,b.max_sdt,
	row_number() over(partition by a.flag,a.customer_no order by b.max_sdt desc) rno1
	--row_number() over(partition by a.customer_no order by a.min_sdt asc) rno2
from 
	(
	select region_name,province_name,
		'日配服务管家' flag,
		customer_no,
		customer_name,
		rp_service_user_work_no_new as service_user_work_no_new,
		rp_service_user_name_new as service_user_name_new,
		rp_service_user_id_new as service_user_id_new
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt=${hiveconf:month_end_day}
	and  customer_no<>''
	and rp_service_user_id_new is not null	
	union all
	select region_name,province_name,
		'福利服务管家' flag,
		customer_no,
		customer_name,
		fl_service_user_work_no_new as service_user_work_no_new,
		fl_service_user_name_new as service_user_name_new,
		fl_service_user_id_new as service_user_id_new
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt=${hiveconf:month_end_day}
	and  customer_no<>''
	and fl_service_user_id_new is not null	
	union all	
	select region_name,province_name,
		'BBC服务管家' flag,
		customer_no,
		customer_name,
		bbc_service_user_work_no_new as service_user_work_no_new,
		bbc_service_user_name_new as service_user_name_new,
		bbc_service_user_id_new as service_user_id_new
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt=${hiveconf:month_end_day}
	and  customer_no<>''
	and bbc_service_user_id_new is not null		
	)a
left join 
	(
	select 
		'日配服务管家' flag,
		customer_no,
		rp_service_user_work_no_new as service_user_work_no_last,
		rp_service_user_name_new as service_user_name_last,
		rp_service_user_id_new as service_user_id_last,
		min(sdt) min_sdt,
		max(sdt) max_sdt
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt>=${hiveconf:month_start_day}
	and sdt<=${hiveconf:month_end_day}
	and rp_service_user_id_new is not null
	group by customer_no,rp_service_user_work_no_new,rp_service_user_name_new,rp_service_user_id_new
	union all
	select 
		'福利服务管家' flag,
		customer_no,
		fl_service_user_work_no_new as service_user_work_no_last,
		fl_service_user_name_new as service_user_name_last,
		fl_service_user_id_new as service_user_id_last,
		min(sdt) min_sdt,
		max(sdt) max_sdt
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt>=${hiveconf:month_start_day}
	and sdt<=${hiveconf:month_end_day}
	and fl_service_user_id_new is not null
	group by customer_no,fl_service_user_work_no_new,fl_service_user_name_new,fl_service_user_id_new
	union all	
	select 
		'BBC服务管家' flag,
		customer_no,
		bbc_service_user_work_no_new as service_user_work_no_last,
		bbc_service_user_name_new as service_user_name_last,
		bbc_service_user_id_new as service_user_id_last,
		min(sdt) min_sdt,
		max(sdt) max_sdt
	from csx_tmp.report_crm_w_a_customer_service_manager_info_business
	where sdt>=${hiveconf:month_start_day}
	and sdt<=${hiveconf:month_end_day}
	and bbc_service_user_id_new is not null
	group by customer_no,bbc_service_user_work_no_new,bbc_service_user_name_new,bbc_service_user_id_new	
	)b on b.customer_no=a.customer_no and b.flag=a.flag
)a 
join 
(
select employee_code jobno,emp_status
from csx_dw.dws_basic_w_a_employee_org_m
where sdt=regexp_replace(date_sub(current_date,1),'-','')
and emp_status='leave'
)b on a.service_user_work_no_last=b.jobno
where a.service_user_work_no_last<>a.service_user_work_no_new
and a.rno1=2;

--客户提成详情
drop table csx_tmp.tc_r_new_cust_salary_tc_0; --17
create table csx_tmp.tc_r_new_cust_salary_tc_0
as
select
	a.smonth,
	--客户地区信息
	e.region_code as region_code_customer,e.region_name as region_name_customer,
	e.province_code as province_code_customer,e.province_name as province_name_customer,
	e.city_group_code as city_group_code_customer,e.city_group_name as city_group_name_customer,
	--销售员地区信息
	coalesce(c.region_code,'') as region_code_sales,
	coalesce(c.region_name,'') as region_name_sales,
	coalesce(c.province_code,'') as province_code_sales,
	coalesce(c.province_name,'') as province_name_sales,
	coalesce(c.city_group_code,'') as city_group_code_sales,
	coalesce(c.city_group_name,'') as city_group_name_sales,
	--日配服务管家地区信息
	coalesce(d.region_code,'') as region_code_rp_service,
	coalesce(d.region_name,'') as region_name_rp_service,
	coalesce(d.province_code,'') as province_code_rp_service,
	coalesce(d.province_name,'') as province_name_rp_service,
	coalesce(d.city_group_code,'') as city_group_code_rp_service,
	coalesce(d.city_group_name,'') as city_group_name_rp_service,
	--福利服务管家地区信息
	coalesce(d2.region_code,'') as region_code_fl_service,
	coalesce(d2.region_name,'') as region_name_fl_service,
	coalesce(d2.province_code,'') as province_code_fl_service,
	coalesce(d2.province_name,'') as province_name_fl_service,
	coalesce(d2.city_group_code,'') as city_group_code_fl_service,
	coalesce(d2.city_group_name,'') as city_group_name_fl_service,
	--BBC服务管家地区信息
	coalesce(d3.region_code,'') as region_code_bbc_service,
	coalesce(d3.region_name,'') as region_name_bbc_service,
	coalesce(d3.province_code,'') as province_code_bbc_service,
	coalesce(d3.province_name,'') as province_name_bbc_service,
	coalesce(d3.city_group_code,'') as city_group_code_bbc_service,
	coalesce(d3.city_group_name,'') as city_group_name_bbc_service,
	a.customer_id,a.customer_no,a.customer_name,
	coalesce(e.sales_id,'') as sales_id,
	coalesce(e.work_no,'') as work_no,
	coalesce(e.sales_name,'') as sales_name,
	coalesce(e.rp_service_user_id,'') as rp_service_user_id,
	coalesce(e.rp_service_user_work_no,'') as rp_service_user_work_no,
	coalesce(e.rp_service_user_name,'') as rp_service_user_name,
	coalesce(e.fl_service_user_id,'') as fl_service_user_id,
	coalesce(e.fl_service_user_work_no,'') as fl_service_user_work_no,
	coalesce(e.fl_service_user_name,'') as fl_service_user_name,
	coalesce(e.bbc_service_user_id,'') as bbc_service_user_id,
	coalesce(e.bbc_service_user_work_no,'') as bbc_service_user_work_no,
	coalesce(e.bbc_service_user_name,'') as bbc_service_user_name,
	f.sales_id_last,
	f.work_no_last,
	f.sales_name_last,
	e1.service_user_id_last as rp_service_user_id_last,
	e1.service_user_work_no_last as rp_service_user_work_no_last,
	e1.service_user_name_last as rp_service_user_name_last,
	e2.service_user_id_last as fl_service_user_id_last,
	e2.service_user_work_no_last as fl_service_user_work_no_last,
	e2.service_user_name_last as fl_service_user_name_last,
	e3.service_user_id_last as bbc_service_user_id_last,
	e3.service_user_work_no_last as bbc_service_user_work_no_last,
	e3.service_user_name_last as bbc_service_user_name_last,
	a.sales_value,a.rp_sales_value,a.bbc_sales_value,a.fl_sales_value,a.rp_bbc_sales_value,
	a.profit,a.rp_profit,a.bbc_profit,a.fl_profit,a.rp_bbc_profit,
	a.sales_prorate,
	a.sales_rp_prorate,
	a.sales_bbc_prorate,
	a.sales_rp_bbc_prorate,
	a.sales_fl_prorate,
	a.rpservice_rp_bbc_prorate,
	a.flservice_fl_prorate,
	a.bbcservice_rp_bbc_prorate,	
	a.salary_sales,		--销售员奖金包
	a.salary_rp_service,		--日配服务管家奖金包
	a.salary_fl_service,		--福利服务管家奖金包
	a.salary_bbc_service,		--bbc服务管家奖金包	
	a.rp_sales_sale_fp_rate,
	a.rp_sales_profit_fp_rate,
	a.fl_sales_sale_fp_rate,
	a.fl_sales_profit_fp_rate,
	a.bbc_sales_sale_fp_rate,
	a.bbc_sales_profit_fp_rate,
	a.rp_service_user_sale_fp_rate,
	a.rp_service_user_profit_fp_rate,
	a.fl_service_user_sale_fp_rate,
	a.fl_service_user_profit_fp_rate,
	a.bbc_service_user_sale_fp_rate,
	a.bbc_service_user_profit_fp_rate,
	a.sales_sales_value_ytd,
	a.sales_rp_bbc_sales_value_ytd,
	a.sales_fl_sales_value_ytd,
	a.rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
	a.flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
	a.bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额	
	a.sales_rp_bbc_sales_value_tc_rate,
	a.sales_fl_sales_value_tc_rate,
	a.sales_rp_bbc_profit_tc_rate,
	a.sales_fl_profit_tc_rate,	
	a.rpservice_rp_bbc_sales_value_tc_rate,
	a.bbcservice_rp_bbc_sales_value_tc_rate,	
	a.rpservice_rp_bbc_profit_tc_rate,
	a.flservice_fl_profit_tc_rate,
	a.bbcservice_rp_bbc_profit_tc_rate,	
	b.source_statement_amount,	--源单据对账金额
	b.unpaid_amount_sss,	--未回款金额_原销售结算 （本月末）
	b.paid_amount,	--核销金额 （本月末）
	b.unpaid_amount_ls,	--本月前历史 未回款金额
	b.payment_amount_ls,	--本月前历史 核销金额
	b.payment_amount_by,	--本月核销金额
	b.payment_amount_100,
	b.payment_amount_80,
	b.payment_amount_60,
	b.payment_amount_50,
	b.payment_amount_10,
	g.is_chunxianjin,
	a.tc_tax_sale,
	if(a.sales_value<0
	--若销售额为负值，若本月核销金额为正，则0，否则（若本月核销金额>销售额-覆盖金额，则本月核销金额，否则销售额-覆盖金额）
		,if(b.payment_amount_by>=0,0,if(b.payment_amount_by>a.sales_value-a.tc_tax_sale,b.payment_amount_by,a.sales_value-a.tc_tax_sale))
	--若销售额为正值，若本月核销金额>销售额-覆盖金额，则销售额-覆盖金额，否则（若本月核销金额>=0，则本月核销金额，否则0）
		,if(b.payment_amount_by>a.sales_value-a.tc_tax_sale,a.sales_value-a.tc_tax_sale,if(b.payment_amount_by>=0,b.payment_amount_by,0))
		) payment_amount_aby,
		
	if(a.sales_value<0
		,if(b.payment_amount_100>=0,0,if(b.payment_amount_100>a.sales_value-a.tc_tax_sale,b.payment_amount_100,a.sales_value-a.tc_tax_sale))
		,if(b.payment_amount_100>a.sales_value-a.tc_tax_sale,a.sales_value-a.tc_tax_sale,if(b.payment_amount_100>=0,b.payment_amount_100,0))
		) payment_amount_a100,
		
	if(a.sales_value<0
		,if(b.payment_amount_80>=0,0,if(b.payment_amount_80>a.sales_value-a.tc_tax_sale-b.payment_amount_100,b.payment_amount_80
		   ,if(a.sales_value-a.tc_tax_sale-b.payment_amount_100>=0,0,a.sales_value-a.tc_tax_sale-b.payment_amount_100)))
		,if(b.payment_amount_80>a.sales_value-a.tc_tax_sale-b.payment_amount_100
		   ,if(a.sales_value-a.tc_tax_sale-b.payment_amount_100<0,0,a.sales_value-a.tc_tax_sale-b.payment_amount_100)
		   ,if(b.payment_amount_80>=0,b.payment_amount_80,0))
		) payment_amount_a80,
		
	if(a.sales_value<0
		,if(b.payment_amount_60>=0,0,if(b.payment_amount_60>a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80,b.payment_amount_60
		   ,if(a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80>=0,0,a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80)))
		,if(b.payment_amount_60>a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80
		   ,if(a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80<0,0,a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80)
		   ,if(b.payment_amount_60>=0,b.payment_amount_60,0))
		) payment_amount_a60,
		
	if(a.sales_value<0
		,if(b.payment_amount_50>=0,0,if(b.payment_amount_50>a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60,b.payment_amount_50
		   ,if(a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60>=0,0,a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60)))
		,if(b.payment_amount_50>a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60
		   ,if(a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60<0,0,a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60)
		   ,if(b.payment_amount_50>=0,b.payment_amount_50,0))
		) payment_amount_a50,
		
	if(a.sales_value<0
		,if(b.payment_amount_10>=0,0,if(b.payment_amount_10>a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60-b.payment_amount_50,b.payment_amount_10
		   ,if(a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60-b.payment_amount_50>=0,0,a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60-b.payment_amount_50)))
		,if(b.payment_amount_10>a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60-b.payment_amount_50
		   ,if(a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60-b.payment_amount_50<0,0,a.sales_value-a.tc_tax_sale-b.payment_amount_100-b.payment_amount_80-b.payment_amount_60-b.payment_amount_50)
		   ,if(b.payment_amount_10>=0,b.payment_amount_10,0))
		) payment_amount_a10
from 
(
	select 
		smonth,	
		customer_id,customer_no,customer_name,	
		sales_value,rp_sales_value,bbc_sales_value,fl_sales_value,rp_bbc_sales_value,
		profit,rp_profit,bbc_profit,fl_profit,rp_bbc_profit,
		sales_prorate,
		sales_rp_prorate,
		sales_bbc_prorate,
		sales_rp_bbc_prorate,
		sales_fl_prorate,
		rpservice_rp_bbc_prorate,
		flservice_fl_prorate,
		bbcservice_rp_bbc_prorate,		
		salary_sales,		--销售员奖金包
		salary_rp_service,		--日配服务管家奖金包
		salary_fl_service,		--福利服务管家奖金包
		salary_bbc_service,		--bbc服务管家奖金包
		rp_sales_sale_fp_rate,
		rp_sales_profit_fp_rate,
		fl_sales_sale_fp_rate,
		fl_sales_profit_fp_rate,
		bbc_sales_sale_fp_rate,
		bbc_sales_profit_fp_rate,
		rp_service_user_sale_fp_rate,
		rp_service_user_profit_fp_rate,
		fl_service_user_sale_fp_rate,
		fl_service_user_profit_fp_rate,
		bbc_service_user_sale_fp_rate,
		bbc_service_user_profit_fp_rate,
		sales_sales_value_ytd,
		sales_rp_bbc_sales_value_ytd,
		sales_fl_sales_value_ytd,
		rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
		flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
		bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额
		sales_rp_bbc_sales_value_tc_rate,
		sales_fl_sales_value_tc_rate,
		sales_rp_bbc_profit_tc_rate,
		sales_fl_profit_tc_rate,	
		rpservice_rp_bbc_sales_value_tc_rate,
		bbcservice_rp_bbc_sales_value_tc_rate,		
		rpservice_rp_bbc_profit_tc_rate,
		flservice_fl_profit_tc_rate,
		bbcservice_rp_bbc_profit_tc_rate,		
		sales_value no_tc_tax_sale,	--未发提成覆盖金额
		0 tc_tax_sale	--已发提成覆盖金额
	from csx_tmp.tc_r_new_cust_salary_info_province
	union all	
	select	
		smonth,	
		customer_id,customer_no,customer_name,	
		sales_value,rp_sales_value,bbc_sales_value,fl_sales_value,rp_bbc_sales_value,
		profit,rp_profit,bbc_profit,fl_profit,rp_bbc_profit,
		sales_prorate,
		sales_rp_prorate,
		sales_bbc_prorate,
		sales_rp_bbc_prorate,
		sales_fl_prorate,
		rpservice_rp_bbc_prorate,
		flservice_fl_prorate,
		bbcservice_rp_bbc_prorate,		
		salary_sales,		--销售员奖金包
		salary_rp_service,		--日配服务管家奖金包
		salary_fl_service,		--福利服务管家奖金包
		salary_bbc_service,		--bbc服务管家奖金包
		rp_sales_sale_fp_rate,
		rp_sales_profit_fp_rate,
		fl_sales_sale_fp_rate,
		fl_sales_profit_fp_rate,
		bbc_sales_sale_fp_rate,
		bbc_sales_profit_fp_rate,
		rp_service_user_sale_fp_rate,
		rp_service_user_profit_fp_rate,
		fl_service_user_sale_fp_rate,
		fl_service_user_profit_fp_rate,
		bbc_service_user_sale_fp_rate,
		bbc_service_user_profit_fp_rate,
		sales_sales_value_ytd,
		sales_rp_bbc_sales_value_ytd,
		sales_fl_sales_value_ytd,
		rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
		flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
		bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额
		sales_rp_bbc_sales_value_tc_rate,
		sales_fl_sales_value_tc_rate,
		sales_rp_bbc_profit_tc_rate,
		sales_fl_profit_tc_rate,	
		rpservice_rp_bbc_sales_value_tc_rate,
		bbcservice_rp_bbc_sales_value_tc_rate,		
		rpservice_rp_bbc_profit_tc_rate,
		flservice_fl_profit_tc_rate,
		bbcservice_rp_bbc_profit_tc_rate,		
		max(sales_value)-max(tc_tax_sale) no_tc_tax_sale,	--未发提成覆盖金额
		max(tc_tax_sale) tc_tax_sale	--已发提成覆盖金额
	from csx_tmp.report_sss_r_m_crm_customer_tc_detail
	where smt<${hiveconf:current_month}	
	group by 
		smonth,	
		customer_id,customer_no,customer_name,	
		sales_value,rp_sales_value,bbc_sales_value,fl_sales_value,rp_bbc_sales_value,
		profit,rp_profit,bbc_profit,fl_profit,rp_bbc_profit,
		sales_prorate,
		sales_rp_prorate,
		sales_bbc_prorate,
		sales_rp_bbc_prorate,
		sales_fl_prorate,
		rpservice_rp_bbc_prorate,
		flservice_fl_prorate,
		bbcservice_rp_bbc_prorate,		
		salary_sales,		--销售员奖金包
		salary_rp_service,		--日配服务管家奖金包
		salary_fl_service,		--福利服务管家奖金包
		salary_bbc_service,		--bbc服务管家奖金包
		rp_sales_sale_fp_rate,
		rp_sales_profit_fp_rate,
		fl_sales_sale_fp_rate,
		fl_sales_profit_fp_rate,
		bbc_sales_sale_fp_rate,
		bbc_sales_profit_fp_rate,
		rp_service_user_sale_fp_rate,
		rp_service_user_profit_fp_rate,
		fl_service_user_sale_fp_rate,
		fl_service_user_profit_fp_rate,
		bbc_service_user_sale_fp_rate,
		bbc_service_user_profit_fp_rate,
		sales_sales_value_ytd,
		sales_rp_bbc_sales_value_ytd,
		sales_fl_sales_value_ytd,
		rpservice_rp_bbc_sales_value_ytd,
		flservice_fl_sales_value_ytd,
		bbcservice_rp_bbc_sales_value_ytd,
		sales_rp_bbc_sales_value_tc_rate,
		sales_fl_sales_value_tc_rate,
		sales_rp_bbc_profit_tc_rate,
		sales_fl_profit_tc_rate,	
		rpservice_rp_bbc_sales_value_tc_rate,
		bbcservice_rp_bbc_sales_value_tc_rate,		
		rpservice_rp_bbc_profit_tc_rate,
		flservice_fl_profit_tc_rate,
		bbcservice_rp_bbc_profit_tc_rate		
)a 
left join 
(
select 
    smonth,
    customer_code,	--客户编码
    sum(source_statement_amount) source_statement_amount,	--源单据对账金额
    sum(unpaid_amount_sss) unpaid_amount_sss,	--未回款金额_原销售结算 （本月末）
    sum(paid_amount) paid_amount,	--核销金额 （本月末）
	sum(payment_amount) payment_amount_by,	--本月核销金额
	sum(unpaid_amount_ls) unpaid_amount_ls,	--本月前历史 未回款金额
	sum(payment_amount_ls) payment_amount_ls,	--本月前历史 核销金额
	sum(payment_amount_100) payment_amount_100,
	sum(payment_amount_80) payment_amount_80,
	sum(payment_amount_60) payment_amount_60,
	sum(payment_amount_50) payment_amount_50,
	sum(payment_amount_10) payment_amount_10
from csx_tmp.tc_r_source_bill_paid
group by smonth,customer_code
)b on a.customer_no=b.customer_code and a.smonth=b.smonth
left join csx_tmp.tmp_cust_chunxianjin g on a.customer_no=g.customer_no and a.smonth=g.smonth
left join csx_tmp.tc_r_customer_service_manager_info_new e on e.customer_no=a.customer_no
	left join csx_tmp.tc_r_person_info c on c.id=e.sales_id
	left join csx_tmp.tc_r_person_info d on d.user_number=split(e.rp_service_user_work_no,';')[0]
	left join csx_tmp.tc_r_person_info d2 on d2.user_number=split(e.fl_service_user_work_no,';')[0]
	left join csx_tmp.tc_r_person_info d3 on d3.user_number=split(e.bbc_service_user_work_no,';')[0]
--客户当月转移前一个离职销售员
left join csx_tmp.tmp_cust_sales_last1 f on a.customer_no=f.customer_no
--客户当月转移前一个离职服务管家
left join csx_tmp.tmp_cust_service_last1 e1 on a.customer_no=e1.customer_no and e1.flag='日配服务管家'
left join csx_tmp.tmp_cust_service_last1 e2 on a.customer_no=e2.customer_no and e2.flag='福利服务管家'
left join csx_tmp.tmp_cust_service_last1 e3 on a.customer_no=e3.customer_no and e3.flag='BBC服务管家'
;




drop table csx_tmp.tc_r_new_cust_salary_tc; --17
create table csx_tmp.tc_r_new_cust_salary_tc
as
select
	smonth,
	--客户地区信息
	region_code_customer,
	region_name_customer,
	province_code_customer,
	province_name_customer,
	city_group_code_customer,
	city_group_name_customer,
	--销售员地区信息
	region_code_sales,
	region_name_sales,
	province_code_sales,
	province_name_sales,
	city_group_code_sales,
	city_group_name_sales,
	--日配服务管家地区信息
	region_code_rp_service,
	region_name_rp_service,
	province_code_rp_service,
	province_name_rp_service,
	city_group_code_rp_service,
	city_group_name_rp_service,
	--福利服务管家地区信息
	region_code_fl_service,
	region_name_fl_service,
	province_code_fl_service,
	province_name_fl_service,
	city_group_code_fl_service,
	city_group_name_fl_service,
	--BBC服务管家地区信息
	region_code_bbc_service,
	region_name_bbc_service,
	province_code_bbc_service,
	province_name_bbc_service,
	city_group_code_bbc_service,
	city_group_name_bbc_service,
	customer_id,customer_no,customer_name,
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
	sales_id_last,
	work_no_last,
	sales_name_last,
	rp_service_user_id_last,
	rp_service_user_work_no_last,
	rp_service_user_name_last,
	fl_service_user_id_last,
	fl_service_user_work_no_last,
	fl_service_user_name_last,
	bbc_service_user_id_last,
	bbc_service_user_work_no_last,
	bbc_service_user_name_last,
	sales_value,rp_sales_value,bbc_sales_value,fl_sales_value,rp_bbc_sales_value,
	profit,rp_profit,bbc_profit,fl_profit,rp_bbc_profit,
	sales_prorate,
	sales_rp_prorate,
	sales_bbc_prorate,
	sales_rp_bbc_prorate,
	sales_fl_prorate,
	rpservice_rp_bbc_prorate,
	flservice_fl_prorate,
	bbcservice_rp_bbc_prorate,	
	salary_sales,		--销售员奖金包
	salary_rp_service,		--日配服务管家奖金包
	salary_fl_service,		--福利服务管家奖金包
	salary_bbc_service,		--bbc服务管家奖金包	
	rp_sales_sale_fp_rate,
	rp_sales_profit_fp_rate,
	fl_sales_sale_fp_rate,
	fl_sales_profit_fp_rate,
	bbc_sales_sale_fp_rate,
	bbc_sales_profit_fp_rate,
	rp_service_user_sale_fp_rate,
	rp_service_user_profit_fp_rate,
	fl_service_user_sale_fp_rate,
	fl_service_user_profit_fp_rate,
	bbc_service_user_sale_fp_rate,
	bbc_service_user_profit_fp_rate,
	sales_sales_value_ytd,
	sales_rp_bbc_sales_value_ytd,
	sales_fl_sales_value_ytd,
	rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
	flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
	bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额
	sales_rp_bbc_sales_value_tc_rate,
	sales_fl_sales_value_tc_rate,
	sales_rp_bbc_profit_tc_rate,
	sales_fl_profit_tc_rate,	
	rpservice_rp_bbc_sales_value_tc_rate,
	bbcservice_rp_bbc_sales_value_tc_rate,	
	rpservice_rp_bbc_profit_tc_rate,
	flservice_fl_profit_tc_rate,
	bbcservice_rp_bbc_profit_tc_rate,	
	source_statement_amount,	--源单据对账金额
	unpaid_amount_sss,	--未回款金额_原销售结算 （本月末）
	paid_amount,	--核销金额 （本月末）
	unpaid_amount_ls,	--本月前历史 未回款金额
	payment_amount_ls,	--本月前历史 核销金额
	payment_amount_by,	--本月核销金额
	payment_amount_100,
	payment_amount_80,
	payment_amount_60,
	payment_amount_50,
	payment_amount_10,
	is_chunxianjin,

	payment_amount_aby,	--本月核销金额
	payment_amount_a100,
	payment_amount_a80,
	payment_amount_a60,
	payment_amount_a50,
	payment_amount_a10,
	
	sales_value-
	if((is_chunxianjin='是' and smonth=${hiveconf:current_month}),sales_value,payment_amount_aby+tc_tax_sale+
		--若源单据对账金额<销售额 差异金额
		if(sales_value>=0 and coalesce(source_statement_amount,0)<sales_value and smonth=${hiveconf:current_month},(sales_value-coalesce(source_statement_amount,0))
			,if(sales_value<0 and source_statement_amount is null and smonth=${hiveconf:current_month},sales_value,0))
	) as no_tc_tax_sale,	--未发提成覆盖金额
	
	if((is_chunxianjin='是' and smonth=${hiveconf:current_month}),sales_value,payment_amount_aby+tc_tax_sale+
		--若源单据对账金额<销售额 差异金额
		if(sales_value>=0 and coalesce(source_statement_amount,0)<sales_value and smonth=${hiveconf:current_month},(sales_value-coalesce(source_statement_amount,0))
			,if(sales_value<0 and source_statement_amount is null and smonth=${hiveconf:current_month},sales_value,0))
	)  tc_tax_sale,	--已发提成覆盖金额

--若 是纯现金用户，则 奖金包
--若本月核销金额>销售额，则
--  若100比例核销金额>销售额 则 奖金包 否则 奖金包*100比例核销金额/销售额*100% +
--  若100比例核销金额+80比例核销金额>销售额 则 奖金包*（销售额-100比例核销金额）/销售额*80% 否则 奖金包*80比例核销金额/销售额*80% +
--  若100比例核销金额+80比例核销金额+60比例核销金额>销售额 则 奖金包*（销售额-100比例核销金额-80比例核销金额）/销售额*60% 否则 奖金包*60比例核销金额/销售额*60% +
--  若100比例核销金额+80比例核销金额+60比例核销金额+50比例核销金额>销售额 则 奖金包*（销售额-100比例核销金额-80比例核销金额-60比例核销金额）/销售额*50% 否则 奖金包*50比例核销金额/销售额*50% +
--  奖金包*（销售额-100比例核销金额-80比例核销金额-60比例核销金额-50比例核销金额）/销售额*10%
--若本月核销金额>=0 and 本月核销金额<=销售额 则 各部分核销金额比例*奖金包*提成比例
--+
--若源单据对账金额<销售额 and 源单据对账金额>0 and 源单据对账金额=本月核销金额（+历史覆盖金额），则 奖金包*差异金额/销售额

	if((is_chunxianjin='是' and smonth=${hiveconf:current_month}),salary_sales,
				(payment_amount_a100/sales_value*salary_sales*1+
				payment_amount_a80/sales_value*salary_sales*0.8+
				payment_amount_a60/sales_value*salary_sales*0.6+
				payment_amount_a50/sales_value*salary_sales*0.5+
				payment_amount_a10/sales_value*salary_sales*0.1)
		+
		--若源单据对账金额<销售额 差异金额占比*奖金包
		if(sales_value>=0 and coalesce(source_statement_amount,0)<sales_value and smonth=${hiveconf:current_month},(sales_value-coalesce(source_statement_amount,0))
			,if(sales_value<0 and source_statement_amount is null and smonth=${hiveconf:current_month},sales_value,0))/sales_value*salary_sales*1		
	) as tc_sales,		--销售员提成

	if((is_chunxianjin='是' and smonth=${hiveconf:current_month}),salary_rp_service,
				(payment_amount_a100/sales_value*salary_rp_service*1+
				payment_amount_a80/sales_value*salary_rp_service*0.8+
				payment_amount_a60/sales_value*salary_rp_service*0.6+
				payment_amount_a50/sales_value*salary_rp_service*0.5+
				payment_amount_a10/sales_value*salary_rp_service*0.1)
		+
		--若源单据对账金额<销售额 差异金额占比*奖金包
		if(sales_value>=0 and coalesce(source_statement_amount,0)<sales_value and smonth=${hiveconf:current_month},(sales_value-coalesce(source_statement_amount,0))
			,if(sales_value<0 and source_statement_amount is null and smonth=${hiveconf:current_month},sales_value,0))/sales_value*salary_rp_service*1		
	) as tc_rp_service,		--日配服务管家提成

	if(is_chunxianjin='是' and smonth=${hiveconf:current_month},salary_fl_service,
				(payment_amount_a100/sales_value*salary_fl_service*1+
				payment_amount_a80/sales_value*salary_fl_service*0.8+
				payment_amount_a60/sales_value*salary_fl_service*0.6+
				payment_amount_a50/sales_value*salary_fl_service*0.5+
				payment_amount_a10/sales_value*salary_fl_service*0.1)
		+
		--若源单据对账金额<销售额 差异金额占比*奖金包
		if(sales_value>=0 and coalesce(source_statement_amount,0)<sales_value and smonth=${hiveconf:current_month},(sales_value-coalesce(source_statement_amount,0))
			,if(sales_value<0 and source_statement_amount is null and smonth=${hiveconf:current_month},sales_value,0))/sales_value*salary_fl_service*1	
	) as tc_fl_service,		--福利服务管家提成
	
	if(is_chunxianjin='是' and smonth=${hiveconf:current_month},salary_bbc_service,
				(payment_amount_a100/sales_value*salary_bbc_service*1+
				payment_amount_a80/sales_value*salary_bbc_service*0.8+
				payment_amount_a60/sales_value*salary_bbc_service*0.6+
				payment_amount_a50/sales_value*salary_bbc_service*0.5+
				payment_amount_a10/sales_value*salary_bbc_service*0.1)
		+
		--若源单据对账金额<销售额 差异金额占比*奖金包
		if(sales_value>=0 and coalesce(source_statement_amount,0)<sales_value and smonth=${hiveconf:current_month},(sales_value-coalesce(source_statement_amount,0))
			,if(sales_value<0 and source_statement_amount is null and smonth=${hiveconf:current_month},sales_value,0))/sales_value*salary_bbc_service*1		
	) as tc_bbc_service		--bbc服务管家提成	
from csx_tmp.tc_r_new_cust_salary_tc_0;



--客户提成宽表
insert overwrite table csx_tmp.report_sss_r_m_crm_customer_tc_detail partition(smt)
select
	concat_ws('-',${hiveconf:current_month},customer_no,smonth) as biz_id,
	smonth,
	region_code_customer,
	region_name_customer,
	province_code_customer,
	province_name_customer,
	city_group_code_customer,
	city_group_name_customer,
	if(region_code_sales<>'',region_code_sales,region_code_customer) region_code_sales,
	if(region_name_sales<>'',region_name_sales,region_name_customer) region_name_sales,
	if(province_code_sales<>'',province_code_sales,province_code_customer) province_code_sales,
	if(province_name_sales<>'',province_name_sales,province_name_customer) province_name_sales,
	if(city_group_code_sales<>'',city_group_code_sales,city_group_code_customer) city_group_code_sales,
	if(city_group_name_sales<>'',city_group_name_sales,city_group_name_customer) city_group_name_sales,
	
	if(region_code_rp_service<>'',region_code_rp_service,region_code_customer) region_code_rp_service,
	if(region_name_rp_service<>'',region_name_rp_service,region_name_customer) region_name_rp_service,
	if(province_code_rp_service<>'',province_code_rp_service,province_code_customer) province_code_rp_service,
	if(province_name_rp_service<>'',province_name_rp_service,province_name_customer) province_name_rp_service,
	if(city_group_code_rp_service<>'',city_group_code_rp_service,city_group_code_customer) city_group_code_rp_service,
	if(city_group_name_rp_service<>'',city_group_name_rp_service,city_group_name_customer) city_group_name_rp_service,
	
	if(region_code_fl_service<>'',region_code_fl_service,region_code_customer) region_code_fl_service,
	if(region_name_fl_service<>'',region_name_fl_service,region_name_customer) region_name_fl_service,
	if(province_code_fl_service<>'',province_code_fl_service,province_code_customer) province_code_fl_service,
	if(province_name_fl_service<>'',province_name_fl_service,province_name_customer) province_name_fl_service,
	if(city_group_code_fl_service<>'',city_group_code_fl_service,city_group_code_customer) city_group_code_fl_service,
	if(city_group_name_fl_service<>'',city_group_name_fl_service,city_group_name_customer) city_group_name_fl_service,
	
	if(region_code_bbc_service<>'',region_code_bbc_service,region_code_customer) region_code_bbc_service,
	if(region_name_bbc_service<>'',region_name_bbc_service,region_name_customer) region_name_bbc_service,
	if(province_code_bbc_service<>'',province_code_bbc_service,province_code_customer) province_code_bbc_service,
	if(province_name_bbc_service<>'',province_name_bbc_service,province_name_customer) province_name_bbc_service,
	if(city_group_code_bbc_service<>'',city_group_code_bbc_service,city_group_code_customer) city_group_code_bbc_service,
	if(city_group_name_bbc_service<>'',city_group_name_bbc_service,city_group_name_customer) city_group_name_bbc_service,
	customer_id,
	customer_no,
	customer_name,
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
	--客户对应当月前一离职的销售员、服务管家
	sales_id_last,
	work_no_last,
	sales_name_last,
	rp_service_user_id_last,
	rp_service_user_work_no_last,
	rp_service_user_name_last,
	fl_service_user_id_last,
	fl_service_user_work_no_last,
	fl_service_user_name_last,
	bbc_service_user_id_last,
	bbc_service_user_work_no_last,
	bbc_service_user_name_last,	
	sales_value,
	rp_sales_value,
	bbc_sales_value,
	fl_sales_value,
	rp_bbc_sales_value,
	profit,
	rp_profit,
	bbc_profit,
	fl_profit,
	rp_bbc_profit,
	sales_prorate,
	sales_rp_prorate,
	sales_bbc_prorate,
	sales_rp_bbc_prorate,
	sales_fl_prorate,
	rpservice_rp_bbc_prorate,
	flservice_fl_prorate,
	bbcservice_rp_bbc_prorate,	
	salary_sales,
	salary_rp_service,
	salary_fl_service,
	salary_bbc_service,
	rp_sales_sale_fp_rate,
	rp_sales_profit_fp_rate,
	fl_sales_sale_fp_rate,
	fl_sales_profit_fp_rate,
	bbc_sales_sale_fp_rate,
	bbc_sales_profit_fp_rate,
	rp_service_user_sale_fp_rate,
	rp_service_user_profit_fp_rate,
	fl_service_user_sale_fp_rate,
	fl_service_user_profit_fp_rate,
	bbc_service_user_sale_fp_rate,
	bbc_service_user_profit_fp_rate,
	sales_sales_value_ytd,
	sales_rp_bbc_sales_value_ytd,
	sales_fl_sales_value_ytd,
	rpservice_rp_bbc_sales_value_ytd,  --日配服务管家_年至今日配BBC销售额
	flservice_fl_sales_value_ytd,	--福利服务管家_年至今福利销售额
	bbcservice_rp_bbc_sales_value_ytd,  --BBC服务管家_年至今日配BBC销售额	
	sales_rp_bbc_sales_value_tc_rate,
	sales_fl_sales_value_tc_rate,
	sales_rp_bbc_profit_tc_rate,
	sales_fl_profit_tc_rate,	
	rpservice_rp_bbc_sales_value_tc_rate,
	bbcservice_rp_bbc_sales_value_tc_rate,	
	rpservice_rp_bbc_profit_tc_rate,
	flservice_fl_profit_tc_rate,
	bbcservice_rp_bbc_profit_tc_rate,	
	source_statement_amount,
	unpaid_amount_sss,
	paid_amount,
	unpaid_amount_ls,
	payment_amount_ls,
	payment_amount_by,
	payment_amount_100,
	payment_amount_80,
	payment_amount_60,
	payment_amount_50,
	payment_amount_10,
	no_tc_tax_sale,
	tc_tax_sale,
	tc_sales,
	tc_rp_service,
	tc_fl_service,
	tc_bbc_service,
	${hiveconf:created_time} update_time,
	${hiveconf:current_month} as smt -- 统计日期 
from csx_tmp.tc_r_new_cust_salary_tc;

--INVALIDATE METADATA csx_tmp.report_sss_r_m_crm_customer_tc_detail;

