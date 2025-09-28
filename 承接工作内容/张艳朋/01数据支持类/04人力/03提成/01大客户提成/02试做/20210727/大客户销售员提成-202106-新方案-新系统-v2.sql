
-- 新建表 销售提成_销售员收入组
--drop table csx_tmp.sales_income_info;
create table if not exists `csx_tmp.sales_income_info` (
  `cust_type` STRING comment '销售员类别',
  `sales_name` STRING comment '业务员名称',
  `work_no` STRING comment '业务员工号',
  `income_type` STRING comment '业务员收入组类'
) comment '销售提成_销售员收入组'
partitioned by (sdt string comment '日期分区')
row format delimited fields terminated by ','
stored as textfile;

--load data inpath '/tmp/zhangyanpeng/sales_income_info_06.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20210630');
--select * from csx_tmp.sales_income_info where sdt='20210630';




--有销售的销售员名单及收入组
select b.work_no,b.sales_name,c.income_type,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(front_profit) front_profit
from
(
select province_code,province_name,customer_no,substr(sdt,1,6) smonth,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(front_profit) front_profit
--from csx_dw.dws_sale_r_d_detail
from csx_dw.dws_sale_r_d_detail
where sdt>='20210601'
and sdt<'20210701'
and channel_code in('1','7')
group by province_code,province_name,customer_no,substr(sdt,1,6)
)a	
left join (select * from csx_dw.dws_crm_w_a_customer where sdt='20210630') b on b.customer_no=a.customer_no
left join (select distinct work_no,income_type from csx_tmp.sales_income_info where sdt='20210630') c on c.work_no=b.work_no
where c.income_type is null
and b.sales_name not like '%B%' 
and b.sales_name not like '%C%'
group by b.work_no,b.sales_name,c.income_type;

--★★★★★★★★~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~★★★★★★★★
--★★★★★★★★首先确认需对哪些销售员补充收入组★★★★★★★★
--★★★★★★★★~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~★★★★★★★★


-- 昨日、昨日月1日，上月1日，上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_12},${hiveconf:i_sdate_11};

--set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

--set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

set i_sdate_11 ='20210630';	
set i_sdate_12 ='20210601';				



---每日销售员提成系数（销额提成比例、前端毛利提成比例）
drop table csx_tmp.tmp_tc_salesname_rate_ytd;
create table csx_tmp.tmp_tc_salesname_rate_ytd
as
select sdt,work_no,sales_name,income_type,ytd,
case when ((ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
			or (ytd>10000000 and ytd<=20000000 and income_type in('Q2','Q3','Q4','Q5'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q3','Q4','Q5'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q4','Q5'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q5'))) then 0.002
	 when ((ytd>10000000 and ytd<=20000000 and income_type in('Q1'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q2'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q3'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q4'))
			or (ytd>50000000 and income_type in('Q5'))) then 0.0025
	 when ((ytd>20000000 and ytd<=30000000 and income_type in('Q1'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q2'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q3'))
			or (ytd>50000000 and income_type in('Q3','Q4'))) then 0.003
	 when ((ytd>30000000 and ytd<=40000000 and income_type in('Q1'))
			or (ytd>40000000 and income_type in('Q2'))) then 0.0035
	 when (ytd>40000000 and income_type in('Q1')) then 0.004			
else 0.002 end sale_rate,

case when ((ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
			or (ytd>10000000 and ytd<=20000000 and income_type in('Q2','Q3','Q4','Q5'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q3','Q4','Q5'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q4','Q5'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q5'))) then 0.1
	 when ((ytd>10000000 and ytd<=20000000 and income_type in('Q1'))
			or (ytd>20000000 and ytd<=30000000 and income_type in('Q2'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q3'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q4'))
			or (ytd>50000000 and income_type in('Q5'))) then 0.125
	 when ((ytd>20000000 and ytd<=30000000 and income_type in('Q1'))
			or (ytd>30000000 and ytd<=40000000 and income_type in('Q2'))
			or (ytd>40000000 and ytd<=50000000 and income_type in('Q3'))
			or (ytd>50000000 and income_type in('Q3','Q4'))) then 0.15
	 when ((ytd>30000000 and ytd<=40000000 and income_type in('Q1'))
			or (ytd>40000000 and income_type in('Q2'))) then 0.175
	 when (ytd>40000000 and income_type in('Q1')) then 0.2			
else 0.1 end profit_rate
from 
	(select 
		a.sdt,b.work_no,b.sales_name,coalesce(c.income_type,'Q1')income_type,
		sum(a.sales_value)over(PARTITION BY b.work_no,b.sales_name,substr(a.sdt,1,4) order by a.sdt ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING )ytd
	from 
		(select 
			sdt,customer_no,substr(sdt,1,6) smonth,
			if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
			) as sdt_last,  --sdt所在月最后1日，当月为昨日
			sum(sales_value) sales_value
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101' and sdt<=${hiveconf:i_sdate_11} --昨日月1日
			and channel_code in('1','7','9')
			and (business_type_code not in('3','4')
			--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
			or customer_no in(
			'116061','117817','116032','114853','117458','114485','119209','118509','117558','118262','118264','111734',
			'116055','115860','116038','119242','108726','107890','112410','112207','119257','119034','119255','119253',
			'119247','116027','111930','118472','119246','118731','115681','119214','111364','117548','114796','119397',
			'117393','112875','111964','116959','112857','114859','114075','117496','119022','113829','113735','115904',
			'115023','107892','118219','109460','119250','119262','115335','109357','119227','117889','118221','113634',
			'108713','113425','114841','115941','116056','114940','119254','119224','109363')
			)		
			--福建泉州签呈，订单12月销售530181.06元，1月全部退货，不算提成
			and (order_no not in ('OM20122800005550','RH21011900000203') or order_no is null)		
			and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
						'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
			--签呈客户不考核，不算提成 2021年3月签呈取消剔除103717
			and customer_no not in('111118','102755','104023','105673','104402')
			and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
			--签呈客户仅4月不考核，不算提成
			--and customer_no not in('PF0320','105177')
			--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
			--and customer_no not in('104179','112092')
			--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
			--and customer_no not in('114265','114248','114401','111933','113080','113392')
			--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
			--and customer_no not in('109484')
			--12月签呈 当月剔除逾期系数,其中'113744','113824','113826','113831'剔除提成
			--and customer_no not in('113744','113824','113826','113831')
			--2月签呈客户仅2月不考核，不算提成
			--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
			--                       '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
			--                       '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
			--                       '106330','102844','114054','109000','114083','114085','115909','115971','116215',
			--					   '111100','116058','116188','105601')			
			--3月签呈 剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
			--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
			and customer_no not in('115721','116877','116883','116015','116556','116826')
			and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
								'105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
								'106321','106325','106326','106330','104609')	
			--4月签呈 剔除逾期系数，不算提成
			--and customer_no not in('114265','117412','116957')
			--4月签呈 每月处理：剔除逾期系数，不算提成，每月处理
			and customer_no not in('102844','117940')
			--5月签呈 当月剔除逾期系数，不算提成
			--and customer_no not in('116957','106805')	
			--202106月签呈，不算提成 每月处理
			and customer_no not in('119861','105525')			
		group by 
			sdt,customer_no,substr(sdt,1,6),
			if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
				)	
		)a 
	left join   --CRM客户信息取每月最后一天
		(select * ,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
			--case when dev_source_code='2' then '业务代理' end as ywdl_cust,
			--case when dev_source_code='4' then '内购' end as ng_cust		
		from csx_dw.dws_crm_w_a_customer 
		--where sdt in('20200131','20200229','20200331','20200430','20200531','20200630','20200731','20200831','20200930','20201031','20201130','20201231')
		where sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
		and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					)  --sdt为每月最后一天
		)b on b.customer_no=a.customer_no and b.sdt=a.sdt_last 	
	left join (select distinct work_no,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_11}) c on c.work_no=b.work_no   --上月最后1日
	--left join   --CRM客户开发来源为业务代理--剔除
	--		(
	--		select distinct customer_no,substr(sdt,1,6) smonth 
	--		from csx_dw.dws_crm_w_a_customer 
	--		where sdt=${hiveconf:i_sdate_11}    --上月最后1日 
	--		and dev_source_code='2'
	--		)d on d.customer_no=a.customer_no
	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
	where (b.ywdl_cust is null or b.customer_no='118689')
	and b.ng_cust is null
	)a;


--01、客户本月每天-销售员销额、最终前端毛利统计
drop table csx_tmp.temp_tc_new_cust_00;
create table csx_tmp.temp_tc_new_cust_00
as
select 
	b.sales_province_name dist,a.customer_no cust_id,b.customer_name cust_name,d.work_no,d.sales_name,d.is_part_time_service_manager,
	d.service_user_work_no,d.service_user_name,
	d.sales_sale_rate,  --销售员_销售额提成比例
	d.sales_front_profit_rate,  --销售员_前端毛利提成比例
	d.service_user_sale_rate,  --服务管家_销售额提成比例
	d.service_user_front_profit_rate,	 --服务管家_前端毛利提成比例
	a.smonth,
	coalesce(c.sale_rate,0.002) sale_rate,coalesce(c.profit_rate,0.1) profit_rate,
	sum(sales_value)sales_value,
	sum(profit) profit,sum(profit)/sum(sales_value) prorate,
	sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate,
	round(sum(a.sales_value)*coalesce(c.sale_rate,0.002)+if(sum(a.front_profit)<0,0,coalesce(sum(a.front_profit),0)*coalesce(c.profit_rate,0.1)),2) salary
from 
	(
	select 
		sdt,substr(sdt,1,6) smonth,province_name,customer_no,
		sum(sales_value)sales_value,
		sum(profit) profit,sum(profit)/abs(sum(sales_value)) prorate,
		sum(front_profit) as front_profit,
		sum(front_profit)/abs(sum(sales_value)) as fnl_prorate
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11} --昨日月1日
		and channel_code in('1','7','9')
		and (business_type_code not in('3','4')
		--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
		or customer_no in(
        '116061','117817','116032','114853','117458','114485','119209','118509','117558','118262','118264','111734',
		'116055','115860','116038','119242','108726','107890','112410','112207','119257','119034','119255','119253',
		'119247','116027','111930','118472','119246','118731','115681','119214','111364','117548','114796','119397',
		'117393','112875','111964','116959','112857','114859','114075','117496','119022','113829','113735','115904',
		'115023','107892','118219','109460','119250','119262','115335','109357','119227','117889','118221','113634',
		'108713','113425','114841','115941','116056','114940','119254','119224','109363') 
			)		
		--福建泉州签呈，订单12月销售530181.06元，1月全部退货，不算提成
		--and (order_no not in ('OM20122800005550','RH21011900000203') or order_no is null)  
		--and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
		--					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
		--签呈客户不考核，不算提成 2021年3月签呈取消剔除 103717
		and customer_no not in('111118','102755','104023','105673','104402')
		and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')
		--签呈客户仅4月不考核，不算提成
		--and customer_no not in('PF0320','105177')
		--签呈客户仅7月不考核，不算提成
		--and customer_no not in('113108','113067','110656','111837','111296','105202')
		--9月签呈 四川 算到业务代理人，每月剔除逾期和销售
		and customer_no not in('104179','112092')
		--9月签呈 重庆 合伙人客户，9月剔除逾期和销售
		--and customer_no not in('114265','114248','114401','111933','113080','113392')
		--9月签呈 重庆 剔除9月逾期，其中'109484'剔除9月的逾期和销售
		--and customer_no not in('109484')
		--12月签呈 当月剔除逾期系数,其中'113744','113824','113826','113831'剔除提成
		--and customer_no not in('113744','113824','113826','113831') 
		--2月签呈客户仅2月不考核，不算提成
		--and customer_no not in('116015','115721','115721','116877','116883','116015','116556','116826','103253','103284','103296',
		--                          '103297','103304','103306','103311','104818','104828','104829','104835','105113','106283','106284',
		--                          '106298','106299','106301','106306','106307','106308','106309','106320','106321','106325','106326',
		--                          '106330','102844','114054','109000','114083','114085','115909','115971','116215',
		--					   '111100','116058','116188','105601')	 
		--3月签呈 剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
		--and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
		and customer_no not in('115721','116877','116883','116015','116556','116826')
		and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
								'105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
								'106321','106325','106326','106330','104609')  
		--4月签呈 剔除逾期系数，不算提成
		--and customer_no not in('114265','117412','116957')
		--4月签呈 每月处理：剔除逾期系数，不算提成，每月处理
		and customer_no not in('102844','117940')
		--5月签呈 当月剔除逾期系数，不算提成
		--and customer_no not in('116957','106805')
		--202106月签呈，不算提成 每月处理
		and customer_no not in('119861','105525')		
	group by 
		sdt,substr(sdt,1,6),province_name,customer_no


	--★★★扣减前端毛利 5月签呈
	--4月签呈，每月扣减
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'105569' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,10000 as front_profit,1 as fnl_prorate

	--202106月签呈，当月扣减
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'110026' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1593.07 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'115993' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3079.84 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'107059' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-14759 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'106989' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2204.75 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'105381' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-17745.5 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'111506' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3000 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'117007' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2958.24 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'111137' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-15774.46 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'上海市' as province_name,'107986' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-12471.92 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'江苏省' as province_name,'113090' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3743.33 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'江苏省' as province_name,'103995' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-18912 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117033' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,52.36 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113564' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-144.96 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117602' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5764.85 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'114813' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1630.39 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118047' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-17.64 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118212' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9377.1 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117340' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-21819.46 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118864' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-6789.86 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'115182' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-161.09 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113400' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1037.26 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'107655' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-15002.2 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117022' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1250.52 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'107912' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4420.95 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'111942' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2389.73 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'110252' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1483.94 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'105287' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2868.04 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'111960' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3312.08 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117093' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-919.91 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'115919' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-227.17 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113590' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1646.2 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'119845' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,16.33 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'108749' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2623.68 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'107867' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3238.74 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'112210' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-10 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'115091' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-0.01 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117148' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.13 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117998' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.09 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118036' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.25 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118086' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.94 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'111940' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2913.04 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'106516' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5797.1 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118317' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,8.53 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'115490' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-610.35 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'110242' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2001.86 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'115710' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9960.86 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'116928' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,602.03 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'114704' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-23.2 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113643' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-12313.9 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'106900' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4418.02 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118576' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4222.25 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'106572' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1658.37 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'115284' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2665.88 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118522' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1721.58 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118670' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,169.92 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117392' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.1 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'112177' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-6105.14 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117911' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1082.25 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113192' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6684 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'107461' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-441.24 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'111805' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-204.89 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'106898' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-16719.8 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117685' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,144.56 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113134' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3503.9 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'109977' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4093.82 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'107276' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-587.01 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117067' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,460.75 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'116461' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1074.94 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117289' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,156.89 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118405' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-93.9 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'119056' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,833.3 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118206' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1081.95 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'115178' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5919.58 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117170' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,12.72 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'105540' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-248.1 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'111984' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-657.44 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'114045' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1426.61 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'108795' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-418.09 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113645' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2366.2 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113571' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3099.24 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'119614' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,43.36 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'118420' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,31.24 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'116943' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-503.57 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'108040' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1034.85 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'119252' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-297.59 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'105569' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2390.05 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'115732' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1496.77 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'106434' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,19216.7 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'105756' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,11077.35 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'119818' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1189.56 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'109282' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,31.33 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'113463' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,39447.87 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'117068' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,50720.13 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'108018' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3039.42 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'119813' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1263.78 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'106559' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,23900.61 as front_profit,1 as fnl_prorate
	union all  select '20210630' as sdt,'202106' as smonth,'重庆市' as province_name,'108185' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,12914.38 as front_profit,1 as fnl_prorate
	)a
	left join 
		(
		select 
			distinct customer_no,customer_name,work_no,sales_name,sales_province_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
			--case when dev_source_code='2' then '业务代理' end as ywdl_cust,
			--case when dev_source_code='4' then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11} 
			--where sdt='20210713'
		)b on b.customer_no=a.customer_no
	left join 
		(
		select  
			work_no,sales_name,sdt,max(sale_rate) sale_rate,max(profit_rate) profit_rate
		from 
			csx_tmp.tmp_tc_salesname_rate_ytd 
		where 
			sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}  --上月1日，昨日月1日
		group by 
			work_no,sales_name,sdt
		)c on c.work_no=b.work_no and c.sales_name=b.sales_name and c.sdt=a.sdt
	----关联服务管家
	--left join		
	--  (  
	--      select customer_no,
	--	 concat_ws(',', collect_list(service_user_work_no)) as service_user_work_no,
	--	 concat_ws(',', collect_list(service_user_name)) as service_user_name
	--	from 
	--	  (select distinct customer_no,service_user_work_no,service_user_name
	--	  from csx_dw.dws_crm_w_a_customer_sales_link 
	--      where sdt=${hiveconf:i_sdate_11} 
	--	  and is_additional_info = 1 and service_user_id <> 0
	--      )a
	--	group by customer_no
	--  )d on d.customer_no=a.customer_no	
	--关联服务管家 5月计算用，客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,
			work_no,sales_name,is_part_time_service_manager,
			sales_sale_rate,  --销售员_销售额提成比例
			sales_front_profit_rate,  --销售员_前端毛利提成比例
			service_user_sale_rate,  --服务管家_销售额提成比例
			service_user_front_profit_rate	 --服务管家_前端毛利提成比例
		from 
			csx_tmp.tmp_tc_customer_service_manager_info	
			--from csx_dw.report_crm_w_a_customer_service_manager_info 
			--where sdt=${hiveconf:i_sdate_11}
			--where sdt='20210617' 
			--where sdt='20210713' 
		)d on d.customer_no=a.customer_no
	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理  
where 
	(b.ywdl_cust is null or b.customer_no='118689')
	and b.ng_cust is null
group by 
	b.sales_province_name,a.customer_no,b.customer_name,d.work_no,d.sales_name,d.is_part_time_service_manager,
	d.service_user_work_no,d.service_user_name,
	d.sales_sale_rate,  --销售员_销售额提成比例
	d.sales_front_profit_rate,  --销售员_前端毛利提成比例
	d.service_user_sale_rate,  --服务管家_销售额提成比例
	d.service_user_front_profit_rate,	 --服务管家_前端毛利提成比例
	a.smonth,c.sale_rate,c.profit_rate;



--大客户前端毛利扣点后结果
drop table csx_tmp.temp_tc_new_cust_01; --7
create table csx_tmp.temp_tc_new_cust_01
as
select 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,  --销售员_销售额提成比例
	a.sales_front_profit_rate,  --销售员_前端毛利提成比例
	a.service_user_sale_rate,  --服务管家_销售额提成比例
	a.service_user_front_profit_rate,	 --服务管家_前端毛利提成比例
	a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) prorate,
	sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)) fnl_profit,
	(sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)))/abs(sum(a.sales_value)) fnl_prorate,
	--round(sum(a.sales_value*coalesce(a.sale_rate,0.002))+
	--	  if((sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)))<=0,0,sum(coalesce(a.front_profit-a.sales_value*coalesce(z.rate,0),0)*coalesce(a.profit_rate,0.1))),2) salary
--销售额奖金包、前端毛利奖金包，未乘分配比例
	round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
	round(if((sum(a.front_profit)-sum(a.sales_value*coalesce(z.rate,0)))<=0,0,sum(coalesce(a.front_profit-a.sales_value*coalesce(z.rate,0),0)*coalesce(a.profit_rate,0.1))),2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
	left join
		(  --福建区域大客户扣点 20200115
		select '104824'cust_id, 0.02 rate
		union all   select '104847'cust_id, 0.02 rate
		union all   select '104854'cust_id, 0.02 rate
		union all   select '104859'cust_id, 0.02 rate
		union all   select '104870'cust_id, 0.02 rate
		union all   select 'PF0649'cust_id, 0.09 rate
		union all   select '102784'cust_id, 0.01 rate
		union all   select '102901'cust_id, 0.01 rate
		union all   select '102734'cust_id, 0.01 rate
		union all   select '103372'cust_id, 0.03 rate
		union all   select '103048'cust_id, 0.02 rate
		union all   select '105249'cust_id, 0.02 rate
		union all   select '106369'cust_id, 0.01 rate
		union all   select '105150'cust_id, 0.1 rate
		union all   select '105177'cust_id, 0.1 rate
		union all   select '105182'cust_id, 0.1 rate
		union all   select '105164'cust_id, 0.1 rate
		union all   select '105181'cust_id, 0.1 rate
		union all   select '105156'cust_id, 0.1 rate
		union all   select '105165'cust_id, 0.1 rate
		union all   select '106423'cust_id, 0.1 rate
		union all   select '106721'cust_id, 0.1 rate
		union all   select '106805'cust_id, 0.1 rate
		union all   select '107404'cust_id, 0.1 rate
		union all   select '105567'cust_id, 0.06 rate
		union all   select '105399'cust_id, 0.01 rate

		--4月签呈福建区域大客户扣点 每月处理
		--union all   select '102734'cust_id, 0.01 rate
		--union all   select '102784'cust_id, 0.01 rate
		--union all   select '102901'cust_id, 0.01 rate
		union all   select '113263'cust_id, 0.03 rate
		union all   select '115366'cust_id, 0.03 rate
		union all   select '114486'cust_id, 0.05 rate
		--union all   select 'PF0649'cust_id, 0.09 rate
		union all   select '108589'cust_id, 0.028 rate
		union all   select '114038'cust_id, 0.048 rate
		union all   select '102633'cust_id, 0.05 rate
		union all   select '117935'cust_id, 0.02 rate
		union all   select '113088'cust_id, 0.04 rate
		union all   select '104281'cust_id, 0.01 rate
		union all   select '105703'cust_id, 0.01 rate
		union all   select '105750'cust_id, 0.01 rate
		union all   select '106698'cust_id, 0.01 rate
		union all   select '112553'cust_id, 0.01 rate
		union all   select '113678'cust_id, 0.01 rate
		union all   select '113679'cust_id, 0.01 rate
		union all   select '113746'cust_id, 0.01 rate
		union all   select '113760'cust_id, 0.01 rate
		union all   select '113805'cust_id, 0.01 rate
		union all   select '115602'cust_id, 0.01 rate
		union all   select '115656'cust_id, 0.01 rate
		union all   select '115826'cust_id, 0.01 rate
		union all   select '102755'cust_id, 0.03 rate
		union all   select '106493'cust_id, 0.005 rate
		union all   select '115073'cust_id, 0.01 rate
		union all   select '115909'cust_id, 0.01 rate
		union all   select '115971'cust_id, 0.02 rate

		--4月签呈重庆大客户扣点 每月处理
		union all   select '112177'cust_id, 0.05 rate
		union all   select '115554'cust_id, 0.05 rate
		union all   select '116666'cust_id, 0.05 rate
		union all   select '118212'cust_id, 0.1 rate
		union all   select '112492'cust_id, 0.02 rate
		union all   select '110866'cust_id, 0.01 rate
		union all   select '106521'cust_id, 0.01 rate
		union all   select '116433'cust_id, 0.03 rate
		union all   select '118206'cust_id, 0.05 rate
		union all   select '115732'cust_id, 0.03 rate
		union all   select '105186'cust_id, 0.01 rate
		union all   select '104965'cust_id, 0.06 rate
		union all   select '112813'cust_id, 0.06 rate
		union all   select '117753'cust_id, 0.06 rate
		union all   select '117120'cust_id, 0.03 rate
		union all   select '116762'cust_id, 0.03 rate
		--union all   select '117871'cust_id, 0.05 rate --202106签呈 取消扣点
		union all   select '117727'cust_id, 0.06 rate
		union all   select '117728'cust_id, 0.06 rate
		union all   select '117729'cust_id, 0.06 rate
		union all   select '117748'cust_id, 0.06 rate
		union all   select '117749'cust_id, 0.06 rate
		union all   select '117761'cust_id, 0.06 rate
		union all   select '117766'cust_id, 0.06 rate
		union all   select '117773'cust_id, 0.06 rate
		union all   select '117776'cust_id, 0.06 rate
		union all   select '117777'cust_id, 0.06 rate
		union all   select '117781'cust_id, 0.06 rate
		union all   select '117782'cust_id, 0.06 rate
		union all   select '117783'cust_id, 0.06 rate
		union all   select '117784'cust_id, 0.06 rate
		union all   select '117785'cust_id, 0.06 rate
		union all   select '117786'cust_id, 0.06 rate
		union all   select '117790'cust_id, 0.06 rate
		union all   select '117791'cust_id, 0.06 rate
		union all   select '117795'cust_id, 0.06 rate
		union all   select '117796'cust_id, 0.06 rate
		union all   select '117800'cust_id, 0.06 rate
		union all   select '117805'cust_id, 0.06 rate
		union all   select '117918'cust_id, 0.06 rate
		--union all   select '117721'cust_id, 0.05 rate   --取消按比例扣减前端毛利金额，每月处理
		union all   select '112803'cust_id, 0.02 rate   --取消按比例扣减前端毛利金额，仅5月处理
		union all   select '118055'cust_id, 0.035 rate
		union all   select '115253'cust_id, 0.02 rate

		--5月签呈重庆大客户扣点 每月处理
		union all   select '113643'cust_id, 0.03 rate
		union all   select '118405'cust_id, 0.03 rate
		
		--202106月签呈重庆大客户扣点 每月处理
		union all   select '119517'cust_id, 0.03 rate
		)z on z.cust_id=a.cust_id
where 
	a.cust_id not in('115935','117762','118689','115971','113536')
	--and a.cust_id not in('116603','117002','115935')
	--and a.cust_id not in('115236')
	--202105月签呈
	--and a.cust_id not in(
	--'105220','105539','106239','106713','106900','107100','107242','107361','107532','108236','110242',
	--'115931','116461','104758','105956','105965','106288','106559','106878','107104','112492','112633',
	--'113423','105480','105540','106469','106524','106538','107438','111892','112210','117022','102534',
	--'102798','102806','105186','114724','115915','115920','103945','103954','104222','104229','104241',
	--'104251','104255','104379','104414','104965','105005','105024','105756','112813','117753'
	--)
	--202106月签呈
	and a.cust_id not in(
	'104758','105685','105956','105965','106288','106559','106878','107104','112492','112633','113423','114615','105480','105540','106469',
	'106524','106538','106704','107438','111892','112210','117022','102534','102798','102806','104741','105186','114724','115915','115920',
	'103945','103954','104222','104229','104241','104251','104255','104379','104414','104965','105005','105024','105756','112813','117753')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth

-- 签呈提成方式前端毛利*6%
union all
select 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) prorate,
	sum(a.front_profit) fnl_profit,
	sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
	--round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.06)),2) salary
	0 salary_sales_value,
	round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.06)),2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	--202105月签呈
	--a.cust_id in(
	--'105220','105539','106239','106713','106900','107100','107242','107361','107532','108236','110242',
	--'115931','116461','104758','105956','105965','106288','106559','106878','107104','112492','112633',
	--'113423','105480','105540','106469','106524','106538','107438','111892','112210','117022','102534',
	--'102798','102806','105186','114724','115915','115920','103945','103954','104222','104229','104241',
	--'104251','104255','104379','104414','104965','105005','105024','105756','112813','117753'
	--)
	--202106月签呈
	a.cust_id in(
	'104758','105685','105956','105965','106288','106559','106878','107104','112492','112633','113423','114615','105480','105540','106469',
	'106524','106538','106704','107438','111892','112210','117022','102534','102798','102806','104741','105186','114724','115915','115920',
	'103945','103954','104222','104229','104241','104251','104255','104379','104414','104965','105005','105024','105756','112813','117753')	
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth

-- 签呈不算销售提成，只算前端毛利提成
--5月签呈，'115236'当月处理
--union all
--select 
	--a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	--a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
	--sum(a.sales_value) sales_value,
	--sum(a.profit) profit,
	--sum(a.profit)/abs(sum(a.sales_value)) prorate,
	--sum(a.front_profit) fnl_profit,
	--sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
	--0 salary_sales_value,
	--round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*coalesce(a.profit_rate,0.1))),2) salary_fnl_profit
--from 
	--csx_tmp.temp_tc_new_cust_00 a
--where 
	--a.cust_id in('115236')
--group by 
	--a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	--a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth

---- 签呈只算销售提成，不算前端毛利提成
-- 3月签呈'115935'每月处理,4月签呈'117762'每月处理:提成方式销售额*0.2%*0.1，前端毛利*0%
--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成，李燕玲（81079631）提成方式销售额*0.2%*0.1，每月
--5月签呈'115971','113536'每月处理
union all
select 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) prorate,
	sum(a.front_profit) fnl_profit,
	sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
	--round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary
	round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
	0 salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	a.cust_id in('115935','117762','118689','115971','113536')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth
; --8

--4月签呈 总提成为服务管家提成，再按比例分配给销售员和服务管家，每月处理
drop table csx_tmp.temp_tc_new_cust_03; --9
create table csx_tmp.temp_tc_new_cust_03
as
select 
  dist,cust_id,cust_name,work_no,sales_name,is_part_time_service_manager,
  service_user_work_no,service_user_name,
  sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,service_user_front_profit_rate,
  smonth,sales_value,profit,prorate,fnl_profit,fnl_prorate,
  case when cust_id in ('107901','114099','115051','115047','109406','115753','117015','111204','110575','113569','115936',
      '103775','108105','109544','112088','106921','112663','107852','105915','108201','110898','111943') then salary_sales_value*0.1 
       else salary_sales_value end salary_sales_value,
  case when cust_id in ('107901','114099','115051','115047','109406','115753','117015','111204','110575','113569','115936',
      '103775','108105','109544','112088','106921','112663','107852','105915','108201','110898','111943') then salary_fnl_profit*0.2
       else salary_fnl_profit end salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_01
; -- 10


--5月签呈 提成方式总提成50%（销售员），当月处理
--drop table csx_tmp.temp_tc_new_cust_03;
--create table csx_tmp.temp_tc_new_cust_03
--as
--select 
--  dist,cust_id,cust_name,work_no,sales_name,is_part_time_service_manager,
--  service_user_work_no,service_user_name,
--  sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,service_user_front_profit_rate,
--  smonth,sales_value,profit,prorate,fnl_profit,fnl_prorate,
--  case when work_no in ('80960714','80937797') then salary_sales_value*0.5
--       else salary_sales_value end salary_sales_value,
--  case when work_no in ('80960714','80937797') then salary_fnl_profit*0.5
--       else salary_fnl_profit end salary_fnl_profit
--from csx_tmp.temp_tc_new_cust_02;



  
--结果表1 

--02客户、销售员逾期系数
--客户当月提成 

drop table csx_tmp.temp_tc_new_cust_salary; --11
create table csx_tmp.temp_tc_new_cust_salary
as
select 
	a.smonth,a.dist,a.cust_id,a.cust_name,
	--4月签呈，将以下客户的销售员调整为xx 每月处理
	--case when a.cust_id in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
	--     when a.cust_id in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
	--   else a.work_no end as work_no,
	--case when a.cust_id in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
	--     when a.cust_id in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
	--   else a.sales_name end as sales_name,	
	--202106月签呈，将以下客户的销售员调整为xx 当月处理
	case when a.cust_id in('106989') then '80950647'
		when a.cust_id in('106135') then '80725128'
	  else a.work_no end as work_no,
	case when a.cust_id in('106989') then '张伟炜'
		when a.cust_id in('106135') then '张丽琴'
	  else a.sales_name end as sales_name,		
	--a.work_no,a.sales_name,
	a.is_part_time_service_manager,
	a.service_user_work_no,a.service_user_name,
	a.sales_value,a.profit,a.profit/abs(a.sales_value) prorate,
	a.fnl_profit,a.fnl_profit/abs(a.sales_value) fnl_prorate,
	--a.salary,
	a.salary_sales_value,
	a.salary_fnl_profit,
	b.receivable_amount,b.over_amt,
	if(a.service_user_work_no<>'','服务管家有提成','服务管家无提成') assigned_type, --分配类别
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,
	--b.over_rate cust_over_rate,
	c.over_rate sale_over_rate,
	d.over_rate service_user_over_rate,
	
	if(z.salary_sales_value is null,if(a.salary_sales_value<0 or c.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_sales_value)*coalesce(a.sales_sale_rate,0) salary_sales_value_sale,
	
	if(z.salary_sales_value is null,if(a.salary_sales_value<0 or d.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	,z.salary_sales_value)*coalesce(a.service_user_sale_rate,0) salary_sales_value_service,
	
	if(z.salary_fnl_profit is null,if(c.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_fnl_profit)*coalesce(a.sales_front_profit_rate,0) salary_fnl_profit_sale,
	if(z.salary_fnl_profit is null,if(d.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	,z.salary_fnl_profit)*coalesce(a.service_user_front_profit_rate,0) salary_fnl_profit_service,
	e.fixed_fee
from  
	(
	select * 
	from csx_tmp.temp_tc_new_cust_03
	--  where cust_id not in(
	--'100326','104086','104217','112072','114486','PF1265','104151','104358','103372','105182','106423',
	--'106721','112054','102565','102790','103784','103855','105150','105181','111844','112976','112980',
	--'117554','102734','107404','102784','111400','102901','112288','112923','113467','103199','104469',
	--'104501','112038','115982','115987','116282','PF0365','102202','105355','105886','108067','114275',
	--'102647','112071')
	)a
	left join csx_tmp.temp_tc_cust_over_rate b on b.customer_no=a.cust_id
	left join csx_tmp.temp_tc_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
	left join csx_tmp.temp_tc_service_user_over_rate d on d.service_user_name=a.service_user_name and coalesce(d.service_user_work_no,0)=coalesce(a.service_user_work_no,0)
	--1月签呈 调整最终提成--按照奖金包特定值发
	left join 
		(
		select 
			'000000'cust_id, 0 salary_sales_value, 0 salary_fnl_profit
		union all
		select 
			'000001'cust_id, 0 salary_sales_value, 0 salary_fnl_profit
		)z on z.cust_id=a.cust_id
	--202106月签呈 安徽 给固定服务费
	left join
		(
		select '119109' as customer_no,500 as fixed_fee
		union all  select '119632' as customer_no,500 as fixed_fee
		union all  select '116915' as customer_no,250 as fixed_fee
		union all  select '115883' as customer_no,250 as fixed_fee
		union all  select '116727' as customer_no,1000 as fixed_fee
		union all  select '116566' as customer_no,1000 as fixed_fee
		union all  select '114834' as customer_no,1000 as fixed_fee
		union all  select '114853' as customer_no,500 as fixed_fee
		union all  select '105086' as customer_no,200 as fixed_fee
		union all  select '107093' as customer_no,200 as fixed_fee
		union all  select '109435' as customer_no,200 as fixed_fee
		union all  select '111255' as customer_no,100 as fixed_fee
		union all  select '116237' as customer_no,100 as fixed_fee
		union all  select '116527' as customer_no,100 as fixed_fee
		union all  select '118758' as customer_no,100 as fixed_fee
		union all  select '107433' as customer_no,500 as fixed_fee
		union all  select '104885' as customer_no,1500 as fixed_fee
		union all  select '115286' as customer_no,0 as fixed_fee
		union all  select '113439' as customer_no,2000 as fixed_fee
		union all  select '113857' as customer_no,250 as fixed_fee
		union all  select '113731' as customer_no,250 as fixed_fee
		union all  select '116038' as customer_no,400 as fixed_fee
		union all  select '116051' as customer_no,300 as fixed_fee
		union all  select '116055' as customer_no,300 as fixed_fee
		union all  select '116032' as customer_no,400 as fixed_fee
		union all  select '116056' as customer_no,300 as fixed_fee
		union all  select '116061' as customer_no,300 as fixed_fee
		union all  select '114485' as customer_no,500 as fixed_fee
		) e on e.customer_no=a.cust_id


----4月签呈 按照服务管家给提成，每月
--union all
--select 
--a.smonth,a.dist,a.cust_id,a.cust_name,
----4月签呈，将以下客户的销售员调整为xx
----case when a.cust_id in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
----     when a.cust_id in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
----   else a.work_no end as work_no,
----case when a.cust_id in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
----     when a.cust_id in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
----   else a.sales_name end as sales_name,	
--a.work_no,a.sales_name,
--a.is_part_time_service_manager,
--a.service_user_work_no,a.service_user_name,
--a.sales_value,a.profit,a.profit/abs(a.sales_value) prorate,
--a.fnl_profit,a.fnl_profit/abs(a.sales_value) fnl_prorate,
--a.salary_sales_value,
--a.salary_fnl_profit,
--b.receivable_amount,b.over_amt,
--if(a.service_user_work_no<>'','服务管家有提成','服务管家无提成') assigned_type, --分配类别
--c.over_rate sale_over_rate,
--d.over_rate service_user_over_rate,
--if(a.salary_sales_value<0 or c.over_rate is null,
--    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
--	*if(a.service_user_work_no<>'',0.1,1) salary_sales_value_sale,	
--if(a.salary_sales_value<0 or d.over_rate is null,
--    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
--	*if(a.service_user_work_no<>'',0.1,0) salary_sales_value_service,
--	
--if(c.over_rate is null,
--    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
--    *if(a.service_user_work_no<>'',0.2,1) salary_fnl_profit_sale,
--if(d.over_rate is null,
--    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
--	*if(a.service_user_work_no<>'',0.2,0) salary_fnl_profit_service	
--from  
--  (select * from csx_tmp.temp_new_cust_03
--  where cust_id in(
--'100326','104086','104217','112072','114486','PF1265','104151','104358','103372','105182','106423',
--'106721','112054','102565','102790','103784','103855','105150','105181','111844','112976','112980',
--'117554','102734','107404','102784','111400','102901','112288','112923','113467','103199','104469',
--'104501','112038','115982','115987','116282','PF0365','102202','105355','105886','108067','114275',
--'102647','112071')
--  )a
--left join csx_tmp.temp_cust_over_rate b on b.customer_no=a.cust_id
--left join csx_tmp.temp_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
--left join csx_tmp.temp_service_user_over_rate d on d.service_user_name=a.service_user_name and coalesce(d.service_user_work_no,0)=coalesce(a.service_user_work_no,0)
; --12


insert overwrite directory '/tmp/zhangyanpeng/tc_kehu' row format delimited fields terminated by '\t'
select 
	smonth,dist,cust_id,cust_name,work_no,sales_name,is_part_time_service_manager,service_user_work_no,service_user_name,sales_value,profit,prorate,fnl_profit,
	fnl_prorate,salary_sales_value,salary_fnl_profit,receivable_amount,over_amt,assigned_type,sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,
	service_user_front_profit_rate,sale_over_rate,service_user_over_rate,salary_sales_value_sale,salary_sales_value_service,salary_fnl_profit_sale,salary_fnl_profit_service,
	-- coalesce(fixed_fee,0) as fixed_fee,
	case when fixed_fee is not null then fixed_fee else coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0) end,
	coalesce(salary_sales_value_service,0)+coalesce(salary_fnl_profit_service,0)
from 
	csx_tmp.temp_tc_new_cust_salary;

--销售员当月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_xiaoshou' row format delimited fields terminated by '\t'
select 
	smonth,dist,work_no,sales_name,
	sum(sales_value)sales_value,
	sum(profit)profit,
	sum(profit)/abs(sum(sales_value)) prorate,
	sum(fnl_profit)fnl_profit,
	sum(fnl_profit)/abs(sum(sales_value)) fnl_prorate,
	sum(salary_sales_value) salary_sales_value,
	sum(salary_fnl_profit) salary_fnl_profit,
	sum(receivable_amount)receivable_amount,
	sum(over_amt)over_amt,
	--sum(salary_1)salary_1,
	sale_over_rate,
	sum(salary_sales_value_sale)salary_sales_value_sale,
	sum(salary_fnl_profit_sale)salary_fnl_profit_sale,
	--coalesce(sum(salary_sales_value_sale),0)+coalesce(sum(salary_fnl_profit_sale),0) salary_sale
	sum(case when fixed_fee is not null then fixed_fee else salary_sales_value_sale+salary_fnl_profit_sale end) as salary_sale
from 
	csx_tmp.temp_tc_new_cust_salary
group by 
	smonth,dist,work_no,sales_name,sale_over_rate;

--服务管家当月提成
insert overwrite directory '/tmp/zhangyanpeng/tc_fuwuguanjia' row format delimited fields terminated by '\t'
select 
	smonth,dist,service_user_work_no,service_user_name,
	sum(sales_value)sales_value,
	sum(profit)profit,
	sum(profit)/abs(sum(sales_value)) prorate,
	sum(fnl_profit)fnl_profit,
	sum(fnl_profit)/abs(sum(sales_value)) fnl_prorate,
	sum(salary_sales_value) salary_sales_value,
	sum(salary_fnl_profit) salary_fnl_profit,
	sum(receivable_amount)receivable_amount,
	sum(over_amt)over_amt,
	--sum(salary_1)salary_1,
	service_user_over_rate,
	sum(salary_sales_value_service)salary_sales_value_service,
	sum(salary_fnl_profit_service)salary_fnl_profit_service,
	coalesce(sum(salary_sales_value_service),0)+coalesce(sum(salary_fnl_profit_service),0) salary_service
from 
	csx_tmp.temp_tc_new_cust_salary
group by 
	smonth,dist,service_user_work_no,service_user_name,service_user_over_rate;





/*
-- 大客户提成：月度新客户
select b.sales_province_name,b.customer_no,b.customer_name,b.attribute_desc,b.attribute_name,b.dev_source_name,b.work_no,b.sales_name,b.sign_date,
	a.first_order_date
from
(
select attribute_desc,attribute_name,dev_source_name,customer_no,customer_name,channel_name,sales_name,work_no,sales_province_name,
regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,estimate_contract_amount*10000 estimate_contract_amount
from csx_dw.dws_crm_w_a_customer
where sdt='current'
and customer_no<>''
and channel_code in('1','7','8')
)b
join
--客户最早销售月 新客月、新客季度
	(select customer_no,
	min(first_order_date) first_order_date
	from csx_dw.dws_crm_w_a_customer_active
	where sdt = 'current'
	group by customer_no
	having min(first_order_date)>='20210601' and min(first_order_date)<'20210701'
	)a on b.customer_no=a.customer_no;

--客户对应销售员与服务管家
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select * 
from  csx_dw.report_crm_w_a_customer_service_manager_info
where  sdt= '20210630'
and channel_code in('1','7')
and (is_sale='是' or is_overdue='是')

--大客户销售员对照表
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select sales_province_name,customer_no,customer_name,work_no,sales_name,dev_source_name,
city_group_name,channel_name,
regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
from csx_dw.dws_crm_w_a_customer
--where sdt='20210617'
where sdt==${hiveconf:i_sdate_11}  
and channel_code in('1','7','8','9');




---截至上月销售员的累计销售额
drop table csx_dw.dws_cust_ytd_sale;
create table csx_dw.dws_cust_ytd_sale
as
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select b.work_no,b.sales_name,a.smonth,c.income_type,
sum(a.sales_value)sales_value,
sum(a.profit)profit
from 
  (select customer_no,substr(sdt,1,6) smonth,
  sum(sales_value) sales_value,
  sum(profit)profit
   from csx_dw.dws_sale_r_d_detail
  where sdt>='20210101' and sdt<=${hiveconf:i_sdate_11}  
  and channel_code in('1','7','9')
  and business_type_code not in('3','4')
  --福建泉州签呈，订单12月销售530181.06元，1月全部退货，不算提成
  and (order_no not in ('OM20122800005550','RH21011900000203') or order_no is null)		
  --签呈客户不考核，不算提成 2021年3月签呈取消剔除103717
  and customer_no not in('111118','102755','104023','105673','104402')
  and customer_no not in('107338','104123','102629','104526','106375','106380','106335','107268','104296','108391','108390','108072','108503')		
  --3月签呈 剔除逾期系数不算提成(其中'PF0065','112574','106782'3-5月不发提成);剔除逾期系数.不算提成.每月*2
  and customer_no not in('115721','116877','116883','116015','116556','116826')
  and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                            '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                            '106321','106325','106326','106330','104609')	
  --4月签呈 每月处理：剔除逾期系数，不算提成，每月处理
  and customer_no not in('102844','117940')  
  group by customer_no,substr(sdt,1,6)
  )a 
left join   --CRM客户信息取每月最后一天
  (select * ,
    substr(sdt,1,6) smonth,
    case when channel_code='9' then '业务代理' end as ywdl_cust,
    case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust	
  from csx_dw.dws_crm_w_a_customer 
  where sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
  and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
             regexp_replace(date_sub(current_date,1),'-',''),
             regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
             )  --sdt为每月最后一天
  )b on b.customer_no=a.customer_no and b.smonth=a.smonth 
left join (select distinct work_no,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_11}) c on c.work_no=b.work_no   --上月最后1日
--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
where (b.ywdl_cust is null or b.customer_no='118689')
and b.ng_cust is null 
group by b.work_no,b.sales_name,a.smonth,c.income_type;




