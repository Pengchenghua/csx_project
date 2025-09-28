
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

--load data inpath '/tmp/zhangyanpeng/sales_income_info_09.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20210930');
--select * from csx_tmp.sales_income_info where sdt='20210930';




--有销售的销售员名单及收入组
select b.work_no,b.sales_name,c.income_type,
	sum(sales_value) sales_value,
	sum(profit) profit,
	sum(front_profit) front_profit
from
	(
	select 
		province_code,province_name,customer_no,substr(sdt,1,6) smonth,
		sum(sales_value) sales_value,
		sum(profit) profit,
		sum(front_profit) front_profit
		--from csx_dw.dws_sale_r_d_detail
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt>='20210901'
		and sdt<'20211001'
		and channel_code in('1','7')
	group by 
		province_code,province_name,customer_no,substr(sdt,1,6)
	)a	
	left join 
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt='20210930'
		) b on b.customer_no=a.customer_no
	left join 
		(
		select 
			distinct work_no,income_type 
		from 
			csx_tmp.sales_income_info 
		where 
			sdt='20210831'
		) c on c.work_no=b.work_no
where 
	c.income_type is null
	and b.sales_name not like '%B%' 
	and b.sales_name not like '%C%'
group by 
	b.work_no,b.sales_name,c.income_type;

--★★★★★★★★~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~★★★★★★★★
--★★★★★★★★首先确认需对哪些销售员补充收入组★★★★★★★★
--★★★★★★★★~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~★★★★★★★★


-- 昨日、昨日月1日，上月1日，上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_12},${hiveconf:i_sdate_11};

--set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

--set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
--set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

--SET hive.execution.engine=spark;

set i_sdate_11 ='20210930';	
set i_sdate_12 ='20210901';				



---每日销售员提成系数（销额提成比例、前端毛利提成比例）
drop table csx_tmp.tmp_tc_salesname_rate_ytd;
create table csx_tmp.tmp_tc_salesname_rate_ytd
as
select 
	sdt,work_no,sales_name,income_type,ytd,
	case when (
			-- 202109月签呈，销售员固定提成比例，每月处理
			work_no in ('80751663','80991769')
			or (ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
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

	case when (
			-- 202109月签呈，销售员固定提成比例，每月处理
			work_no in ('80751663','80991769')	
			or (ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
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
	(
	select 
		a.sdt,b.work_no,b.sales_name,coalesce(c.income_type,'Q1')income_type,
		sum(a.sales_value)over(PARTITION BY b.work_no,b.sales_name,substr(a.sdt,1,4) order by a.sdt ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING )ytd
	from 
		(
		select 
			sdt,customer_no,substr(sdt,1,6) smonth,
			if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
			) as sdt_last,  --sdt所在月最后1日，当月为昨日
			--202107月签呈，W0K4仓不计算销售额，仅计算前端毛利额，每月处理
			sum(case when dc_code='W0K4' then 0 else sales_value end) sales_value --202107月签呈，W0K4仓不计算销售额，仅计算前端毛利额，每月处理
		from 
			csx_dw.dws_sale_r_d_detail
		where 
			sdt>='20210101' and sdt<=${hiveconf:i_sdate_11} --昨日月1日
			and channel_code in('1','7','9')
			and (business_type_code not in('3','4')
			--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
			or customer_no in(
			'107892','112207','113617','113634','114485','114853','114940','115151','115392','116027','116032','116038','116959','117496','117548','118078','118221','118472',
			'119254','120255','120939','121475','121855','122264','109357','112857','113425','116055','117458','117558','117817','117889','118219','118463','118509','119209',
			'119214','119224','119255','119397','119892','119911','120294','120321','120826','121020','121032','121039','121276','107890','109363','111364','111473','112410',
			'113735','114809','114859','115023','115450','115589','115600','115941','116024','118264','119022','119242','119247','119257','120376','120846','120872','120879',
			'120999','121287','121337','121994','122335','122394','105163','108713','109460','111734','112906','113829','114796','115681','116056','116061','118259','118262',
			'118870','119132','119227','119246','119250','119253','119262','120147','120768','121398','121467','121483','121495','122406')
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
								'105113','104609')	
			--4月签呈 剔除逾期系数，不算提成
			--and customer_no not in('114265','117412','116957')
			--4月签呈 每月处理：剔除逾期系数，不算提成，每月处理
			and customer_no not in('102844','117940')
			--5月签呈 当月剔除逾期系数，不算提成
			--and customer_no not in('116957','106805')	
			--202106月签呈，不算提成，每月处理
			and customer_no not in('119861','105525')
			--202107月签呈，不算提成，每月处理
			and customer_no not in('114075','115971')
			--202107月签呈，不算提成，当月处理
			--and customer_no not in('111473','113511','119730','115110','119965','115410')		
			--202109月签呈，不算提成，当月处理
			and customer_no not in('116870','118537','121981')				
		group by 
			sdt,customer_no,substr(sdt,1,6),
			if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
				regexp_replace(date_sub(current_date,1),'-',''),
				regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
				)	
		)a 
		left join   --CRM客户信息取每月最后一天
			(
			select 
				* ,
				case when channel_code='9' then '业务代理' end as ywdl_cust,
				case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
				--case when dev_source_code='2' then '业务代理' end as ywdl_cust,
				--case when dev_source_code='4' then '内购' end as ng_cust		
			from 
				csx_dw.dws_crm_w_a_customer 
				--where sdt in('20200131','20200229','20200331','20200430','20200531','20200630','20200731','20200831','20200930','20201031','20201130','20201231')
			where 
				sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
				and sdt=if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					)  --sdt为每月最后一天
			)b on b.customer_no=a.customer_no and b.sdt=a.sdt_last 	
		left join 
			(
			select 
				distinct work_no,income_type 
			from 
				csx_tmp.sales_income_info 
			where 
				sdt=${hiveconf:i_sdate_11}
			) c on c.work_no=b.work_no   --上月最后1日
		--left join   --CRM客户开发来源为业务代理--剔除
		--		(
		--		select distinct customer_no,substr(sdt,1,6) smonth 
		--		from csx_dw.dws_crm_w_a_customer 
		--		where sdt=${hiveconf:i_sdate_11}    --上月最后1日 
		--		and dev_source_code='2'
		--		)d on d.customer_no=a.customer_no
	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
	--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理
	where 
		(b.ywdl_cust is null or b.customer_no in ('118689','116957','116629'))
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
		--202107月签呈，W0K4仓不计算销售额，仅计算前端毛利额，每月处理
		sum(case when dc_code='W0K4' then 0 else sales_value end)sales_value, --202107月签呈，W0K4仓不计算销售额，仅计算前端毛利额，每月处理
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
		'107892','112207','113617','113634','114485','114853','114940','115151','115392','116027','116032','116038','116959','117496','117548','118078','118221','118472',
		'119254','120255','120939','121475','121855','122264','109357','112857','113425','116055','117458','117558','117817','117889','118219','118463','118509','119209',
		'119214','119224','119255','119397','119892','119911','120294','120321','120826','121020','121032','121039','121276','107890','109363','111364','111473','112410',
		'113735','114809','114859','115023','115450','115589','115600','115941','116024','118264','119022','119242','119247','119257','120376','120846','120872','120879',
		'120999','121287','121337','121994','122335','122394','105163','108713','109460','111734','112906','113829','114796','115681','116056','116061','118259','118262',
		'118870','119132','119227','119246','119250','119253','119262','120147','120768','121398','121467','121483','121495','122406')
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
								'105113','104609')  
		--4月签呈 剔除逾期系数，不算提成
		--and customer_no not in('114265','117412','116957')
		--4月签呈 每月处理：剔除逾期系数，不算提成，每月处理
		and customer_no not in('102844','117940')
		--5月签呈 当月剔除逾期系数，不算提成
		--and customer_no not in('116957','106805')
		--202106月签呈，不算提成 每月处理
		and customer_no not in('119861','105525')
		--202107月签呈，不算提成，每月处理
		and customer_no not in('114075','115971')
		--202107月签呈，不算提成，当月处理
		--and customer_no not in('111473','113511','119730','115110','119965','115410')		
		--202109月签呈，不算提成，当月处理
		and customer_no not in('116870','118537','121981')	
	group by 
		sdt,substr(sdt,1,6),province_name,customer_no


	--★★★扣减前端毛利 5月签呈
	--4月签呈，每月扣减
	--202108月签呈，每月处理，注意更改时间
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'105569' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-10000 as front_profit,1 as fnl_prorate


	--202109月签呈，当月扣减
	union all  select '20210930' as sdt,'202109' as smonth,'上海松江' as province_name,'110026' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3247.58 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'上海松江' as province_name,'106989' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1994.66 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'上海松江' as province_name,'115993' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3520.12 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'上海松江' as province_name,'121462' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5328 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'上海松江' as province_name,'114272' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3799.84 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'上海松江' as province_name,'107059' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3909.58 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'118738' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-145085.49 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'113564' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-263.3 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'119776' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-265.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'120706' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-36324 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'115710' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9311.6 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'116928' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-468.2 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'120435' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-735.5 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'120240' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3.3 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'113920' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3172.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'106898' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-28990.72 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'112631' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-422 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'117340' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-30946.77 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'119659' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-11952.72 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'107655' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-11960.61 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'113134' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-7168.73 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'112177' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5749.41 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'113643' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5999.61 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'120105' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-11918.14 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'108040' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2784.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'109977' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5192.96 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'106900' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4118.35 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'107912' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2215.29 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'110252' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3967.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'104776' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-210 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'107099' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3014.12 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'105287' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2855.35 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'111932' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2896.17 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'110242' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2525.29 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'111960' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1572.8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'111896' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-551.6 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'111984' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1984.56 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'120108' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-466.85 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'108795' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-717.73 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'111956' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1287.5 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'119765' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-52.97 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'111942' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-795.6 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'107461' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-692.84 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'107500' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4964 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'115284' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-648.3 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'117093' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-557.2 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'106572' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-616.88 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'113400' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-146.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'111926' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-150.3 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'120643' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-55.68 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'重庆市' as province_name,'113643' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9301.72 as front_profit,1 as fnl_prorate
	--202109月，前端毛利报价问题，增加前端毛利额，当月处理
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'100326' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,4.6 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'101585' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,15.15 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'101916' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,79.71 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102202' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,431 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102229' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,27.93 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102508' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,114.24 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102680' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,25.7 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102751' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102754' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,54.48 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102790' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102901' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,20 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'102995' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,18.41 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103320' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.58 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103332' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.28 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103355' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.29 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'103700' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,15.72 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103714' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,136.14 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103717' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,12.15 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103759' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.39 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103810' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,24.28 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103830' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,10.52 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'103835' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,16.41 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'103859' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.5 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'104086' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,42.38 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'104165' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,19.84 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'104324' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,18.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'104469' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,120.03 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'104478' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,12.7 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'104493' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,25.2 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'104590' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.32 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'104612' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,71.89 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'104730' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,111.12 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'104877' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.95 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'105081' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.6 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'105171' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.35 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'105265' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.56 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'105394' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.13 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'105441' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,10.97 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'105552' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.74 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'105641' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.15 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'105757' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.05 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'105827' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,21.78 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'105862' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.44 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'105882' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,15.25 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'105896' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9.41 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'106031' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106183' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.66 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106265' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'106346' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.42 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106371' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,74.44 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106376' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.86 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106482' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9.81 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106539' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106563' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9.33 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'106600' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106727' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.91 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106782' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.28 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'106923' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.93 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106929' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,8.51 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'106933' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,53.55 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'107084' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.78 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'107188' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,15.32 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'107204' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.96 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'107237' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.99 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'107260' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.49 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'107337' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'107408' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.06 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'107746' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.66 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'107784' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,15.09 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'107798' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,11.7 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'107897' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9.91 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'107901' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,17.33 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'107914' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,8.88 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'107946' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.39 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108079' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,29.51 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108105' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.22 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108107' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,14.04 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108115' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,41.46 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108127' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,16.9 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'108179' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.9 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108201' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.05 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108367' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.93 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108739' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,21.46 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'108774' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,48.31 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'108797' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.22 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'109206' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.53 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'109377' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'109401' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.22 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'109544' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.69 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'109685' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.02 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'109974' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.33 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'109981' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.12 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'110017' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,49.56 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'110554' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.18 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'110602' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,71.8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'110690' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'110693' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,104.15 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'110779' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.42 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'110898' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.52 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'111058' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,13.31 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'111350' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.92 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'111388' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,52.16 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'111399' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.68 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'111560' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.17 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'111562' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.39 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'111788' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,13.42 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'111860' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,41.11 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'111921' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,23.24 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112088' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.67 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112288' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,61.17 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112327' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9.13 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112380' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,204.03 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112409' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.38 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112574' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,18.88 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112658' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.4 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'112662' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.2 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112663' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.88 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'112664' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.75 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112722' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,13.02 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112747' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,30.23 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'112823' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.6 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'112952' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,13.25 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113088' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.42 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'113096' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.44 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113159' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.61 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113197' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,38.9 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113390' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.42 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'113461' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,37.08 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'113487' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.2 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'113499' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.88 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113555' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9.97 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113615' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,10.97 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'113641' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.12 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113646' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.82 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'113720' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.51 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113736' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,17.43 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'113749' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.95 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'113762' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.05 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'113774' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.08 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'113781' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'113840' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.35 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'113873' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.2 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'113878' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,14.36 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'113918' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,25.6 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'113940' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.56 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'114412' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.66 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'114463' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,10.45 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'114471' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.21 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'114481' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,11.11 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'114739' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.56 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'114812' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.13 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'114843' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,32.74 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'114852' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.13 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'114873' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.66 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115073' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,84.96 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115188' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,20.26 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115204' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,14.21 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115205' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.9 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'115486' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.6 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'115549' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,19.42 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'115566' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.51 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115643' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,62.5 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115646' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,11.49 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'115689' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.35 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'115728' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.33 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115753' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,23.89 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'115790' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.9 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115857' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,40.5 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'115875' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.7 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'115883' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,11.93 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'115892' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,12.04 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115906' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,490.19 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'115936' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.77 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'116025' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.14 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'117004' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.48 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'117143' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'117145' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9.4 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'117160' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.23 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'117209' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,36.59 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'117304' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.67 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'117423' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.4 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'117566' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.77 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'117593' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.21 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'117611' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'117676' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.07 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'117842' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.82 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'117857' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.58 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'117862' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.36 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'118120' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.73 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'118172' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.7 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'118205' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.07 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'118450' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'118503' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,28.22 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'118579' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.78 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'118649' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'118704' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.05 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'118752' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.78 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'118824' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.37 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'118847' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.38 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'118978' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.03 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'119045' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,13.69 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'119142' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.59 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'119319' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.83 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'119333' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'119434' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,13.04 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'119473' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.15 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'119488' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,5.65 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'119502' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.36 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'119525' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,33.09 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'119716' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.08 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'119754' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.84 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'119799' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.34 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'119847' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,13.87 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'119853' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,17.09 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'120000' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,19.74 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120023' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.24 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'120050' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,10.12 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'120183' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,380.35 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120227' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.4 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120341' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.02 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120347' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.63 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120350' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.65 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'120418' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,33.84 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'120428' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.59 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'120459' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,122.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'120465' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,16.56 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'120538' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,54.88 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120563' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.02 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'120708' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.01 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'120757' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,18.7 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120901' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,10.14 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120907' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.76 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120910' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.43 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'120921' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.4 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'120923' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'120934' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'120955' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,6.16 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'121010' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.09 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'121018' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'121049' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,33.64 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'121080' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,27 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'121122' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,15.84 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'121141' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,9.22 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'121153' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.32 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'121160' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.8 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121193' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.16 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121233' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,178.19 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121255' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,13.16 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'121258' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.29 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'121291' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,14.67 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'121295' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.52 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'121309' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.5 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'121331' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.85 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'121365' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.23 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121396' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.95 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'121447' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.59 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121449' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,7.67 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121488' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,0.1 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'贵州省' as province_name,'121778' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,1.14 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121861' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.63 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121871' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,10.08 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'安徽省' as province_name,'121967' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,2.02 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'四川省' as province_name,'122119' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,63.5 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'PF0458' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,3.39 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'PF0649' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,12.7 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'PF0937' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,18.82 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'福建省' as province_name,'PF1209' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,4.61 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'陕西省' as province_name,'118815' as customer_no,
		-2664000 as sales_value, 0 as profit,1 prorate,-11840 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'陕西省' as province_name,'122323' as customer_no,
		-542552 as sales_value, 0 as profit,1 prorate,-50102.41 as front_profit,1 as fnl_prorate
	union all  select '20210930' as sdt,'202109' as smonth,'上海松江' as province_name,'110026' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-60275.6 as front_profit,1 as fnl_prorate


	)a
	left join 
		(
		select 
			distinct customer_no,customer_name,work_no,sales_name,
			sales_province_name,
			case when channel_code='9' then '业务代理' end as ywdl_cust,
			case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
			--case when dev_source_code='2' then '业务代理' end as ywdl_cust,
			--case when dev_source_code='4' then '内购' end as ng_cust
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:i_sdate_11} 
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
	--202108月签呈，'116957','116629'系统中为业务代理人，但需要人为计算销售员大客户提成，每月处理	
where 
	(b.ywdl_cust is null or b.customer_no in ('118689','116957','116629'))
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
		union all   select '118212'cust_id, 0.1 rate --202107月签呈，取消扣点，当月处理
		union all   select '112492'cust_id, 0.02 rate
		union all   select '110866'cust_id, 0.01 rate
		union all   select '106521'cust_id, 0.01 rate
		union all   select '116433'cust_id, 0.05 rate
		union all   select '118206'cust_id, 0.05 rate
		union all   select '115732'cust_id, 0.03 rate
		--202108月签呈，取消扣点，当月处理 '105186'
		--union all   select '105186'cust_id, 0.01 rate
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
		union all   select '112803'cust_id, 0.02 rate   --202107月签呈 取消按比例扣减前端毛利金额，当月处理
		union all   select '118055'cust_id, 0.035 rate --202107月签呈 取消按比例扣减前端毛利金额，当月处理
		union all   select '115253'cust_id, 0.02 rate

		--5月签呈重庆大客户扣点 每月处理
		--202108月签呈，取消扣点，当月处理 '113643'
		--union all   select '113643'cust_id, 0.03 rate
		union all   select '118405'cust_id, 0.03 rate
		
		--202106月签呈重庆大客户扣点 每月处理
		union all   select '119517'cust_id, 0.03 rate
		--202108月签呈，大客户扣点，每月处理
		union all   select '121061'cust_id, 0.01 rate
		--202109月签呈，大客户扣点，每月处理
		union all   select '112024'cust_id, 0.02 rate
		
		--202109月签呈，大客户扣点，每月处理
		union all   select '117920'cust_id, 0.03 rate
		union all   select '119659'cust_id, 0.03 rate
		union all   select '120105'cust_id, 0.03 rate
		union all   select '120479'cust_id, 0.03 rate
		union all   select '120623'cust_id, 0.03 rate
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
	--and a.cust_id not in(
	--'104758','105685','105956','105965','106288','106559','106878','107104','112492','112633','113423','114615','105480','105540','106469',
	--'106524','106538','106704','107438','111892','112210','117022','102534','102798','102806','104741','105186','114724','115915','115920')
	--202107月签呈，当月处理
	--and a.cust_id not in(
	--'107685','110907','111987','113763','113913','114065','118317','119945','104758','105685','105956','105965','106288','106559','106878','107104','112492','112633',
	--'113423','114615','105480','105540','106469','106524','106538','106704','107438','111892','112210','117022','102534','102798','102806','104741','105186','114724',
	--'115915','115920','110807')
	--202107签呈，每月处理，'119760'前端毛利额5%，销售额部分不变
	and a.cust_id not in ('119760')
	--202108月签呈，不算销售额提成，只算毛利提成，每月处理
	and a.cust_id not in ('110807')
	--202108月签呈，不算销售额提成，前端毛利额6%计算，每月处理
	and a.cust_id not in ('107685','110907','111987','113763','113913','118047','118169','118570','119945','104758','105685','105956','105965','106288','106559','106878','107104',
	'112492','112633','113423','120425','105480','105540','106469','106524','106538','107438','111892','112210','117022','102534','102798','102806','105186','114724','115915')
	--202109月签呈，不算销售额提成，只算前端毛利额提成，每月处理
	and a.cust_id not in ('108152','108180','115829')
	--202109月签呈，销售额部分不变，前端1%，每月处理
	and a.cust_id not in ('122007','122390','122411','118281','122335')
	--202109月签呈，销售额部分不变，前端2%，每月处理
	and a.cust_id not in ('112717','118311','119760')
	--202109月签呈，前端毛利6%，每月处理
	and a.cust_id not in ('107685','110907','111987','113763','118169','118570','119945','106603','106704','118408','104758','105480','105540','105956','105965','106288','106469',
	'106524','106538','106559','106878','107104','107438','111892','112210','112492','112633','113423','117022','120425','102534','102798','102806','105186','115915')	
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
	--a.cust_id in(
	--'104758','105685','105956','105965','106288','106559','106878','107104','112492','112633','113423','114615','105480','105540','106469',
	--'106524','106538','106704','107438','111892','112210','117022','102534','102798','102806','104741','105186','114724','115915','115920')
	--202107月签呈，当月处理
	--a.cust_id in(
	--'107685','110907','111987','113763','113913','114065','118317','119945','104758','105685','105956','105965','106288','106559','106878','107104','112492','112633',
	--'113423','114615','105480','105540','106469','106524','106538','106704','107438','111892','112210','117022','102534','102798','102806','104741','105186','114724',
	--'115915','115920','110807')	
	--202108月签呈，每月处理
	a.cust_id in ('107685','110907','111987','113763','113913','118047','118169','118570','119945','104758','105685','105956','105965','106288','106559','106878','107104','112492',
	'112633','113423','120425','105480','105540','106469','106524','106538','107438','111892','112210','117022','102534','102798','102806','105186','114724','115915')
	--202109月签呈，前端毛利6%，每月处理
	and a.cust_id in ('107685','110907','111987','113763','118169','118570','119945','106603','106704','118408','104758','105480','105540','105956','105965','106288','106469',
	'106524','106538','106559','106878','107104','107438','111892','112210','112492','112633','113423','117022','120425','102534','102798','102806','105186','115915')	
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth

-- 签呈不算销售提成，只算前端毛利提成
--202108月签呈，'110807'不算销售额提成，只算毛利提成，每月处理
union all
select 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) prorate,
	sum(a.front_profit) fnl_profit,
	sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
	0 salary_sales_value,
	round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*coalesce(a.profit_rate,0.1))),2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	--202109月签呈，'108152','108180','115829' 不算销售额提成，只算前端毛利额提成，每月处理
	a.cust_id in('110807','108152','108180','115829')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth

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

-- 签呈提成方式前端毛利*5%，销售额部分不变
--union all
--select 
--	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
--	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
--	sum(a.sales_value) sales_value,
--	sum(a.profit) profit,
--	sum(a.profit)/abs(sum(a.sales_value)) prorate,
--	sum(a.front_profit) fnl_profit,
--	sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
--	--round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.06)),2) salary
--	round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
--	round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.05)),2) salary_fnl_profit
--from 
--	csx_tmp.temp_tc_new_cust_00 a
--where 
--	--202107签呈，每月处理，'119760'前端毛利额5%，销售额部分不变
--	a.cust_id in ('119760')
--group by 
--	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
--	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth
--; --8

-- 签呈提成方式前端毛利*1%，销售额部分不变
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
	round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
	round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.01)),2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	--202109月签呈，销售额部分不变，前端1%，每月处理
	a.cust_id in ('122007','122390','122411','118281','122335')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth


-- 签呈提成方式前端毛利*2%，销售额部分不变
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
	round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
	round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.02)),2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	--202109月签呈，销售额部分不变，前端2%，每月处理
	a.cust_id in ('112717','118311','119760')
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
  case when cust_id in ('107901','114099','115051','115047','109406','115753','117015','115936',
      '108105','109544','106921','112663','107852','105915','110898','111943','120621','120630') then salary_sales_value*0.1 
       else salary_sales_value end salary_sales_value,
  case when cust_id in ('107901','114099','115051','115047','109406','115753','117015','115936',
      '108105','109544','106921','112663','107852','105915','110898','111943','120621','120630') then salary_fnl_profit*0.2
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
	--case when a.cust_id in('106989') then '80950647'
	--	when a.cust_id in('106135') then '80725128'
	--  else a.work_no end as work_no,
	--case when a.cust_id in('106989') then '张伟炜'
	--	when a.cust_id in('106135') then '张丽琴'
	--  else a.sales_name end as sales_name,		
	a.work_no,a.sales_name,
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
	select 
		* 
	from 
		csx_tmp.temp_tc_new_cust_03
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
	--202107月签呈,安徽,给固定服务费,每月处理
	left join
		(
		select '999999x' as customer_no,0 as fixed_fee
		union all  select '113439' as customer_no,2000 as fixed_fee
		union all  select '119109' as customer_no,0 as fixed_fee
		union all  select '119632' as customer_no,1000 as fixed_fee
		union all  select '115883' as customer_no,200 as fixed_fee
		union all  select '116915' as customer_no,300 as fixed_fee
		union all  select '116727' as customer_no,1000 as fixed_fee
		union all  select '113223' as customer_no,500 as fixed_fee
		union all  select '119861' as customer_no,500 as fixed_fee
		union all  select '115221' as customer_no,500 as fixed_fee
		union all  select '104732' as customer_no,300 as fixed_fee
		union all  select '106891' as customer_no,300 as fixed_fee
		union all  select '105525' as customer_no,0 as fixed_fee
		union all  select '116566' as customer_no,1000 as fixed_fee
		union all  select '114834' as customer_no,1000 as fixed_fee
		union all  select '107093' as customer_no,500 as fixed_fee
		union all  select '117697' as customer_no,300 as fixed_fee
		union all  select '118330' as customer_no,300 as fixed_fee
		union all  select '107433' as customer_no,500 as fixed_fee
		union all  select '104885' as customer_no,400 as fixed_fee
		union all  select '115286' as customer_no,400 as fixed_fee
		union all  select '116001' as customer_no,200 as fixed_fee
		union all  select '113731' as customer_no,300 as fixed_fee
		union all  select '113857' as customer_no,200 as fixed_fee
		union all  select '113073' as customer_no,500 as fixed_fee
		union all  select '116038' as customer_no,400 as fixed_fee
		union all  select '116055' as customer_no,400 as fixed_fee
		union all  select '117548' as customer_no,300 as fixed_fee
		union all  select '116032' as customer_no,300 as fixed_fee
		union all  select '116056' as customer_no,500 as fixed_fee
		union all  select '116061' as customer_no,200 as fixed_fee
		union all  select '114485' as customer_no,500 as fixed_fee
		--202109月签呈，固定服务费，当月处理
		union all  select '115206' as customer_no,4000 as fixed_fee
		--202109月签呈，固定服务费，每月处理
		union all  select '120723' as customer_no,200 as fixed_fee
		union all  select '121452' as customer_no,200 as fixed_fee
		union all  select '117459' as customer_no,200 as fixed_fee
		union all  select '104805' as customer_no,300 as fixed_fee
		union all  select '107415' as customer_no,300 as fixed_fee
		union all  select '121551' as customer_no,300 as fixed_fee
		union all  select '104114' as customer_no,500 as fixed_fee
		union all  select '111832' as customer_no,500 as fixed_fee
		
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
	--case when fixed_fee is not null then fixed_fee else coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0) end,
	if(fixed_fee is not null,if(coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0)>=fixed_fee,fixed_fee,coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0)),coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0)),
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
	sum(if(fixed_fee is not null,if(coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0)>=fixed_fee,fixed_fee,coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0)),coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0))) as salary_sale
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


--===============================================================================================================================================================================


/*
-- 大客户提成：月度新客户
select 
	b.sales_province_name,b.customer_no,b.customer_name,b.attribute_desc,b.dev_source_name,b.work_no,b.sales_name,b.sign_date,
	a.first_order_date
from
	(
	select 
		attribute_desc,dev_source_name,customer_no,customer_name,channel_name,sales_name,work_no,sales_province_name,
		regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,estimate_contract_amount*10000 estimate_contract_amount
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt='current'
		and customer_no<>''
		and channel_code in('1','7','8')
	)b
	join --客户最早销售月 新客月、新客季度
		(
		select 
			customer_no,
			min(first_order_date) first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = 'current'
		group by 
			customer_no
		having 
			min(first_order_date)>='20210901' and min(first_order_date)<'20211001'
		)a on b.customer_no=a.customer_no;

--客户对应销售员与服务管家
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	* 
from  
	csx_dw.report_crm_w_a_customer_service_manager_info
where  
	sdt= '20210930'
	and channel_code in('1','7')
	and (is_sale='是' or is_overdue='是')
	

--大客户销售员对照表
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	sales_province_name,customer_no,customer_name,work_no,sales_name,dev_source_name,
	city_group_name,channel_name,
	regexp_replace(split(first_sign_time, ' ')[0], '-', '') as first_sign_date,
	regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
from 
	csx_dw.dws_crm_w_a_customer
	--where sdt='20210617'
where 
	sdt=${hiveconf:i_sdate_11}  
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


--9月客户销售员对照表
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	customer_no,customer_name,sales_province_name,work_no,sales_name,service_user_work_no,service_user_name,
	is_part_time_service_manager,sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,service_user_front_profit_rate
from  
	csx_dw.report_crm_w_a_customer_service_manager_info
where  
	sdt= '20210930'
	and channel_code in('1','7')
	and (is_sale='是' or is_overdue='是')
*/
