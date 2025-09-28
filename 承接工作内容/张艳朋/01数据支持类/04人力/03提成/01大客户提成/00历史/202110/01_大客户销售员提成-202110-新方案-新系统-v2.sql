
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

--load data inpath '/tmp/zhangyanpeng/sales_income_info_10.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20211031');
--select * from csx_tmp.sales_income_info where sdt='20211031';




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
		sdt>='20211001'
		and sdt<'20211101'
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
			sdt='20211031'
		) b on b.customer_no=a.customer_no
	left join 
		(
		select 
			distinct work_no,income_type 
		from 
			csx_tmp.sales_income_info 
		where 
			sdt='20210930'
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

set i_sdate_11 ='20211031';	
set i_sdate_12 ='20211001';				



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
			--work_no in ('80751663','80991769')	
			(ytd<=10000000 and income_type in('Q1','Q2','Q3','Q4','Q5')) 
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
			and business_type_code not in('3')
			and (business_type_code not in('4')
			--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
			or customer_no in(
			'107890','109363','111364','112410','113735','114859','115023','115738','115904','115941','118264','119022','119242','119247','119257','120376','120846','120879',
			'120999','121287','121337','122394','108713','109460','111734','112906','113829','115681','116056','116061','118259','118262','119227','119246','119250','119253',
			'119262','120147','120768','121384','121398','121467','121994','122406','122497','112207','113617','113634','114485','114853','114940','115151','115392','116027',
			'116032','116038','116959','117496','117548','118078','118221','119254','120404','120939','121483','121495','121855','122603','122623','108726','109357','114246',
			'116055','117558','117817','117889','118219','118509','119209','119214','119224','119255','119397','119892','120294','120826','121020','121032','121039','121276',
			'121298','122534','122559','122567','122577','122988')
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
			--and customer_no not in('116870','118537','121981')				
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
		and business_type_code not in('3')
		and (business_type_code not in('4')
		--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
			or customer_no in(
			'107890','109363','111364','112410','113735','114859','115023','115738','115904','115941','118264','119022','119242','119247','119257','120376','120846','120879',
			'120999','121287','121337','122394','108713','109460','111734','112906','113829','115681','116056','116061','118259','118262','119227','119246','119250','119253',
			'119262','120147','120768','121384','121398','121467','121994','122406','122497','112207','113617','113634','114485','114853','114940','115151','115392','116027',
			'116032','116038','116959','117496','117548','118078','118221','119254','120404','120939','121483','121495','121855','122603','122623','108726','109357','114246',
			'116055','117558','117817','117889','118219','118509','119209','119214','119224','119255','119397','119892','120294','120826','121020','121032','121039','121276',
			'121298','122534','122559','122567','122577','122988')
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
		--and customer_no not in('116870','118537','121981')	
	group by 
		sdt,substr(sdt,1,6),province_name,customer_no


	--★★★扣减前端毛利 5月签呈
	--4月签呈，每月扣减
	--202108月签呈，每月处理，注意更改时间
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'105569' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-10000 as front_profit,1 as fnl_prorate
	--202110月签呈，每月处理，注意更改时间
	union all  select '20211031' as sdt,'202110' as smonth,'厦门市' as province_name,'119758' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1200 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'龙岩市' as province_name,'117108' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1000 as front_profit,1 as fnl_prorate

	--202110月签呈，当月扣减
	union all  select '20211031' as sdt,'202110' as smonth,'安徽省' as province_name,'122390' as customer_no,
		-1122798 as sales_value, 0 as profit,1 prorate,-6538.38 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'113643' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-7729.36 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'121113' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9360.94 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'113423' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1184.97 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'111940' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-103.4 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'121464' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-18.6 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'113105' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-46.3 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'110252' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-188.5 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'117340' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-25265.6 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'105287' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2059.9 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'121026' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-84.4 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'117120' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-281.6 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'107525' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-130.1 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'120435' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-220.1 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'108960' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2300 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'111960' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2520.8 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'111932' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2164.1 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'111942' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1120.9 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'111984' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1314 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'117093' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-813.8 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'110242' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2374.9 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'113400' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-712.6 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'111956' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-848 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'106900' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3973.8 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'113134' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4401.4 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'107655' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9353.9 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'119801' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-242.6 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'118405' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2866.1 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'109977' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4433.7 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'111926' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1067.4 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'120657' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-282.1 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'106572' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-483 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'121211' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-34.4 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'119701' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1773.1 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'120108' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-172.2 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'112177' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-6513.7 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'122509' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-138.4 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'117643' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-798.3 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'115178' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9731.8 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'118522' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1178.7 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'106898' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-11305.1 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'107912' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1192.9 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'120964' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-507 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'107058' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1495 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'114045' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1770.6 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'107276' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-536.9 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'107842' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-741.4 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'重庆市' as province_name,'107461' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-417.7 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'上海松江' as province_name,'108910' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-419.4 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'上海松江' as province_name,'114872' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2504.75 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'上海宝山' as province_name,'117007' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3007.56 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'上海宝山' as province_name,'107986' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-10226.8 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'上海宝山' as province_name,'111137' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-6856.01 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'上海松江' as province_name,'105381' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-6059.1 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'上海松江' as province_name,'104901' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-7181.22 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'上海松江' as province_name,'107059' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1159.36 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'福建省' as province_name,'110807' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-98570.4418 as front_profit,1 as fnl_prorate
	union all  select '20211031' as sdt,'202110' as smonth,'福建省' as province_name,'112148' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-11514.2644 as front_profit,1 as fnl_prorate




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
		union all   select '106493'cust_id, 0.01 rate
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
		--union all   select '112803'cust_id, 0.02 rate   --202107月签呈 取消按比例扣减前端毛利金额，当月处理
		--union all   select '118055'cust_id, 0.035 rate --202107月签呈 取消按比例扣减前端毛利金额，当月处理
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
		--202110月签呈，大客户扣点，每月处理
		union all   select '115380'cust_id, 0.02 rate
		union all   select '115885'cust_id, 0.02 rate
		union all   select '102924'cust_id, 0.03 rate
		union all   select '120855'cust_id, 0.03 rate
		union all   select '102524'cust_id, 0.03 rate
		union all   select '117244'cust_id, 0.01 rate
		union all   select '107398'cust_id, 0.01 rate
		union all   select '119213'cust_id, 0.01 rate
		union all   select '113515'cust_id, 0.01 rate
		union all   select '115898'cust_id, 0.02 rate
		union all   select '115971'cust_id, 0.02 rate
		union all   select '119847'cust_id, 0.02 rate
		union all   select '119833'cust_id, 0.02 rate
		union all   select '110808'cust_id, 0.02 rate
		union all   select '119906'cust_id, 0.05 rate
		union all   select '120362'cust_id, 0.05 rate
		union all   select '115787'cust_id, 0.04 rate
		union all   select '120317'cust_id, 0.05 rate
		union all   select '120459'cust_id, 0.03 rate
		union all   select '118653'cust_id, 0.06 rate
		union all   select '118654'cust_id, 0.06 rate
		union all   select '118682'cust_id, 0.06 rate
		union all   select '118705'cust_id, 0.06 rate
		union all   select '118730'cust_id, 0.06 rate
		union all   select '118934'cust_id, 0.06 rate
		union all   select '118961'cust_id, 0.06 rate
		union all   select '119185'cust_id, 0.06 rate
		union all   select '119172'cust_id, 0.06 rate
		union all   select '120666'cust_id, 0.06 rate
		union all   select '119100'cust_id, 0.05 rate
		union all   select '115961'cust_id, 0.03 rate
		union all   select '115985'cust_id, 0.03 rate
		union all   select '118689'cust_id, 0.02 rate
		union all   select '116355'cust_id, 0.04 rate
		union all   select '121054'cust_id, 0.02 rate
		union all   select '119990'cust_id, 0.1 rate
		union all   select '109401'cust_id, 0.08 rate
		union all   select '105569'cust_id, 0.05 rate
		union all   select '122247'cust_id, 0.03 rate
		union all   select '120976'cust_id, 0.02 rate
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
	--202110月签呈，销售额不变，前端毛利额2%
	and a.cust_id not in ('119760')
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
	--202110月签呈，销售额不变，前端毛利额2% '119760'
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
		--202110月签呈 '113439' 该客户不享受提成，每月处理
		union all  select '113439' as customer_no,0 as fixed_fee
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
		--union all  select '115206' as customer_no,4000 as fixed_fee
		--202109月签呈，固定服务费，每月处理
		union all  select '120723' as customer_no,200 as fixed_fee
		union all  select '121452' as customer_no,200 as fixed_fee
		union all  select '117459' as customer_no,200 as fixed_fee
		union all  select '104805' as customer_no,300 as fixed_fee
		union all  select '107415' as customer_no,300 as fixed_fee
		union all  select '121551' as customer_no,300 as fixed_fee
		union all  select '104114' as customer_no,500 as fixed_fee
		union all  select '111832' as customer_no,500 as fixed_fee
		--202110月签呈，固定服务费，每月处理
		union all  select '115766' as customer_no,200 as fixed_fee
		union all  select '105090' as customer_no,0 as fixed_fee
		union all  select '122818' as customer_no,0 as fixed_fee
		union all  select '115110' as customer_no,0 as fixed_fee
		
		
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
			min(first_order_date)>='20211001' and min(first_order_date)<'20211101'
		)a on b.customer_no=a.customer_no;

--客户对应销售员与服务管家
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	* 
from  
	csx_dw.report_crm_w_a_customer_service_manager_info
where  
	sdt= '20211031'
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


--10月客户销售员对照表
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	customer_no,customer_name,sales_province_name,work_no,sales_name,service_user_work_no,service_user_name,
	is_part_time_service_manager,sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,service_user_front_profit_rate
from  
	csx_dw.report_crm_w_a_customer_service_manager_info
where  
	sdt= '20211031'
	and channel_code in('1','7')
	and (is_sale='是' or is_overdue='是')
*/
