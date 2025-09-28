
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



load data inpath '/tmp/zhangyanpeng/sales_income_info_01.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20220131');
select * from csx_tmp.sales_income_info where sdt='20220131';




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
		sdt>='20220101'
		and sdt<='20220131'
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
			sdt='20220131'
		) b on b.customer_no=a.customer_no
	left join 
		(
		select 
			distinct work_no,income_type 
		from 
			csx_tmp.sales_income_info 
		where 
			sdt='20211231'
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

set i_sdate_11 ='20220131';	
set i_sdate_12 ='20220101';				



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
			sdt>='20220101' and sdt<=${hiveconf:i_sdate_11} --昨日月1日
			and channel_code in('1','7','9')
			--202112月签呈，剔除飞天茅台酒销售额及前端毛利额，每月处理,'8718','8708','8649'
			and goods_code not in ('8718','8708','8649')
			and business_type_code not in('3')
			and (business_type_code not in('4')
			--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
			or customer_no in(
			'120939','124473','121298','125284','124601','124498','122567','123244','121625','117817')
			--202111月签呈，由于没有仓储配送，客户从城市服务商仓库过机，正常计算提成，每月处理
			or customer_no in('121444','121229','121443')
			)	
			--福建泉州签呈，订单12月销售530181.06元，1月全部退货，不算提成
			and (order_no not in ('OM20122800005550','RH21011900000203') or order_no is null)		
			and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
						'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
			--签呈客户不考核，不算提成 2021年3月签呈取消剔除103717
			and customer_no not in('111118','102755','104023','105673')
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
			--202110月签呈，战略客户，不考核提成，每月处理
			--202111月签呈，该战略客户恢复提成，每月处理
			--and customer_no not in('119925','122495')
			--202111月签呈，不算提成，每月处理
			and customer_no not in ('101653','119977')
			--202111月签呈，不算提成，当月处理
			--and customer_no not in ('123104','123127','123128','123131','123135','123136')	
			--202112月签呈，不算提成，当月处理
			--and customer_no not in ('124079')	
			--202201月签呈，不算提成，每月
			and customer_no not in ('104192','123395','117927','115589','117409','113073','114853','116233','117416','121780','122417','122501','122763','123299')				
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
				--sdt>=regexp_replace(trunc(date_sub(current_date,1),'YY'),'-','')  --昨日所在年第1天
				sdt>='20220101'
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
		--202112月签呈，剔除飞天茅台酒销售额及前端毛利额，每月处理,'8718','8708','8649'
		and goods_code not in ('8718','8708','8649')
		and business_type_code not in('3')
		and (business_type_code not in('4')
			--5月签呈，不剔除城市服务商2.0，按大客户提成方案计算
			or customer_no in(
			'120939','124473','121298','125284','124601','124498','122567','123244','121625','117817')
			--202111月签呈，由于没有仓储配送，客户从城市服务商仓库过机，正常计算提成，每月处理
			or customer_no in('121444','121229','121443')
		)		
		--福建泉州签呈，订单12月销售530181.06元，1月全部退货，不算提成
		--and (order_no not in ('OM20122800005550','RH21011900000203') or order_no is null)  
		--and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
		--					'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
		--签呈客户不考核，不算提成 2021年3月签呈取消剔除 103717
		and customer_no not in('111118','102755','104023','105673')
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
		--202110月签呈，战略客户，不考核提成，每月处理
		--202111月签呈，该战略客户恢复提成，每月处理
		--and customer_no not in('119925','122495')
		--202111月签呈，不算提成，每月处理
		and customer_no not in ('101653','119977')
		--202111月签呈，不算提成，当月处理
		--and customer_no not in ('123104','123127','123128','123131','123135','123136')
		--202112月签呈，不算提成，当月处理
		--and customer_no not in ('124079')	
		--202201月签呈，不算提成，每月
		and customer_no not in ('104192','123395','117927','115589','117409','113073','114853','116233','117416','121780','122417','122501','122763','123299')				
	group by 
		sdt,substr(sdt,1,6),province_name,customer_no


	--★★★扣减前端毛利 5月签呈
	--4月签呈，每月扣减
	--202108月签呈，每月处理，注意更改时间
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'105569' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-10000 as front_profit,1 as fnl_prorate
	--202110月签呈，每月处理，注意更改时间
	union all  select '20220131' as sdt,'202201' as smonth,'厦门市' as province_name,'119758' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1200 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'龙岩市' as province_name,'117108' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1000 as front_profit,1 as fnl_prorate
	--202201月签呈，扣减前端毛利，每月
	union all  select '20220131' as sdt,'202201' as smonth,'四川省' as province_name,'124403' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-6300 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'四川省' as province_name,'108835' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3500 as front_profit,1 as fnl_prorate

	--202112月签呈，当月扣减
	--union all  select '20220131' as sdt,'202201' as smonth,'上海宝山' as province_name,'107986' as customer_no,
	--	0 as sales_value, 0 as profit,1 prorate,-12743.38 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'福建省' as province_name,'102866' as customer_no,
    -267194.8 as sales_value, 0 as profit,1 prorate,0 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'江苏苏州' as province_name,'113090' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1022.29 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'江苏苏州' as province_name,'125287' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3978.78 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'江苏苏州' as province_name,'107975' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-8551.87 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'104901' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9068.76 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'105381' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-7804.97 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'107059' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-624.27 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'108910' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-20768.25 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'114872' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2411.66 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'117755' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-10410.76 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'118288' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9612.72 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'107806' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-43426 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'125143' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1548 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'107844' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-640 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'114391' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4350.48 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'105518' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1200 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'124212' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-7344 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'124218' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-496 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'124433' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2045 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'119659' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4880 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120105' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3251 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'121061' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-31047.76 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'122129' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-13334.8 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'124606' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5000 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'105186' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-16864.39 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'118206' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2582.67 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'104965' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1909.06 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'112813' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1873.31 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117753' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1135.29 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117727' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1569.41 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117728' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2146.53 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117729' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2867.15 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117748' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1982.62 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117773' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1148.49 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117776' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1728.97 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117782' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1463.21 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117790' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1970.92 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117791' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-982.93 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117795' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1263.36 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117800' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1239.4 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117805' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3629.1 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117918' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-278.9 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117920' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2163.81 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'121113' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-14584.52 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'119659' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4252.75 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120105' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2066.03 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120623' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-18976.98 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'110866' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-853.74 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'113643' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-9290.84 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'119517' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-365.87 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'121061' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-37078.47 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120924' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-12949.68 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'123084' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-14089.24 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'115206' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1620 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'118212' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-15401.35 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'124589' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-17130 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'112177' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-8350.27 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'113423' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1812.21 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'105569' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-10000 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'115554' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-7194.38 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'114391' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-639.85 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'122247' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2233.89 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120976' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-383.89 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'115253' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-676.5 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'112803' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1562.91 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'河北省' as province_name,'123035' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-10000 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'河北省' as province_name,'124519' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5000 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'河北省' as province_name,'125105' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-10000 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'福建省' as province_name,'103096' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2068.5 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'114784' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-85.94 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'118405' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-659.34 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'121265' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-51.66 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'109291' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-122.38 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'123824' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1089.23 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'108021' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-21.58 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'116508' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-16.41 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120435' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-412.99 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117093' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1631.49 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120914' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3474.06 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'123899' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1249.75 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'111932' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3806.87 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'111984' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1121.26 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'111960' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1247.26 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117920' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-11548.41 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'116988' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2360.7 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'110242' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2926.24 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'107655' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-16244.3 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'122571' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5073.48 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'111956' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-967.82 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'111926' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-612.38 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'105518' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-531.93 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'119701' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2587.98 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'105287' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2791.53 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'108960' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1100.44 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'105802' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-671.31 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'106572' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-734.5 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'111942' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-339.66 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'106900' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-6815.87 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'115082' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1104.33 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'114354' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-375.2 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'118522' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1135.93 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'107842' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1799.46 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'106925' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1280.87 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'121045' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2112.9 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'122672' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2271.33 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'118061' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-256.66 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'107058' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1660.79 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'109977' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3784.57 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'113134' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2876.54 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'116461' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-802.39 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120660' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1635.2 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'112177' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5218.01 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'106898' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-21047.27 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120105' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-508.6 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'107912' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-864.68 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'115178' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-6163.68 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117230' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3233.35 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'115554' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-451.59 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'119659' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-387.84 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'112625' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1342.01 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'120983' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1345.06 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'107812' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1897.22 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'107749' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1337.33 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'108040' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2771.79 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'106516' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-2736.5 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117206' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-195.58 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'105768' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-504.52 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'108824' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-555.19 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'119252' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-753.38 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'123528' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1564 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'117340' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-931.11 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'重庆市' as province_name,'119124' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-643.12 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'河南省' as province_name,'124524' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3810 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海宝山' as province_name,'107986' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-4668.96 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海宝山' as province_name,'111137' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-13205.13 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海宝山' as province_name,'112920' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-1469.36 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海宝山' as province_name,'117007' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-5152.26 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海宝山' as province_name,'120467' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-3579.62 as front_profit,1 as fnl_prorate
	--202112月签呈，上海市，当月扣减
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'104901' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-31026.96 as front_profit,1 as fnl_prorate
	union all  select '20220131' as sdt,'202201' as smonth,'上海松江' as province_name,'105235' as customer_no,
		0 as sales_value, 0 as profit,1 prorate,-7803.71 as front_profit,1 as fnl_prorate


	
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
		union all   select '117676'cust_id, 0.05 rate
		union all   select '123245'cust_id, 0.06 rate
		union all   select '123252'cust_id, 0.08 rate
		union all   select '121540'cust_id, 0.05 rate
		
		union all   select '119308'cust_id, 0.05 rate
		union all   select '106587'cust_id, 0.05 rate
		union all   select '105384'cust_id, 0.05 rate
		union all   select '108557'cust_id, 0.02 rate
		union all   select '116131'cust_id, 0.06 rate
		union all   select '103782'cust_id, 0.06 rate
		union all   select '120494'cust_id, 0.06 rate
		
		union all   select '116707'cust_id, 0.03 rate
		union all   select '116561'cust_id, 0.05 rate
		union all   select '120250'cust_id, 0.03 rate

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
		-- union all   select '106521'cust_id, 0.01 rate --202111月签呈，取消扣点，当月处理
		union all   select '116433'cust_id, 0.05 rate
		union all   select '118206'cust_id, 0.05 rate
		union all   select '115732'cust_id, 0.03 rate
		--202108月签呈，取消扣点，当月处理 '105186'
		--union all   select '105186'cust_id, 0.01 rate
		union all   select '104965'cust_id, 0.06 rate
		union all   select '112813'cust_id, 0.06 rate
		union all   select '117753'cust_id, 0.06 rate
		union all   select '117120'cust_id, 0.03 rate
		--union all   select '116762'cust_id, 0.03 rate --202111月签呈，取消扣点，当月处理
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
		union all   select '113643'cust_id, 0.03 rate
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
		--union all   select '109401'cust_id, 0.08 rate
		union all   select '105569'cust_id, 0.05 rate
		union all   select '122247'cust_id, 0.03 rate
		union all   select '120976'cust_id, 0.02 rate
		--202111月签呈，固定扣点，每月处理
		union all   select '120924'cust_id, 0.06 rate
		union all   select '112803'cust_id, 0.02 rate
		--202112月签呈，固定扣点，每月处理
		union all   select '105186'cust_id, 0.01 rate
		union all   select '123084'cust_id, 0.05 rate	

		union all   select '115206'cust_id, 0.03 rate
		union all   select '124589'cust_id, 0.1 rate
		union all   select '114391'cust_id, 0.05 rate

		union all   select '116401'cust_id, 0.02 rate
		union all   select '104493'cust_id, 0.06 rate
		union all   select '118041'cust_id, 0.55 rate
		union all   select '118208'cust_id, 0.03 rate
		union all   select '117217'cust_id, 0.11 rate
		union all   select '119354'cust_id, 0.05 rate
		union all   select '122347'cust_id, 0.04 rate
		union all   select '122860'cust_id, 0.04 rate
		union all   select '123706'cust_id, 0.04 rate
		union all   select '124061'cust_id, 0.04 rate
		union all   select '123755'cust_id, 0.04 rate
		union all   select '115191'cust_id, 0.12 rate
		union all   select '118564'cust_id, 0.04 rate
		union all   select '122628'cust_id, 0.1 rate
		union all   select '118770'cust_id, 0.3 rate
		union all   select '120567'cust_id, 0.5 rate
		union all   select '118299'cust_id, 0.04 rate
		union all   select '113974'cust_id, 0.3 rate
		union all   select '125201'cust_id, 0.08 rate
		union all   select '125191'cust_id, 0.02 rate
		union all   select '125344'cust_id, 0.05 rate
		
		--202201月签呈，固定扣点，每月
		union all   select '113873'cust_id, 0.03 rate
		union all   select '113918'cust_id, 0.03 rate
		union all   select '113935'cust_id, 0.03 rate
		union all   select '113940'cust_id, 0.03 rate
		union all   select '117137'cust_id, 0.05 rate
		union all   select '117142'cust_id, 0.05 rate
		union all   select '117143'cust_id, 0.05 rate
		union all   select '119589'cust_id, 0.03 rate
		union all   select '115479'cust_id, 0.05 rate
		union all   select '123999'cust_id, 0.03 rate

		--202201月签呈，固定扣点，每月
		union all   select '112285'cust_id, 0.04 rate
		union all   select '112024'cust_id, 0.02 rate
		union all   select '122551'cust_id, 0.04 rate
		
		--202201月签呈，固定扣点，每月
		union all   select '106921'cust_id, 0.08 rate
		--福建
		union all   select '124200'cust_id, 0 rate
		union all   select '116947'cust_id, 0.01 rate
		union all   select '111207'cust_id, 0.01 rate
		union all   select '125534'cust_id, 0.01 rate
		union all   select '115537'cust_id, 0.01 rate


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
	--and a.cust_id not in ('119760')
	--202108月签呈，不算销售额提成，只算毛利提成，每月处理
	and a.cust_id not in ('110807')
	--202108月签呈，不算销售额提成，前端毛利额6%计算，每月处理
	and a.cust_id not in ('107685','110907','111987','113763','113913','118047','118169','118570','119945','104758','105685','105956','105965','106288','106559','106878','107104',
	'112492','112633','113423','120425','105480','105540','106469','106524','106538','107438','111892','112210','117022','102534','102798','102806','105186','114724','115915')
	--202109月签呈，不算销售额提成，只算前端毛利额提成，每月处理
	--202111月签呈，恢复销售额提成
	--and a.cust_id not in ('108152','108180','115829')
	--202109月签呈，销售额部分不变，前端1%，每月处理
	and a.cust_id not in ('122335')
	--202109月签呈，销售额部分不变，前端2%，当月处理
	--and a.cust_id not in ('112717','119760')
	--202109月签呈，前端毛利6%，每月处理
	and a.cust_id not in ('107685','110907','111987','113763','118169','118570','119945','106603','106704','118408','104758','105480','105540','105956','105965','106288','106469',
	'106524','106538','106559','106878','107104','107438','111892','112210','112492','112633','113423','117022','120425','102534','102798','102806','105186','115915')	
	--202110月签呈，销售额不变，前端毛利额2%，每月处理
	--and a.cust_id not in ('119760')
	--202110月签呈，前端毛利额=销售额*(定价毛利率-8%)，每月处理
	and a.cust_id not in ('109401')
	--202111月签呈，销售额*0.2%，不计算前端毛利，每月处理
	and a.cust_id not in ('121444','121229','121443')
	--202111月签呈，前端毛利额=销售额*1%，每月处理
	--and a.cust_id not in ('111832','115221','116566','120723','122467','122547')
	--202111月签呈，前端毛利额=销售额*2%，每月处理
	--and a.cust_id not in ('119760','115883','117740')
	--202112月签呈，前端毛利额=销售额*1%，每月处理
	and a.cust_id not in ('111832','116566','120723','119760','115883','117740','123685','123553','124169','121425','121841')
	--202201月签呈，仅核算销售额提成，不核算前端毛利额提成，每月
	and a.cust_id not in ('123311','124033','118376')
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
--202111月签呈，'108152','108180','115829' 恢复销售额提成，每月处理
--202111月签呈，'110807'因直采/地采占比升高导致前端和定价毛利率倒挂，调整前端毛利率为6%，前端毛利额=销售额*6%，每月处理
union all
select 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) prorate,
	sum(a.sales_value)*0.06 fnl_profit,
	sum(a.sales_value)*0.06/abs(sum(a.sales_value)) fnl_prorate,
	0 salary_sales_value,
	--round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*coalesce(a.profit_rate,0.1))),2) salary_fnl_profit
	round(sum(coalesce(a.sales_value,0)*0.06*coalesce(a.profit_rate,0.1)),2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	--202109月签呈，'108152','108180','115829' 不算销售额提成，只算前端毛利额提成，每月处理
	a.cust_id in('110807') -- '108152','108180','115829' 202111月签呈，恢复销售额提成，每月处理
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
	a.cust_id in('115935','117762','118689','115971','113536','123311','124033','118376')
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
	a.cust_id in ('122335')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth


-- 签呈提成方式：前端毛利额=销售额*1%，销售额部分不变
union all
select 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) prorate,
	sum(a.sales_value)*0.01 fnl_profit,
	0.01 fnl_prorate,
	round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
	round(if(sum(a.sales_value)*0.01<=0,0,sum(a.sales_value)*0.01*0.1),2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	a.cust_id in ('111832','116566','120723','119760','115883','117740','123685','123553','124169','121425','121841')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth
	
-- 签呈提成方式：前端毛利额=销售额*2%，销售额部分不变
union all
select 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) prorate,
	sum(a.sales_value)*0.02 fnl_profit,
	0.02 fnl_prorate,
	round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
	round(if(sum(a.sales_value)*0.02<=0,0,sum(a.sales_value)*0.02*0.1),2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	a.cust_id in ('X000000')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth
	
	
--202110月签呈，前端毛利额=销售额*(定价毛利率-8%)，每月处理
union all
select 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth,
	sum(a.sales_value) sales_value,
	sum(a.profit) profit,
	sum(a.profit)/abs(sum(a.sales_value)) prorate,
	--sum(a.front_profit) fnl_profit,
	sum(a.sales_value)*(sum(a.profit)/abs(sum(a.sales_value))-0.08) fnl_profit,
	--sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
	sum(a.profit)/abs(sum(a.sales_value))-0.08 fnl_prorate,
	round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
	--round(sum(coalesce(a.front_profit,0)*0.02),2) salary_fnl_profit
	--round(sum(a.sales_value)*(sum(a.profit)/abs(sum(a.sales_value))-0.08)*coalesce(a.profit_rate,0.1),2) salary_fnl_profit
	round(sum(a.sales_value)*(sum(a.profit)/abs(sum(a.sales_value))-0.08)*0.1,2) salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	a.cust_id in ('109401')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth

--202111月签呈，销售额*0.2%，不计算前端毛利，每月处理
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
	round(sum(a.sales_value*0.002),2) salary_sales_value,
	0 salary_fnl_profit
from 
	csx_tmp.temp_tc_new_cust_00 a
where 
	a.cust_id in('121444','121229','121443')
group by 
	a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.is_part_time_service_manager,a.service_user_work_no,a.service_user_name,
	a.sales_sale_rate,a.sales_front_profit_rate,a.service_user_sale_rate,a.service_user_front_profit_rate,a.smonth
; --8

--4月签呈 总提成为服务管家提成，再按比例分配给销售员和服务管家，每月处理
--202111月签呈，'109406','114099'恢复为正常提成
drop table csx_tmp.temp_tc_new_cust_03; --9
create table csx_tmp.temp_tc_new_cust_03
as
select 
  dist,cust_id,cust_name,work_no,sales_name,is_part_time_service_manager,
  service_user_work_no,service_user_name,
  sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,service_user_front_profit_rate,
  smonth,sales_value,profit,prorate,fnl_profit,fnl_prorate,
  case when cust_id in ('115051','115047','117015','115936',
      '108105','109544','106921','112663','107852','105915','110898','111943','120621','120630') then salary_sales_value*0.1 
       else salary_sales_value end salary_sales_value,
  case when cust_id in ('115051','115047','117015','115936',
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
	
	--if(z.salary_sales_value is null,if(a.salary_sales_value<0 or c.over_rate is null,
    --a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	--,z.salary_sales_value)*coalesce(a.sales_sale_rate,0) salary_sales_value_sale,
	if(z.salary_sales_value is null,if(a.salary_sales_value<0 or c.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_sales_value)*if(a.cust_id='114387',0.94025,coalesce(a.sales_sale_rate,0)) salary_sales_value_sale,
	
	--if(z.salary_sales_value is null,if(a.salary_sales_value<0 or d.over_rate is null,
    --a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	--,z.salary_sales_value)*coalesce(a.service_user_sale_rate,0) salary_sales_value_service,
	if(z.salary_sales_value is null,if(a.salary_sales_value<0 or d.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	,z.salary_sales_value)*if(a.cust_id='114387',0.059752,coalesce(a.service_user_sale_rate,0)) salary_sales_value_service,
	
	--if(z.salary_fnl_profit is null,if(c.over_rate is null,
    --a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	--,z.salary_fnl_profit)*coalesce(a.sales_front_profit_rate,0) salary_fnl_profit_sale,
	if(z.salary_fnl_profit is null,if(c.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_fnl_profit)*if(a.cust_id='114387',0.8511268,coalesce(a.sales_front_profit_rate,0)) salary_fnl_profit_sale,
	
	--if(z.salary_fnl_profit is null,if(d.over_rate is null,
    --a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	--,z.salary_fnl_profit)*coalesce(a.service_user_front_profit_rate,0) salary_fnl_profit_service,
	if(z.salary_fnl_profit is null,if(d.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	,z.salary_fnl_profit)*if(a.cust_id='114387',0.148873,coalesce(a.service_user_front_profit_rate,0)) salary_fnl_profit_service,	
	
	e.fixed_fee,
	coalesce(f.deduction_amount,0) as deduction_amount,
	g.service_fee,
	g.service_fee*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) as final_service_fee
from  
	(
	select 
		dist,cust_id,cust_name,work_no,sales_name,is_part_time_service_manager,
		service_user_work_no,service_user_name,
		sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,service_user_front_profit_rate,
		smonth,sales_value,profit,prorate,fnl_profit,fnl_prorate,
		salary_sales_value,
		salary_fnl_profit
	from 
		csx_tmp.temp_tc_new_cust_03
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
		select '999999x' as customer_no,0.00 as fixed_fee
		union all  select '120519' as customer_no,666.67 as fixed_fee
		union all  select '122390' as customer_no,666.67 as fixed_fee
		union all  select '122818' as customer_no,666.67 as fixed_fee
		union all  select '125009' as customer_no,0.00 as fixed_fee
		union all  select '125274' as customer_no,0.00 as fixed_fee
		union all  select '120595' as customer_no,200.00 as fixed_fee
		--union all  select '124584' as customer_no,750.00 as fixed_fee
		union all  select '124784' as customer_no,750.00 as fixed_fee
		union all  select '125017' as customer_no,500.00 as fixed_fee
		union all  select '125029' as customer_no,1000.00 as fixed_fee
		union all  select '116932' as customer_no,1000.00 as fixed_fee
		union all  select '120830' as customer_no,500.00 as fixed_fee
		union all  select '122837' as customer_no,500.00 as fixed_fee
		union all  select '117017' as customer_no,500.00 as fixed_fee
		union all  select '120823' as customer_no,500.00 as fixed_fee
		union all  select '124668' as customer_no,1000.00 as fixed_fee
		union all  select '125028' as customer_no,1000.00 as fixed_fee
		union all  select '124584' as customer_no,1500.00 as fixed_fee



		) e on e.customer_no=a.cust_id
	--202111月签呈，客户总提成金额减去部分金额，当月处理
	left join
		(
		--select  '120924' as customer_no,249 as deduction_amount
		select  '000000X' as customer_no,249 as deduction_amount
		) f on f.customer_no=a.cust_id
	--202111月签呈，服务费*逾期系数，每月处理
	left join
		(
		select '999999x' as customer_no,0.00 as service_fee
		union all  select '115286' as customer_no,200.00 as service_fee
		union all  select '123287' as customer_no,200.00 as service_fee
		union all  select '122549' as customer_no,200.00 as service_fee
		union all  select '105090' as customer_no,200.00 as service_fee
		union all  select '104885' as customer_no,200.00 as service_fee
		union all  select '121452' as customer_no,300.00 as service_fee
		union all  select '107415' as customer_no,200.00 as service_fee
		union all  select '113731' as customer_no,200.00 as service_fee
		union all  select '113857' as customer_no,200.00 as service_fee
		union all  select '122147' as customer_no,100.00 as service_fee
		union all  select '122159' as customer_no,100.00 as service_fee
		union all  select '122167' as customer_no,100.00 as service_fee
		union all  select '122185' as customer_no,100.00 as service_fee
		union all  select '122186' as customer_no,100.00 as service_fee
		union all  select '122188' as customer_no,100.00 as service_fee

		) g on g.customer_no=a.cust_id

; --12


insert overwrite directory '/tmp/zhangyanpeng/tc_kehu' row format delimited fields terminated by '\t'
select 
	smonth,dist,cust_id,cust_name,work_no,sales_name,is_part_time_service_manager,service_user_work_no,service_user_name,sales_value,profit,prorate,fnl_profit,
	fnl_prorate,salary_sales_value,salary_fnl_profit,receivable_amount,over_amt,assigned_type,sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,
	service_user_front_profit_rate,sale_over_rate,service_user_over_rate,salary_sales_value_sale,salary_sales_value_service,salary_fnl_profit_sale,salary_fnl_profit_service,
	total_sale-deduction_amount as total_sale,
	total_service
from
	(
	select
		smonth,dist,cust_id,cust_name,work_no,sales_name,is_part_time_service_manager,service_user_work_no,service_user_name,sales_value,profit,prorate,fnl_profit,
		fnl_prorate,salary_sales_value,salary_fnl_profit,receivable_amount,over_amt,assigned_type,sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,
		service_user_front_profit_rate,sale_over_rate,service_user_over_rate,salary_sales_value_sale,salary_sales_value_service,salary_fnl_profit_sale,salary_fnl_profit_service,
		-- coalesce(fixed_fee,0) as fixed_fee,
		--case when fixed_fee is not null then fixed_fee else coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0) end,
		if(fixed_fee is not null,fixed_fee,if(service_fee is not null,final_service_fee,coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0))) as total_sale,
		if(service_fee is not null,0,coalesce(salary_sales_value_service,0)+coalesce(salary_fnl_profit_service,0)) as total_service,deduction_amount
	from 
		csx_tmp.temp_tc_new_cust_salary
	) a 
;

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
	sum(if(fixed_fee is not null,fixed_fee,if(service_fee is not null,final_service_fee,coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0)-deduction_amount))) as salary_sale
from 
	csx_tmp.temp_tc_new_cust_salary
group by 
	smonth,dist,work_no,sales_name,sale_over_rate
		
;

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
	--coalesce(sum(salary_sales_value_service),0)+coalesce(sum(salary_fnl_profit_service),0) salary_service
	sum(if(service_fee is not null or fixed_fee is not null,0,coalesce(salary_sales_value_service,0)+coalesce(salary_fnl_profit_service,0))) as salary_service
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
			min(first_order_date)>='20211201' and min(first_order_date)<='20211231'
		)a on b.customer_no=a.customer_no;

--客户对应销售员与服务管家
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	* 
from  
	csx_dw.report_crm_w_a_customer_service_manager_info
where  
	sdt= '20220131'
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


--01月客户销售员对照表
insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	customer_no,customer_name,sales_province_name,work_no,sales_name,service_user_work_no,service_user_name,
	is_part_time_service_manager,sales_sale_rate,sales_front_profit_rate,service_user_sale_rate,service_user_front_profit_rate
from  
	csx_dw.report_crm_w_a_customer_service_manager_info
where  
	sdt= '20220131'
	and channel_code in('1','7')
	and (is_sale='是' or is_overdue='是')
*/
