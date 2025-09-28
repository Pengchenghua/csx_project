
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

--load data inpath '/tmp/raoyanhua/sales_income_info_04.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20210430');
--select * from csx_tmp.sales_income_info where sdt='20210430';




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
where sdt>='20210401'
and sdt<'20210501'
and channel_code in('1','7')
group by province_code,province_name,customer_no,substr(sdt,1,6)
)a	
left join (select * from csx_dw.dws_crm_w_a_customer where sdt='20210430') b on b.customer_no=a.customer_no
left join (select distinct work_no,income_type from csx_tmp.sales_income_info where sdt='20210430') c on c.work_no=b.work_no
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

set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

set i_sdate_11 ='20210430';	
set i_sdate_12 ='20210401';				



---每日销售员提成系数（销额提成比例、前端毛利提成比例）
drop table csx_tmp.tmp_salesname_rate_ytd;
create table csx_tmp.tmp_salesname_rate_ytd
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
	(select a.sdt,b.work_no,b.sales_name,coalesce(c.income_type,'Q1')income_type,
	sum(a.sales_value)over(PARTITION BY b.work_no,b.sales_name,substr(a.sdt,1,4) order by a.sdt ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING )ytd
	from 
		(select sdt,customer_no,substr(sdt,1,6) smonth,
				if(substr(sdt,1,6)=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6),
					regexp_replace(date_sub(current_date,1),'-',''),
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd')))),'-','')
					) as sdt_last,  --sdt所在月最后1日，当月为昨日
		sum(sales_value) sales_value
		from csx_dw.dws_sale_r_d_detail
		where sdt>='20210101' and sdt<=${hiveconf:i_sdate_11} --昨日月1日
		and channel_code in('1','7','9')
		and business_type_code not in('3','4')
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
	    and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
	    and customer_no not in('115721','116877','116883','116015','116556','116826')
	    and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                               '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                               '106321','106325','106326','106330','104609')	
	    --4月签呈 剔除逾期系数，不算提成
        and customer_no not in('114265','117412','116957')
	    --4月签呈 每月处理：剔除逾期系数，不算提成，每月处理
        and customer_no not in('102844','117940')								   
		group by sdt,customer_no,substr(sdt,1,6),
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
drop table csx_tmp.temp_new_cust_00;
create table csx_tmp.temp_new_cust_00
as
select 
b.sales_province_name dist,a.customer_no cust_id,b.customer_name cust_name,b.work_no,b.sales_name,d.service_user_work_no,d.service_user_name,a.smonth,
coalesce(c.sale_rate,0.002) sale_rate,coalesce(c.profit_rate,0.1) profit_rate,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate,
round(sum(a.sales_value)*coalesce(c.sale_rate,0.002)+if(sum(a.front_profit)<0,0,coalesce(sum(a.front_profit),0)*coalesce(c.profit_rate,0.1)),2) salary
from 
  (
  select sdt,substr(sdt,1,6) smonth,province_name,customer_no,
    sum(sales_value)sales_value,
    sum(profit) profit,sum(profit)/abs(sum(sales_value)) prorate,
    sum(front_profit) as front_profit,
    sum(front_profit)/abs(sum(sales_value)) as fnl_prorate
  from csx_dw.dws_sale_r_d_detail
  where sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11} --昨日月1日
  and channel_code in('1','7','9')
  and business_type_code not in('3','4')
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
  and customer_no not in('PF0065','112574','106782')  --'116957','116629','116215'仅3月
  and customer_no not in('115721','116877','116883','116015','116556','116826')
  and customer_no not in('103253','103284','103296','103297','103304','103306','103311','104818','104828','104829','104835',
                         '105113','106283','106284','106298','106299','106301','106306','106307','106308','106309','106320',
                         '106321','106325','106326','106330','104609')  
  --4月签呈 剔除逾期系数，不算提成
  and customer_no not in('114265','117412','116957')
  --4月签呈 每月处理：剔除逾期系数，不算提成，每月处理
  and customer_no not in('102844','117940')						 
  group by sdt,substr(sdt,1,6),province_name,customer_no


--★★★扣减前端毛利 4月签呈
   union all
  select '20210430' as sdt,'202104' as smonth,'上海市' as province_name,'105381' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-6557.99 as front_profit,1 as fnl_prorate
   union all
  select '20210430' as sdt,'202104' as smonth,'上海市' as province_name,'111506' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3000 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'上海市' as province_name,'107059' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2455.28 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'上海市' as province_name,'115993' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3436.85 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'上海市' as province_name,'110026' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2090.39 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'上海市' as province_name,'107986' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-6740.4 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'上海市' as province_name,'117007' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3131.33 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'115147' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,8.28 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'115236' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,336.84 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117602' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-7755.87 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'107525' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,1230.8 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'114813' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,532.4 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'108749' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,21.46 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'115175' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,2229.51 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'110252' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2154 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117340' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-16020.15 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117022' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,990.42 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117093' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-665.22 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'111960' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2522.1 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'111942' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1575.62 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'111940' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3355.21 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'107109' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2509.4 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'113134' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-4331.37 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'105287' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1901.6 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'115178' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-10230.6 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'106898' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-640.1 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'112177' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-8944.49 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'115554' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-8312.16 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'116666' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1125.25 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'105569' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-10000 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'118212' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1941.28 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'112492' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1754.96 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'110866' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1255.4 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'106521' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-11376.47 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'116433' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3270.74 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'118206' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1120.13 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'115732' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-41047.86 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'105186' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-9959.78 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'104965' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3117.28 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'112813' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-4639.93 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117753' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1810.93 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117120' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-5493.06 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'116762' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1538.98 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117871' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-585 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117727' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2794.02 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117728' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3624.69 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117729' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3811.45 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117748' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2580.75 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117749' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3048.74 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117761' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3337.98 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117766' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3475.71 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117773' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1940.08 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117776' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3525.44 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117777' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-4174.07 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117781' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-4821.33 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117782' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2311.16 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117783' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2570.36 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117784' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-4902.48 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117785' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1889.57 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117786' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3822.55 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117790' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2911.44 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117791' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2369.1 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117795' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2146.27 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117796' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3280.95 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117800' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-2272.79 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117805' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-6310.58 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117918' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-600.18 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'117721' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-6262.27 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'112803' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-1488.76 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'118055' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-3748.27 as front_profit,1 as fnl_prorate
	union all
  select '20210430' as sdt,'202104' as smonth,'重庆市' as province_name,'115253' as customer_no,
    0 as sales_value, 0 as profit,1 prorate,-12215.11 as front_profit,1 as fnl_prorate

--★★★合并返利签呈处理		
 	 
	 
--★★★合并返利新增  		
  )a
left join 
	(select distinct customer_no,customer_name,work_no,sales_name,sales_province_name,
		case when channel_code='9' then '业务代理' end as ywdl_cust,
		case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
		--case when dev_source_code='2' then '业务代理' end as ywdl_cust,
		--case when dev_source_code='4' then '内购' end as ng_cust
	from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11}   --上月最后1日
	)b on b.customer_no=a.customer_no
left join 
	(select  work_no,sales_name,sdt,max(sale_rate) sale_rate,max(profit_rate) profit_rate
	from csx_tmp.tmp_salesname_rate_ytd where sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}  --上月1日，昨日月1日
	group by work_no,sales_name,sdt
	)c on c.work_no=b.work_no and c.sales_name=b.sales_name and c.sdt=a.sdt
--关联服务管家
left join		
  (  
      select customer_no,
	 concat_ws(',', collect_list(service_user_work_no)) as service_user_work_no,
	 concat_ws(',', collect_list(service_user_name)) as service_user_name
	from 
	  (select distinct customer_no,service_user_work_no,service_user_name
	  from csx_dw.dws_crm_w_a_customer_sales_link 
      where sdt=${hiveconf:i_sdate_11} 
	  and is_additional_info = 1 and service_user_id <> 0
	  and customer_no not in('113672','118249','118439','113783','115575','117811','118174','114948','115715','118126','114154',
          '111265','112670','112808','113784','115535','115706','116170','118032','111000','117945','115102',
          '117249','115042','114799','114295','111647','114652','116071','116683','118595','118815','113544',
          '111135','118802','113151','117317','109461','113873','113918','113935','113940','111999','112016',
          '113666','112747','114054','114083','114085','115205','115909','116857','116858','116861','115656',
          '117244','115826','115602','104281','107398','103830','104035','104036','105638','105947','105975',
          '106000','106875','113443','113576','113785','113979','115287','116785','118117','107877','110575',
          '111038','111204','111952','113450','113455','113569','113588','115244','115657','115936','117015',
          '115906','109401','110696','113536','113583','117680','104034','111195','112302','113652','114516',
          '118102','103775','108105','108425','109544','112088','113659','115831','117225','117516','106921',
          '112663','114830','115215','105915','107852','108201','110898','111943','113249','113860','116169',
          '116650','118498','115308','117145','108283','109722','113082','114680','115881','107901','114099',
          '115051','116821','118461','109406','115047','115753','118379')
	  ----4月签呈，将以下客户的服务管家调整为xx
union all	  
select '113672' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118249' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118439' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '113783' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115575' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '117811' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118174' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '114948' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115715' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118126' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '114154' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '111265' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '112670' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '112808' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '113784' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115535' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115706' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '116170' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '118032' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '111000' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '117945' customer_no,'签呈,未知' service_user_work_no,'潘阳' service_user_name
union all
select '115102' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '117249' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '115042' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '114799' customer_no,'签呈,未知' service_user_work_no,'唐楠' service_user_name
union all
select '114295' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '111647' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '114652' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '116071' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '116683' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118595' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118815' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '113544' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '111135' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '118802' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '113151' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '117317' customer_no,'签呈,未知' service_user_work_no,'王媛' service_user_name
union all
select '109461' customer_no,'签呈,未知' service_user_work_no,'王媛，唐楠' service_user_name
union all
select '113873' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113918' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113935' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '113940' customer_no,'签呈,未知' service_user_work_no,'郭志江' service_user_name
union all
select '111999' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '112016' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '113666' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '112747' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114054' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114083' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '114085' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '115205' customer_no,'81088296' service_user_work_no,'陈慧燕' service_user_name
union all
select '115909' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116857' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116858' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '116861' customer_no,'XM000001' service_user_work_no,'彭东京' service_user_name
union all
select '115656' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '117244' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '115826' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '115602' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '104281' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '107398' customer_no,'80974184' service_user_work_no,'郭荔丽' service_user_name
union all
select '103830' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104035' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104036' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105638' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105947' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '105975' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '106000' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '106875' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113443' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113576' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113785' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113979' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115287' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '116785' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '118117' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '107877' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '110575' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111038' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111204' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '111952' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113450' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113455' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113569' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113588' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115244' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115657' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115936' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '117015' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '115906' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '109401' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '110696' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113536' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '113583' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '117680' customer_no,'81003172' service_user_work_no,'庄丽明' service_user_name
union all
select '104034' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '111195' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112302' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113652' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '114516' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '118102' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '103775' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108105' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108425' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '109544' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112088' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113659' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115831' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117225' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117516' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '106921' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '112663' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '114830' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115215' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '105915' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '107852' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108201' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '110898' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '111943' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113249' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '113860' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '116169' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '116650' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '118498' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '115308' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '117145' customer_no,'81011095' service_user_work_no,'周巧丽' service_user_name
union all
select '108283' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '109722' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '113082' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '114680' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115881' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '107901' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '114099' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115051' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '116821' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '118461' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '109406' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115047' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '115753' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name
union all
select '118379' customer_no,'80832481' service_user_work_no,'廖锶尔' service_user_name	  
	  )a
	group by customer_no
  )d on d.customer_no=a.customer_no	
--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理  
where (b.ywdl_cust is null or b.customer_no='118689')
and b.ng_cust is null
group by b.sales_province_name,a.customer_no,b.customer_name,b.work_no,b.sales_name,d.service_user_work_no,d.service_user_name,c.sale_rate,c.profit_rate,a.smonth;



--大客户前端毛利扣点后结果
drop table csx_tmp.temp_new_cust_01;
create table csx_tmp.temp_new_cust_01
as
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth,
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
from csx_tmp.temp_new_cust_00 a
left join
(  --福建区域大客户扣点 20200115
union all   select '104824'cust_id, 0.02 rate
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
)z on z.cust_id=a.cust_id
where a.cust_id not in('115935')
and a.cust_id not in('105220','105539','106239','106713','106900','107100','107242','107361','107532','108236','110242',
'115274','115931','116461','117704','118119','102534','102798','102806','104741','105186','114724',
'115915','115920','104758','105956','105965','106288','106559','106878','107104','112492','112633',
'113423','117689','105480','105483','105540','106300','106469','106524','106538','107438','111892',
'112210','112474','117022','117067','103945','103954','104222','104229','104241','104251','104255',
'104414','104965','105005','105024','105756','112813','117753')
--and a.cust_id not in('116603','117002','115935')
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth

-- 签呈提成方式前端毛利*6%
union all
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth,
sum(a.sales_value) sales_value,
sum(a.profit) profit,
sum(a.profit)/abs(sum(a.sales_value)) prorate,
sum(a.front_profit) fnl_profit,
sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
--round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.06)),2) salary
0 salary_sales_value,
round(if(sum(a.front_profit)<=0,0,sum(coalesce(a.front_profit,0)*0.06)),2) salary_fnl_profit
from csx_tmp.temp_new_cust_00 a
where a.cust_id in(
'105220','105539','106239','106713','106900','107100','107242','107361','107532','108236','110242',
'115274','115931','116461','117704','118119','102534','102798','102806','104741','105186','114724',
'115915','115920','104758','105956','105965','106288','106559','106878','107104','112492','112633',
'113423','117689','105480','105483','105540','106300','106469','106524','106538','107438','111892',
'112210','112474','117022','117067','103945','103954','104222','104229','104241','104251','104255',
'104414','104965','105005','105024','105756','112813','117753'
)
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth

---- 签呈只算销售提成，不算前端毛利提成
-- 2月签呈 115935 客户每月提成方式前端毛利*0%
--union all
--select 
--a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth,
--sum(a.sales_value) sales_value,
--sum(a.profit) profit,
--sum(a.profit)/abs(sum(a.sales_value)) prorate,
--sum(a.front_profit) fnl_profit,
--sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
----round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary
--round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary_sales_value,
--0 salary_fnl_profit
--from csx_tmp.temp_new_cust_00 a
--where a.cust_id in('115935')
--group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth
-- 3月签呈'115935'每月处理,4月签呈'117762'每月处理:提成方式销售额*0.2%*0.1，前端毛利*0%
--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成，李燕玲（81079631）提成方式销售额*0.2%*0.1，每月
union all
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth,
sum(a.sales_value) sales_value,
sum(a.profit) profit,
sum(a.profit)/abs(sum(a.sales_value)) prorate,
sum(a.front_profit) fnl_profit,
sum(a.front_profit)/abs(sum(a.sales_value)) fnl_prorate,
--round(sum(a.sales_value*coalesce(a.sale_rate,0.002)),2) salary
round(sum(a.sales_value*0.002*0.1),2) salary_sales_value,
0 salary_fnl_profit
from csx_tmp.temp_new_cust_00 a
where a.cust_id in('115935','117762','118689')
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.service_user_work_no,a.service_user_name,a.smonth;


--4月签呈 总提成为服务管家提成，再按比例分配给销售员和服务管家，每月处理
drop table csx_tmp.temp_new_cust_02;
create table csx_tmp.temp_new_cust_02
as
select 
  dist,cust_id,cust_name,work_no,sales_name,
  service_user_work_no,service_user_name,
  smonth,sales_value,profit,prorate,fnl_profit,fnl_prorate,
  case when cust_id in ('107901','114099','115051','115047','109406','115753','117015','111204','110575','113569','115936',
      '103775','108105','109544','112088','106921','112663','107852','105915','108201','110898','111943') then salary_sales_value*0.1 
       else salary_sales_value end salary_sales_value,
  case when cust_id in ('107901','114099','115051','115047','109406','115753','117015','111204','110575','113569','115936',
      '103775','108105','109544','112088','106921','112663','107852','105915','108201','110898','111943') then salary_fnl_profit*0.2
       else salary_fnl_profit end salary_fnl_profit
from csx_tmp.temp_new_cust_01;



  
--结果表1 


--02客户、销售员逾期系数
--客户当月提成 

drop table csx_tmp.temp_new_cust_salary;
create table csx_tmp.temp_new_cust_salary
as
select 
a.smonth,a.dist,a.cust_id,a.cust_name,
--4月签呈，将以下客户的销售员调整为xx 每月处理
case when a.cust_id in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
     when a.cust_id in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
   else a.work_no end as work_no,
case when a.cust_id in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
     when a.cust_id in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
   else a.sales_name end as sales_name,		
--a.work_no,a.sales_name,
a.service_user_work_no,a.service_user_name,
a.sales_value,a.profit,a.profit/abs(a.sales_value) prorate,
a.fnl_profit,a.fnl_profit/abs(a.sales_value) fnl_prorate,
--a.salary,
a.salary_sales_value,
a.salary_fnl_profit,
b.receivable_amount,b.over_amt,
if(a.service_user_work_no<>'','服务管家有提成','服务管家无提成') assigned_type, --分配类别
--b.over_rate cust_over_rate,
c.over_rate sale_over_rate,
d.over_rate service_user_over_rate,
--if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ) salary_2
--if(z.salary3 is null,if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ),z.salary3) salary_2
if(z.salary_sales_value is null,if(a.salary_sales_value<0 or c.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_sales_value)*if(a.service_user_work_no<>'',0.9,1) salary_sales_value_sale,	
if(z.salary_sales_value is null,if(a.salary_sales_value<0 or d.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	,z.salary_sales_value)*if(a.service_user_work_no<>'',0.1,0) salary_sales_value_service,
	
if(z.salary_fnl_profit is null,if(c.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_fnl_profit)*if(a.service_user_work_no<>'',0.8,1) salary_fnl_profit_sale,
if(z.salary_fnl_profit is null,if(d.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	,z.salary_fnl_profit)*if(a.service_user_work_no<>'',0.2,0) salary_fnl_profit_service	
from  
  (select * from csx_tmp.temp_new_cust_02
  where cust_id not in(
'100326','104086','104217','112072','114486','PF1265','104151','104358','103372','105182','106423',
'106721','112054','102565','102790','103784','103855','105150','105181','111844','112976','112980',
'117554','102734','107404','102784','111400','102901','112288','112923','113467','103199','104469',
'104501','112038','115982','115987','116282','PF0365','102202','105355','105886','108067','114275',
'102647','112071')
  )a
left join csx_tmp.temp_cust_over_rate b on b.customer_no=a.cust_id
left join csx_tmp.temp_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
left join csx_tmp.temp_service_user_over_rate d on d.service_user_name=a.service_user_name and coalesce(d.service_user_work_no,0)=coalesce(a.service_user_work_no,0)
--1月签呈 调整最终提成--按照奖金包特定值发
left join 
(
select '000000'cust_id, 0 salary_sales_value, 0 salary_fnl_profit
union all
select '000001'cust_id, 0 salary_sales_value, 0 salary_fnl_profit
)z on z.cust_id=a.cust_id

--4月签呈 按照服务管家给提成，每月
union all
select 
a.smonth,a.dist,a.cust_id,a.cust_name,
--4月签呈，将以下客户的销售员调整为xx
case when a.cust_id in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '签呈,未知'
     when a.cust_id in('114054','109000','114083','114085','115909','115971') then '签呈,未知'
   else a.work_no end as work_no,
case when a.cust_id in('113873','113918','113935','113940','115656','117244','115826','115602','104281','107398') then '虚拟AA'
     when a.cust_id in('114054','109000','114083','114085','115909','115971') then '虚拟AA'
   else a.sales_name end as sales_name,	
--a.work_no,a.sales_name,
a.service_user_work_no,a.service_user_name,
a.sales_value,a.profit,a.profit/abs(a.sales_value) prorate,
a.fnl_profit,a.fnl_profit/abs(a.sales_value) fnl_prorate,
a.salary_sales_value,
a.salary_fnl_profit,
b.receivable_amount,b.over_amt,
if(a.service_user_work_no<>'','服务管家有提成','服务管家无提成') assigned_type, --分配类别
c.over_rate sale_over_rate,
d.over_rate service_user_over_rate,
if(a.salary_sales_value<0 or c.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	*if(a.service_user_work_no<>'',0.1,1) salary_sales_value_sale,	
if(a.salary_sales_value<0 or d.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	*if(a.service_user_work_no<>'',0.1,0) salary_sales_value_service,
	
if(c.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
    *if(a.service_user_work_no<>'',0.2,1) salary_fnl_profit_sale,
if(d.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(d.over_rate<=0.5,d.over_rate,1),0)) )
	*if(a.service_user_work_no<>'',0.2,0) salary_fnl_profit_service	
from  
  (select * from csx_tmp.temp_new_cust_02
  where cust_id in(
'100326','104086','104217','112072','114486','PF1265','104151','104358','103372','105182','106423',
'106721','112054','102565','102790','103784','103855','105150','105181','111844','112976','112980',
'117554','102734','107404','102784','111400','102901','112288','112923','113467','103199','104469',
'104501','112038','115982','115987','116282','PF0365','102202','105355','105886','108067','114275',
'102647','112071')
  )a
left join csx_tmp.temp_cust_over_rate b on b.customer_no=a.cust_id
left join csx_tmp.temp_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
left join csx_tmp.temp_service_user_over_rate d on d.service_user_name=a.service_user_name and coalesce(d.service_user_work_no,0)=coalesce(a.service_user_work_no,0)
; 


insert overwrite directory '/tmp/raoyanhua/tc_kehu' row format delimited fields terminated by '\t'
select *,
coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0),
coalesce(salary_sales_value_service,0)+coalesce(salary_fnl_profit_service,0)
from csx_tmp.temp_new_cust_salary;

--销售员当月提成
insert overwrite directory '/tmp/raoyanhua/tc_xiaoshou' row format delimited fields terminated by '\t'
select smonth,dist,work_no,sales_name,
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
coalesce(sum(salary_sales_value_sale),0)+coalesce(sum(salary_fnl_profit_sale),0) salary_sale
from csx_tmp.temp_new_cust_salary
group by smonth,dist,work_no,sales_name,sale_over_rate;

--服务管家当月提成
insert overwrite directory '/tmp/raoyanhua/tc_fuwuguanjia' row format delimited fields terminated by '\t'
select smonth,dist,service_user_work_no,service_user_name,
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
from csx_tmp.temp_new_cust_salary
group by smonth,dist,service_user_work_no,service_user_name,service_user_over_rate;





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
	having min(first_order_date)>='20210401' and min(first_order_date)<'20210501'
	)a on b.customer_no=a.customer_no;





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




