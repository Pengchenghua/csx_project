
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
	--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理
	where (b.ywdl_cust is null or b.customer_no='118689')
	and b.ng_cust is null
	)a;


--01、客户本月每天-销售员销额、最终前端毛利统计
drop table csx_tmp.temp_new_cust_00;
create table csx_tmp.temp_new_cust_00
as
select 
b.sales_province_name dist,a.customer_no cust_id,b.customer_name cust_name,b.work_no,b.sales_name,a.smonth,
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
--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成,每月处理  
where (b.ywdl_cust is null or b.customer_no='118689')
and b.ng_cust is null
group by b.sales_province_name,a.customer_no,b.customer_name,b.work_no,b.sales_name,c.sale_rate,c.profit_rate,a.smonth;



--大客户前端毛利扣点后结果
drop table csx_tmp.temp_new_cust_01;
create table csx_tmp.temp_new_cust_01
as
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.smonth,
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
)z on z.cust_id=a.cust_id
where a.cust_id not in('115935')
and a.cust_id not in('105220','105539','106239','106713','106900','107100','107242','107361','107532','108236','110242',
'115274','115931','116461','117704','118119','102534','102798','102806','104741','105186','114724',
'115915','115920','104758','105956','105965','106288','106559','106878','107104','112492','112633',
'113423','117689','105480','105483','105540','106300','106469','106524','106538','107438','111892',
'112210','112474','117022','117067','103945','103954','104222','104229','104241','104251','104255',
'104414','104965','105005','105024','105756','112813','117753')
--and a.cust_id not in('116603','117002','115935')
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.smonth

-- 签呈提成方式前端毛利*6%
union all
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.smonth,
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
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.smonth

--4月签呈 '118689'系统中为业务代理人，但需要人为计算销售员大客户提成，李燕玲（81079631）提成方式销售额*0.2%*0.1，每月
union all
select 
a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.smonth,
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
group by a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.smonth;


  
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
a.sales_value,a.profit,a.profit/abs(a.sales_value) prorate,
a.fnl_profit,a.fnl_profit/abs(a.sales_value) fnl_prorate,
--a.salary,
a.salary_sales_value,
a.salary_fnl_profit,
b.receivable_amount,b.over_amt,
--b.over_rate cust_over_rate,
c.over_rate sale_over_rate,
--if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ) salary_2
--if(z.salary3 is null,if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ),z.salary3) salary_2
if(z.salary_sales_value is null,if(a.salary_sales_value<0 or c.over_rate is null,
    a.salary_sales_value,a.salary_sales_value*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_sales_value) salary_sales_value_sale,	
	
if(z.salary_fnl_profit is null,if(c.over_rate is null,
    a.salary_fnl_profit,a.salary_fnl_profit*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) )
	,z.salary_fnl_profit) salary_fnl_profit_sale
from  
  (select * from csx_tmp.temp_new_cust_01
  where cust_id not in(
'111100','102844','104609','104828','104829','105113','106283','106284','106298','106299','106301',
'106306','106307','106308','106309','106320','106321','106325','106326','106330','106782','112574',
'115721','116826','116883','116957','117940','PF0065','114265','117412')
  )a
left join csx_tmp.temp_cust_over_rate b on b.customer_no=a.cust_id
left join csx_tmp.temp_salesname_over_rate c on c.sales_name=a.sales_name and coalesce(c.work_no,0)=coalesce(a.work_no,0)
--1月签呈 调整最终提成--按照奖金包特定值发
left join 
(
select '000000'cust_id, 0 salary_sales_value, 0 salary_fnl_profit
union all
select '000001'cust_id, 0 salary_sales_value, 0 salary_fnl_profit
)z on z.cust_id=a.cust_id
; 


insert overwrite directory '/tmp/raoyanhua/tc_kehu' row format delimited fields terminated by '\t'
select *,
coalesce(salary_sales_value_sale,0)+coalesce(salary_fnl_profit_sale,0)
from csx_tmp.temp_new_cust_salary;





