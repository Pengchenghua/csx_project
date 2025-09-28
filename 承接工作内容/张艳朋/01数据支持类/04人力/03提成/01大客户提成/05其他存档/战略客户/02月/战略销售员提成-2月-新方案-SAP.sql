-- 新建表 战略客户清单
--drop table csx_tmp.strategic_cust_info_new;
CREATE TABLE IF NOT EXISTS `csx_tmp.strategic_cust_info_new` (
  `province_name` STRING COMMENT '省区',
  `customer_no` STRING COMMENT '客户编号',
  `customer_name` STRING COMMENT '客户名称',
  `attribute` STRING COMMENT '客户属性',
  `sales_name_strat` STRING COMMENT '业务员(战略)',
  `cooperate_type` STRING COMMENT '模式'
) COMMENT '战略客户清单'
partitioned by (sdt string comment '日期分区')
row format delimited fields terminated by ','
stored as textfile;

--load data inpath '/tmp/raoyanhua/strategic_cust_info_01.csv' overwrite into table csx_tmp.strategic_cust_info_new partition (sdt='202101');
--select * from csx_tmp.strategic_cust_info_new where sdt='202101';


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

--load data inpath '/tmp/raoyanhua/sales_income_info_6.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20200630');
--select * from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_23};


-- 昨日、昨日月1日，上月1日，上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
	
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');


set i_sdate_11 ='20201231';
set i_sdate_12 ='20210101';
	
set i_sdate_22 ='20201201';				
set i_sdate_23 ='20201231';




---每日销售员提成系数（销额提成比例、前端毛利提成比例）
drop table csx_tmp.strategic_salesname_rate_ytd;
create table csx_tmp.strategic_salesname_rate_ytd
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
(select a.sdt,c.work_no,c.sales_name,coalesce(c.income_type,'Q1')income_type,
sum(a.sales_value)over(PARTITION BY c.work_no,c.sales_name,substr(a.sdt,1,4) order by a.sdt ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING )ytd
from 
(select sdt,customer_no,substr(sdt,1,6) smonth,
sum(sales_value) sales_value
 from csx_dw.dws_sale_r_d_detail
where sdt>='20210101' and sdt<${hiveconf:i_sdate_12}  
and channel_code in('1','7')
group by sdt,customer_no,substr(sdt,1,6))a
join (select distinct a.province_name,a.customer_no,a.sales_name_strat sales_name,a.sdt smonth,b.work_no,b.income_type
	  from csx_tmp.strategic_cust_info_new a
	  left join 
		(select distinct work_no,sales_name,income_type 
		from csx_tmp.sales_income_info 
		where sdt=${hiveconf:i_sdate_23} and cust_type='战略客户'
		)b on b.sales_name=a.sales_name_strat
	 where a.sdt>='202101'
	  )c on c.customer_no=a.customer_no and c.smonth=a.smonth
)a;

 



--01、客户本月每天-销售员销额、最终前端毛利统计
drop table csx_tmp.temp_strategic_new_cust_01;
create table csx_tmp.temp_strategic_new_cust_01
as
select 
a.province_name dist,a.customer_no cust_id,b.customer_name cust_name,c.work_no,c.sales_name,
--coalesce(e.sale_rate,0.002) sale_rate,coalesce(e.profit_rate,0.1) profit_rate,
a.smonth,
sum(if(a.s_type='销售',sales_value,0))sales_value_0,
sum(if(a.s_type='销售',profit,0)) profit_0,
sum(if(a.s_type='销售',front_profit,0)) front_profit_0,
sum(if(a.s_type='返利',sales_value,0))sales_value_fanli,

sum(sales_value)sales_value,
sum(prorate*sales_value) profit,sum(prorate*sales_value)/sum(sales_value) prorate,
sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate,
round(sum(a.sales_value*coalesce(e.sale_rate,0.002))+if(sum(a.fnl_prorate*a.sales_value)<0,0,sum(coalesce(a.fnl_prorate*a.sales_value,0)*coalesce(e.profit_rate,0.1))),2) salary
from 
(
select if(sales_type='fanli','返利','销售') s_type,sdt,substr(sdt,1,6) smonth,province_name,customer_no,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) as front_profit,sum(front_profit)/sum(sales_value) as fnl_prorate
from csx_dw.dws_sale_r_d_detail
where sdt>=${hiveconf:i_sdate_22} and sdt<${hiveconf:i_sdate_12}
and channel_code in('1','7')
group by if(sales_type='fanli','返利','销售'),sdt,substr(sdt,1,6),province_name,customer_no
)a
left join 
	(select distinct customer_no,customer_name,work_no,sales_name
	from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_23}) b on b.customer_no=a.customer_no
join (select distinct province_name,customer_no,sales_name_strat from csx_tmp.strategic_cust_info_new where sdt=substr(${hiveconf:i_sdate_23},1,6)) d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_23} and cust_type='战略客户') c on c.sales_name=d.sales_name_strat
left join 
	(select  work_no,sales_name,sdt,max(sale_rate) sale_rate,max(profit_rate) profit_rate
	from csx_tmp.strategic_salesname_rate_ytd where sdt>=${hiveconf:i_sdate_22} and sdt<${hiveconf:i_sdate_12} 
	group by work_no,sales_name,sdt
	)e on e.work_no=c.work_no and e.sales_name=c.sales_name and e.sdt=a.sdt
group by a.province_name,a.customer_no,b.customer_name,c.work_no,c.sales_name,a.smonth;


--逾期系数
--战略客户销售员逾期率
drop table csx_tmp.temp_strategic_salesname_over_rate;
create table csx_tmp.temp_strategic_salesname_over_rate
as
select 
	a.channel,
	b.work_no,
	b.sales_name,
	sum(case when ac_all>=0 then ac_all else 0 end) ac_all,
	sum(case when over_amt>=0 then over_amt else 0 end) over_amt,
	sum(case when over_amt>=0 then over_amt_1 else 0 end) over_amt_1,
	sum(case when ac_all>=0 then diff_ac_all else 0 end) diff_ac_all,
    coalesce(round(case  when coalesce(SUM(case when ac_all>=0 then ac_all else 0 end), 0) <= 1 then 0  
				else coalesce(SUM(case when over_amt>=0 then over_amt_1 else 0 end), 0)/(sum(case when ac_all>=0 then diff_ac_all else 0 end)) end
		  , 6),0) over_rate
from
	(select
		channel,
		customer_no,
		customer_name,
		zterm,
		payment_days,
		payment_terms,
		comp_code,
		comp_name ,
		--sum(case when over_days>=0 then ac_all else 0 end) as ac_all,
		sum(ac_all) as ac_all,
		sum(case when over_days>=1 then ac_all else 0 end ) as over_amt,
		SUM(case when over_days>=1 then ac_all*over_days else 0 end) as over_amt_1,
		sum(ac_all)* if(payment_days=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days)) as diff_ac_all
	from csx_dw.ads_fis_r_a_customer_days_overdue_dtl a 
	where channel = '大客户' and sdt = ${hiveconf:i_sdate_23} 
	and subject_code='1122010000'
	group by channel,customer_no,customer_name,zterm,payment_days,payment_terms,comp_code,comp_name)a
join (select distinct province_name,customer_no,sales_name_strat from csx_tmp.strategic_cust_info_new where sdt=substr(${hiveconf:i_sdate_23},1,6)) d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_23} and cust_type='战略客户') b on b.sales_name=d.sales_name_strat
group by a.channel,b.work_no,b.sales_name;

--02客户、销售员逾期系数
--客户当月提成 

drop table csx_tmp.temp_strategic_new_cust_salary;
create table csx_tmp.temp_strategic_new_cust_salary
as
select 
a.smonth,a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,
a.sales_value_0,a.profit_0,a.front_profit_0,a.sales_value_fanli,

a.sales_value,a.profit,a.profit/a.sales_value prorate,
a.front_profit,a.front_profit/a.sales_value fnl_prorate,
a.salary,
b.ac_all,b.over_amt,
--b.over_rate cust_over_rate,
--if(a.salary<0 or b.over_rate is null,a.salary,a.salary*(1-coalesce(if(b.over_rate<=0.5,b.over_rate,1),0)) ) salary_1,
c.over_rate sale_over_rate,
if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ) salary_2
from  csx_tmp.temp_strategic_new_cust_01 a
left join csx_tmp.temp_cust_over_rate_1 b on b.customer_no=a.cust_id
left join csx_tmp.temp_strategic_salesname_over_rate c on c.sales_name=a.sales_name
; 

insert overwrite directory '/tmp/raoyanhua/zl_kehu' row format delimited fields terminated by '\t'
select * from csx_tmp.temp_strategic_new_cust_salary;

--销售员当月提成
insert overwrite directory '/tmp/raoyanhua/zl_xiaoshou' row format delimited fields terminated by '\t'
select smonth,dist,work_no,sales_name,
sum(sales_value_0) sales_value_0,
sum(profit_0) profit_0,
sum(front_profit_0) front_profit_0,
sum(sales_value_fanli) sales_value_fanli,

sum(sales_value)sales_value,
sum(profit)profit,
sum(profit)/sum(sales_value) prorate,
sum(front_profit)front_profit,
sum(front_profit)/sum(sales_value)fnl_prorate,
sum(salary)salary,
sum(ac_all)ac_all,
sum(over_amt)over_amt,
--sum(salary_1)salary_1,
sale_over_rate,
sum(salary_2)salary_2
from csx_tmp.temp_strategic_new_cust_salary
group by smonth,dist,work_no,sales_name,sale_over_rate ;


--计算逾期率
insert overwrite directory '/tmp/raoyanhua/zl_yqxs' row format delimited fields terminated by '\t'
select 
	c.sales_province_name,a.channel,b.work_no,b.sales_name,a.customer_no,a.customer_name,a.zterm,
	if(a.zterm like 'Y%',if(a.payment_days=31,45,a.payment_days+15),a.payment_days) payment_days,a.payment_terms,a.comp_code,a.comp_name,
	a.ac_all,a.over_amt,a.over_amt_1,a.diff_ac_all,a.over_rate
from
	(select
		channel,
		customer_no,
		customer_name,
		zterm,
		payment_days,
		payment_terms,
		comp_code,
		comp_name ,
		--sum(case when over_days>=0 then ac_all else 0 end) as ac_all,
		sum(ac_all) as ac_all,
		sum(case when over_days>=1 then ac_all else 0 end ) as over_amt,
		SUM(case when over_days>=1 then ac_all*over_days else 0 end) as over_amt_1,
		--sum(case when over_days>=0 then ac_all else 0 end)* if(payment_days=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days)) as diff_ac_all,
		sum(case when ac_all>=0 then ac_all else 0 end)* if(payment_days=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days)) as diff_ac_all,
		coalesce(round(case  when coalesce(SUM(case when ac_all>=0 then ac_all else 0 end), 0) <= 1 then 0  
					else coalesce(SUM(case when over_days>=1 then ac_all*over_days else 0 end), 0)
						/(sum(case when ac_all>=0 then ac_all else 0 end)* if(payment_days=0,1,if(zterm like 'Y%',if(payment_days=31,45,payment_days+15),payment_days))) end
			  , 6),0) over_rate
	  
	from csx_dw.ads_fis_r_a_customer_days_overdue_dtl a 
	where channel = '大客户' and sdt = ${hiveconf:i_sdate_23} 
	and subject_code='1122010000'
	group by channel, customer_no, customer_name, zterm, payment_days, payment_terms, comp_code, comp_name )a
left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_23}) c on c.customer_no=a.customer_no
join (select distinct province_name,customer_no,sales_name_strat from csx_tmp.strategic_cust_info_new where sdt=substr(${hiveconf:i_sdate_23},1,6)) d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_23} and cust_type='战略客户') b on b.sales_name=d.sales_name_strat;








