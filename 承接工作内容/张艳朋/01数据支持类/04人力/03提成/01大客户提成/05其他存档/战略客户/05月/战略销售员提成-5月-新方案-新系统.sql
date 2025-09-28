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

--load data inpath '/tmp/raoyanhua/strategic_cust_info_05.csv' overwrite into table csx_tmp.strategic_cust_info_new partition (sdt='202105');
--select * from csx_tmp.strategic_cust_info_new where sdt='202105';


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

--load data inpath '/tmp/raoyanhua/strategic_cust_info_04.csv' overwrite into table csx_tmp.sales_income_info partition (sdt='20210430');
--select * from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_11};


-- 昨日、昨日月1日，上月1日，上月最后一日
--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_12},${hiveconf:i_sdate_11};

set i_sdate_11 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');	
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');					

set i_sdate_11 ='20210531';	
set i_sdate_12 ='20210501';		




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
where sdt>='20210101' and sdt<=${hiveconf:i_sdate_11} 
and channel_code in('1','7')
group by sdt,customer_no,substr(sdt,1,6))a
join (select distinct a.province_name,a.customer_no,a.sales_name_strat sales_name,a.sdt smonth,b.work_no,b.income_type
	  from csx_tmp.strategic_cust_info_new a
	  left join 
		(select distinct work_no,sales_name,income_type 
		from csx_tmp.sales_income_info 
		where sdt=${hiveconf:i_sdate_11} and cust_type='战略客户'
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
sum(sales_value)sales_value,
sum(prorate*sales_value) profit,sum(prorate*sales_value)/sum(sales_value) prorate,
sum(front_profit) front_profit,sum(front_profit)/sum(sales_value) fnl_prorate,
round(sum(a.sales_value*coalesce(e.sale_rate,0.002))+if(sum(a.fnl_prorate*a.sales_value)<0,0,sum(coalesce(a.fnl_prorate*a.sales_value,0)*coalesce(e.profit_rate,0.1))),2) salary
from 
(
select sdt,substr(sdt,1,6) smonth,province_name,customer_no,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) as front_profit,sum(front_profit)/sum(sales_value) as fnl_prorate
from csx_dw.dws_sale_r_d_detail
where sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11}
and channel_code in('1','7')
group by sdt,substr(sdt,1,6),province_name,customer_no
)a
left join 
	(select distinct customer_no,customer_name,work_no,sales_name
	from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11}) b on b.customer_no=a.customer_no
join (select distinct province_name,customer_no,sales_name_strat from csx_tmp.strategic_cust_info_new where sdt=substr(${hiveconf:i_sdate_11},1,6)) d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_11} and cust_type='战略客户') c on c.sales_name=d.sales_name_strat
left join 
	(select  work_no,sales_name,sdt,max(sale_rate) sale_rate,max(profit_rate) profit_rate
	from csx_tmp.strategic_salesname_rate_ytd where sdt>=${hiveconf:i_sdate_12} and sdt<=${hiveconf:i_sdate_11} 
	group by work_no,sales_name,sdt
	)e on e.work_no=c.work_no and e.sales_name=c.sales_name and e.sdt=a.sdt
group by a.province_name,a.customer_no,b.customer_name,c.work_no,c.sales_name,a.smonth;


--逾期系数
--战略客户销售员逾期率
drop table csx_tmp.temp_strategic_salesname_over_rate;
create table csx_tmp.temp_strategic_salesname_over_rate
as
select 
	a.channel_name,	-- 渠道
	b.work_no,	-- 销售员工号
	b.sales_name,	-- 销售员
	sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount,	-- 应收金额
	sum(case when over_amt>=0 and receivable_amount>0 then over_amt else 0 end) over_amt,	-- 逾期金额
	sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end) over_amt_s,	-- 逾期金额*逾期天数
	sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end) receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case  when coalesce(sum(case when receivable_amount>=0 then receivable_amount else 0 end), 0) <= 1 then 0  
				else coalesce(sum(case when over_amt_s>=0 and receivable_amount>0 then over_amt_s else 0 end), 0)
				/(sum(case when receivable_amount_s>=0 and receivable_amount>0 then receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
from
	(select
		channel_name,
		customer_no,
		customer_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl a  
	where channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11} 
	group by channel_name,customer_no,customer_name,company_code,company_name)a
join (select distinct province_name,customer_no,sales_name_strat from csx_tmp.strategic_cust_info_new where sdt=substr(${hiveconf:i_sdate_11},1,6)) d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_11} and cust_type='战略客户') b on b.sales_name=d.sales_name_strat
group by a.channel_name,b.work_no,b.sales_name;

--02客户、销售员逾期系数
--客户当月提成 

drop table csx_tmp.temp_strategic_new_cust_salary;
create table csx_tmp.temp_strategic_new_cust_salary
as
select 
a.smonth,a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,
a.sales_value,a.profit,a.profit/a.sales_value prorate,
a.front_profit,a.front_profit/a.sales_value fnl_prorate,
a.salary,
b.receivable_amount,b.over_amt,
--b.over_rate cust_over_rate,
--if(a.salary<0 or b.over_rate is null,a.salary,a.salary*(1-coalesce(if(b.over_rate<=0.5,b.over_rate,1),0)) ) salary_1,
c.over_rate sale_over_rate,
if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ) salary_2
from  csx_tmp.temp_strategic_new_cust_01 a
left join csx_tmp.temp_cust_over_rate b on b.customer_no=a.cust_id
left join csx_tmp.temp_strategic_salesname_over_rate c on c.sales_name=a.sales_name
; 

insert overwrite directory '/tmp/raoyanhua/zl_kehu' row format delimited fields terminated by '\t'
select * from csx_tmp.temp_strategic_new_cust_salary;

--销售员当月提成
insert overwrite directory '/tmp/raoyanhua/zl_xiaoshou' row format delimited fields terminated by '\t'
select smonth,dist,work_no,sales_name,
sum(sales_value)sales_value,
sum(profit)profit,
sum(profit)/sum(sales_value) prorate,
sum(front_profit)front_profit,
sum(front_profit)/sum(sales_value)fnl_prorate,
sum(salary)salary,
sum(receivable_amount)receivable_amount,
sum(over_amt)over_amt,
--sum(salary_1)salary_1,
sale_over_rate,
sum(salary_2)salary_2
from csx_tmp.temp_strategic_new_cust_salary
group by smonth,dist,work_no,sales_name,sale_over_rate ;


--计算逾期率
insert overwrite directory '/tmp/raoyanhua/zl_yqxs' row format delimited fields terminated by '\t'
select 
	a.channel_name,	-- 渠道
	b.sales_province_name,	-- 省区
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	e.work_no,	-- 销售员工号
	e.sales_name,	-- 销售员
	c.account_period_code,	-- 账期编码
	if(c.account_period_code like 'Y%',if(c.account_period_val=31,45,c.account_period_val+15),c.account_period_val) account_period_val,	-- 帐期天数
	c.account_period_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称
	case when a.receivable_amount>=0 then a.receivable_amount else 0 end receivable_amount,	-- 应收金额
	case when a.over_amt>=0 and a.receivable_amount>0 then a.over_amt else 0 end over_amt,	-- 逾期金额
	case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end over_amt_s,	-- 逾期金额*逾期天数
	case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end receivable_amount_s,	-- 应收金额*帐期天数	
    coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
						else (coalesce(case when a.over_amt_s>=0 and a.receivable_amount>0 then a.over_amt_s else 0 end,0)
						/(case when a.receivable_amount_s>=0 and a.receivable_amount>0 then a.receivable_amount_s else 0 end)) end
		  , 6),0) over_rate 	-- 逾期系数
from
	(select
		channel_name,
		customer_no,
		customer_name,
		company_code,
		company_name ,
		sum(receivable_amount) as receivable_amount,
		sum(case when over_days>=1 then receivable_amount else 0 end ) as over_amt,
		sum(case when over_days>=1 then receivable_amount*over_days else 0 end) as over_amt_s,
		sum(case when receivable_amount>=0 then receivable_amount else 0 end* if(COALESCE(account_period_val,0)=0,1,acc_val_calculation_factor)) as receivable_amount_s
	from csx_tmp.tmp_cust_order_overdue_dtl a  
	where channel_name = '大客户' and sdt = ${hiveconf:i_sdate_11} 
	group by channel_name,customer_no,customer_name,company_code,company_name)a
left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:i_sdate_11})b on b.customer_no=a.customer_no 
join (select distinct province_name,customer_no,sales_name_strat from csx_tmp.strategic_cust_info_new where sdt=substr(${hiveconf:i_sdate_11},1,6)) d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_tmp.sales_income_info where sdt=${hiveconf:i_sdate_11} and cust_type='战略客户') e on e.sales_name=d.sales_name_strat
left join
	(select
		customer_no,
		company_code,
		payment_terms account_period_code,
		case when payment_terms like 'Y%' then concat('月结',COALESCE(cast(payment_days as int),0)) 
			 else concat('票到',COALESCE(cast(payment_days as int),0)) end account_period_name,
		COALESCE(cast(payment_days as int),0) account_period_val
	from csx_dw.dws_crm_w_a_customer_company a
	where sdt='current'
	and customer_no<>''
	)c on (a.customer_no=c.customer_no and a.company_code=c.company_code)  
;









