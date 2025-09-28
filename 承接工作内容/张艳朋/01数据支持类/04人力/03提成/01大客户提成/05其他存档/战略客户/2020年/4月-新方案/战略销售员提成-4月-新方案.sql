-- 新建表 战略客户清单
--drop table csx_dw.strategic_cust_info_new;
CREATE TABLE IF NOT EXISTS `csx_dw.strategic_cust_info_new` (
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

--load data inpath '/tmp/raoyanhua/strategic_cust_info.csv' overwrite into table csx_dw.strategic_cust_info_new partition (sdt='202004');
--select * from csx_dw.strategic_cust_info_new where sdt='202004';


-- 新建表 销售提成_销售员收入组
--drop table csx_dw.sales_income_info;
create table if not exists `csx_dw.sales_income_info` (
  `cust_type` STRING comment '销售员类别',
  `sales_name` STRING comment '业务员名称',
  `work_no` STRING comment '业务员工号',
  `income_type` STRING comment '业务员收入组类'
) comment '销售提成_销售员收入组'
partitioned by (sdt string comment '日期分区')
row format delimited fields terminated by ','
stored as textfile;

--load data inpath '/tmp/raoyanhua/sales_income_info.csv' overwrite into table csx_dw.sales_income_info partition (sdt='20200430');
--select * from csx_dw.sales_income_info where sdt='20200430';

---每日销售员提成系数（销额提成比例、前端毛利提成比例）
drop table b2b_tmp.strategic_salesname_rate_ytd;
create table b2b_tmp.strategic_salesname_rate_ytd
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
 from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200101' and sdt<'20200501' 
and sales_type in('qyg','sapqyg','sapgc','sc','bbc')  
and item_channel_code in('1','7')
group by sdt,customer_no,substr(sdt,1,6))a 
join (select distinct province_name,customer_no,sales_name_strat from csx_dw.strategic_cust_info_new where sdt='202004') d on d.customer_no=a.customer_no
--left join (select * from csx_dw.dws_crm_w_a_customer_m where sdt='20200430') b on b.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_dw.sales_income_info where sdt='20200430' and cust_type='战略客户') c on c.sales_name=d.sales_name_strat
)a;



--01、客户本月每天-销售员销额、最终前端毛利统计
drop table b2b_tmp.temp_strategic_new_cust_01;
create table b2b_tmp.temp_strategic_new_cust_01
as
select 
a.sales_province dist,a.customer_no cust_id,b.customer_name cust_name,c.work_no,c.sales_name,coalesce(e.sale_rate,0.002) sale_rate,coalesce(e.profit_rate,0.1) profit_rate,a.smonth,
sum(sales_value)sales_value,
sum(prorate*sales_value) profit,sum(prorate*sales_value)/sum(sales_value) prorate,
sum(fnl_prorate*sales_value) fnl_profit,sum(fnl_prorate*sales_value)/sum(sales_value) fnl_prorate,
round(sum(a.sales_value)*coalesce(e.sale_rate,0.002)+if(sum(a.fnl_prorate*a.sales_value)<0,0,coalesce(sum(a.fnl_prorate*a.sales_value),0)*coalesce(e.profit_rate,0.1)),2) salary
from 
(select sdt,substr(sales_date,1,6) smonth,sales_province,customer_no,
sum(sales_value)sales_value,
sum(profit) profit,sum(profit)/sum(sales_value) prorate,
sum(front_profit) as fnl_profit,sum(front_profit)/sum(sales_value) as fnl_prorate
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200401' and sdt<'20200501'
and sales_type in ('qyg','sapqyg','sapgc','sc','bbc') 
and item_channel_code in('1','7')
group by sdt,substr(sales_date,1,6),sales_province,customer_no)a
left join 
	(select distinct customer_no,customer_name,work_no,sales_name
	from csx_dw.dws_crm_w_a_customer_m where sdt='20200430') b on b.customer_no=a.customer_no
join (select distinct province_name,customer_no,sales_name_strat from csx_dw.strategic_cust_info_new where sdt='202004') d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_dw.sales_income_info where sdt='20200430' and cust_type='战略客户') c on c.sales_name=d.sales_name_strat
left join 
	(select  work_no,sales_name,sdt,max(sale_rate) sale_rate,max(profit_rate) profit_rate
	from b2b_tmp.strategic_salesname_rate_ytd where sdt>='20200401' and sdt<'20200501' 
	group by work_no,sales_name,sdt
	)e on e.work_no=c.work_no and e.sales_name=c.sales_name and e.sdt=a.sdt
group by a.sales_province,a.customer_no,b.customer_name,c.work_no,c.sales_name,e.sale_rate,e.profit_rate,a.smonth;

--逾期系数
--战略客户销售员逾期率
drop table b2b_tmp.temp_strategic_salesname_over_rate;
create table b2b_tmp.temp_strategic_salesname_over_rate
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
	where channel = '大客户' and sdt = '20200430' 
	and subject_code='1122010000'
	group by channel,customer_no,customer_name,zterm,payment_days,payment_terms,comp_code,comp_name)a
join (select distinct province_name,customer_no,sales_name_strat from csx_dw.strategic_cust_info_new where sdt='202004') d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_dw.sales_income_info where sdt='20200430' and cust_type='战略客户') b on b.sales_name=d.sales_name_strat
group by a.channel,b.work_no,b.sales_name;

--02客户、销售员逾期系数
--客户当月提成 

drop table b2b_tmp.temp_strategic_new_cust_salary;
create table b2b_tmp.temp_strategic_new_cust_salary
as
select 
a.smonth,a.dist,a.cust_id,a.cust_name,a.work_no,a.sales_name,a.sale_rate,a.profit_rate,
a.sales_value,a.profit,a.profit/a.sales_value prorate,
a.fnl_profit,a.fnl_profit/a.sales_value fnl_prorate,
a.salary,
b.ac_all,b.over_amt,
--b.over_rate cust_over_rate,
--if(a.salary<0 or b.over_rate is null,a.salary,a.salary*(1-coalesce(if(b.over_rate<=0.5,b.over_rate,1),0)) ) salary_1,
c.over_rate sale_over_rate,
if(a.salary<0 or c.over_rate is null,a.salary,a.salary*(1-coalesce(if(c.over_rate<=0.5,c.over_rate,1),0)) ) salary_2
from  b2b_tmp.temp_strategic_new_cust_01 a
left join b2b_tmp.temp_cust_over_rate b on b.customer_no=a.cust_id
left join b2b_tmp.temp_strategic_salesname_over_rate c on c.sales_name=a.sales_name
; 

insert overwrite directory '/tmp/raoyanhua/linshi04' row format delimited fields terminated by '\t'
select * from b2b_tmp.temp_strategic_new_cust_salary;

--销售员当月提成
insert overwrite directory '/tmp/raoyanhua/linshi05' row format delimited fields terminated by '\t'
select smonth,dist,work_no,sales_name,sale_rate,profit_rate,
sum(sales_value)sales_value,
sum(profit)profit,
sum(profit)/sum(sales_value) prorate,
sum(fnl_profit)fnl_profit,
sum(fnl_profit)/sum(sales_value)fnl_prorate,
sum(salary)salary,
sum(ac_all)ac_all,
sum(over_amt)over_amt,
--sum(salary_1)salary_1,
sale_over_rate,
sum(salary_2)salary_2
from b2b_tmp.temp_strategic_new_cust_salary
group by smonth,dist,work_no,sales_name,sale_rate,profit_rate,sale_over_rate ;


--计算逾期率
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select 
	c.sales_province,a.channel,b.work_no,b.sales_name,a.customer_no,a.customer_name,a.zterm,
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
	where channel = '大客户' and sdt = '20200430' 
	and subject_code='1122010000'
	group by channel, customer_no, customer_name, zterm, payment_days, payment_terms, comp_code, comp_name )a
left join (select * from csx_dw.dws_crm_w_a_customer_m where sdt='20200430') c on c.customer_no=a.customer_no
join (select distinct province_name,customer_no,sales_name_strat from csx_dw.strategic_cust_info_new where sdt='202004') d on d.customer_no=a.customer_no
left join (select distinct work_no,sales_name,income_type from csx_dw.sales_income_info where sdt='20200430' and cust_type='战略客户') b on b.sales_name=d.sales_name_strat;





---截至上月销售员的累计销售额
drop table csx_dw.dws_cust_ytd_sale;
create table csx_dw.dws_cust_ytd_sale
as
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select b.work_no,b.sales_name,
sum(a.sales_value)sales_value,
sum(a.profit)profit
from 
(select customer_no,substr(sdt,1,6) smonth,
sum(sales_value) sales_value,
sum(profit)profit
 from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200101' and sdt<'20200401' 
and sales_type in('qyg','sapqyg','sapgc','sc','bbc')  
and item_channel_code in('1','7')
group by customer_no,substr(sdt,1,6))a 
left join (select * from csx_dw.dws_crm_w_a_customer_m where sdt='20200430') b on b.customer_no=a.customer_no
left join (select distinct customer_no,substr(sdt,1,6) smonth from csx_dw.csx_partner_list ) d on d.customer_no=a.customer_no and d.smonth=a.smonth
where d.customer_no is null
group by b.work_no,b.sales_name;




