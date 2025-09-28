-- 战略客户
1、最后一列中离职人员客户、9月之前开发客户、未获得开发确认单客户均不纳入此次提成核算范畴。
2、空白的按模式3；其他的根据备注有代理商、合伙人，合伙人的按合伙人的第2种

--107222去掉，业务刚给反馈是去年8月的
--106566客户 何玫本月离职，她的提成需要算本月回款，导数据给她加上2月汇款吧
--108267客户属于袁军的，不是万长安的，计算提成计算一月以后的过机
--万长安的客户只计算一月以后的过机
--负毛利显示毛利为0(销售额10万，净利润-2万，提成=10万*1%+0万）*15%=5000元,计算逻辑是这个）
--第三种模式中台报价要加一个点（就是抛开销售的一个点的提成）算净利润，代理商模式需要扣除代理商的点数，合伙人模式无需扣点
--102755中国人民解放军73051部队生活服务中心，代理商模式提成系数3%，不扣省区基准利率

-- 新建表 战略客户基本信息表
--drop table csx_dw.strategic_cust_info;
CREATE TABLE IF NOT EXISTS `csx_dw.strategic_cust_info` (
  `province_name` STRING COMMENT '省区',
  `customer_no` STRING COMMENT '客户编号',
  `customer_name` STRING COMMENT '客户名称',
  `attribute` STRING COMMENT '客户属性',
  `channel` STRING COMMENT '渠道',
  `sales_name` STRING COMMENT '业务员(系统)',
  `sign_time` STRING COMMENT '建档日期',
  `sales_name_strat` STRING COMMENT '业务员(战略)',
  `present_date` STRING COMMENT '提报日期',
  `confirmation` STRING COMMENT '确认单',
  `remarks` STRING COMMENT '备注',
  `count_flag` STRING COMMENT '是否计算',
  `cooperate_type` STRING COMMENT '模式',
  `proportion` decimal(26,6) comment '提成系数',
  `sdt` STRING COMMENT '日期'
) COMMENT '战略客户基本信息表'
ROW format delimited fields terminated by ','
STORED AS TEXTFILE;

--load data inpath '/tmp/raoyanhua/aa.csv' overwrite into table csx_dw.strategic_cust_info;



-----00、订单明细
drop table b2b_tmp.temp_strategic_cust_order;
create table b2b_tmp.temp_strategic_cust_order
as
select a.*,b.province_name,b.customer_name,b.attribute,b.channel,b.sales_name,b.sign_time,
b.sales_name_strat,b.present_date,b.confirmation,b.remarks,b.count_flag,b.cooperate_type,b.proportion,
case when cooperate_type like '合伙人%' then 0
	 when cooperate_type like '代理商%' then proportion
	 else 0.01
	 end rate,
coalesce(c.pro_std,0.08) pro_std
from 
(select report_price ptype,origin_order_no,order_no,
customer_no,sdt,goods_code,goods_name,substr(sales_date,1,6) smonth,
sum(sales_qty)sales_qty,
sum(sales_value)sales_value,
sum(sales_cost)sales_cost,
sum(coalesce(middle_office_price,0)*sales_qty) zt_cost,
sum(profit)sap_profit,
sum(profit)/sum(sales_value) sap_prorate,
--case when report_price=1 then sum(coalesce(promotion_price,sales_price)*sales_qty) end zt_sale,
case when report_price=1 then sum(coalesce(promotion_price,sales_price)*sales_qty)-sum(coalesce(middle_office_price,0)*sales_qty) end zt_profit,
case when report_price=1 then (sum(coalesce(promotion_price,sales_price)*sales_qty)-sum(coalesce(middle_office_price,0)*sales_qty))/sum(coalesce(promotion_price,sales_price)*sales_qty) end zt_prorate
from (select * from csx_dw.sale_item_m
where sdt>='20200201' and sdt<'20200301'
and sales_type in ('qyg','gc','anhui','sc') and customer_no not like 'S%'
union all
select * from csx_dw.bbc_sale_item_m_price
where sdt>='20200201' and sdt<'20200301'
and customer_no not like 'S%')a
group by report_price,origin_order_no,order_no,
customer_no,sdt,goods_code,goods_name,substr(sales_date,1,6))a
join 
(select * from csx_dw.strategic_cust_info
where count_flag='是'
)b on b.customer_no =a.customer_no 
left join 
(select '上海' dist,0.100 pro_std
union all 
select '江苏' dist,0.090 pro_std
union all 
select '浙江' dist,0.090 pro_std
union all 
select '北京' dist,0.100 pro_std
union all 
select '平台' dist,0.100 pro_std
union all 
select '安徽' dist,0.080 pro_std
union all 
select '四川' dist,0.080 pro_std
union all 
select '福建' dist,0.080 pro_std
union all 
select '重庆' dist,0.080 pro_std
union all 
select 'BBC' dist,0.080 pro_std
union all 
select '广东' dist,0.100 pro_std
union all 
select '河北' dist,0.100 pro_std)c on substr(b.province_name,1,2)=substr(c.dist,1,2);

--province_name,customer_no,customer_name,attribute,channel,sales_name,sign_time,sales_name_strat,present_date,confirmation,remarks,count_flag,cooperate_type,proportion,



--01、客户每月-销售员销额、最终前端毛利统计
drop table b2b_tmp.temp_strategic_cust_01;
create table b2b_tmp.temp_strategic_cust_01
as
select 
a.smonth,a.province_name dist,a.customer_no cust_id,a.customer_name,a.attribute,a.channel,a.sales_name,a.sign_time,
a.sales_name_strat,a.present_date,a.confirmation,a.remarks,a.count_flag,a.cooperate_type,a.proportion,
--sum(sales_cost)sales_cost,
--sum(zt_cost)zt_cost,
sum(sales_value)sales_value,
sum(sap_prorate*sales_value) sap_profit,sum(sap_prorate*sales_value)/sum(sales_value) sap_prorate,
--sum(zt_prorate*sales_value) zt_profit,sum(zt_prorate*sales_value)/sum(sales_value) zt_prorate,
sum(fnl_prorate*sales_value) fnl_profit,sum(fnl_prorate*sales_value)/sum(sales_value) fnl_prorate
from (
select sdt,ptype,origin_order_no,order_no,smonth,
province_name,customer_no,customer_name,attribute,channel,sales_name,sign_time,
sales_name_strat,present_date,confirmation,remarks,count_flag,cooperate_type,proportion,
sales_qty,sales_value,sales_cost,zt_cost,sap_profit,zt_profit,sap_prorate,zt_prorate,
case when ptype=1 then zt_prorate-rate 
		else if(customer_no='102755',sap_prorate-rate,sap_prorate-pro_std-rate) end as fnl_prorate,

(case when ptype=1 then zt_prorate-rate 
		else if(customer_no='102755',sap_prorate-rate,sap_prorate-pro_std-rate) end)*sales_qty as fnl_profit
from b2b_tmp.temp_strategic_cust_order )a 
group by a.smonth,a.province_name,a.customer_no,a.customer_name,a.attribute,a.channel,a.sales_name,a.sign_time,
a.sales_name_strat,a.present_date,a.confirmation,a.remarks,a.count_flag,a.cooperate_type,a.proportion;

insert overwrite directory '/tmp/raoyanhua/zhanlueyeji01' row format delimited fields terminated by '\t' 
select * from b2b_tmp.temp_strategic_cust_01;

/*
-- 新建表 战略客户每月销额前端毛利
--drop table csx_dw.sale_strategic_income_res;
CREATE TABLE IF NOT EXISTS `csx_dw.sale_strategic_income_res` (
  `dist` STRING COMMENT '省区',
  `cust_id` STRING COMMENT '客户编号',
  `customer_name` STRING COMMENT '客户名称',
  `attribute` STRING COMMENT '客户属性',
  `channel` STRING COMMENT '渠道',
  `sales_name` STRING COMMENT '业务员(系统)',
  `sales_name_strat` STRING COMMENT '业务员(战略)',
  `confirmation` STRING COMMENT '确认单',
  `remarks` STRING COMMENT '备注',
  `count_flag` STRING COMMENT '是否计算',
  `cooperate_type` STRING COMMENT '模式',
  `proportion` decimal(26,6) comment '提成系数',
  `sales_value` decimal(26,6) comment '销售额',
  `sap_profit` decimal(26,6) comment '毛利额',
  `sap_prorate` decimal(26,6) comment '毛利率',  
  `fnl_profit` decimal(26,6) comment '前台毛利',
  `fnl_prorate` decimal(26,6) comment '前台毛利率'
) COMMENT '战略客户每月销额前端毛利'
partitioned by (smonth string comment '年月')
row format delimited
stored as parquet;
*/
--107852 客户2月添加，但201912以后有数据
--alter table csx_dw.sale_strategic_income_res drop partition(smm='202001');
--SELECT smm,count(1) from csx_dw.sale_strategic_income_res group by smm;

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table csx_dw.sale_strategic_income_res partition (smonth)
select dist,cust_id,customer_name,attribute,channel,sales_name,
sales_name_strat,confirmation,remarks,count_flag,cooperate_type,proportion,
sales_value,sap_profit,sap_prorate,fnl_profit,fnl_prorate,smonth
 from b2b_tmp.temp_strategic_cust_01;






-- 计算薪酬
drop table b2b_tmp.temp_strategic_res031;
create table b2b_tmp.temp_strategic_res031
as
select 
smonth,dist,cust_id,customer_name,attribute,channel,sales_name,
sales_name_strat,confirmation,remarks,count_flag,cooperate_type,proportion,
sales_value,sap_profit,sap_prorate,fnl_profit,fnl_prorate,
case when cooperate_type like '合伙人%' then sales_value*proportion
	 when cooperate_type like '代理商%' then sales_value*proportion
	 else 0
	 end salary_dl,
case when cooperate_type like '合伙人%' then sales_value*proportion*0.1
	 when cooperate_type like '代理商%' then if(fnl_profit<0,0,coalesce(fnl_profit,0)*0.15)
	 else sales_value*0.01+if(fnl_profit<0,0,coalesce(fnl_profit,0)*0.15)
	 end salary
from  csx_dw.sale_strategic_income_res
where smonth <='202002'                             --该行4月刷数添加
and cust_id not in('109575','103141','104501','109401','110602','110807','110881','PF0458') ;    --该行4月刷数添加，这些客户3月计算时添加

--#######################
--同一销售员下的客户当月薪酬合计后按照销额重新分配
drop table b2b_tmp.temp_strategic_res032;
CREATE temporary table b2b_tmp.temp_strategic_res032
as
select a.smonth,a.dist,a.cust_id,a.customer_name,a.attribute,a.channel,a.sales_name,
a.sales_name_strat,a.confirmation,a.remarks,a.count_flag,a.cooperate_type,a.proportion,
a.sales_value,a.sap_profit,a.sap_prorate,a.fnl_profit,a.fnl_prorate,
a.salary_dl,a.salary,
a.sales_value*if(b.salary>0,b.salary,0)/b.sales_value salery_cal
 from b2b_tmp.temp_strategic_res031 a 
join (select dist,sales_name_strat,smonth,sum(sales_value)sales_value,sum(salary)salary
 from b2b_tmp.temp_strategic_res031
group by dist,sales_name_strat,smonth)b on (a.dist=b.dist and a.sales_name_strat=b.sales_name_strat and a.smonth=b.smonth);




--02 回款
drop table b2b_tmp.temp_strategic_current02;
CREATE table b2b_tmp.temp_strategic_current02
as
select x.kunnr,x.in_value,x.in_amt
from 
(select kunnr,sum(amount)in_value,sum(coalesce(amount,0)+coalesce(ac_all,0))in_amt
from 
(select kunnr,sum(a.dmbtr)amount,0 ac_all from ods_ecc.ecc_ytbcustomer  a where hkont='1122010000' 
and sdt=regexp_replace(current_date,'-','') and a.budat>='20200201' and a.budat<'20200301' 
and (belnr like '01%'or belnr like '1%' or belnr like '009%')  and mandt='800' 
and (substr(kunnr,1,1) not in ('G','L','V','S') or kunnr='S9961')
group by kunnr
union all 
select kunnr,sum(a.dmbtr)amount,0 ac_all from ods_ecc.ecc_ytbcustomer  a where hkont='1122010000' 
and sdt=regexp_replace(current_date,'-','') and a.budat>='20191201' and a.budat<'20200201' 
and (belnr like '01%'or belnr like '1%' or belnr like '009%')  and mandt='800' 
and (substr(kunnr,1,1) not in ('G','L','V','S') or kunnr='S9961')
and kunnr like'%107852%'
group by kunnr
union all 
select kunnr,0 amount,sum(ac_all) ac_all from csx_dw.account_age_dtl_fct where sdt='20200131' and hkont='1122010000'
and (substr(kunnr,1,1) not in ('G','L','V','S') or kunnr='S9961') group by kunnr)a
where a.amount<0 or a.ac_all<0
and kunnr not like'%107852%'
group by kunnr)x;


--结果表1  客户各月销售额、毛利、奖金包，当月回款金额，历史回款覆盖金额
insert overwrite directory '/tmp/raoyanhua/zhanlueyeji02' row format delimited fields terminated by '\t' 
select a.smonth,a.dist,a.cust_id,a.customer_name,a.attribute,a.channel,a.sales_name,
a.sales_name_strat,a.confirmation,a.remarks,a.count_flag,a.cooperate_type,a.proportion,
a.sales_value,a.sap_profit,a.sap_prorate,a.fnl_profit,a.fnl_prorate,
a.salary_dl,a.salary,a.salery_cal,
b.in_value,b.in_amt,c.tax_value
from b2b_tmp.temp_strategic_res032 a
left join b2b_tmp.temp_strategic_current02 b on lpad(a.cust_id,10,'0')=lpad(b.kunnr,10,'0')
left join (select regexp_extract(cust_id, '(0|^)([^0].*)',2) cust_id,smonth,
max(tax_sale)tax_sale,sum(tax_value)tax_value --最大回款覆盖金额，销售额
from  csx_dw.sale_strategic_salary_cal where smm<'202002' group by regexp_extract(cust_id, '(0|^)([^0].*)',2),smonth) c 
on (lpad(a.cust_id,10,'0')=lpad(c.cust_id,10,'0') and a.smonth=c.smonth); 



-- 客户扣除福利单优先回款后剩余金额、各月剩余的未消销额、剩余(直送/福利)毛利、剩余奖金包
-- b表客户历史月销售额、回款覆盖金额
-- c表回款额
drop table b2b_tmp.temp_strategic_res003;
create table b2b_tmp.temp_strategic_res003
as
select a.smonth,a.dist,a.cust_id,a.customer_name cust_name,a.sales_name,a.sales_name_strat,
c.in_value,c.in_amt,--left_amt 
a.sales_value-coalesce(b.tax_value,0) tax_sale,
a.sap_profit*(1-coalesce(b.tax_value,0)/coalesce(b.tax_sale,1)) sap_profit,
a.fnl_profit*(1-coalesce(b.tax_value,0)/coalesce(b.tax_sale,1)) fnl_profit,
a.salery_cal*(1-coalesce(b.tax_value,0)/coalesce(b.tax_sale,1))salery_cal
from b2b_tmp.temp_strategic_res032  a 
left join b2b_tmp.temp_strategic_current02 c on lpad(a.cust_id,10,'0')=lpad(c.kunnr,10,'0')
left join (select regexp_extract(cust_id, '(0|^)([^0].*)',2) cust_id,smonth,
max(tax_sale)tax_sale,sum(tax_value)tax_value --最大回款覆盖金额，销售额
from  csx_dw.sale_strategic_salary_cal where smm<'202002' group by regexp_extract(cust_id, '(0|^)([^0].*)',2),smonth) b 
on (lpad(a.cust_id,10,'0')=lpad(b.cust_id,10,'0') and a.smonth=b.smonth); 


--客户回款在历史各月先出先回的覆盖金额
drop table b2b_tmp.temp_strategic_res031;
create table b2b_tmp.temp_strategic_res031
as
select dist,cust_id,cust_name,sales_name,sales_name_strat,
in_value,in_amt,smonth,tax_sale,
sap_profit,fnl_profit,salery_cal,
case when in_amt+sum_bq<=0 then tax_sale 
when in_amt+coalesce(sum_sq,0)<0  and in_amt+sum_bq>0 then -1*(in_amt+coalesce(sum_sq,0)) else 0 end tax_value
from 
(select a.*,
row_number() OVER(PARTITION BY cust_id ORDER BY smonth)rno,
sum(tax_sale)over(PARTITION BY cust_id order by smonth ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ) sum_sq,
sum(tax_sale)over(PARTITION BY cust_id order by smonth ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING )sum_bq
 from b2b_tmp.temp_strategic_res003 a where smonth<='202002' and tax_sale<>0)m;




-- 各月未消销额、奖金包 当月回款覆盖金额
drop table b2b_tmp.temp_strategic_res03;
CREATE table b2b_tmp.temp_strategic_res03
as
select x.dist,x.cust_id,x.cust_name,x.sales_name,x.sales_name_strat,
x.in_value,x.in_amt,x.smonth,sum(x.tax_sale)tax_sale,
sum(x.sap_profit) sap_profit,
sum(x.fnl_profit) fnl_profit,
sum(x.salery_cal)salery_cal,
sum(x.tax_value)tax_value,'202002' smm
from b2b_tmp.temp_strategic_res031 x
group by x.dist,x.cust_id,x.cust_name,x.sales_name,x.sales_name_strat,
x.in_value,x.in_amt,x.smonth;



 
/*
-- 新建表 战略客户计算薪酬
--drop table csx_dw.sale_strategic_salary_cal;
CREATE TABLE IF NOT EXISTS `csx_dw.sale_strategic_salary_cal` (
  `smonth` STRING COMMENT '年月',
  `dist` STRING COMMENT '省区',
  `cust_id` STRING COMMENT '客户编号',
  `cust_name` STRING COMMENT '客户名称',
  `sales_name` STRING COMMENT '业务员(系统)',
  `sales_name_strat` STRING COMMENT '业务员(战略)',
  `tax_sale` decimal(26,6) comment '未消销额',
  `in_value` decimal(26,6) comment '本月回款金额',  
  `in_amt` decimal(26,6) comment '本月回款+上月',
  `sap_profit` decimal(26,6) comment '毛利额', 
  `fnl_profit` decimal(26,6) comment '前台毛利',
  `salery_cal` decimal(26,6) comment '奖金包',  
  `tax_value` decimal(26,6) comment '回款覆盖金额'
) COMMENT '战略客户计算薪酬'
partitioned by (smm string comment '计算月份')
row format delimited
stored as parquet;
*/

--alter table csx_dw.sale_strategic_salary_cal drop partition(smm='202001');
--SELECT smm,count(1) from csx_dw.sale_strategic_salary_cal group by smm;

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table csx_dw.sale_strategic_salary_cal partition (smm)
select smonth,dist,cust_id,cust_name,sales_name,sales_name_strat,
tax_sale,in_value,in_amt,sap_profit,fnl_profit,salery_cal,tax_value,'202002' smm
 from b2b_tmp.temp_strategic_res03;

/*
insert overwrite table csx_dw.sale_strategic_salary_cal partition (smm)
select smonth,dist,cust_id,customer_name,sales_name,sales_name_strat,
sales_value,''in_value,''in_amt,sap_profit,fnl_profit,salery_cal,tax_value,'202001' smm
 from b2b_tmp.temp_strategic_res032;
*/




--当月提成
insert overwrite directory '/tmp/raoyanhua/zhanlueyeji03' row format delimited fields terminated by '\t' 
select 
smonth,dist,cust_id,cust_name,sales_name,sales_name_strat,
tax_sale,in_value,in_amt,
sap_profit,sap_profit/tax_sale sap_prorate,
fnl_profit,fnl_profit/tax_sale fnl_prorate,
salery_cal,
salery_cal*(tax_value/tax_sale) salery_cal_1,
tax_value
from  b2b_tmp.temp_strategic_res03;


/*
 drop table b2b_tmp.temp_111;
CREATE table b2b_tmp.temp_111
as
 select *
 from b2b_tmp.temp_strategic_res032; 
*/


