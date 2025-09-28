

--省区大区对应关系
drop table b2b_tmp.dist_region;
create table b2b_tmp.dist_region
as
select '1' dist_no,'北京市' dist,'华北' region
union all
select '2' dist_no,'上海市' dist,'华东' region
union all
select '6' dist_no,'河北省' dist,'华北' region
union all
select '10' dist_no,'江苏省' dist,'华东' region
union all
select '11' dist_no,'安徽省' dist,'华东' region
union all
select '13' dist_no,'浙江省' dist,'华东' region
union all
select '15' dist_no,'福建省' dist,'华南' region
union all
select '20' dist_no,'广东省' dist,'华南' region
union all
select '23' dist_no,'贵州省' dist,'华西' region
union all
select '24' dist_no,'四川省' dist,'华西' region
union all
select '26' dist_no,'陕西省' dist,'华北' region 
union all
select '32' dist_no,'重庆市' dist,'华西' region
union all
select '34' dist_no,'平台-大宗' dist,'大宗' region
union all
select '35' dist_no,'平台-生鲜采购' dist,'供应链' region
union all
select '36' dist_no,'平台-食百采购' dist,'供应链' region;



-- 新建表 顺序
--drop table b2b_tmp.caibao_yue;
CREATE TABLE IF NOT EXISTS `b2b_tmp.caibao_yue` (
  `NO_1` STRING COMMENT '模块编号',
  `NO2_lable_detail` STRING COMMENT '模块内容编号',  
  `lable1` STRING COMMENT '模块',
  `lable_detail` STRING COMMENT '模块内容'
) COMMENT '财报标题顺序_月'
ROW format delimited fields terminated by ','
STORED AS TEXTFILE;

--load data inpath '/tmp/raoyanhua/aa.csv' overwrite into table b2b_tmp.caibao_yue;
--select * from b2b_tmp.caibao_yue;


-- 销售额
drop table b2b_tmp.region_dist_sales;
create temporary table b2b_tmp.region_dist_sales
as
select '01'NO_1,'销售额' lable,b.region,
case when a.channel_name like '大客户%' then 'B端' 
	when a.channel_name like '商超%' then 'M端' 
	when a.channel_name like '企业购%' then 'BBC' 
	else '其他' end lable01,
a.smonth,province_code,province_name,sum(a.sales_value) lable_value
from (
select substr(sdt,1,6) smonth,
province_code,province_name,channel_name,sum(excluding_tax_sales)/10000 sales_value
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200301'
and sdt<'20200501'
and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
group by substr(sdt,1,6),
province_code,province_name,channel_name )a
left join b2b_tmp.dist_region b on b.dist_no=a.province_code
group by b.region,
case when a.channel_name like '大客户%' then 'B端' 
	when a.channel_name like '商超%' then 'M端' 
	when a.channel_name like '企业购%' then 'BBC' 
	else '其他' end,
province_code,province_name,a.smonth;


--销售额占比
drop table b2b_tmp.region_dist_sales_rate01;
create temporary table b2b_tmp.region_dist_sales_rate01
as
select '01'NO_1,'销售额占比' lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth,a.lable_value/b.lable_value lable_value
from b2b_tmp.region_dist_sales a
left join 
(select '01'NO_1,'销售额' lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales --销售额
group by region,province_code,province_name,smonth
) b on b.region=a.region and b.province_code=a.province_code and b.smonth=a.smonth;



--定价毛利 (订单毛利)
drop table b2b_tmp.region_dist_profit01;
create temporary table b2b_tmp.region_dist_profit01
as
select '03'NO_1,'定价毛利' lable,b.region,
case when a.channel_name like '大客户%' then 'B端'
 	when a.channel_name like '商超%' then 'M端' 
	when a.channel_name like '企业购%' then 'BBC'
 	else '其他' end lable01,
province_code,province_name,
a.smonth,sum(a.profit) lable_value
from (
select substr(sdt,1,6) smonth,
province_code,province_name,channel_name,sum(excluding_tax_profit)/10000 profit
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200301'
and sdt<'20200501'
and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
group by substr(sdt,1,6),
province_code,province_name,channel_name )a
left join b2b_tmp.dist_region b on b.dist_no=a.province_code
group by b.region,
case when a.channel_name like '大客户%' then 'B端'
 	when a.channel_name like '商超%' then 'M端' 
	when a.channel_name like '企业购%' then 'BBC'
 	else '其他' end,
province_code,province_name,a.smonth;



--定价毛利率
drop table b2b_tmp.region_dist_prorate01;
create temporary table b2b_tmp.region_dist_prorate01
as
select '04'NO_1,'定价毛利率' lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth,b.lable_value/a.lable_value lable_value
from b2b_tmp.region_dist_sales a
left join b2b_tmp.region_dist_profit01 b on b.region=a.region and b.province_code=a.province_code and b.lable01=a.lable01 and b.smonth=a.smonth;



--定价成本 
drop table b2b_tmp.region_dist_cost01;
create temporary table b2b_tmp.region_dist_cost01
as
select '02'NO_1,'定价成本' lable,a.region,a.lable01,
a.province_code,a.province_name,a.smonth,(a.lable_value-b.lable_value) lable_value
from b2b_tmp.region_dist_sales a
left join b2b_tmp.region_dist_profit01 b on b.region=a.region and b.province_code=a.province_code and b.lable01=a.lable01 and b.smonth=a.smonth;


-- 总客户
-- 客户属性-人数（成交客户）
drop table b2b_tmp.region_dist_cust_counts02;
create temporary table b2b_tmp.region_dist_cust_counts02
as
select '06'NO_1,'总客户-成交客户人数' lable,b.region,attribute lable01,a.province_code,a.province_name,
a.smonth,count(distinct a.customer_no) lable_value
from 
(select a.*,
case when b.attribute is null then '日配客户' else attribute end attribute
from 
(select substr(sdt,1,6) smonth,
province_code,province_name,customer_no,sum(excluding_tax_sales) sales_value
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200301'
and sdt<'20200501'
and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
and channel in('1','7')
group by substr(sdt,1,6),
province_code,province_name,customer_no)a
left join 
(select customer_no,
case when attribute is null and channel_code='1' then '日配客户'
	when attribute is null and channel_code<>'1'then '商贸批发和其他'
	when attribute not in('日配客户','福利客户','贸易客户') then '商贸批发和其他' 
	else attribute end attribute
  from csx_dw.dws_crm_w_a_customer_m
  where sdt = regexp_replace(date_sub(current_date, 2), '-', '')
  and customer_no<>''
  and channel_code in('1','7'))b on b.customer_no=a.customer_no )a
left join b2b_tmp.dist_region b on b.dist_no=a.province_code
group by b.region,attribute,a.province_code,a.province_name,a.smonth;


-- 客户属性-销售额（成交客户）
drop table b2b_tmp.region_dist_cust_sales;
create temporary table b2b_tmp.region_dist_cust_sales
as
select '07'NO_1,'总客户-成交客户销售额' lable,b.region,attribute lable01,a.province_code,a.province_name,
a.smonth,sum(a.sales_value) lable_value
from 
(select a.*,
case when b.attribute is null then '日配客户' else attribute end attribute
from 
(select substr(sdt,1,6) smonth,
province_code,province_name,customer_no,sum(excluding_tax_sales)/10000 sales_value
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200301'
and sdt<'20200501'
and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
and channel in('1','7')
group by substr(sdt,1,6),
province_code,province_name,customer_no)a
left join 
(select customer_no,
case when attribute is null and channel_code='1' then '日配客户'
	when attribute is null and channel_code<>'1'then '商贸批发和其他'
	when attribute not in('日配客户','福利客户','贸易客户') then '商贸批发和其他' 
	else attribute end attribute
  from csx_dw.dws_crm_w_a_customer_m
  where sdt = regexp_replace(date_sub(current_date, 2), '-', '')
  and customer_no<>''
  and channel_code in('1','7'))b on b.customer_no=a.customer_no )a
left join b2b_tmp.dist_region b on b.dist_no=a.province_code
group by b.region,attribute,a.province_code,a.province_name,a.smonth;


/*
-- 01客户最小成交日期 、首单日期、首单金额 首单--首日
drop table b2b_tmp.tmp_cust_min_sale;
create table b2b_tmp.tmp_cust_min_sale
as 
select
customer_no,min(sales_date) as min_sales_date,max(sales_date) as max_sales_date,count(distinct sales_date) as count_day
from csx_dw.sale_item_m
where sdt<'20200501'
and sales_type in ('qyg','gc','anhui','sc','bbc')
group by customer_no;

--CRM签约客户数
drop table b2b_tmp.tmp_cust_count_info;
create table b2b_tmp.tmp_cust_count_info
as 
select c.region,coalesce(a.province_code,c.dist_no)province_code,
coalesce(a.province_name,c.dist) province_name,
a.customer_no,a.sign_time,b.min_sales_date
from
(select a.customer_no,a.channel_code,a.sign_time,a.province_code,a.province_name
from
(select customer_no,channel_code,regexp_replace(split(sign_time, ' ')[0], '-', '') sign_time,
case when sales_province ='成都省' then '24' 
	 when (province_code='16' and channel_code='1') then '15' 
	 when customer_no='102268' then '15'
	 else sales_province_code end province_code,

case when sales_province ='成都省' then '四川省' 
	 when (province_code='16' and channel_code='1') then '福建省' 
	 when customer_no='102268' then '福建省'	 
	 when province_name='BBC' then '福建省' 	 
	 else sales_province end province_name
  from csx_dw.dws_crm_w_a_customer_m
  where sdt = '20200430'
  and customer_no<>''
  and channel_code in('1','7'))a
)a
left join b2b_tmp.tmp_cust_min_sale b on a.customer_no=b.customer_no
left join b2b_tmp.dist_region c on c.dist=a.province_name;
*/


 
-- 客户状态-人数
drop table b2b_tmp.region_dist_cust_counts01;
create temporary table b2b_tmp.region_dist_cust_counts01
as
select '05'NO_1,'总客户-客户数' lable,region,'成交客户'lable01,
province_code,province_name,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts02
group by region,province_code,province_name,smonth
union all
select '05'NO_1,'总客户-客户数' lable,b.region,a.lable01,a.province_code,a.province_name,
a.smonth,sum(a.lable_value) lable_value
from (
select substr(regexp_replace(add_months(trunc(date_sub(current_date,0),'MM'),-1),'-',''),1,6) smonth,
province_code,province_name,'总客户'lable01,count(distinct customer_no) as lable_value 
from b2b_tmp.tmp_cust_count_info
group by province_code,province_name
union all
select substr(sign_time,1,6) smonth,
province_code,province_name,'新签约客户'lable01,count(distinct customer_no) as lable_value 
from b2b_tmp.tmp_cust_count_info
where sign_time>='20200301'
and sign_time<'20200501'
group by substr(sign_time,1,6) ,
province_code,province_name
union all
select substr(min_sales_date,1,6) smonth,
province_code,province_name,'新成交客户'lable01,count(distinct customer_no) as lable_value 
from b2b_tmp.tmp_cust_count_info
where min_sales_date>='20200301'
and min_sales_date<'20200501'
group by substr(min_sales_date,1,6),
province_code,province_name
)a
left join b2b_tmp.dist_region b on b.dist=a.province_name
group by b.region,a.lable01,a.province_code,a.province_name,a.smonth;


--总客户-客户属性-人数占比
drop table b2b_tmp.region_dist_cust_counts02_rate;
create temporary table b2b_tmp.region_dist_cust_counts02_rate
as
select '06'NO_1,'总客户-成交客户人数占比' lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth,a.lable_value/b.lable_value lable_value
from b2b_tmp.region_dist_cust_counts02 a
left join 
(select '05'NO_1,'成交客户' lable,region,'z小计'lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_counts01  -- 客户状态-人数
where lable01='成交客户'
) b on b.region=a.region and b.province_code=a.province_code and b.smonth=a.smonth;


--总客户-客户属性-金额占比
drop table b2b_tmp.region_dist_cust_sales_rate;
create temporary table b2b_tmp.region_dist_cust_sales_rate
as
select '06'NO_1,'总客户-成交客户销售额占比' lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth,a.lable_value/b.lable_value lable_value
from b2b_tmp.region_dist_cust_sales a
left join 
(select '05'NO_1,'成交客户' lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales  -- B客户销售额
where lable01 in('B端','BBC')
group by region,province_code,province_name,smonth
) b on b.region=a.region and b.province_code=a.province_code and b.smonth=a.smonth;


-- 结果表-拼接
drop table b2b_tmp.region_dist_result01;
create table b2b_tmp.region_dist_result01
as
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_sales  --销售额
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_sales_rate01  --销售额占比
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cost01  --定价成本
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_profit01  --定价毛利
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_prorate01  --定价毛利率
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_counts01  --总客户-客户数
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_counts02  --总客户-成交客户人数
union all                    
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_sales      --总客户-成交客户销售额
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_counts02_rate  --总客户-成交客户-客户属性-人数占比
union all                    
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_sales_rate      --总客户-成交客户-客户属性-金额占比
union all     
select '01'NO_1,'销售额' lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales --销售额-z小计
group by region,province_code,province_name,smonth
union all
select '02'NO_1,'定价成本'lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_cost01  --定价成本-z小计
group by region,province_code,province_name,smonth
union all
select '02'NO_1,'定价毛利'lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_profit01  --定价毛利-z小计
group by region,province_code,province_name,smonth
union all                           --定价毛利率-z小计
select '04'NO_1,'定价毛利率'lable,a.region,'z小计'lable01,a.province_code,a.province_name,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_profit01  --定价毛利
group by region,province_code,province_name,smonth) a
left join 
(select region,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales  --销售额
group by region,province_code,province_name,smonth
) b on b.region=a.region and b.province_code=a.province_code and b.smonth=a.smonth;


-- 结果表-客户类型 
drop table b2b_tmp.region_dist_result02;
create temporary table b2b_tmp.region_dist_result02
as
select b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1 NO_01,a.lable,a.region,a.lable01,
a.province_code,a.province_name,
a.smonth,sum(a.lable_value)lable_value
from b2b_tmp.region_dist_result01 a
left join b2b_tmp.caibao_yue b on a.lable=b.lable1 and a.lable01=b.lable_detail
group by b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1,a.lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth;


-- 结果表-省区 
drop table b2b_tmp.region_dist_result03;
create table b2b_tmp.region_dist_result03
as
select coalesce(a.no_1,b.no_1),coalesce(a.no2_lable_detail,b.no2_lable_detail),coalesce(a.lable1,b.lable1),
coalesce(a.lable_detail,b.lable_detail),coalesce(a.no_01,b.no_01),coalesce(a.lable,b.lable),coalesce(a.region,b.region),coalesce(a.lable01,b.lable01),
coalesce(a.province_code,b.province_code),coalesce(a.province_name,b.province_name),coalesce(a.smonth,b.smonth),
a.lable_value,b.lable_value lable_value_0,
case when coalesce(a.no_1,b.no_1)='4' then a.lable_value-b.lable_value
	when coalesce(a.no2_lable_detail,b.no2_lable_detail) like '9%' then a.lable_value-b.lable_value
	else (a.lable_value-b.lable_value)/b.lable_value end rate
from b2b_tmp.region_dist_result02 a
full join 
(select no_1,no2_lable_detail,lable1,lable_detail,no_01,lable,region,lable01,
province_code,province_name,
substr(regexp_replace(add_months(to_date(from_unixtime(unix_timestamp(concat(smonth,'01'),'yyyyMMdd'))),1),'-',''),1,6) smonth,
lable_value
from b2b_tmp.region_dist_result02
where smonth<'202004' 
)b on a.no_1=b.no_1 and a.no2_lable_detail=b.no2_lable_detail 
and a.province_code=b.province_code and a.province_name=b.province_name
and a.region=b.region and a.smonth=b.smonth;


insert overwrite directory '/tmp/raoyanhua/caibao_yue01' row format delimited fields terminated by '\t' 
select * from b2b_tmp.region_dist_result03;

------------------------------------------------------------------------------------------------
--------------- 大区
/*
--销售额占比
drop table b2b_tmp.region_sales_rate01;
create temporary table b2b_tmp.region_sales_rate01
as
select '01'NO_1,'销售额占比' lable,a.region,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_sales
group by region,lable01,smonth) a
left join 
(select '01'NO_1,'销售额' lable,region,'z小计'lable01,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales --销售额
group by region,smonth
) b on b.region=a.region and b.smonth=a.smonth;

--定价毛利率
drop table b2b_tmp.region_prorate01;
create temporary table b2b_tmp.region_prorate01
as
select '04'NO_1,'定价毛利率' lable,a.region,a.lable01,a.smonth,b.lable_value/a.lable_value lable_value
from 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_sales
group by region,lable01,smonth) a
left join 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_profit01
group by region,lable01,smonth) b on b.region=a.region and b.lable01=a.lable01 and b.smonth=a.smonth;


--总客户-客户属性-人数占比
drop table b2b_tmp.region_cust_counts02_rate;
create temporary table b2b_tmp.region_cust_counts02_rate
as
select '06'NO_1,'总客户-成交客户人数占比' lable,a.region,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts02
group by region,lable01,smonth )a
left join 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts01  -- 客户状态-人数
where lable01='成交客户'
group by region,lable01,smonth
) b on b.region=a.region and b.smonth=a.smonth;



--总客户-客户属性-金额占比
drop table b2b_tmp.region_cust_sales_rate;
create temporary table b2b_tmp.region_cust_sales_rate
as
select '06'NO_1,'总客户-成交客户销售额占比' lable,a.region,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_sales
group by region,lable01,smonth ) a
left join 
(select '05'NO_1,'成交客户' lable,region,'z小计'lable01,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales  -- B客户销售额
where lable01 in('B端','BBC')
group by region,smonth
) b on b.region=a.region and b.smonth=a.smonth;


-- 结果表-拼接
drop table b2b_tmp.region_result01;
create table b2b_tmp.region_result01
as
select NO_1,lable,region,lable01,smonth,sum(lable_value) lable_value
from b2b_tmp.region_dist_result01
where NO_1<>'4' 
and lable not like'%率%'
and lable not like'%占比%'
group by NO_1,lable,region,lable01,smonth
union all  
select NO_1,lable,region,lable01,smonth,lable_value
from b2b_tmp.region_sales_rate01  --销售额占比
union all  
select NO_1,lable,region,lable01,smonth,lable_value
from b2b_tmp.region_prorate01  --定价毛利率
union all  
select NO_1,lable,region,lable01,smonth,lable_value
from b2b_tmp.region_cust_counts02_rate  --总客户-客户属性-人数占比
union all  
select NO_1,lable,region,lable01,smonth,lable_value
from b2b_tmp.region_cust_sales_rate  --总客户-客户属性-金额占比
union all                           --定价毛利率-z小计
select '04'NO_1,'定价毛利率'lable,a.region,'z小计'lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_profit01  --定价毛利
group by region,smonth) a
left join 
(select region,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales  --销售额
group by region,smonth
) b on b.region=a.region and b.smonth=a.smonth;

-- 结果表-客户类型 
drop table b2b_tmp.region_result02;
create temporary table b2b_tmp.region_result02
as
select b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1 NO_01,a.lable,a.region,a.lable01,
a.smonth,sum(a.lable_value)lable_value
from b2b_tmp.region_result01 a
left join b2b_tmp.caibao_yue b on a.lable=b.lable1 and a.lable01=b.lable_detail
group by b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1,a.lable,a.region,a.lable01,a.smonth;


-- 结果表-大区 
drop table b2b_tmp.region_result03;
create table b2b_tmp.region_result03
as
select coalesce(a.no_1,b.no_1),coalesce(a.no2_lable_detail,b.no2_lable_detail),coalesce(a.lable1,b.lable1),
coalesce(a.lable_detail,b.lable_detail),coalesce(a.no_01,b.no_01),coalesce(a.lable,b.lable),coalesce(a.region,b.region),coalesce(a.lable01,b.lable01),
coalesce(a.smonth,b.smonth),
a.lable_value,b.lable_value lable_value_0,
case when coalesce(a.no_1,b.no_1)='4' then a.lable_value-b.lable_value
	when coalesce(a.no2_lable_detail,b.no2_lable_detail) like '9%' then a.lable_value-b.lable_value
	else (a.lable_value-b.lable_value)/b.lable_value end rate
from b2b_tmp.region_result02 a
full join 
(select no_1,no2_lable_detail,lable1,lable_detail,no_01,lable,region,lable01,
substr(regexp_replace(add_months(to_date(from_unixtime(unix_timestamp(concat(smonth,'01'),'yyyyMMdd'))),1),'-',''),1,6) smonth,
lable_value
from b2b_tmp.region_result02
where smonth<'202004' 
)b on a.no_1=b.no_1 and a.no2_lable_detail=b.no2_lable_detail 
and a.region=b.region and a.smonth=b.smonth;


insert overwrite directory '/tmp/raoyanhua/caibao_yue02' row format delimited fields terminated by '\t' 
select * from b2b_tmp.region_result03;
*/

------------------------------------------------------------------------------------------------
--------------- 全国

--销售额占比
drop table b2b_tmp.all_sales_rate01;
create temporary table b2b_tmp.all_sales_rate01
as
select '01'NO_1,'销售额占比' lable,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_sales
where region in('华南','华北','华西','华东')
group by lable01,smonth) a
left join 
(select '01'NO_1,'销售额' lable,'z小计'lable01,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales --销售额
where region in('华南','华北','华西','华东')
group by smonth
) b on b.smonth=a.smonth;

--定价毛利率
drop table b2b_tmp.region_prorate01;
create temporary table b2b_tmp.region_prorate01
as
select '04'NO_1,'定价毛利率' lable,a.lable01,a.smonth,b.lable_value/a.lable_value lable_value
from 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_sales
where region in('华南','华北','华西','华东')
group by lable01,smonth) a
left join 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_profit01
where region in('华南','华北','华西','华东')
group by lable01,smonth) b on b.lable01=a.lable01 and b.smonth=a.smonth;


--总客户-客户属性-人数占比
drop table b2b_tmp.region_cust_counts02_rate;
create temporary table b2b_tmp.region_cust_counts02_rate
as
select '06'NO_1,'总客户-成交客户人数占比' lable,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts02
where region in('华南','华北','华西','华东')
group by lable01,smonth )a
left join 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts01  -- 客户状态-人数
where lable01='成交客户'
and region in('华南','华北','华西','华东')
group by lable01,smonth
) b on b.smonth=a.smonth;



--总客户-客户属性-金额占比
drop table b2b_tmp.region_cust_sales_rate;
create temporary table b2b_tmp.region_cust_sales_rate
as
select '06'NO_1,'总客户-成交客户销售额占比' lable,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_sales
where region in('华南','华北','华西','华东')
group by lable01,smonth ) a
left join 
(select '05'NO_1,'成交客户' lable,'z小计'lable01,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales  -- B客户销售额
where region in('华南','华北','华西','华东')
and lable01 in('B端','BBC')
group by smonth
) b on b.smonth=a.smonth;


-- 结果表-拼接
drop table b2b_tmp.region_result01;
create table b2b_tmp.region_result01
as
select NO_1,lable,lable01,smonth,sum(lable_value) lable_value
from b2b_tmp.region_dist_result01
where NO_1<>'4' 
and lable not like'%率%'
and lable not like'%占比%'
and region in('华南','华北','华西','华东')
group by NO_1,lable,lable01,smonth
union all  
select NO_1,lable,lable01,smonth,lable_value
from b2b_tmp.all_sales_rate01  --销售额占比
union all  
select NO_1,lable,lable01,smonth,lable_value
from b2b_tmp.region_prorate01  --定价毛利率
union all  
select NO_1,lable,lable01,smonth,lable_value
from b2b_tmp.region_cust_counts02_rate  --总客户-客户属性-人数占比
union all  
select NO_1,lable,lable01,smonth,lable_value
from b2b_tmp.region_cust_sales_rate  --总客户-客户属性-金额占比
union all                           --定价毛利率-z小计
select '04'NO_1,'定价毛利率'lable,'z小计'lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_profit01  --定价毛利
where region in('华南','华北','华西','华东')
group by smonth) a
left join 
(select smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_sales  --销售额
where region in('华南','华北','华西','华东')
group by smonth
) b on b.smonth=a.smonth;

-- 结果表-客户类型 
drop table b2b_tmp.region_result02;
create temporary table b2b_tmp.region_result02
as
select b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1 NO_01,a.lable,a.lable01,
a.smonth,sum(a.lable_value)lable_value
from b2b_tmp.region_result01 a
left join b2b_tmp.caibao_yue b on a.lable=b.lable1 and a.lable01=b.lable_detail
group by b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1,a.lable,a.lable01,a.smonth;


-- 结果表-全国 
drop table b2b_tmp.region_result03;
create table b2b_tmp.region_result03
as
select coalesce(a.no_1,b.no_1),coalesce(a.no2_lable_detail,b.no2_lable_detail),coalesce(a.lable1,b.lable1),
coalesce(a.lable_detail,b.lable_detail),coalesce(a.no_01,b.no_01),coalesce(a.lable,b.lable),coalesce(a.lable01,b.lable01),
coalesce(a.smonth,b.smonth),
a.lable_value,b.lable_value lable_value_0,
case when coalesce(a.no_1,b.no_1)='4' then a.lable_value-b.lable_value
	when coalesce(a.no2_lable_detail,b.no2_lable_detail) like '9%' then a.lable_value-b.lable_value
	else (a.lable_value-b.lable_value)/b.lable_value end rate
from b2b_tmp.region_result02 a
full join 
(select no_1,no2_lable_detail,lable1,lable_detail,no_01,lable,lable01,
substr(regexp_replace(add_months(to_date(from_unixtime(unix_timestamp(concat(smonth,'01'),'yyyyMMdd'))),1),'-',''),1,6) smonth,
lable_value
from b2b_tmp.region_result02
where smonth<'202004' 
)b on a.no_1=b.no_1 and a.no2_lable_detail=b.no2_lable_detail 
and a.smonth=b.smonth;


insert overwrite directory '/tmp/raoyanhua/caibao_yue03' row format delimited fields terminated by '\t' 
select * from b2b_tmp.region_result03;



