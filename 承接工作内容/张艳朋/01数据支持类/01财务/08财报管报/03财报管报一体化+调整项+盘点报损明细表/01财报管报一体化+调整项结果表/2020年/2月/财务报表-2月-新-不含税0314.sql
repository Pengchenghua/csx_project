

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
select '26' dist_no,'陕西省' dist,'华北' region --手动加的
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







--数据源，处理省区、渠道信息
drop table b2b_tmp.cust_sales_m;
create table b2b_tmp.cust_sales_m
as
select 
  a.AA,a.sdt,coalesce(b.channel,a.channel) channel,a.sales_type,a.dc_code,a.origin_shop_id,b.supervisor_id,
  a.division_code,a.customer_no,b.customer_name,a.sales_value,excluding_tax_sales,a.sales_cost,excluding_tax_cost,
  a.profit,excluding_tax_profit,b.sales_province,sales_city,sign_company_code,new_or_old,is_factory_goods_name,
  b.attribute,b.first_category,b.second_category,b.third_category,b.sales_name,b.work_no,
  a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name
from
(
  select 
    AA,credential_no,a.goods_code,goods_name,order_no,department_code,department_name,sdt,
    channel,sales_type,dc_code,origin_shop_id,division_code,customer_no,sales_value,
    excluding_tax_sales,sales_cost,excluding_tax_cost,profit,excluding_tax_profit,
    sign_company_code,new_or_old,if(c.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name
  from
  (
    select 
      '成本'AA,credential_no,product_code goods_code,product_name goods_name,source_order_no order_no,
      purchase_group_code department_code,purchase_group_name department_name,
      regexp_replace(split(create_time, ' ')[0], '-', '') as sdt,
      case when source_sys='1' then '大客户' when source_sys='2' then '企业购'
        when source_sys='3' then '商超' else '其他' end channel,
      case when source_sys='2' then 'bbc' end sales_type,
      location_code dc_code,  
      '' origin_shop_id,
      root_category	division_code, 
      customer_no,
      sale_amt sales_value,
      sale_amt_no_tax as excluding_tax_sales,
      cost_amt sales_cost,
      cost_amt_no_tax as excluding_tax_cost,
      profit_amt profit,
      profit_amt_no_tax as excluding_tax_profit,
      sign_company_code,
      '新系统' as new_or_old
    from csx_dw.dwd_sync_r_d_data_relation_cas_sale_detail
    where create_time>='2020-02-01' and create_time<'2020-03-01'
  ) a left join
  (
    select shop_id,province_code from csx_dw.shop_m where sdt = 'current'
  ) b on a.dc_code = b.shop_id
  left outer join
  (
    select
      workshop_code, province_code, goods_code
    from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
    where sdt='current' and new_or_old=1
  )c on b.province_code=c.province_code and a.goods_code=c.goods_code
  union all
  select 
    '销售'AA,''credential_no,goods_code,goods_name,order_no,department_code,department_name,
    sdt,
    channel_name as channel,
    '' sales_type,
    dc_code,
    sap_origin_dc_code as origin_shop_id,
    division_code,
    customer_no,
    sales_value,
    excluding_tax_sales,
    sales_cost,
    excluding_tax_cost,
    profit,
    excluding_tax_profit,
    '' as sign_company_code,
    '老系统' as new_or_old,
    is_factory_goods_name
  from csx_dw.dws_sale_r_d_customer_sale
  where sdt>='20200201' and sdt<'20200301' and sales_type in ('sapqyg','sapgc') 
)a left join 
(
  select
    customer_no, customer_name, attribute, channel, sales_id, sales_name, work_no, first_supervisor_name,
    second_supervisor_name, third_supervisor_name, fourth_supervisor_name, sales_province, sales_city,
    first_category, second_category, third_category, supervisor_id
  from
  (
    select
      customer_no, customer_name, attribute, channel, sales_id, sales_name, work_no, first_supervisor_name,
      second_supervisor_name, third_supervisor_name, fourth_supervisor_name, sales_province, sales_city,
      first_category, second_category, third_category, first_supervisor_code as supervisor_id,
      row_number()over(partition by customer_no order by sales_province desc) ranks
    from csx_dw.dws_crm_w_a_customer_m
    where sdt >= '20200101' and customer_no <> '' and customer_no is not null
  )a where ranks = 1 
)b on a.customer_no=b.customer_no;


drop table b2b_tmp.cust_sales_m1;
create table b2b_tmp.cust_sales_m1
as
select
  AA,sdt,shop_id,customer_no,customer_name,sales_value,excluding_tax_sales,sales_cost,excluding_tax_cost,profit,supervisor_id,
  excluding_tax_profit,channel,sales_type,attribute,first_category,second_category,third_category,sales_name,work_no,
  credential_no,goods_code,goods_name,order_no,department_code,department_name,sign_company_code,new_or_old,is_factory_goods_name,
  category_code,category_name,
  case when channel = '1'  OR channel = '' then '大客户'
    when channel = '2' then '商超'
    when channel = '3' then '商超(对外)'
    when channel = '4' then '大宗'
    when channel = '5' then '供应链(食百)'
    when channel = '6' then '供应链(生鲜)'
    when channel = '7' then '企业购 '
    when channel = '8' then '其他' end as channel_name,
  case when channel = '5' then '平台-食百采购'
    when channel = '6' then '平台-生鲜采购'
    when channel = '4' then '平台-大宗' else a.province_name end as province_name,
  case when channel like '商超%' then a.city_name else coalesce(a.sales_city, '-') end as city_name
from
(
  select
    a.AA,a.sdt,a.shop_id,a.customer_no,customer_name,sales_value,excluding_tax_sales,sales_cost,excluding_tax_cost,profit,excluding_tax_profit,
    a.attribute,a.first_category,a.second_category,third_category,a.sales_name,a.work_no,supervisor_id,is_factory_goods_name,
    a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name,sign_company_code,new_or_old,
    a.category_code,c.category_name,
    case when  a.shop_id like 'E%' then '2'
      when a.origin_shop_id = 'W0B6' or a.channel like '%企业购%' or a.sales_type='bbc' then '7'
      when (a.shop_id = 'W0H4' and a.customer_no like 'S%' and a.category_code in ('12','13','14') ) 
        or (a.channel like '供应链%' and a.category_code in ('12','13','14'))then '5'
      when (a.shop_id = 'W0H4' and a.customer_no like 'S%' and a.category_code in ('10','11'))
        or (a.channel like '供应链%' and a.category_code in ('10', '11'))then '6'  
      when a.channel = '大客户' or a.channel = 'B端' then '1'
      when a.channel ='M端'  or a.channel like '%对内%' or a.channel='商超' then '2'
      when a.channel like '%对外%' then '3'
      when a.channel = '大宗' then '4'  
      when a.channel='其他' then '8' else '' end as channel,
    sales_type,
    case when a.shop_id in ('W0M1','W0M4','W0J6','W0M6') then '商超平台' 
      when a.customer_no is not null and a.sales_province='BBC' then '福建省'
      when a.sales_province is not null and a.channel <> 'M端' and a.channel not like '商超%' then a.sales_province
      when a.customer_no in('SW055','W055')  then '上海市'
      else d.province_name end as province_name,
    sales_city, city_name
  from 
  (
    select 
      AA,sdt,channel,sales_type,dc_code shop_id,origin_shop_id,division_code as category_code,customer_no,customer_name,
      sales_value,excluding_tax_sales,sales_cost,excluding_tax_cost,profit,excluding_tax_profit,sales_province,sales_city,
      attribute,first_category,second_category,third_category,sales_name,work_no,supervisor_id,is_factory_goods_name,
      credential_no,goods_code,goods_name,order_no,department_code,department_name,sign_company_code,new_or_old,
      case when dc_code like 'E%' then concat('9',substr(dc_code,2,3)) else dc_code end shop_no 
    from b2b_tmp.cust_sales_m 
  ) a left outer join
  (
    select
      shop_id,
      shop_name,
      province_name,
      city_name
    from csx_dw.shop_m
    where sdt = 'current' 
  ) d on a.shop_no = d.shop_id 
  left outer join
  (
    select distinct division_code,division_name as category_name from csx_dw.goods_m where sdt = 'current'
  ) c on a.category_code = c.division_code
)a;


drop table b2b_tmp.cust_sales_m2;
create table b2b_tmp.cust_sales_m2
as
select 
  sign_company_code,
  if(sign_company_code not in ('2115','2116','2126','2207','2210','2211','2216','2304','2408','2814','2815','3505','3506','3750','3751','8030','2127','2128','2129','2130')
    and sign_company_code <> '', 0.01*sales_value, '') as channel_value,
  if(sign_company_code not in ('2115','2116','2126','2207','2210','2211','2216','2304','2408','2814','2815','3505','3506','3750','3751','8030','2127','2128','2129','2130')
    and sign_company_code <> '', 0.99*sales_value, '') as caiwu_sales_value,
  if(sign_company_code not in ('2115','2116','2126','2207','2210','2211','2216','2304','2408','2814','2815','3505','3506','3750','3751','8030','2127','2128','2129','2130')
    and sign_company_code <> '', 0.01*excluding_tax_sales, '') as excluding_tax_channel_value,
  if(sign_company_code not in ('2115','2116','2126','2207','2210','2211','2216','2304','2408','2814','2815','3505','3506','3750','3751','8030','2127','2128','2129','2130')
    and sign_company_code <> '', 0.99*excluding_tax_sales, '') as excluding_tax_caiwu_sales_value,
  new_or_old,a.AA,a.sdt,a.shop_id,a.customer_no,customer_name,is_factory_goods_name,category_code,category_name,
  sales_value,excluding_tax_sales,sales_cost,excluding_tax_cost,profit,excluding_tax_profit,
  a.attribute,a.first_category,a.second_category,third_category,a.sales_name,a.work_no,
  a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name,
  a.channel,a.sales_type,a.channel_name,
  case when a.province_name='商超平台'  then '-100' else g.province_code end province_code,
  case when a.province_name='平台-B' then '大客户平台' else a.province_name end province_name,
  case when a.province_name = '福建省' then coalesce(b.city_real,'福州、宁德、三明')  
    when a.province_name = '江苏省' then coalesce(b.city_real,'苏州')  
    when a.province_name = '浙江省' and  supervisor_id='1000000211087' then '宁波'
    when a.province_name = '浙江省' and  supervisor_id<>'1000000211087' then '杭州' else '-' end city_real
from
(
  select
    AA,sdt,shop_id,customer_no,customer_name,sales_value,excluding_tax_sales,sales_cost,excluding_tax_cost,profit,excluding_tax_profit,
    attribute,first_category,second_category,third_category,sales_name,work_no,supervisor_id,is_factory_goods_name,
    credential_no,goods_code,goods_name,order_no,category_code,category_name,department_code,department_name,sign_company_code,new_or_old,
    case when channel is null or channel='' then '1' when province_name='平台-B' and channel='1' then '1' else channel end channel,sales_type,
    case when channel is null or channel='' then '大客户' when province_name='平台-B' and channel='1' then '大客户' else channel_name end channel_name,
    case when province_name ='成都省' then '四川省'   else province_name end province_name, city_name
  from b2b_tmp.cust_sales_m1
)a left outer join 
(
  select
    province_code,
    province
  from csx_ods.sys_province_ods
)g on a.province_name=g.province
left outer join
(
  select '泉州'city,'泉州'city_real
  union all 
  select '莆田'city,'莆田'city_real
  union all 
  select '南平'city,'南平'city_real
  union all 
  select '厦门'city,'厦门、龙岩、漳州'city_real
  union all 
  select '漳州'city,'厦门、龙岩、漳州'city_real
  union all 
  select '龙岩'city,'厦门、龙岩、漳州'city_real
  union all 
  select '福州'city,'福州、宁德、三明'city_real
  union all 
  select '宁德'city,'福州、宁德、三明'city_real
  union all 
  select '三明'city,'福州、宁德、三明'city_real
  union all 
  select '南京'city,'南京'city_real
)b on substr(a.city_name,1,2)=b.city;


--明细数据 客户-每天-课组
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
-- 按课组汇总
select sign_company_code,sum(channel_value) as channel_value,sum(caiwu_sales_value) as caiwu_sales_value,
  sum(excluding_tax_channel_value) as excluding_tax_channel_value, sum(excluding_tax_caiwu_sales_value) as excluding_tax_caiwu_sales_value,
  new_or_old,a.sdt,a.customer_no,a.customer_name,a.channel_name,a.province_code,a.province_name,city_real,a.shop_id,
  a.attribute,a.first_category,a.second_category,third_category,a.sales_name,a.work_no,category_code,category_name,a.department_code,
  a.department_name,
  sum(a.sales_value) sales_value,sum(excluding_tax_sales) as excluding_tax_sales,
  sum(a.sales_cost) sales_cost,sum(a.excluding_tax_cost) excluding_tax_cost,
  sum(a.profit) profit, sum(a.excluding_tax_profit) excluding_tax_profit,is_factory_goods_name
from b2b_tmp.cust_sales_m2 a 
group by sign_company_code,new_or_old,a.AA,a.sdt,a.customer_no,a.customer_name,a.channel_name,a.province_code,
  a.province_name,city_real,a.shop_id,a.attribute,a.first_category,a.second_category,third_category,a.sales_name,a.work_no,
  a.department_code,a.department_name;

/*
-- 导出重庆明细数据
select sign_company_code,channel_value,caiwu_sales_value,excluding_tax_channel_value,excluding_tax_caiwu_sales_value,new_or_old,
  a.sdt,a.customer_no,a.customer_name,a.channel_name,a.province_code,a.province_name,city_real,a.shop_id,
  a.attribute,a.first_category,a.second_category,third_category,a.sales_name,a.work_no,category_code,category_name,a.department_code,
  a.department_name,goods_code, goods_name,
  sales_value,excluding_tax_sales,
  sales_cost,excluding_tax_cost,
  profit,excluding_tax_profit,is_factory_goods_name
from b2b_tmp.cust_sales_m2 a 
where province_name like '重庆%';





/*
select 'cust_sales_m' aa,count(1) from b2b_tmp.cust_sales_m
union all
select 'cust_sales_m1' aa,count(1) from b2b_tmp.cust_sales_m1
union all
select 'cust_sales_m2' aa,count(1) from b2b_tmp.cust_sales_m2;
*/





-- 销售额
drop table b2b_tmp.region_dist_salesA1;
create temporary table b2b_tmp.region_dist_salesA1
as
select '01'NO_1,'销售额' lable,b.region,
case when a.channel_name like '大客户%' then 'B端' 
	when a.channel_name like '商超%' then 'M端' 
	when a.channel_name like '企业购%' then 'BBC' 
	else '其他' end lable01,
a.smonth,province_code,province_name,sum(a.excluding_tax_sales) lable_value
from (
select substr(sdt,1,6) smonth,
province_code,province_name,channel_name,sum(excluding_tax_sales)/10000 excluding_tax_sales
from b2b_tmp.cust_sales_m2
where sdt>='20200201'
and sdt<'20200301'
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
drop table b2b_tmp.region_dist_sales_rate01A1;
create temporary table b2b_tmp.region_dist_sales_rate01A1
as
select '01'NO_1,'销售额占比' lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth,a.lable_value/b.lable_value lable_value
from b2b_tmp.region_dist_salesA1 a
left join 
(select '01'NO_1,'销售额' lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1 --销售额
group by region,province_code,province_name,smonth
) b on b.region=a.region and b.province_code=a.province_code and b.smonth=a.smonth;



--定价毛利 (订单毛利)
drop table b2b_tmp.region_dist_excluding_tax_profit01A1;
create temporary table b2b_tmp.region_dist_excluding_tax_profit01A1
as
select '03'NO_1,'定价毛利' lable,b.region,
case when a.channel_name like '大客户%' then 'B端'
 	when a.channel_name like '商超%' then 'M端' 
	when a.channel_name like '企业购%' then 'BBC'
 	else '其他' end lable01,
province_code,province_name,
a.smonth,sum(a.excluding_tax_profit) lable_value
from (
select substr(sdt,1,6) smonth,
province_code,province_name,channel_name,sum(excluding_tax_profit)/10000 excluding_tax_profit
from b2b_tmp.cust_sales_m2
where sdt>='20200201'
and sdt<'20200301'
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
drop table b2b_tmp.region_dist_prorate01A1;
create temporary table b2b_tmp.region_dist_prorate01A1
as
select '04'NO_1,'定价毛利率' lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth,b.lable_value/a.lable_value lable_value
from b2b_tmp.region_dist_salesA1 a
left join b2b_tmp.region_dist_excluding_tax_profit01A1 b on b.region=a.region and b.province_code=a.province_code and b.lable01=a.lable01 and b.smonth=a.smonth;



--定价成本 
drop table b2b_tmp.region_dist_cost01A1;
create temporary table b2b_tmp.region_dist_cost01A1
as
select '02'NO_1,'定价成本' lable,a.region,a.lable01,
a.province_code,a.province_name,a.smonth,(a.lable_value-b.lable_value) lable_value
from b2b_tmp.region_dist_salesA1 a
left join b2b_tmp.region_dist_excluding_tax_profit01A1 b on b.region=a.region and b.province_code=a.province_code and b.lable01=a.lable01 and b.smonth=a.smonth;


-- 总客户
-- 客户属性-人数（成交客户）
drop table b2b_tmp.region_dist_cust_counts02A1;
create temporary table b2b_tmp.region_dist_cust_counts02A1
as
select '06'NO_1,'总客户-成交客户人数' lable,b.region,attribute lable01,a.province_code,a.province_name,
a.smonth,count(distinct a.customer_no) lable_value
from 
(select a.*,
case when b.attribute is null then '日配客户' else attribute end attribute
from 
(select substr(sdt,1,6) smonth,
province_code,province_name,customer_no,sum(sales_value) sales_value
from csx_dw.sale_goods_m1
where sdt>='20200201'
and sdt<'20200301'
and sales_type in ('qyg','gc','anhui','sc','bbc')
and channel in('1','7')
group by substr(sdt,1,6),
province_code,province_name,customer_no)a
left join 
(select customer_no,
case when attribute is null and channel_code='1' then '日配客户'
	when attribute is null and channel_code<>'1'then '商贸批发和其他'
	when attribute not in('日配客户','福利客户','贸易客户') then '商贸批发和其他' 
	else attribute end attribute
  from csx_dw.customer_m
  where sdt = regexp_replace(date_sub(current_date, 2), '-', '')
  and customer_no<>''
  and channel_code in('1','7'))b on b.customer_no=a.customer_no )a
left join b2b_tmp.dist_region b on b.dist_no=a.province_code
group by b.region,attribute,a.province_code,a.province_name,a.smonth;


-- 客户属性-销售额（成交客户）
drop table b2b_tmp.region_dist_cust_salesA1;
create temporary table b2b_tmp.region_dist_cust_salesA1
as
select '07'NO_1,'总客户-成交客户销售额' lable,b.region,attribute lable01,a.province_code,a.province_name,
a.smonth,sum(a.excluding_tax_sales) lable_value
from 
(select a.*,
case when b.attribute is null then '日配客户' else attribute end attribute
from 
(select substr(sdt,1,6) smonth,
province_code,province_name,customer_no,sum(excluding_tax_sales)/10000 excluding_tax_sales
from b2b_tmp.cust_sales_m2
where sdt>='20200201'
and sdt<'20200301'
and channel in('1','7')
group by substr(sdt,1,6),
province_code,province_name,customer_no)a
left join 
(select customer_no,
case when attribute is null and channel_code='1' then '日配客户'
	when attribute is null and channel_code<>'1'then '商贸批发和其他'
	when attribute not in('日配客户','福利客户','贸易客户') then '商贸批发和其他' 
	else attribute end attribute
  from csx_dw.customer_m
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
from csx_dw.sale_goods_m1
where sdt<'20200301'
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
(select a.customer_no,a.channel_code,a.sign_time,a.province_code,
case when a.channel_code='2' then c.province_name 
	 when a.channel_code='2' then c.province_name
     else a.province_name end province_name
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
  from csx_dw.customer_m
  where sdt = regexp_replace(date_sub(current_date, 2), '-', '')
  and customer_no<>''
  and channel_code in('1','7'))a
left join 
(select shop_id,
    case when shop_id in ('W055','W056') then '上海市'  else province_name end province_name,
    case when province_name like '%市' then province_name else city_name end city_name     
  from csx_dw.shop_m 
  where sdt = 'current'
)c on a.customer_no = concat('S',c.shop_id))a
left join b2b_tmp.tmp_cust_min_sale b on a.customer_no=b.customer_no
left join b2b_tmp.dist_region c on c.dist=a.province_name;
*/


 
-- 客户状态-人数
drop table b2b_tmp.region_dist_cust_counts01A1;
create temporary table b2b_tmp.region_dist_cust_counts01A1
as
select '05'NO_1,'总客户-客户数' lable,region,'成交客户'lable01,
province_code,province_name,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts02A1
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
where sign_time>='20200201'
and sign_time<'20200301'
group by substr(sign_time,1,6) ,
province_code,province_name
union all
select substr(min_sales_date,1,6) smonth,
province_code,province_name,'新成交客户'lable01,count(distinct customer_no) as lable_value 
from b2b_tmp.tmp_cust_count_info
where min_sales_date>='20200201'
and min_sales_date<'20200301'
group by substr(min_sales_date,1,6),
province_code,province_name
)a
left join b2b_tmp.dist_region b on b.dist=a.province_name
group by b.region,a.lable01,a.province_code,a.province_name,a.smonth;


--总客户-客户属性-人数占比
drop table b2b_tmp.region_dist_cust_counts02A1_rate;
create temporary table b2b_tmp.region_dist_cust_counts02A1_rate
as
select '06'NO_1,'总客户-成交客户人数占比' lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth,a.lable_value/b.lable_value lable_value
from b2b_tmp.region_dist_cust_counts02A1 a
left join 
(select '05'NO_1,'成交客户' lable,region,'z小计'lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_counts01A1  -- 客户状态-人数
where lable01='成交客户'
) b on b.region=a.region and b.province_code=a.province_code and b.smonth=a.smonth;


--总客户-客户属性-金额占比
drop table b2b_tmp.region_dist_cust_salesA1_rate;
create temporary table b2b_tmp.region_dist_cust_salesA1_rate
as
select '06'NO_1,'总客户-成交客户销售额占比' lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth,a.lable_value/b.lable_value lable_value
from b2b_tmp.region_dist_cust_salesA1 a
left join 
(select '05'NO_1,'成交客户' lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1  -- B客户销售额
where lable01 in('B端','BBC')
group by region,province_code,province_name,smonth
) b on b.region=a.region and b.province_code=a.province_code and b.smonth=a.smonth;


-- 结果表-拼接
drop table b2b_tmp.region_dist_result01A1;
create table b2b_tmp.region_dist_result01A1
as
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_salesA1  --销售额
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_sales_rate01A1  --销售额占比
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cost01A1  --定价成本
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_excluding_tax_profit01A1  --定价毛利
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_prorate01A1  --定价毛利率
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_counts01A1  --总客户-客户数
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_counts02A1  --总客户-成交客户人数
union all                    
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_salesA1      --总客户-成交客户销售额
union all
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_counts02A1_rate  --总客户-成交客户-客户属性-人数占比
union all                    
select NO_1,lable,region,lable01,province_code,province_name,smonth,lable_value
from b2b_tmp.region_dist_cust_salesA1_rate      --总客户-成交客户-客户属性-金额占比
union all     
select '01'NO_1,'销售额' lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1 --销售额-z小计
group by region,province_code,province_name,smonth
union all
select '02'NO_1,'定价成本'lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_cost01A1  --定价成本-z小计
group by region,province_code,province_name,smonth
union all
select '02'NO_1,'定价毛利'lable,region,'z小计'lable01,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_excluding_tax_profit01A1  --定价毛利-z小计
group by region,province_code,province_name,smonth
union all                           --定价毛利率-z小计
select '04'NO_1,'定价毛利率'lable,a.region,'z小计'lable01,a.province_code,a.province_name,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_excluding_tax_profit01A1  --定价毛利
group by region,province_code,province_name,smonth) a
left join 
(select region,province_code,province_name,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1  --销售额
group by region,province_code,province_name,smonth
) b on b.region=a.region and b.province_code=a.province_code and b.smonth=a.smonth;


-- 结果表-客户类型 
drop table b2b_tmp.region_dist_result02A1;
create temporary table b2b_tmp.region_dist_result02A1
as
select b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1 NO_01,a.lable,a.region,a.lable01,
a.province_code,a.province_name,
a.smonth,sum(a.lable_value)lable_value
from b2b_tmp.region_dist_result01A1 a
left join b2b_tmp.caibao_yue b on a.lable=b.lable1 and a.lable01=b.lable_detail
group by b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1,a.lable,a.region,a.lable01,a.province_code,a.province_name,a.smonth;


insert overwrite directory '/tmp/raoyanhua/caibao_yue01A1' row format delimited fields terminated by '\t' 
select * from b2b_tmp.region_dist_result02A1;

------------------------------------------------------------------------------------------------
--------------- 大区

--销售额占比
drop table b2b_tmp.region_sales_rate01A1;
create temporary table b2b_tmp.region_sales_rate01A1
as
select '01'NO_1,'销售额占比' lable,a.region,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_salesA1
group by region,lable01,smonth) a
left join 
(select '01'NO_1,'销售额' lable,region,'z小计'lable01,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1 --销售额
group by region,smonth
) b on b.region=a.region and b.smonth=a.smonth;

--定价毛利率
drop table b2b_tmp.region_prorate01A1;
create temporary table b2b_tmp.region_prorate01A1
as
select '04'NO_1,'定价毛利率' lable,a.region,a.lable01,a.smonth,b.lable_value/a.lable_value lable_value
from 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_salesA1
group by region,lable01,smonth) a
left join 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_excluding_tax_profit01A1
group by region,lable01,smonth) b on b.region=a.region and b.lable01=a.lable01 and b.smonth=a.smonth;


--总客户-客户属性-人数占比
drop table b2b_tmp.region_cust_counts02_rateA1;
create temporary table b2b_tmp.region_cust_counts02_rateA1
as
select '06'NO_1,'总客户-成交客户人数占比' lable,a.region,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts02A1
group by region,lable01,smonth )a
left join 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts01A1  -- 客户状态-人数
where lable01='成交客户'
group by region,lable01,smonth
) b on b.region=a.region and b.smonth=a.smonth;



--总客户-客户属性-金额占比
drop table b2b_tmp.region_cust_sales_rateA1;
create temporary table b2b_tmp.region_cust_sales_rateA1
as
select '06'NO_1,'总客户-成交客户销售额占比' lable,a.region,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_salesA1
group by region,lable01,smonth ) a
left join 
(select '05'NO_1,'成交客户' lable,region,'z小计'lable01,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1  -- B客户销售额
where lable01 in('B端','BBC')
group by region,smonth
) b on b.region=a.region and b.smonth=a.smonth;


-- 结果表-拼接
drop table b2b_tmp.region_result01A1;
create table b2b_tmp.region_result01A1
as
select NO_1,lable,region,lable01,smonth,sum(lable_value) lable_value
from b2b_tmp.region_dist_result01A1
where NO_1<>'4' 
and lable not like'%率%'
and lable not like'%占比%'
group by NO_1,lable,region,lable01,smonth
union all  
select NO_1,lable,region,lable01,smonth,lable_value
from b2b_tmp.region_sales_rate01A1  --销售额占比
union all  
select NO_1,lable,region,lable01,smonth,lable_value
from b2b_tmp.region_prorate01A1  --定价毛利率
union all  
select NO_1,lable,region,lable01,smonth,lable_value
from b2b_tmp.region_cust_counts02_rateA1  --总客户-客户属性-人数占比
union all  
select NO_1,lable,region,lable01,smonth,lable_value
from b2b_tmp.region_cust_sales_rateA1  --总客户-客户属性-金额占比
union all                           --定价毛利率-z小计
select '04'NO_1,'定价毛利率'lable,a.region,'z小计'lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select region,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_excluding_tax_profit01A1  --定价毛利
group by region,smonth) a
left join 
(select region,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1  --销售额
group by region,smonth
) b on b.region=a.region and b.smonth=a.smonth;

-- 结果表-客户类型 
drop table b2b_tmp.region_result02A1;
create temporary table b2b_tmp.region_result02A1
as
select b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1 NO_01,a.lable,a.region,a.lable01,
a.smonth,sum(a.lable_value)lable_value
from b2b_tmp.region_result01A1 a
left join b2b_tmp.caibao_yue b on a.lable=b.lable1 and a.lable01=b.lable_detail
group by b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1,a.lable,a.region,a.lable01,a.smonth;




insert overwrite directory '/tmp/raoyanhua/caibao_yue02A1' row format delimited fields terminated by '\t' 
select * from b2b_tmp.region_result02A1;


------------------------------------------------------------------------------------------------
--------------- 全国

--销售额占比
drop table b2b_tmp.all_sales_rate01A1;
create temporary table b2b_tmp.all_sales_rate01A1
as
select '01'NO_1,'销售额占比' lable,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_salesA1
where region in('华南','华北','华西','华东')
group by lable01,smonth) a
left join 
(select '01'NO_1,'销售额' lable,'z小计'lable01,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1 --销售额
where region in('华南','华北','华西','华东')
group by smonth
) b on b.smonth=a.smonth;

--定价毛利率
drop table b2b_tmp.region_prorate01A1;
create temporary table b2b_tmp.region_prorate01A1
as
select '04'NO_1,'定价毛利率' lable,a.lable01,a.smonth,b.lable_value/a.lable_value lable_value
from 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_salesA1
where region in('华南','华北','华西','华东')
group by lable01,smonth) a
left join 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_excluding_tax_profit01A1
where region in('华南','华北','华西','华东')
group by lable01,smonth) b on b.lable01=a.lable01 and b.smonth=a.smonth;


--总客户-客户属性-人数占比
drop table b2b_tmp.region_cust_counts02_rateA1;
create temporary table b2b_tmp.region_cust_counts02_rateA1
as
select '06'NO_1,'总客户-成交客户人数占比' lable,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts02A1
where region in('华南','华北','华西','华东')
group by lable01,smonth )a
left join 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_counts01A1  -- 客户状态-人数
where lable01='成交客户'
and region in('华南','华北','华西','华东')
group by lable01,smonth
) b on b.smonth=a.smonth;



--总客户-客户属性-金额占比
drop table b2b_tmp.region_cust_sales_rateA1;
create temporary table b2b_tmp.region_cust_sales_rateA1
as
select '06'NO_1,'总客户-成交客户销售额占比' lable,a.lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select lable01,smonth,sum(lable_value)lable_value 
from b2b_tmp.region_dist_cust_salesA1
where region in('华南','华北','华西','华东')
group by lable01,smonth ) a
left join 
(select '05'NO_1,'成交客户' lable,'z小计'lable01,smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1  -- B客户销售额
where region in('华南','华北','华西','华东')
and lable01 in('B端','BBC')
group by smonth
) b on b.smonth=a.smonth;


-- 结果表-拼接
drop table b2b_tmp.region_result01A1;
create table b2b_tmp.region_result01A1
as
select NO_1,lable,lable01,smonth,sum(lable_value) lable_value
from b2b_tmp.region_dist_result01A1
where NO_1<>'4' 
and lable not like'%率%'
and lable not like'%占比%'
and region in('华南','华北','华西','华东')
group by NO_1,lable,lable01,smonth
union all  
select NO_1,lable,lable01,smonth,lable_value
from b2b_tmp.all_sales_rate01A1  --销售额占比
union all  
select NO_1,lable,lable01,smonth,lable_value
from b2b_tmp.region_prorate01A1  --定价毛利率
union all  
select NO_1,lable,lable01,smonth,lable_value
from b2b_tmp.region_cust_counts02_rateA1  --总客户-客户属性-人数占比
union all  
select NO_1,lable,lable01,smonth,lable_value
from b2b_tmp.region_cust_sales_rateA1  --总客户-客户属性-金额占比
union all                           --定价毛利率-z小计
select '04'NO_1,'定价毛利率'lable,'z小计'lable01,a.smonth,a.lable_value/b.lable_value lable_value
from 
(select smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_excluding_tax_profit01A1  --定价毛利
where region in('华南','华北','华西','华东')
group by smonth) a
left join 
(select smonth,sum(lable_value)lable_value
from b2b_tmp.region_dist_salesA1  --销售额
where region in('华南','华北','华西','华东')
group by smonth
) b on b.smonth=a.smonth;

-- 结果表-客户类型 
drop table b2b_tmp.region_result02A1;
create temporary table b2b_tmp.region_result02A1
as
select b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1 NO_01,a.lable,a.lable01,
a.smonth,sum(a.lable_value)lable_value
from b2b_tmp.region_result01A1 a
left join b2b_tmp.caibao_yue b on a.lable=b.lable1 and a.lable01=b.lable_detail
group by b.no_1,b.no2_lable_detail,b.lable1,b.lable_detail,a.NO_1,a.lable,a.lable01,a.smonth;




insert overwrite directory '/tmp/raoyanhua/caibao_yue03A1' row format delimited fields terminated by '\t' 
select * from b2b_tmp.region_result02A1;

 ------------------------------------------------------------------------------------------------


