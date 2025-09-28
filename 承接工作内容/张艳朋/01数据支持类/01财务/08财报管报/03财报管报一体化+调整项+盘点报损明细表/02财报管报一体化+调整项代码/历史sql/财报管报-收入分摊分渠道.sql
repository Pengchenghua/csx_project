--每月关账后跑一次
--收入分摊（不含税），按省区城市-课组的收入，与销售业绩省区城市-课组-各渠道；收入本身未分渠道，根据销售端各渠道各城市各课组销额占比分配
--依赖表：csx_ods.settle_settle_bill_ods，csx_dw.dws_sale_r_d_customer_sale，csx_dw.dws_basic_w_a_csx_product_m

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

--后台收入明细
drop table csx_tmp.tmp_cbgb_tz_sr_1;
create table csx_tmp.tmp_cbgb_tz_sr_1
as 
select case when a.settle_place_code='W0H4' then '供应链' else b.province_name end province_name,
case when a.settle_place_code='W0H4' then '供应链' else b.city_name end city_name,
a.settle_no,a.agreement_no,a.settle_date,a.purchase_org_code,a.purchase_org_name,a.purchase_code dept_id,a.purchase_name dept_name,a.cost_code,a.cost_name,
a.attribution_date,a.supplier_code,a.supplier_name,a.settle_place_code,a.settle_place_name,a.company_code,a.company_name,
a.net_value,a.tax_amount,a.value_tax_total,a.bill_total_amount,a.invoice_code,a.invoice_name,substr(${hiveconf:i_sdate_22},1,6) as smonth
from 
( select * from csx_ods.settle_settle_bill_ods 
where sdt='19990101'
and attribution_date >= ${hiveconf:i_sdate_12} 
and attribution_date < ${hiveconf:i_sdate_11} )a
left join (select shop_id,shop_name,province_name,city_name
from csx_dw.dws_basic_w_a_csx_shop_m where sdt = 'current') b on b.shop_id=a.settle_place_code;


--后台收入-明细数据
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
--select * from csx_tmp.tmp_cbgb_tz_sr_1;


--渠道-省区-课组销售
drop table csx_tmp.tmp_cbgb_tz_sr_2;
create temporary table csx_tmp.tmp_cbgb_tz_sr_2
as 
select a.province_name,
case when a.city_name='厦门市,龙岩市,漳州市' then '厦门市'
	when a.city_name='福州市,宁德市,三明市' then '福州市' else a.city_name end city_name,
a.channel_name,
d.department_id dept_id,d.department_name dept_name,
sum(a.untax_sale)untax_sale,sum(a.sale)sale 
from 
	(select province_name,city_real city_name,channel_name,goods_code,sum(excluding_tax_sales)untax_sale,sum(sales_value)sale
	from csx_dw.dws_sale_r_d_customer_sale
	where sdt>=${hiveconf:i_sdate_22}
	and sdt<${hiveconf:i_sdate_21}
	and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
	group by province_name,city_real,channel_name,goods_code)a 
left join (select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' 
			) d on d.goods_id=a.goods_code			
group by a.province_name,
case when a.city_name='厦门市,龙岩市,漳州市' then '厦门市'
	when a.city_name='福州市,宁德市,三明市' then '福州市' else a.city_name end,
a.channel_name,
d.department_id,d.department_name;


--省区-课组销售总额，及各渠道占比
drop table csx_tmp.tmp_cbgb_tz_sr_3;
create temporary table csx_tmp.tmp_cbgb_tz_sr_3
as 
select a.province_name,a.city_name,a.dept_id,a.dept_name,
a.untax_sale,a.untax_sale_B,a.untax_sale_M,a.untax_sale_BBC,b.net_value_sr,
b.net_value_sr*(a.untax_sale_B/a.untax_sale) net_value_sr_B,
b.net_value_sr*(a.untax_sale_M/a.untax_sale) net_value_sr_M,
b.net_value_sr*(a.untax_sale_BBC/a.untax_sale) net_value_sr_BBC
from
	(select province_name,
	case when province_name in('上海市','北京市','四川省','安徽省','广东省','河北省','贵州省','重庆市','陕西省') then '-' else city_name end city_name,
	dept_id,dept_name,
	sum(untax_sale) untax_sale,
	sum(case when channel_name='大客户' then untax_sale end) untax_sale_B,
	sum(case when channel_name='商超' then untax_sale end) untax_sale_M,
	sum(case when channel_name like '企业购%' then untax_sale end) untax_sale_BBC
	from csx_tmp.tmp_cbgb_tz_sr_2 
	where province_name not like '平台%'
	group by province_name,
	case when province_name in('上海市','北京市','四川省','安徽省','广东省','河北省','贵州省','重庆市','陕西省') then '-' else city_name end,
	dept_id,dept_name)a
left join 
	(select province_name,
	case when province_name in('上海市','北京市','四川省','安徽省','广东省','河北省','贵州省','重庆市','陕西省') then '-' else city_name end city_name,
	dept_id,
	sum(net_value) net_value_sr
	from csx_tmp.tmp_cbgb_tz_sr_1 
	group by province_name,
	case when province_name in('上海市','北京市','四川省','安徽省','广东省','河北省','贵州省','重庆市','陕西省') then '-' else city_name end,
	dept_id 
	)b on a.province_name=b.province_name and a.city_name=b.city_name and a.dept_id=b.dept_id;


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.cbgb_tz_m_cbgb_srft_qd partition(sdt)
select province_name,city_name,dept_id,dept_name,
untax_sale,untax_sale_B,untax_sale_M,untax_sale_BBC,net_value_sr,
net_value_sr_B,net_value_sr_M,net_value_sr_BBC,
substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_sr_3;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.cbgb_tz_m_cbgb_srft_qd  调整收入按课组销售比例分摊到渠道

drop table if exists csx_dw.cbgb_tz_m_cbgb_srft_qd;
create table csx_dw.cbgb_tz_m_cbgb_srft_qd(
  `province_name` string COMMENT  '省区',
  `city_name` string COMMENT '城市',
  `dept_id` string COMMENT '课组编号',
  `dept_name` string COMMENT  '课组名称',
  `untax_sale` decimal(26,6)  COMMENT  '未税销售额',
  `untax_sale_B` decimal(26,6)  COMMENT  '未税销售额_B',
  `untax_sale_M` decimal(26,6)  COMMENT  '未税销售额_M',
  `untax_sale_BBC` decimal(26,6)  COMMENT  '未税销售额_BBC',
  `net_value_sr` decimal(26,6)  COMMENT '未税收入分摊',
  `net_value_sr_B` decimal(26,6)  COMMENT '未税收入分摊_B',
  `net_value_sr_M` decimal(26,6)  COMMENT '未税收入分摊_M',
  `net_value_sr_BBC` decimal(26,6)  COMMENT '未税收入分摊_BBC'
) COMMENT '调整收入按课组销售比例分摊到渠道'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

--------------------------------- mysql建表语句 -------------------------------
drop table if exists `cbgb_tz_m_cbgb_srft_qd`;
create table `cbgb_tz_m_cbgb_srft_qd`(
  `province_name` varchar(64) DEFAULT NULL COMMENT '省区',
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市',
  `dept_id` varchar(64) DEFAULT NULL COMMENT '课组编号',
  `dept_name` varchar(64) DEFAULT NULL COMMENT '课组名称',
  `untax_sale` decimal(26,6)  COMMENT '未税销售额',
  `untax_sale_B` decimal(26,6)  COMMENT '未税销售额_B',
  `untax_sale_M` decimal(26,6)  COMMENT '未税销售额_M',
  `untax_sale_BBC` decimal(26,6)  COMMENT '未税销售额_BBC',
  `net_value_sr` decimal(26,6)  COMMENT '未税收入分摊',
  `net_value_sr_B` decimal(26,6)  COMMENT '未税收入分摊_B',
  `net_value_sr_M` decimal(26,6)  COMMENT '未税收入分摊_M',
  `net_value_sr_BBC` decimal(26,6)  COMMENT '未税收入分摊_BBC',  
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='调整收入按课组销售比例分摊到渠道 ';




select province_name,city_name,
sum(net_value_sr_B) net_value_sr_B,
sum(net_value_sr_M) net_value_sr_M,
sum(net_value_sr_BBC) net_value_sr_BBC
from csx_dw.cbgb_tz_m_cbgb_srft_qd
where sdt='202005'
group by province_name,city_name;

*/

