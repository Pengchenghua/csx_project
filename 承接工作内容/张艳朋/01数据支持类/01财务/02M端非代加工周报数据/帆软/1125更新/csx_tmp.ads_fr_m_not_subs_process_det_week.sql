--============================================================================================================
--M端代加工数据_月环比明细 每周六运行sql

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日

set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');
set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_21 =regexp_replace(add_months(date_sub(current_date,1),-1),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');

--昨日分区样式
set i_sdate_1 =regexp_replace(date_sub(current_date,1),'-','');

--今日
set i_sdate_0 =regexp_replace(current_date,'-','');

--本周六
set i_sdate_31 =regexp_replace(date_sub(current_date,7),'-','');

--上周六
set i_sdate_42 =regexp_replace(date_sub(current_date,14),'-','');

--上周五
set i_sdate_41 =regexp_replace(date_sub(current_date,8),'-','');


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.ads_fr_m_not_subs_process_det_week partition(sdt)

				
select
	a.sdt as agg_date,
	a.province_name, 
	a.dc_code,
	a.dc_name,
	a.customer_no,
	a.customer_name,
	b.sales_belong_flag,
	a.department_code,
	a.department_name,
	a.goods_code,
	a.goods_name,
	a.sales_qty,
	a.unit,
	a.sales_value,
	a.sales_cost,
	a.profit,
	a.profit/a.sales_value as profit_rate,
	case when b.sales_belong_flag like '%云超%' then '云超'
		when b.sales_belong_flag like '%云创%' then '云创'
		when a.customer_name rlike '红旗|中百' then '关联方'
		when b.sales_belong_flag='' or b.sales_belong_flag is null then '外部'
		else '其他'
	end as sales_belong_type,
	if(a.department_code in ('U01','H03','H05','H01','H02','H04','104'),a.department_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	c.region_name,
	case when a.sdt between ${hiveconf:i_sdate_31} and ${hiveconf:i_sdate_11} then '本周'
		when a.sdt between ${hiveconf:i_sdate_42} and ${hiveconf:i_sdate_41} then '上周'
		else '其他'
	end as week_type,	
	${hiveconf:i_sdate_0} as update_time,
	'19000101' as sdt
from	
	(	
	select
		sdt,
		province_code,
		province_name, 
		dc_code,
		dc_name,
		customer_no,
		customer_name,					
		department_code,
		department_name,
		goods_code,
		goods_name,
		unit,
		sum(sales_qty) as sales_qty,
		sum(sales_value) as sales_value,
		sum(sales_cost) as sales_cost,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between ${hiveconf:i_sdate_42} and ${hiveconf:i_sdate_11} 
		and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and (dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5') or (dc_code in ('W0M4') and department_code not in ('H03','H01')))--数据不含代加工DC 
	group by
		sdt,
		province_code,
		province_name, 
		dc_code,
		dc_name,
		customer_no,
		customer_name,					
		department_code,
		department_name,
		goods_code,
		goods_name,
		unit
	) as a 
	left join
		(
		select 
			shop_id,company_code,sales_belong_flag
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt = 'current'
		) b on a.customer_no = concat('S', b.shop_id)
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) c on c.province_code=a.province_code
;



INVALIDATE METADATA csx_tmp.ads_fr_m_not_subs_process_det_week;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.ads_fr_m_not_subs_process_det_week  M端非代加工周环比数据明细

drop table if exists csx_tmp.ads_fr_m_not_subs_process_det_week;
create table csx_tmp.ads_fr_m_not_subs_process_det_week(
`agg_date`                 string              COMMENT    '期间',
`province_name`            string              COMMENT    '省区',
`dc_code`                  string              COMMENT    'DC编码',
`dc_name`                  string              COMMENT    'DC名称',
`customer_no`              string              COMMENT    '客户编码',
`customer_name`            string              COMMENT    '客户名称',
`sales_belong_flag`        string              COMMENT    '业态',
`department_code`          string              COMMENT    '课组编码',
`department_name`          string              COMMENT    '课组名称',
`goods_code`               string              COMMENT    '商品编码',
`goods_name`               string              COMMENT    '商品名称',
`sales_qty`                decimal(26,6)       COMMENT    '销售数量',
`unit`                     string              COMMENT    '单位',
`sales_value`              decimal(26,6)       COMMENT    '含税销售',
`sales_cost`               decimal(26,6)       COMMENT    '含税成本',
`profit`                   decimal(26,6)       COMMENT    '定价毛利额',
`profit_rate`              decimal(26,6)       COMMENT    '定价毛利率',
`sales_belong_type`        string              COMMENT    '业态分类',
`department_type`          string              COMMENT    '课组分类',
`region_name`              string              COMMENT    '大区',
`week_type`                string              COMMENT    '周度',
`update_time`              string              COMMENT    '更新时间'
) COMMENT 'zhangyanpeng:M端非代加工周环比数据明细'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/		