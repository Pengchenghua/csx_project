-- OKR数据-缺货数据 
-- 核心逻辑： 

-- 切换tez计算引擎
set mapred.job.name=report_scm_r_m_lack_sku_cnt;
SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;

-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 计算日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月第一天
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');

-- 目标表
set target_table=csx_tmp.report_scm_r_m_lack_sku_cnt;
	
with current_lack_sku as 	
(
select 
	c.sales_region_code,c.sales_region_name,c.province_code,c.province_name,c.city_group_code,c.city_group_name,a.shop_id_in,a.shop_name,
	a.qtr,a.sdt,order_code,a.vendor_id,b.vendor_name,
	goodsid,plan_qty,plan_amount,receive_qty,amount
from 
	(
	select 
		case when sdt>='20210101' and sdt<='20210331' then 'Q1'  when sdt>='20210401' and sdt<='20210630' then 'Q2' else '其他' end as qtr,
		sdt,order_code,supplier_code vendor_id,goods_code as goodsid,receive_location_code as shop_id_in,receive_location_name as shop_name,
		max(case when return_flag='Y' then -1*plan_qty else plan_qty end) plan_qty,
		max(case when return_flag='Y' then -1*plan_qty*price else plan_qty*price end) plan_amount,
		sum(case when return_flag='Y' then -1*receive_qty else receive_qty end) receive_qty,
		sum(case when return_flag='Y' then -1*receive_qty*price else receive_qty*price end) amount
	from 
		csx_dw.dws_wms_r_d_entry_detail
	where 
		sdt>='20210101' and sdt<='20210630'
		and receive_status=2  --已关业务
		and order_type_code LIKE 'P%' and order_type_code<>'P02' --剔除调拨
	group by 
		case when sdt>='20210101' and sdt<='20210331' then 'Q1'  when sdt>='20210401' and sdt<='20210630' then 'Q2' else '其他' end,
		sdt,order_code,supplier_code,goods_code,receive_location_code,receive_location_name
	)a 
	join 
		(
		select 
			shop_id,sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name 
		from 
			csx_dw.dws_basic_w_a_csx_shop_m cs 
		where 
			sdt='current'and cs.purpose in ('01','02','03') -- 1仓库2工厂3	门店
		)c on a.shop_id_in =c.shop_id
	left join 
		(
		select 
			vendor_id,vendor_name 
		from 
			csx_dw.dws_basic_w_a_csx_supplier_m 
		where 
			sdt='current' 
			and frozen='0'
		)b on lpad(a.vendor_id,10,'0')=lpad(b.vendor_id,10,'0')
)

insert overwrite table ${hiveconf:target_table} partition(quarter)			
			
select 
	'' as biz_id,
	sales_region_code as region_code,
	sales_region_name as region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	division_type,
	sum(q1_total_sku) as last_qtr_total_sku,
	sum(q1_lack_sku) as last_qtr_lack_sku,
	sum(q2_total_sku) as cur_qtr_total_sku,
	sum(q2_lack_sku) as cur_qtr_lack_sku,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	'202102' as quarter -- 季度
from 
	(
	select
		sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,
		vendor_id,vendor_name,sdt,order_code,division_type,department_id,department_name,
		count(distinct case when qtr='Q1' then a.goodsid else null end) q1_total_sku,
		count(distinct case when qtr='Q1' and receive_qty<plan_qty then a.goodsid else null end) q1_lack_sku,
		count(distinct case when qtr='Q2' then a.goodsid else null end) q2_total_sku,
		count(distinct case when qtr='Q2' and receive_qty<plan_qty then a.goodsid else null end) q2_lack_sku
	from  
		csx_tmp.vendor_sku01 a 
	join 
		(
		select 
			goods_id,
			case when division_code in ('10','11') then '生鲜事业部'
				when division_code in ('12','13','14','15') then '食百事业部'
				else '其他' 
			end as division_type, -- '事业部名称' 
			department_id,
			department_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m 
		where 
			sdt='current'
		) b on a.goodsid=b.goods_id 
	group by 
		sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,
		vendor_id,vendor_name,sdt,order_code,division_type,department_id,department_name
	) a
group by 
	sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,division_type	
;



INVALIDATE METADATA csx_tmp.report_scm_r_m_lack_sku_cnt;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_scm_r_m_lack_sku_cnt  OKR数据 缺货数据

drop table if exists csx_tmp.report_scm_r_m_lack_sku_cnt;
create table csx_tmp.report_scm_r_m_lack_sku_cnt(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省份编码',
`province_name`                  string              COMMENT    '省份名称',
`city_group_code`                string              COMMENT    '城市组编码',
`city_group_name`                string              COMMENT    '城市组名称',
`division_type`                  string              COMMENT    '事业部类型',
`last_qtr_total_sku`             int                 COMMENT    '上季度总SKU数',
`last_qtr_lack_sku`              int                 COMMENT    '上季度缺货SKU数',
`cur_qtr_total_sku`              int                 COMMENT    '本季度总SKU数',
`cur_qtr_lack_sku`               int                 COMMENT    '本季度缺货SKU数',
`update_time`                    string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:OKR数据-缺货数据'
PARTITIONED BY (quarter string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	