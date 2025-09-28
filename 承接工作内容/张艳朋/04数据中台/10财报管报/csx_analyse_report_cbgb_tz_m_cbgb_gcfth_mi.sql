--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


with tmp_cbgb_tz_gcfth_0 as 
(
select  
	case when a.location_code='W0H4' then '-' else b.performance_province_code end performance_province_code,
	case when a.location_code='W0H4' then '供应链' else b.performance_province_name end performance_province_name,
	case when a.location_code='W0H4' then '-' else b.performance_city_code end performance_city_code,
	case when a.location_code='W0H4' then '供应链' else b.performance_city_name end performance_city_name,		
	a.location_code,a.location_name,
	d.dept_id,d.dept_name,
	sum(a.d_cost_subtotal) d_cost_subtotal
from 
	(
	select 
		* 
	from 
		-- csx_ods.source_mms_r_a_factory_report_diff_apportion_header
		csx_ods.csx_ods_csx_b2b_factory_factory_report_diff_apportion_header_df
	where 
		sdt='${today}'
		and period = substr(add_months(trunc('${ytd_date}','MM'),-1),1,7)   -- '2020-08'
		and notice_status = '3'
	)a
	join 
		(
		select 
			shop_code,shop_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
		from 
			-- csx_dw.dws_basic_w_a_csx_shop_m 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current'
			and purpose<>'09'
		) b on b.shop_code=a.location_code
	left join 
		(
		select 
			goods_code,goods_name,purchase_group_code dept_id,purchase_group_name dept_name,
			category_small_code small_category_code,category_small_name small_category_name
		from 
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where 
			sdt = 'current' 
		)d on a.product_code=d.goods_code
group by 
	case when a.location_code='W0H4' then '-' else b.performance_province_code end,
	case when a.location_code='W0H4' then '供应链' else b.performance_province_name end,
	case when a.location_code='W0H4' then '-' else b.performance_city_code end,
	case when a.location_code='W0H4' then '供应链' else b.performance_city_name end,
	a.location_code,a.location_name,
	d.dept_id,d.dept_name
)


insert overwrite table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_gcfth_mi partition(sdt)

select
	concat_ws('&',performance_city_code,location_code,dept_id,substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6)) as biz_id,
	performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	location_code,location_name,dept_id,dept_name,
	d_cost_subtotal amount,
	substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6) as sdt
from tmp_cbgb_tz_gcfth_0;


/*
create table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_gcfth_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`location_code`                  string              COMMENT    '地点编码',
`location_name`                  string              COMMENT    '地点名称',
`dept_id`                        string              COMMENT    '课组编号',
`dept_name`                      string              COMMENT    '课组名称',
`amount`                         decimal(26,6)       COMMENT    '金额'

) COMMENT '财报管报-调整项-工厂分摊后'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	
