--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


with tmp_cbgb_tz_jlc as 
(
select 
	a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,
	a.location_code,a.cost_center_code,a.product_code,b.goods_name product_name,b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(amount)amount
from
	(
	select 
		a.*,
		case when a.location_code='W0H4' then '-' else b.performance_province_code end performance_province_code,
		case when a.location_code='W0H4' then '供应链' else b.performance_province_name end performance_province_name,
		case when a.location_code='W0H4' then '-' else b.performance_city_code end performance_city_code,
		case when a.location_code='W0H4' then '供应链' else b.performance_city_name end performance_city_name,
		b.shop_code,b.shop_name
	from 
		(
		select 
			* 
		from 
			-- csx_ods.source_mms_r_a_factory_report_no_share_product
			csx_ods.csx_ods_csx_b2b_factory_factory_report_no_share_product_df
		where 
			sdt='${today}'
			and period in(substr(add_months(trunc('${ytd_date}','MM'),-1),1,7))
		)a 
		left join 
			(
			select 
				shop_code,shop_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
			from 
				csx_dim.csx_dim_shop
			where 
				sdt = 'current'
			) b on b.shop_code=a.location_code
	) a
	left join 
		(
		select 
			regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') goods_name,
			goods_code,purchase_group_code dept_id,purchase_group_name dept_name
		from 
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where 
			sdt = 'current' 
		)b on a.product_code=b.goods_code
	left join
		(
		select
			workshop_code,province_code,goods_code
		from 
			-- csx_dw.dws_mms_w_a_factory_setting_craft_once_all
			csx_dws.csx_dws_mms_factory_setting_craft_once_all_df
		where 
			sdt='current' and new_or_old=1
		)d on a.performance_province_code=d.province_code and a.product_code=d.goods_code
group by 
	a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,
	a.location_code,a.cost_center_code,a.product_code,b.goods_name,b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品')
)

insert overwrite table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_jlc_mi partition(sdt)

select
	concat_ws('&',performance_city_code,location_code,cost_center_code,product_code,substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6)) as biz_id,
	performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	location_code,cost_center_code,
	product_code,product_name,dept_id,dept_name,is_factory_goods_name,
	amount,
	substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6) as sdt
from tmp_cbgb_tz_jlc;

/*
create table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_jlc_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`location_code`                  string              COMMENT    '地点编码',
`cost_center_code`               string              COMMENT    '成本中心编码',
`product_code`                   string              COMMENT    '商品编码',
`product_name`                   string              COMMENT    '商品名称',
`dept_id`                        string              COMMENT    '课组编号',
`dept_name`                      string              COMMENT    '课组名称',
`is_factory_goods_name`          string              COMMENT    '是否工厂商品',
`amount`                         decimal(26,6)       COMMENT    '金额'

) COMMENT '财报管报-调整项-价量差'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	
