-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;


insert overwrite table csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df partition(sdt)

select
	a.id as biz_id,
	coalesce(b.performance_region_code,'0') as performance_region_code,
	coalesce(b.performance_region_name,'其他') as performance_region_name,
	coalesce(b.basic_performance_province_code,'0') as performance_province_code,
	coalesce(b.basic_performance_province_name,'其他') as performance_province_name,
	coalesce(b.basic_performance_city_code,'0') as performance_city_code,
	coalesce(b.basic_performance_city_name,'其他') as performance_city_name,
	a.out_no,
	a.dc_code,
	a.dc_name,
	if(a.shop_code='' or a.shop_code is null,'0',a.shop_code) as shop_code,
	if(a.shop_name='' or a.shop_name is null,'其他',a.shop_name) as shop_name,
	a.ex_big_class,
	a.ex_big_class_name,
	if(a.ex_small_class='' or a.ex_small_class is null,'0',a.ex_small_class) as ex_small_class,
	if(a.ex_small_class_name='' or a.ex_small_class_name is null,'其他',a.ex_small_class_name) as ex_small_class_name,
	a.status,
	a.status_name,
	a.supplier_code,
	a.remarks,
	a.inner_order_no,
	a.created_time,
	a.updated_time,
	a.created_by,
	a.updated_by,
	a.update_time,
	a.create_time,
	concat(if(a.ex_small_class='' or a.ex_small_class is null,'0',a.ex_small_class),' ',if(a.ex_small_class_name='' or a.ex_small_class_name is null,'其他',a.ex_small_class_name)) as ex_small_class_union,
	(unix_timestamp()-unix_timestamp(a.created_time))/3600 as residence_time_hour,
	(unix_timestamp()-unix_timestamp(a.created_time))/3600/24 as residence_time_day,
	'${ytd}' as sdt
from
	(
	select 
		id,out_no,shop_code,shop_name,dc_code,dc_name,supplier_code,ex_big_class,
		case when ex_big_class=1 then '生单异常' 
			when ex_big_class=2 then '签收异常' 
			when ex_big_class=3 then '退货申请异常'
			when ex_big_class=4 then '退货入库异常'
			when ex_big_class=5 then '联营VB单异常'
		end as ex_big_class_name,
		ex_small_class,
		case when ex_small_class=11 then '订单明细不存在' 
			when ex_small_class=12 then '门店编码不存在' 
			when ex_small_class=13 then '商品信息不存在' 
			when ex_small_class=14 then '供应商编码未配置库存地点' 
			when ex_small_class=15 then '门店采购组未绑定库存地点'
			when ex_small_class=16 then '商品在库存地点下无档' 
			when ex_small_class=17 then '云超下发价格异常' 
			when ex_small_class=18 then '货到即配主单未查询到子单' 
			when ex_small_class=19 then '货到即配主单与子单不匹配' 
			when ex_small_class=21 then '订单未生单' 
			when ex_small_class=22 then '订单未接单' 
			when ex_small_class=23 then '订单未发货' 
			when ex_small_class=24 then '供应商直送单供应链未审核' 
			when ex_small_class=25 then '订单发货信息未推送' 
			when ex_small_class=26 then '订单已完成' 
			when ex_small_class=27 then '订单出库成本WMS未全部返回'
			when ex_small_class=31 then '原正向单不存在'
			when ex_small_class=32 then '原正向单未完成'
			when ex_small_class=41 then '原正向单补单未出库'
			when ex_small_class=51 then '商品在虚拟门店下无档'
		end as ex_small_class_name,
		status,
		case status 
			when 1 then '初始' 
			when 2 then '已更新' 
			when 3 then '已解决' 
		end as status_name,
		regexp_replace(remarks,'\n|\t|\r|\,|\"|\\\\n','') as remarks,
		inner_order_no,created_time,updated_time,created_by,updated_by,update_time,create_time
	from 
		csx_ods.csx_ods_csx_b2b_sell_exceptions_monitor_df
	where
		sdt='${ytd}'
		and to_date(created_time)<='${ytd_date}'
		and status in (1,2)
	) a 
	left join
		(
		select
			distinct shop_code,shop_name,
			performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
			basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name
		from 
			csx_dim.csx_dim_shop
		where
			sdt='${ytd}'
		) b on b.shop_code=a.dc_code
;

/*

create table csx_analyse.csx_analyse_fr_sell_exceptions_monitor_m_df(
`biz_id`                             string              COMMENT    '业务主键',
`performance_region_code`            string              COMMENT    '大区编码',
`performance_region_name`            string              COMMENT    '大区名称',
`performance_province_code`          string              COMMENT    '省区编码',
`performance_province_name`          string              COMMENT    '省区名称',
`performance_city_code`              string              COMMENT    '城市编码',
`performance_city_name`              string              COMMENT    '城市名称',
`out_no`                             string              COMMENT    '外部单号',
`dc_code`                            string              COMMENT    '库存dc 编码',
`dc_name`                            string              COMMENT    '库存dc 名称',
`shop_code`                          string              COMMENT    '门店编码',
`shop_name`                          string              COMMENT    '门店名称',
`ex_big_class`                       int                 COMMENT    '异常大类',
`ex_big_class_name`                  string              COMMENT    '异常大类名称',
`ex_small_class`                     int                 COMMENT    '异常小类',
`ex_small_class_name`                string              COMMENT    '异常小类名称',
`status`                             int                 COMMENT    '状态',
`status_name`                        string              COMMENT    '状态名称',
`supplier_code`                      string              COMMENT    '供应商编码',
`remarks`                            string              COMMENT    '补充说明',
`inner_order_no`                     string              COMMENT    '拆单号，生单异常没有拆单号',
`created_time`                       timestamp           COMMENT    '申请时间(创建时间)',
`updated_time`                       timestamp           COMMENT    '更新时间',
`created_by`                         string              COMMENT    '申请人',
`updated_by`                         string              COMMENT    '更新人',
`update_time`                        timestamp           COMMENT    '更新时间',
`create_time`                        timestamp           COMMENT    '创建时间',
`ex_small_class_union`               string              COMMENT    '异常小类合并',
`residence_time_hour`                decimal(26,6)       COMMENT    '停留时长-小时',
`residence_time_day`                 decimal(26,6)       COMMENT    '停留时长-天'

) COMMENT '商超-异常监控'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	
			