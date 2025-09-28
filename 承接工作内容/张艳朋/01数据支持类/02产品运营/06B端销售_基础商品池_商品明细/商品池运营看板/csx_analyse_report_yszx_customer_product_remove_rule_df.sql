
insert overwrite table csx_analyse.csx_analyse_report_yszx_customer_product_remove_rule_df partition(sdt)
select
	a.id as biz_id, -- 主键id
	a.dc_code as inventory_dc_code, -- dc编码
	a.big_category_code, -- 大类编码
	a.mid_category_code, -- 中类编码
	a.small_category_code, -- 小类编码
	a.category_type, -- 类型（1.小类 2.中类 3.大类）
	a.category_name, -- 类名称
	a.remove_date, -- 移除期限（天数）
	a.status, -- 规则状态： 0-禁用 1-启用
	a.create_time, -- 创建时间
	a.create_by, -- 创建者
	a.update_time, -- 更新时间
	a.update_by, -- 更新者
	'${ytd}' as sdt_date, -- 日期分区
	'${ytd}' as sdt -- 分区
from
	(
	select
		id,dc_code,big_category_code,mid_category_code,small_category_code,category_type,category_name,remove_date,status,create_time,create_by,update_time,update_by
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_remove_rule_df
	where
		sdt='${ytd}'
	) a 
	join	
		(
		select
			big_category_code
		from
			csx_analyse.csx_analyse_fr_category_rule_config_mf
		group by 
			big_category_code
		) b on b.big_category_code=a.big_category_code	
;

/*

create table csx_analyse.csx_analyse_report_yszx_customer_product_remove_rule_df(
`biz_id`                                bigint              COMMENT    '业务主键id',
`inventory_dc_code`                     string              COMMENT    'dc编码',
`big_category_code`                     string              COMMENT    '大类编码',
`mid_category_code`                     string              COMMENT    '中类编码',
`small_category_code`                   string              COMMENT    '小类编码',
`category_type`                         int                 COMMENT    '类型（1.小类 2.中类 3.大类）',
`category_name`                         string              COMMENT    '类名称',
`remove_date`                           int                 COMMENT    '移除期限（天数）',
`status`                                int                 COMMENT    '规则状态： 0-禁用 1-启用',
`create_time`                           timestamp           COMMENT    '创建时间',
`create_by`                             string              COMMENT    '创建者',
`update_time`                           timestamp           COMMENT    '更新时间',
`update_by`                             string              COMMENT    '更新者',
`sdt_date`                              string              COMMENT    '日期分区'

) COMMENT '商品池运营看板-商品池线上化程度-商品池移除规则'
PARTITIONED BY (sdt string COMMENT '分区')
STORED AS PARQUET;

*/	