
insert overwrite table csx_analyse.csx_analyse_report_yszx_customer_dc_rule_df partition(sdt)
select
	b.id as biz_id, -- 业务主键id
	coalesce(a.dc_code,'') as inventory_dc_code, -- dc编码
	b.customer_code, -- 客户编码
	c.customer_name, -- 客户名称
	coalesce(a.customer_flag,'') as customer_flag, -- 客户标识
	b.bind_common_product_flag, -- 绑定基础商品池 0-否 1-是
	b.create_order_auto_add_flag, -- 下单自动添加标识 0-未启动 1-已启动
	b.price_auto_add_flag, -- 报价自动添加标识 0-未启动 1-已启动
	b.remove_customer_product_flag, -- 自动移除商品池 0-关闭 1-开启
	b.must_sale_auto_add_flag, -- 必售商品自动添加标识 0-未启动 1-已启动
	b.lock_customer_product_flag, -- 锁定小程序商品池 0-未锁定 1-已锁定
	b.update_by, -- 更新者
	b.update_time, -- 更新时间
	b.create_by, -- 创建者
	b.create_time, -- 创建时间
	b.lock_mall_product_flag, -- 锁定中台商品池 0-未锁定 1-已锁定
	b.filter_zs_flag, -- 过滤直送单标识 0-否，1-是
	b.filter_patch_flag, -- 过滤补单标识 0-否，1-是
	'${ytd}' as sdt_date, -- 日期分区
	'${ytd}' as sdt -- 分区
from
	(
	select
		dc_code,customer_code,customer_flag
	from
		csx_analyse.csx_analyse_fr_crm_customer_dc_rule_mf
	group by 
		dc_code,customer_code,customer_flag
	) a 
	right join
		(
		select
			id,customer_code,bind_common_product_flag,create_order_auto_add_flag,price_auto_add_flag,remove_customer_product_flag,
			must_sale_auto_add_flag,lock_customer_product_flag,update_by,update_time,create_by,create_time,lock_mall_product_flag,filter_zs_flag,filter_patch_flag
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_cus_product_rule_df
		where
			sdt='${ytd}'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) c on c.customer_code=b.customer_code
;

/*

create table csx_analyse.csx_analyse_report_yszx_customer_dc_rule_df(
`biz_id`                                bigint              COMMENT    '业务主键id',
`inventory_dc_code`                     string              COMMENT    'dc编码',
`customer_code`                         string              COMMENT    '客户编码',
`customer_name`                         string              COMMENT    '客户名称',
`customer_flag`                         string              COMMENT    '客户标识',
`bind_common_product_flag`              int                 COMMENT    '绑定基础商品池 0-否 1-是',
`create_order_auto_add_flag`            int                 COMMENT    '下单自动添加标识 0-未启动 1-已启动',
`price_auto_add_flag`                   int                 COMMENT    '报价自动添加标识 0-未启动 1-已启动',
`remove_customer_product_flag`          int                 COMMENT    '自动移除商品池 0-关闭 1-开启',
`must_sale_auto_add_flag`               int                 COMMENT    '必售商品自动添加标识 0-未启动 1-已启动',
`lock_customer_product_flag`            int                 COMMENT    '锁定小程序商品池 0-未锁定 1-已锁定',
`update_by`                             string              COMMENT    '更新者',
`update_time`                           timestamp           COMMENT    '更新时间',
`create_by`                             string              COMMENT    '创建者',
`create_time`                           timestamp           COMMENT    '创建时间',
`lock_mall_product_flag`                int                 COMMENT    '锁定中台商品池 0-未锁定 1-已锁定',
`filter_zs_flag`                        int                 COMMENT    '过滤直送单标识 0-否，1-是',
`filter_patch_flag`                     int                 COMMENT    '过滤补单标识 0-否，1-是',
`sdt_date`                              string              COMMENT    '日期分区'

) COMMENT '商品池运营看板-商品池线上化程度-客户商品池规则'
PARTITIONED BY (sdt string COMMENT '分区')
STORED AS PARQUET;

*/	