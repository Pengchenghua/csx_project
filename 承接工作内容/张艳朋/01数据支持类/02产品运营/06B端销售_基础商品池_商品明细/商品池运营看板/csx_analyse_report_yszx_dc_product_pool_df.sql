
insert overwrite table csx_analyse.csx_analyse_report_yszx_dc_product_pool_df partition(sdt)
select
	a.id as biz_id, -- 主键id
	a.inventory_dc_code, -- dc编码
	a.product_code, -- 商品编码
	b.goods_name as product_name, -- 商品名称
	a.sync_customer_product_flag, -- 是否同步客户商品池 0-否 1-是
	a.base_product_tag, -- 基础商品标签 0-否 1-是
	a.base_product_status, -- 主数据商品状态：0-正常 3-停售 6-退场 7-停购
	a.created_by, -- 创建者
	a.updated_by, -- 更新者
	a.update_time, -- 更新时间
	a.create_time, -- 创建时间
	b.classify_large_code, -- 管理大类编号
	b.classify_large_name, -- 管理大类名称
	b.classify_middle_code, -- 管理中类编号
	b.classify_middle_name, -- 管理中类名称
	b.classify_small_code, -- 管理小类编号
	b.classify_small_name, -- 管理小类名称
	b.business_division_code, -- 业务部编码(11.生鲜 12.食百)
	b.business_division_name, -- 业务部名称(11.生鲜 12.食百)
	c.purchase_price, -- 采购报价
	d.suggest_price_mid, -- 建议售价-中
	'${ytd}' as sdt_date, -- 日期分区
	'${ytd}' as sdt -- 分区
from
	(
	select
		id,inventory_dc_code,product_code,sync_customer_product_flag,base_product_tag,base_product_status,created_by,updated_by,update_time,create_time
	from
		csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
	where
		sdt='${ytd}'
	) a 
	left join
		(
		select
			goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,business_division_code,business_division_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) b on b.goods_code=a.product_code		
	left join -- 有采购报价的基础商品
		(
		select 
			warehouse_code,product_code,purchase_price
		from 
			(
			select 
				*,row_number()over(partition by warehouse_code,product_code order by last_put_time) as pm 
			from 
				csx_ods.csx_ods_csx_price_prod_effective_purchase_prices_df  
			where 
				sdt='${ytd}' 
				and regexp_replace(to_date(price_begin_time), '-', '')<='${ytd}' 
				and regexp_replace(to_date(price_end_time), '-', '')>='${ytd}' 
				and effective='true'
			) a 
		where
			a.pm=1
		group by 
			warehouse_code,product_code,purchase_price
		) c on c.warehouse_code=a.inventory_dc_code and c.product_code=a.product_code
	left join -- 有建议售价的基础商品
		(	
		select 
			warehouse_code,product_code,suggest_price_mid
		from 
			csx_ods.csx_ods_csx_price_prod_goods_price_guide_df 
		where 
			sdt='${ytd}'
			and regexp_replace(to_date(price_begin_time),'-','')<='${ytd}' 
			and regexp_replace(to_date(price_end_time),'-','')>='${ytd}' 
			-- and is_expired='false' 
		group by 
			warehouse_code,product_code,suggest_price_mid
		) d on d.warehouse_code=a.inventory_dc_code and d.product_code=a.product_code
;

/*

create table csx_analyse.csx_analyse_report_yszx_dc_product_pool_df(
`biz_id`                                bigint              COMMENT    '主键id',
`inventory_dc_code`                     string              COMMENT    'dc编码',
`product_code`                          string              COMMENT    '商品编码',
`product_name`                          string              COMMENT    '商品名称',
`sync_customer_product_flag`            int                 COMMENT    '是否同步客户商品池 0-否 1-是',
`base_product_tag`                      int                 COMMENT    '基础商品标签 0-否 1-是',
`base_product_status`                   int                 COMMENT    '主数据商品状态：0-正常 3-停售 6-退场 7-停购',
`created_by`                            string              COMMENT    '创建者',
`updated_by`                            string              COMMENT    '更新者',
`update_time`                           timestamp           COMMENT    '更新时间',
`create_time`                           timestamp           COMMENT    '创建时间',
`classify_large_code`                   string              COMMENT    '管理大类编号',
`classify_large_name`                   string              COMMENT    '管理大类名称',
`classify_middle_code`                  string              COMMENT    '管理中类编号',
`classify_middle_name`                  string              COMMENT    '管理中类名称',
`classify_small_code`                   string              COMMENT    '管理小类编号',
`classify_small_name`                   string              COMMENT    '管理小类名称',
`business_division_code`                string              COMMENT    '业务部编码(11.生鲜 12.食百)',
`business_division_name`                string              COMMENT    '业务部名称(11.生鲜 12.食百)',
`purchase_price`                        decimal(26,6)       COMMENT    '采购报价',
`suggest_price_mid`                     decimal(26,6)       COMMENT    '建议售价-中',
`sdt_date`                              string              COMMENT    '日期分区'

) COMMENT '商品池运营看板-商品池线上化程度-商品池'
PARTITIONED BY (sdt string COMMENT '分区')
STORED AS PARQUET;

*/	