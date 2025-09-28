
insert overwrite table csx_analyse.csx_analyse_report_yszx_change_product_task_df partition(sdt)
				
select
	a.id as biz_id, -- 业务主键id
	a.config_id, -- 换品配置ID
	a.inventory_dc_code, -- 库存dc编码
	a.inventory_dc_name, -- 库存dc名称
	a.sap_cus_code, -- 客户编码
	a.sap_cus_name, -- 客户名称	
	a.main_product_code, -- 待换品编码
	a.main_product_name, -- 待换品名称
	a.unit, -- 单位
	a.sales_amount, -- 销售额
	a.profit_margin, -- 毛利率
	a.profit_margin_type, -- 毛利率类型： 1：负毛利 2：低毛利 3：正常毛利
	a.change_product_code, -- 推荐品编码
	a.change_product_name, -- 推荐品名称
	a.change_unit, -- 推荐品单位
	a.status, -- 换品任务状态 1：待处理 2：已完成 3：已拒绝
	a.create_time, -- 创建时间
	a.create_by, -- 提交人
	a.update_time, -- 更新时间
	a.update_by, -- 更新者
	a.update_by_id, -- 更新者id
	h.performance_province_code, -- 省区编码
	h.performance_province_name, -- 省区名称
	h.performance_city_code, -- 城市编码
	h.performance_city_name, -- 城市名称	
	if(a.status=1,coalesce(c.rp_service_user_id_new,c.fl_service_user_id_new,c.bbc_service_user_id_new,c.sales_id_new),a.update_by_id) as operator_by_id, -- 操作人id
	if(a.status=1,coalesce(c.rp_service_user_work_no_new,c.fl_service_user_work_no_new,c.bbc_service_user_work_no_new,c.work_no_new),g.work_no) as operator_by_user_number, -- 操作人工号
	if(a.status=1,coalesce(c.rp_service_user_name_new,c.fl_service_user_name_new,c.bbc_service_user_name_new,c.sales_name_new),a.update_by) as operator_by_user_name, -- 操作人名称	
	if(b.create_time is null,0,1) as release_time_flag, -- 发布时间标识(0:无,1:有)
	if(f.customer_code is null,0,1) as change_customer_flag, -- 是否可换品客户(0:否,1:是)
	e.classify_large_code, -- 管理大类编码
	e.classify_large_name, -- 管理大类名称
	e.classify_middle_code, -- 管理中类编码
	e.classify_middle_name, -- 管理中类名称
	e.classify_small_code, -- 管理小类编码
	e.classify_small_name, -- 管理小类名称
	coalesce(d.next_finish_date,'') as next_finish_date,
	if(b.create_time is null,0,coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0)) as change_before_sale_amt,
	if(b.create_time is null,0,coalesce(a.month_ago_1_profit,a.month_ago_2_profit,a.month_ago_3_profit,0)) as change_before_profit,
	if(b.create_time is null,0,coalesce(a.month_ago_1_profit,a.month_ago_2_profit,a.month_ago_3_profit,0))/
	abs(if(b.create_time is null,0,coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0))) as change_before_profit_rate,
	'${ytd}' as sdt_date, -- 日期分区
	i.first_category_code,
	i.first_category_name,
	i.second_category_code,
	i.second_category_name,
	i.third_category_code,
	i.third_category_name,
	'${ytd}' as sdt -- 分区		
from
	(
	select
		a.id,a.config_id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
		a.profit_margin_type,a.update_by,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,a.create_date,a.create_by,a.update_by_id,
		a.month_ago_1,a.month_ago_2,a.month_ago_3,
		sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_1_sale_amt,
		sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_2_sale_amt,
		sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_3_sale_amt,					
		sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.profit else null end) as month_ago_1_profit,
		sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.profit else null end) as month_ago_2_profit,
		sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.profit else null end) as month_ago_3_profit
	from	
		(
		select
			id,config_id,inventory_dc_code,inventory_dc_name,sap_cus_code,sap_cus_name,main_product_code,main_product_name,unit,sales_amount,profit_margin,
			profit_margin_type,change_product_code,change_product_name,change_unit,status,create_time,create_by,update_time,update_by,update_by_id,
			regexp_replace(to_date(create_time),'-','') as create_date,
			regexp_replace(add_months(to_date(create_time),-1),'-','') as month_ago_1,
			regexp_replace(add_months(to_date(create_time),-2),'-','') as month_ago_2,
			regexp_replace(add_months(to_date(create_time),-3),'-','') as month_ago_3
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
		where
			sdt='${ytd}'
		) a 
		left join
			(
			select
				sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
			from
				csx_dws.csx_dws_sale_detail_di
			where
				sdt>='20220101'
				and sdt<='${ytd}'
				and channel_code in ('1','7','9')
				and business_type_code in (1)
				and order_channel_code !=4
			group by 
				sdt,inventory_dc_code,customer_code,goods_code
			) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
	group by 
		a.id,a.config_id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
		a.profit_margin_type,a.update_by,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,a.create_date,a.create_by,a.update_by_id,
		a.month_ago_1,a.month_ago_2,a.month_ago_3
	) a 
	left join
		(
		select
			id,inventory_dc_code,main_product_code,update_time,create_time
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df
		where
			sdt='${ytd}'
		) b on b.id=a.config_id
	left join
		(
		select
			customer_no,sales_id_new,work_no_new,sales_name_new,
			rp_service_user_id_new,fl_service_user_id_new,bbc_service_user_id_new,
			rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_work_no_new,bbc_service_user_name_new
		from
			csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where
			sdt='${ytd}'
		) c on c.customer_no=a.sap_cus_code	
	left join
		(
		select
			id,
			regexp_replace(to_date(update_time),'-','') as finish_date,
			regexp_replace(to_date(lead(update_time,1,'9999-12-31')over(partition by inventory_dc_code,change_product_code,sap_cus_code order by update_time)),'-','') as next_finish_date
		from
			csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
		where
			sdt='${ytd}'	
			and status=2 -- 已完成	
		) d on d.id=a.id
	left join
		(		
		select
			goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) e on e.goods_code=a.main_product_code
	left join
		(
		select
			dc_code,customer_code
		from
			csx_analyse.csx_analyse_fr_crm_customer_dc_rule_mf
		where
			customer_flag in ('Ⅰ类客户','Ⅱ类客户')
		group by 
			dc_code,customer_code
		) f on f.dc_code=a.inventory_dc_code and f.customer_code=a.sap_cus_code
	left join
		(
		select 
			id,work_no 
		from 
			csx_dim.csx_dim_uc_user_number_df 
		where 
			sdt='${ytd}'
		group by 
			id,work_no
		) g on g.id=a.update_by_id
	left join
		(
		select
			shop_code,performance_province_code,performance_province_name,performance_city_code,performance_city_name
		from
			csx_dim.csx_dim_shop
		where
			sdt='current'
		) h on h.shop_code=a.inventory_dc_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) i on i.customer_code=a.sap_cus_code
;

/*

create table csx_analyse.csx_analyse_report_yszx_change_product_task_df(
`biz_id`                                bigint              COMMENT    '业务主键id',
`config_id`                             bigint              COMMENT    '换品配置ID',
`inventory_dc_code`                     string              COMMENT    '库存dc编码',
`inventory_dc_name`                     string              COMMENT    '库存dc名称',
`sap_cus_code`                          string              COMMENT    '客户编码',
`sap_cus_name`                          string              COMMENT    '客户名称',
`main_product_code`                     string              COMMENT    '待换品编码',
`main_product_name`                     string              COMMENT    '待换品名称',
`unit`                                  string              COMMENT    '单位',
`sales_amount`                          string              COMMENT    '销售额',
`profit_margin`                         string              COMMENT    '毛利率',
`profit_margin_type`                    int                 COMMENT    '毛利率类型： 1：负毛利 2：低毛利 3：正常毛利',
`change_product_code`                   string              COMMENT    '推荐品编码',
`change_product_name`                   string              COMMENT    '推荐品名称',
`change_unit`                           string              COMMENT    '推荐品单位',
`status`                                string              COMMENT    '换品任务状态 1：待处理 2：已完成 3：已拒绝',
`create_time`                           timestamp           COMMENT    '创建时间',
`create_by`                             string              COMMENT    '提交人',
`update_time`                           timestamp           COMMENT    '更新时间',
`update_by`                             string              COMMENT    '更新者',
`update_by_id`                          bigint              COMMENT    '更新者id',
`performance_province_code`             string              COMMENT    '省区编码',
`performance_province_name`             string              COMMENT    '省区名称',
`performance_city_code`                 string              COMMENT    '城市编码',
`performance_city_name`                 string              COMMENT    '城市名称',
`operator_by_id`                        string              COMMENT    '操作人id',
`operator_by_user_number`               string              COMMENT    '操作人工号',
`operator_by_user_name`                 string              COMMENT    '操作人名称',
`release_time_flag`                     int                 COMMENT    '发布时间标识(0:无,1:有)',
`change_customer_flag`                  int                 COMMENT    '是否可换品客户(0:否,1:是)',
`classify_large_code`                   string              COMMENT    '管理大类编码',
`classify_large_name`                   string              COMMENT    '管理大类名称',
`classify_middle_code`                  string              COMMENT    '管理中类编码',
`classify_middle_name`                  string              COMMENT    '管理中类名称',
`classify_small_code`                   string              COMMENT    '管理小类编码',
`classify_small_name`                   string              COMMENT    '管理小类名称',
`next_finish_date`                      string              COMMENT    '下次完成时间',
`change_before_sale_amt`                decimal(15,4)       COMMENT    '换前商品销售金额',
`change_before_profit`                  decimal(15,4)       COMMENT    '换前商品毛利额',
`change_before_profit_rate`             decimal(15,4)       COMMENT    '换前商品毛利率',
`sdt_date`                              string              COMMENT    '日期分区',
`first_category_code`                   string              COMMENT    '一级客户分类编码',
`first_category_name`                   string              COMMENT    '一级客户分类名称',
`second_category_code`                  string              COMMENT    '二级客户分类编码',
`second_category_name`                  string              COMMENT    '二级客户分类名称',
`third_category_code`                   string              COMMENT    '三级客户分类编码',
`third_category_name`                   string              COMMENT    '三级客户分类名称'

) COMMENT '商品池运营看板-商品推荐落地表现-换品任务表'
PARTITIONED BY (sdt string COMMENT '分区')
STORED AS PARQUET;

*/	