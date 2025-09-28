
insert overwrite table csx_analyse.csx_analyse_fr_oms_complaint_detail_di partition(sdt)

select
	id,complaint_code,complaint_time,complaint_status_code,performance_region_code,performance_region_name,performance_province_code,performance_province_name,
	performance_city_code,performance_city_name,sale_order_code,customer_code,customer_name,sub_customer_code,sub_customer_name,sales_user_id,sales_user_number,
	sales_user_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,sign_company_code,
	sign_company_name,inventory_dc_code,inventory_dc_name,inventory_dc_province_code,inventory_dc_province_name,inventory_dc_city_code,inventory_dc_city_name,
	require_delivery_date,complaint_dimension,complaint_type_code,complaint_type_name,main_category_code,main_category_name,sub_category_code,sub_category_name,
	goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,complaint_qty,
	unit_name,purchase_qty,purchase_unit_name,purchase_unit_rate,sale_price,complaint_amt,complaint_describe,
	regexp_replace(evidence_imgs,'\\[\\"|\\"\\]|\\[|\\]','') as evidence_imgs,
	channel_type_code,responsible_user_id,
	responsible_user_name,designee_user_id,designee_user_name,first_level_department_code,first_level_department_name,second_level_department_code,second_level_department_name,
	department_responsible_user_id,department_responsible_user_name,replenishment_order_code,create_by_id,create_by,create_time,update_by,update_time,complaint_deal_time,
	reason,
	regexp_replace(result,'\n|\t|\r|\,|\"|\\\\n','') as result,
	-- result,
	plan,complaint_deal_status,disagree_reason,replay_report,reject_reason,disagreement_reason,complaint_reject_status,task_sync_time,complaint_date,
	basic_performance_province_code,basic_performance_province_name,basic_performance_city_code,basic_performance_city_name,
	feedback_user_id,feedback_user_name,feedback_time,refund_code,cancel_reason,need_process,generate_reason,process_result,deal_detail_id,complaint_node_code,
	complaint_node_name,recep_order_user_number,recep_order_by,
	complaint_date_time,complaint_time_de,deal_date,
	processing_time,complaint_status_name,complaint_deal_status_name,
	b.user_number as create_by_user_number,a.complaint_level,
	sdt
from 
	(
	select 
		*,
		concat(substr(create_time,1,4),'年',substr(create_time,6,2),'月',substr(create_time,9,2),'日') as complaint_date_time,
		substr(create_time,12,8) as complaint_time_de,
		regexp_replace(substr(complaint_deal_time,1,10),'-','') as deal_date,
		round((unix_timestamp(complaint_deal_time)-unix_timestamp(create_time))/3600,2) as processing_time,
		case when complaint_status_code=10 then '待判责'
			when complaint_status_code=20 then '处理中'
			when complaint_status_code=21 then '待审核'
			when complaint_status_code=30 then '已完成'
			when complaint_status_code=-1 then '已取消'
		end as complaint_status_name,
		case when complaint_deal_status=10 then '待处理'
			when complaint_deal_status=20 then '待修改'
			when complaint_deal_status=30 then '已处理待审'
			when complaint_deal_status=31 then '已驳回待审核'
			when complaint_deal_status=40 then '已完成'
			when complaint_deal_status=-1 then '已取消'
		end as complaint_deal_status_name,
		complaint_level
	from 
		csx_dws.csx_dws_oms_complaint_detail_di
	where
		1=1
	) a 
	left join
		(
		select 
			user_id,user_number,user_name 
		from 
			-- csx_dw.dws_basic_w_a_user 
			csx_dim.csx_dim_uc_user
		where 
			sdt='current' 
			and delete_flag = '0'
		group by 
			user_id,user_number,user_name 
	) b on b.user_id=a.create_by_id
;
	
/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_oms_complaint_detail_di  客诉明细表

drop table if exists csx_analyse.csx_analyse_fr_oms_complaint_detail_di;
create table csx_analyse.csx_analyse_fr_oms_complaint_detail_di(
`id`                             bigint              COMMENT    'id',
`complaint_code`                 string              COMMENT    '客诉单编码',
`complaint_time`                 timestamp           COMMENT    '客诉发生时间',
`complaint_status_code`          int                 COMMENT    '客诉状态: 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消',
`performance_region_code`        string              COMMENT    '业绩大区编码',
`performance_region_name`        string              COMMENT    '业绩大区名称',
`performance_province_code`      string              COMMENT    '业绩省区编码',
`performance_province_name`      string              COMMENT    '业绩省区名称',
`performance_city_code`          string              COMMENT    '业绩城市编码',
`performance_city_name`          string              COMMENT    '业绩城市名称',
`sale_order_code`                string              COMMENT    '销售订单编码',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sub_customer_code`              string              COMMENT    '子客户编码',
`sub_customer_name`              string              COMMENT    '子客户名称',
`sales_user_id`                  bigint              COMMENT    '业务员id',
`sales_user_number`              string              COMMENT    '业务员工号',
`sales_user_name`                string              COMMENT    '业务员名称',
`first_category_code`            string              COMMENT    '一级客户分类编码',
`first_category_name`            string              COMMENT    '一级客户分类名称',
`second_category_code`           string              COMMENT    '二级客户分类编码',
`second_category_name`           string              COMMENT    '二级客户分类名称',
`third_category_code`            string              COMMENT    '三级客户分类编码',
`third_category_name`            string              COMMENT    '三级客户分类名称',
`sign_company_code`              string              COMMENT    '签约公司编码',
`sign_company_name`              string              COMMENT    '签约公司名称',
`inventory_dc_code`              string              COMMENT    '库存地点编码',
`inventory_dc_name`              string              COMMENT    '库存地点名称',
`inventory_dc_province_code`     string              COMMENT    '库存地点省区编码',
`inventory_dc_province_name`     string              COMMENT    '库存地点省区名称',
`inventory_dc_city_code`         string              COMMENT    '库存地点城市编码',
`inventory_dc_city_name`         string              COMMENT    '库存地点城市名称',
`require_delivery_date`          int                 COMMENT    '要求送货日期',
`complaint_dimension`            int                 COMMENT    '客诉维度 1-订单 2-商品',
`complaint_type_code`            string              COMMENT    '客诉类型编码(小类)',
`complaint_type_name`            string              COMMENT    '客诉类型',
`main_category_code`             string              COMMENT    '客诉大类编码',
`main_category_name`             string              COMMENT    '客诉大类名称',
`sub_category_code`              string              COMMENT    '客诉小类编码',
`sub_category_name`              string              COMMENT    '客诉小类名称',
`goods_code`                     string              COMMENT    '商品编码',
`goods_name`                     string              COMMENT    '商品名称',
`classify_large_code`            string              COMMENT    '管理大类编号',
`classify_large_name`            string              COMMENT    '管理大类名称',
`classify_middle_code`           string              COMMENT    '管理中类编号',
`classify_middle_name`           string              COMMENT    '管理中类名称',
`classify_small_code`            string              COMMENT    '管理小类编号',
`classify_small_name`            string              COMMENT    '管理小类名称',
`complaint_qty`                  decimal(11,3)       COMMENT    '基础单位客诉商品数量',
`unit_name`                      string              COMMENT    '基础单位',
`purchase_qty`                   decimal(11,3)       COMMENT    '下单数量',
`purchase_unit_name`             string              COMMENT    '下单单位',
`purchase_unit_rate`             decimal(13,2)       COMMENT    '下单单位转换比例',
`sale_price`                     decimal(15,2)       COMMENT    '商品单价',
`complaint_amt`                  decimal(15,2)       COMMENT    '客诉金额',
`complaint_describe`             string              COMMENT    '问题描述',
`evidence_imgs`                  string              COMMENT    '凭证地址',
`channel_type_code`              int                 COMMENT    '渠道 1-oms 2-crm',
`responsible_user_id`            bigint              COMMENT    '客诉责任人id',
`responsible_user_name`          string              COMMENT    '客诉负责人姓名',
`designee_user_id`               bigint              COMMENT    '被指派人id',
`designee_user_name`             string              COMMENT    '被指派人姓名',
`first_level_department_code`    string              COMMENT    '一级部门编码',
`first_level_department_name`    string              COMMENT    '一级部门名称',
`second_level_department_code`   string              COMMENT    '二级部门编码',
`second_level_department_name`   string              COMMENT    '二级部门名称',
`department_responsible_user_id` bigint              COMMENT    '部门责任人id',
`department_responsible_user_name` string              COMMENT    '部门责任人姓名',
`replenishment_order_code`       string              COMMENT    '补货单号',
`create_by_id`                   bigint              COMMENT    '创建人id',
`create_by`                      string              COMMENT    '创建人',
`create_time`                    timestamp           COMMENT    '创建时间',
`update_by`                      string              COMMENT    '更新人',
`update_time`                    timestamp           COMMENT    '更新时间',
`complaint_deal_time`            timestamp           COMMENT    '客诉处理时间',
`reason`                         string              COMMENT    '产生原因',
`result`                         string              COMMENT    '处理结果',
`plan`                           string              COMMENT    '改进方案',
`complaint_deal_status`          int                 COMMENT    '责任环节状态编码',
`disagree_reason`                string              COMMENT    '不认同理由',
`replay_report`                  string              COMMENT    '复盘报告',
`reject_reason`                  string              COMMENT    '驳回理由',
`disagreement_reason`            string              COMMENT    '不同意驳回理由',
`complaint_reject_status`        string              COMMENT    '状态 1-新的驳回 2-处理完成',
`task_sync_time`                 timestamp           COMMENT    '任务同步时间',
`complaint_date`                 string              COMMENT    '客诉发生日期',
`basic_performance_province_code`string              COMMENT    '主数据业绩归属省区',
`basic_performance_province_name`string              COMMENT    '主数据业绩归属省区名称',
`basic_performance_city_code`    string              COMMENT    '主数据业绩归属城市',
`basic_performance_city_name`    string              COMMENT    '主数据业绩归属城市名称',
`feedback_user_id`               bigint              COMMENT    '处理人id',
`feedback_user_name`             string              COMMENT    '处理人',
`feedback_time`                  string              COMMENT    '反馈时间',
`refund_code`                    string              COMMENT    '退货单号',
`cancel_reason`                  string              COMMENT    '取消原因',
`need_process`                   int                 COMMENT    '是否判责(-1.待判责 0-无需判责 1-已判责)',
`generate_reason`                string              COMMENT    '产生原因',
`process_result`                 string              COMMENT    '处理结果',
`deal_detail_id`                 bigint              COMMENT    '客诉处理id',
`complaint_node_code`            string              COMMENT    '客诉环节编码',
`complaint_node_name`            string              COMMENT    '客诉环节名称',
`recep_order_user_number`        string              COMMENT    '接单人工号',
`recep_order_by`                 string              COMMENT    '接单人名称',
`complaint_date_time`            string              COMMENT    '客诉日期',
`complaint_time_de`              string              COMMENT    '客诉时间',
`deal_date`                      string              COMMENT    '处理日期',
`processing_time`                decimal(15,6)       COMMENT    '处理时长',
`complaint_status_name`          string              COMMENT    '客诉单状态',
`complaint_deal_status_name`     string              COMMENT    '责任环节状态',
`create_by_user_number`          string              COMMENT    '创建人工号'

) COMMENT '客诉明细表'
PARTITIONED BY (sdt string COMMENT '客诉日期分区')
STORED AS PARQUET;

*/


