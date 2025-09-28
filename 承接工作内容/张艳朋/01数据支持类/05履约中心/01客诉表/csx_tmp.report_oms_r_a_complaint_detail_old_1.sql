-- 切换tez计算引擎
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
set one_day_ago = regexp_replace(date_sub(current_date,1),'-','');

-- 目标表
set target_table=csx_tmp.report_oms_r_a_complaint_detail;

	
with current_complaint_detail as 	
(	
select
	b.sales_region_code,
	b.sales_region_name,
	b.province_code,
	b.province_name,
	b.city_group_code,
	b.city_group_name,
	a.complaint_no,
	regexp_replace(substr(a.create_time,1,10),'-','') as create_date,
	concat(substr(a.create_time,1,4),'年',substr(a.create_time,6,2),'月',substr(a.create_time,9,2),'日') as complaint_date,
	substr(a.create_time,12,8) as complaint_time,
	a.complaint_status,
	a.complaint_type_code,
	a.complaint_type_name,
	a.channel_type,
	b.work_no,
	b.sales_name,
	a.customer_code,
	b.customer_name,
	a.sub_customer_code,
	a.sub_customer_name,
	a.sale_order_no,
	c.main_category_code,
	c.main_category_name,
	c.sub_category_code,
	c.sub_category_name,
	coalesce(a.product_code,'') as product_code,
	coalesce(d.goods_name,'') as goods_name,
	coalesce(regexp_replace(a.complaint_describe,'\t',''),'') as complaint_describe,
	coalesce(a.complaint_price,'') as complaint_price,
	--concat('=HYPERLINK("',get_json_object(a.evidence_imgs,'$.[0]'),'","',get_json_object(a.evidence_imgs,'$.[0]'),'")') as evidence_imgs,
	coalesce(get_json_object(a.evidence_imgs,'$.[0]'),'') as evidence_imgs,
	coalesce(f.responsible_department_code,'') as responsible_department_code,
	coalesce(f.responsible_department_name,'') as responsible_department_name,
	coalesce(f.responsible_person_id,'') as responsible_person_id,
	coalesce(f.responsible_person_name,'') as responsible_person_name,
	coalesce(regexp_replace(e.result,'\t',''),'') as result,
	coalesce(e.create_time,'') as deal_time,
	coalesce(regexp_replace(substr(e.create_time,1,10),'-',''),'') as deal_date,
	coalesce((unix_timestamp(e.create_time)-unix_timestamp(a.create_time)),'') as processing_time,
	coalesce(regexp_replace(e.reason,'\t',''),'') as reason,
	coalesce(regexp_replace(e.plan,'\t',''),'') as plan
from
	(
	select
		id,complaint_no,sale_order_no,customer_code,complaint_dimension,product_code,complaint_type_code,complaint_type_name,sub_customer_code,sub_customer_name,
		company_code,company_name,require_delivery_date,
		product_name,qty,unit,purchase_qty,purchase_unit,purchase_unit_rate,sale_price,complaint_price,complaint_describe,evidence_imgs,channel_type,responsible_person_id,
		responsible_person_name,designee_id,designee_name,complaint_status,create_by_id,create_by,create_time,sdt
	from
		csx_ods.source_oms_r_a_complaint_detail -- 客诉单
	where
		sdt=${hiveconf:one_day_ago}
	) a 
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = ${hiveconf:one_day_ago}
		)b on b.customer_no=a.customer_code	
	left join
		(
		select
			main_category_code,main_category_name,sub_category_code,sub_category_name
		from
			csx_ods.source_oms_r_a_complaint_type_config -- 客诉类型表
		where
			sdt=${hiveconf:one_day_ago}
			and status=1
		group by 
			main_category_code,main_category_name,sub_category_code,sub_category_name
		)c on c.sub_category_code=a.complaint_type_code	
	left join
		(
		select 
			goods_id,goods_name,classify_large_code,classify_large_name,classify_middle_code,
			classify_middle_name,unit_name,standard,bar_code
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt=${hiveconf:one_day_ago}
		)d on d.goods_id=a.product_code
	left join
		(
		select
			id,complaint_no,reason,result,plan,status,disagree_reason,replay_report,create_by,create_time,update_by,update_time,sdt
		from
			csx_ods.source_oms_r_a_complaint_deal_detail -- 客诉处理详情
		where
			sdt=${hiveconf:one_day_ago}
		)e on e.complaint_no=a.complaint_no
	left join
		(
		select
			deal_detail_id,
			concat_ws(',',collect_list(responsible_department_name)) as responsible_department_name,
			concat_ws(',',collect_list(responsible_department_code)) as responsible_department_code,
			concat_ws(',',collect_list(cast(responsible_person_id as string))) as responsible_person_id,
			concat_ws(',',collect_list(responsible_person_name)) as responsible_person_name
		from
			csx_ods.source_oms_r_a_complaint_deal_detail_department -- 客诉处理责任部门
		where
			sdt=${hiveconf:one_day_ago}
		group by 
			deal_detail_id
		)f on f.deal_detail_id=e.id
)

insert overwrite table ${hiveconf:target_table} partition(sdt)	

select
	'' as biz_id,
	sales_region_code as region_code,
	sales_region_name as region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	complaint_no,
	create_date,
	complaint_date,
	complaint_time,
	complaint_status,
	complaint_type_code,
	complaint_type_name,
	channel_type,
	work_no,
	sales_name,
	customer_code,
	customer_name,
	sub_customer_code,
	sub_customer_name,
	sale_order_no,
	main_category_code,
	main_category_name,
	sub_category_code,
	sub_category_name,
	product_code,
	goods_name,
	complaint_describe,
	complaint_price,
	evidence_imgs,
	responsible_department_code,
	responsible_department_name,
	responsible_person_id,
	responsible_person_name,
	result,
	deal_time,
	deal_date,
	processing_time,
	reason,
	plan,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	${hiveconf:one_day_ago} as sdt
from
	current_complaint_detail
;

--INVALIDATE METADATA csx_tmp.report_oms_r_a_complaint_detail;
	

/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_oms_r_a_complaint_detail  客诉数据

drop table if exists csx_tmp.report_oms_r_a_complaint_detail;
create table csx_tmp.report_oms_r_a_complaint_detail(
`biz_id`                         string              COMMENT    '业务主键',
`region_code`                    string              COMMENT    '大区编码',
`region_name`                    string              COMMENT    '大区名称',
`province_code`                  string              COMMENT    '省区编码',
`province_name`                  string              COMMENT    '省区名称',
`city_group_code`                string              COMMENT    '城市编码',
`city_group_name`                string              COMMENT    '城市',
`complaint_no`                   string              COMMENT    '客诉单号',
`create_date`                    string              COMMENT    '创建日期',
`complaint_date`                 string              COMMENT    '投诉日期',
`complaint_time`                 string              COMMENT    '投诉时间',
`complaint_status`               int                 COMMENT    '投诉状态',
`complaint_type_code`            string              COMMENT    '客诉类型编码(小类)',
`complaint_type_name`            string              COMMENT    '客诉类型',
`channel_type`                   int                 COMMENT    '渠道 1-OMS 2-CRM',
`work_no`                        string              COMMENT    '业务员工号',
`sales_name`                     string              COMMENT    '业务员名称',
`customer_code`                  string              COMMENT    '客户编码',
`customer_name`                  string              COMMENT    '客户名称',
`sub_customer_code`              string              COMMENT    '子客户编码',
`sub_customer_name`              string              COMMENT    '子客户名称',
`sale_order_no`                  string              COMMENT    '销售单号',
`main_category_code`             string              COMMENT    '客诉大类编码',
`main_category_name`             string              COMMENT    '客诉大类名称',
`sub_category_code`              string              COMMENT    '客诉小类编码',
`sub_category_name`              string              COMMENT    '客诉小类名称',
`product_code`                   string              COMMENT    '商品编码',
`goods_name`                     string              COMMENT    '商品名称',
`complaint_describe`             string              COMMENT    '问题描述',
`complaint_price`                decimal(15,6)       COMMENT    '客诉金额',
`evidence_imgs`                  string              COMMENT    '凭证',
`responsible_department_code`    string              COMMENT    '责任(二级)部门编码',
`responsible_department_name`    string              COMMENT    '责任部门名称',
`responsible_person_id`          string              COMMENT    '责任人id',
`responsible_person_name`        string              COMMENT    '责任人姓名',
`result`                         string              COMMENT    '处理结果',
`deal_time`                      string              COMMENT    '处理时间',
`deal_date`                      string              COMMENT    '处理日期',
`processing_time`                int                 COMMENT    '处理时长(秒)',
`reason`                         string              COMMENT    '产生原因',
`plan`                           string              COMMENT    '改进方案',
`update_time`                    string              COMMENT    '数据更新时间'

) COMMENT '客诉数据'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	