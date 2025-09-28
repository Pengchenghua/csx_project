--csx_ods.source_oms_r_a_complaint_deal_detail   客诉处理详情
--csx_ods.source_oms_r_a_complaint_deal_detail_department 客诉处理责任部门
--csx_ods.source_oms_r_a_complaint_detail 客诉单
--csx_ods.source_oms_r_a_complaint_reject_record 客诉驳回记录
--csx_ods.source_oms_r_a_complaint_responsible_department 责任部门表
--csx_ods.source_oms_r_a_complaint_responsible_person 客诉责任人表
--csx_ods.source_oms_r_a_complaint_type_config 客诉类型表


--select
--	id,complaint_no,sale_order_no,customer_code,customer_name,sub_customer_code,sub_customer_name,company_code,company_name,complaint_dimension,complaint_type_name,product_code,
--	product_name,qty,unit,purchase_qty,purchase_unit,purchase_unit_rate,sale_price,complaint_price,complaint_describe,evidence_imgs,channel_type,responsible_person_id,
--	responsible_person_name,designee_id,designee_name,complaint_status,create_by_id,create_by,create_time,sdt
--from
--	csx_ods.source_oms_r_a_complaint_detail -- 客诉单
--where
--	sdt='20211206'
--
--	
--select
--	id,complaint_no,reason,result,plan,status,disagree_reason,replay_report,create_by,create_time,update_by,update_time,sdt
--from
--	csx_ods.source_oms_r_a_complaint_deal_detail -- 客诉处理详情
--where
--	sdt>='20211201'
--	
--select
--	id,deal_detail_id,responsible_department_name,responsible_department_code,responsible_person_id,responsible_person_name,create_by,create_time,sdt
--from
--	csx_ods.source_oms_r_a_complaint_deal_detail_department -- 客诉处理责任部门
--where
--	sdt>='20211201'
--	
--	
--select
--	id,first_level_department_code,first_level_department_name,second_level_department_code,second_level_department_name,status,sdt
--from
--	csx_ods.source_oms_r_a_complaint_responsible_department -- 责任部门表
--where
--	sdt='20211206'
--	
--select 
--	id,inventory_dc_code,responsible_person_id,responsible_person_name,sales_support_id,sales_support_name,sdt
--from
--	csx_ods.source_oms_r_a_complaint_responsible_person -- 客诉责任人表
--where
--	sdt>='20211201'
--	
--select
--	id,main_category_code,main_category_name,sub_category_code,sub_category_name,complaint_dimension,status,sdt
--from
--	csx_ods.source_oms_r_a_complaint_type_config
--where
--	sdt='20211206'
--	and status=1
--=============================================================================================================================================================================	
	
select
	a.complaint_no,
	b.province_name,
	b.city_group_name,
	concat(substr(a.create_time,1,4),'年',substr(a.create_time,6,2),'月',substr(a.create_time,9,2),'日') as complaint_date,
	substr(a.create_time,12,8) as complaint_time,
	b.sales_name,
	a.customer_code,
	b.customer_name,
	a.sale_order_no,
	c.main_category_name,
	c.sub_category_name,
	a.product_code,
	d.goods_name,
	a.complaint_describe,
	a.complaint_price,
	--a.evidence_imgs,
	concat('=HYPERLINK("',get_json_object(a.evidence_imgs,'$.[0]'),'","',get_json_object(a.evidence_imgs,'$.[0]'),'")') as evidence_imgs,
	f.responsible_department_name,
	f.responsible_person_name,
	e.result,
	e.create_time,
	round((unix_timestamp(e.create_time)-unix_timestamp(a.create_time))/60,2) as processing_time,
	e.reason,
	e.plan
from
	(
	select
		id,complaint_no,sale_order_no,customer_code,complaint_dimension,complaint_type_name,product_code,complaint_type_code,
		product_name,qty,unit,purchase_qty,purchase_unit,purchase_unit_rate,sale_price,complaint_price,complaint_describe,evidence_imgs,channel_type,responsible_person_id,
		responsible_person_name,designee_id,designee_name,complaint_status,create_by_id,create_by,create_time,sdt
	from
		csx_ods.source_oms_r_a_complaint_detail -- 客诉单
	where
		sdt='20211212'
	) a 
	left join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		)b on b.customer_no=a.customer_code	
	left join
		(
		select
			main_category_code,main_category_name,sub_category_code,sub_category_name
		from
			csx_ods.source_oms_r_a_complaint_type_config -- 客诉类型表
		where
			sdt='20211212'
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
			sdt='20211212'
		)d on d.goods_id=a.product_code
	left join
		(
		select
			id,complaint_no,reason,result,plan,status,disagree_reason,replay_report,create_by,create_time,update_by,update_time,sdt
		from
			csx_ods.source_oms_r_a_complaint_deal_detail -- 客诉处理详情
		where
			sdt='20211212'
		)e on e.complaint_no=a.complaint_no
	left join
		(
		select
			id,deal_detail_id,responsible_department_name,responsible_department_code,responsible_person_id,responsible_person_name,create_by,create_time,sdt
		from
			csx_ods.source_oms_r_a_complaint_deal_detail_department -- 客诉处理责任部门
		where
			sdt='20211212'
		)f on f.deal_detail_id=e.id
	