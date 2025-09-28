select 
	biz_id,
	region_code,
	region_name,
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
	round(processing_time/3600,2) as processing_time,
	reason,
	plan,
	update_time, -- 更新时间
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	sdt
from 
	csx_tmp.report_oms_r_a_complaint_detail
where
	sdt=regexp_replace(substr(cast(date_sub(now(),1) as string),1,10),'-','')
	and complaint_status=30-- 10-待处理 20-已处理待确认 21-驳回待确认  30-已处理 -1-已取消
	--and create_date>='${SDATE}' and create_date<='${EDATE}'
	--and deal_date>='${SBJ}' and deal_date<='${EBJ}'
	${if(len(SDATE)==0,"","AND create_date>='"+SDATE+"'")}
	${if(len(EDATE)==0,"","AND create_date<='"+EDATE+"'")}
	${if(len(SBJ)==0,"","AND deal_date>='"+SBJ+"'")}
	${if(len(EBJ)==0,"","AND deal_date<='"+EBJ+"'")}
	${if(len(sq)==0,"","AND province_name in( '"+sq+"') ")}
	
	