-- 公海客户

insert overwrite directory '/tmp/zhangyanpeng/20211109_01_01' row format delimited fields terminated by '\t'

select
	row_number() over() as rn,
	concat_ws('/',b.region_name,c.region_name,d.region_name) as area_type,
	a.customer_name,
	a.first_category_name,
	a.second_category_name,
	a.third_category_name,
	e.name,
	a.contact_person,
	a.contact_phone,
	a.customer_address_full
from
	(
	select 
		customer_name,first_category_name,second_category_name,third_category_name,registered_capital,contact_person,contact_phone,customer_address_full,customer_address_details
	from 
		csx_dw.dws_crm_w_a_customer_union
	where 
		sdt = 'current' 
		and sales_id = 0
	) a 
	left join 
		(
		select 
			region_code,region_name
		from 
			csx_ods.source_crm_w_a_sys_region
		) b on get_json_object(a.customer_address_details, '$.province')=b.region_code
	left join 
		(
		select 
			region_code,region_name
		from 
			csx_ods.source_crm_w_a_sys_region
		) c on get_json_object(a.customer_address_details, '$.city')=c.region_code		
	left join 
		(
		select 
			region_code,region_name
		from 
			csx_ods.source_crm_w_a_sys_region
		) d on get_json_object(a.customer_address_details, '$.area')=d.region_code	
	left join
		(
		select 
			code,name 
		from 
			csx_ods.source_crm_w_a_sys_dict
		where 
			parent_code = 'registered_capital'
		group by 
			code,name 
		) e on e.code=a.registered_capital
where
	b.region_name = '北京市' 
		
		
	
;

-- 无归属人的线索

insert overwrite directory '/tmp/zhangyanpeng/20211109_01_02' row format delimited fields terminated by '\t'

select
	row_number() over() as rn,
	concat_ws('/',b.region_name,c.region_name,d.region_name) as area_type,
	a.customer_name,
	a.first_category_name,
	a.second_category_name,
	a.third_category_name,
	e.name,
	a.contact_person,
	a.contact_phone,
	a.customer_address_full
from
	(
	select 
		customer_id,customer_name,first_category_name,second_category_name,third_category_name,registered_capital,contact_person,contact_phone,customer_address_full,customer_address_details
	from 
		csx_dw.dws_crm_w_a_customer_union
	where 
		sdt = 'current' 
		--and sales_id = 0
	) a 
	left join 
		(
		select 
			region_code,region_name
		from 
			csx_ods.source_crm_w_a_sys_region
		) b on get_json_object(a.customer_address_details, '$.province')=b.region_code
	left join 
		(
		select 
			region_code,region_name
		from 
			csx_ods.source_crm_w_a_sys_region
		) c on get_json_object(a.customer_address_details, '$.city')=c.region_code		
	left join 
		(
		select 
			region_code,region_name
		from 
			csx_ods.source_crm_w_a_sys_region
		) d on get_json_object(a.customer_address_details, '$.area')=d.region_code	
	left join
		(
		select 
			code,name 
		from 
			csx_ods.source_crm_w_a_sys_dict
		where 
			parent_code = 'registered_capital'
		group by 
			code,name 
		) e on e.code=a.registered_capital
	join
		(
		select 
			customer_id,customer_no
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current' 
			and customer_no = '' 
			and sales_id = 0
		) f on f.customer_id=a.customer_id
where
	b.region_name = '北京市' 
;
