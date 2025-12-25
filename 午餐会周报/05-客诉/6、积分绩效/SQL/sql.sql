-- 客诉详情表


select * from 
(select
	complaint_code
	,performance_region_name
	,performance_province_name
	,performance_city_name
	,complaint_date_time
	,complaint_time_de
	,delivery_time
	,delivery_date
	,inventory_dc_code
	,delivery_type_name
	,new_direct_delivery_type
	,sales_user_name
	,rp_service_user_name_new
	,customer_code
	,customer_name
	,sub_customer_code
	,sub_customer_name
	,sale_order_code
	,business_type_name
	,main_category_name
	,sub_category_name
	,goods_code
	,goods_name
	,regionalized_goods_name
	,goods_remarks
	,concat(purchase_qty,purchase_unit_name) purchase_qty_unit_name
	,send_qty
	,regexp_replace(complaint_describe,'\n|\t|\r|\,|\"|\\\\n','') as complaint_describe_name
	,refund_qty
	,concat(complaint_qty,unit_name)complaint_qty_unit_name
	,complaint_amt
	,evidence_imgs
	,department_name
	,department_responsible_user_name
	,result
	,complaint_deal_time
	,processing_time
	,reason
	,plan
	,detail_plan
	,is_repeat
	,classify_large_name
	,classify_middle_name
	,classify_small_name
	,complaint_status_name
	,complaint_deal_status_name
	,complaint_node_name
	,create_by_user_number
	,create_by
	,cost_center_name
	,complaint_level_name
	,purchase_qty
	,purchase_unit_name
	,complaint_qty
	,unit_name
	,strategy_status_name
	,strategy_user_name
	,disagree_reason
	,update_by
	,cancel_reason
	,recep_order_by
	,complaint_source_name
	,refund_code
	,replenishment_order_code
	,customer_large_level
	,first_person
	,hand_person
	,end_person
	,sdt_refund
	,has_goods_name
	,reason_detail
	,refund_create_by
	,stock_process_type
	,complaint_category
	,reason_original
	,change_content_after
	,change_content_1
	,reject_reason
	,submit_by_flag
	,picking_type
	,pick_by
	,touxian_type
	,touxian_by
	,carrier
	,carrier_name
	,driver_name
	,supplier_info
    ,get_json_object(person_json, '$.userNumber') as numberuser
    ,get_json_object(person_json, '$.userName') as nameuser

from
    csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di a
LATERAL VIEW posexplode(
    split(
        regexp_replace(
            regexp_replace(responsible_person, '^\\[|\\]$', ''),
            '\\}\\,\\s*\\{',
            '\\}\\|\\|\\{'
        ),
        '\\|\\|'
    )
) persons as pos, person_json
	WHERE sdt>='20250901' 
) a where nameuser is not null;




-- 员工信息
select 
   user_number,
   user_name,
   user_position,
    province_name,
    city_name
from csx_dim.csx_dim_uc_user
where sdt ='current' -- and status=1
and province_name is not null;



-- 在职离职人员信息表在哪里取
select 
	a.*,
	b.province_name,
	b.city_name
from 
(select 
	employee_code,
	employee_name,
	position_title_name,
	employee_type,
	employee_org_name -- 人员组织名称
from csx_dim.csx_dim_basic_employee
where sdt='current' and employee_status =3
)a
left join 
	(select * from csx_dim.csx_dim_uc_user 
	where sdt ='current'
	) b on a.employee_code =b.user_number