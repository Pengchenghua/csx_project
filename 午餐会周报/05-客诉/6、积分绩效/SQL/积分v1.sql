-- 客诉详情表：
-- ！！！！！！！
-- 
-- 要和唐哥那边的清单对齐！！！！
-- 他那边有剔除，不要全量的数据，以他的版本整理：
-- 1、除订单部门以外，其他的剔除没有责任人的
-- 2、关联是否发起错误，这些数据复制出去一份，贴到发起错误那里
-- 3、订单发过来要扣分的客诉单号+责任人，保留这部分订单部门其他的都删掉；责任人也用他们的
-- 4、物流剔除
-- 5、采购剔除

-- 拿出来责任人，一人一条数据，这里有的人取不到工号
-- 发起错误客诉单，每周唐寅发出来，手动导入 csx_analyse_tmp.complaint_error
-- 供应商名称含有“彩食鲜”字段的定位“自营司机”，其他的为承运商司机；
-- 扣分标准：
-- 1、发起错误-自营司机，扣给自营司机，1分；发起错误-承运商司机，扣分给发运主管，打五折，扣一分
-- 2、再根据客诉等级扣分，再加品类打折，再加直送打折
	

-- 手动表：

-- 唐客诉清单
select * from csx_analyse_tmp.complaint_code_list_use
where smonth ='202510'

-- 发起错误清单
csx_analyse_tmp.complaint_error

-- 发运主管信息
csx_analyse_tmp.complaint_shipper_manager 

	
drop table if exists csx_analyse_tmp.csx_analyse_tmp_jf;
create table csx_analyse_tmp.csx_analyse_tmp_jf as 
SELECT 
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
	,regexp_replace(sub_customer_name,'\n|\t|\r|\,|\"|\\\\n','') sub_customer_name
	,sale_order_code
	,business_type_name
	,main_category_name
	,sub_category_name
	,goods_code
	,regexp_replace(goods_name,'\n|\t|\r|\,|\"|\\\\n','') goods_name
	,regionalized_goods_name
	,regexp_replace(goods_remarks,'\n|\t|\r|\,|\"|\\\\n','') goods_remarks_name
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
    ,get_json_object(persons.person_json, '$.userNumber') as usernumber
    ,get_json_object(persons.person_json, '$.userName') as username
FROM 
	(
    SELECT * 
    FROM csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
    WHERE sdt >= '20251101' 
        AND sdt <= '20251130' 
        AND reason NOT IN (
            '质量问题-客户超标准验收',
            '数量问题-客户下错', 
            '数量问题-客户换补/零星补货',
            '规格问题-客户下错',
            '价格问题-价格不认可',
            '客户原因-客户超标准验收')
	) a
	LATERAL VIEW OUTER posexplode(                            
    split(
        regexp_replace(
            regexp_replace(
                COALESCE(NULLIF(responsible_person, ''), '[]'), 
                '^\\[|\\]$', ''
            ),
            '\\}\\,\\s*\\{',
            '\\}\\|\\|\\{'
        ),
        '\\|\\|'
    )
	) persons AS pos, person_json;	
	
select * from csx_analyse_tmp.csx_analyse_tmp_jf	
	
SELECT * FROM csx_analyse_tmp.csx_analyse_tmp_jf a
-- 添加的条件：department_name='订单共享'时，username空和不空的都要
-- department_name<>'订单共享'时，只取username有值的
WHERE 
    CASE 
        WHEN department_name LIKE '订单共享%' THEN 1  -- 全部返回
        WHEN username IS NOT NULL THEN 1             -- username不为空时返回
        ELSE 0
    END = 1;
	
	

SELECT 
    a.*
	,IF(a.driver_type='承运商司机' AND a.main_category_name='配送问题','承运商司机客诉-发运主管扣分','') as driver_manager_fault,
	,IF(c.complaint_code IS NOT NULL, '发起错误', '') AS has_error
    ,d.managernumber  -- 发运主管工号
    ,d.managername
	,CASE complaint_level_name
		WHEN '特大' THEN 5
		WHEN '一级' THEN 2  
		WHEN '二级' THEN 1
		WHEN '三级' THEN 0.5
	ELSE 0 END as complaint_level_score -- 客诉分级扣分
	,CASE classify_large_name
		WHEN '蔬菜水果' THEN 0.5
		WHEN '肉禽水产' THEN 0.8   
	ELSE 1 END as classify_large_discount_score -- 管理大类打折
	,CASE when delivery_type_name ='直送' then 0.5 
	ELSE 1 END as delivery_discount_score -- 直送打折	
		
(SELECT *,
	IF(carrier_name LIKE '%彩食鲜%', '自营司机', '承运商司机') AS driver_type
FROM csx_analyse_tmp.csx_analyse_tmp_jf
WHERE 
    CASE 
        WHEN a.department_name LIKE '订单共享%' THEN 1  -- 全部返回
        WHEN a.username IS NOT NULL THEN 1             -- username不为空时返回
        ELSE 0
    END = 1
)  a
JOIN csx_analyse_tmp.complaint_code_list_use b ON a.complaint_code=b.complaint_code   -- 与唐哥清单保持一致
LEFT JOIN csx_analyse_tmp.complaint_error c ON a.complaint_code = c.complaint_code    -- 是否发起错误
LEFT JOIN csx_analyse_tmp.complaint_shipper_manager d ON a.performance_city_name = d.performance_city_name  -- 发运主管信息


/*	
SELECT 
    a.*
    ,IF(a.carrier_name LIKE '%彩食鲜%', '自营司机', '承运商司机') AS driver_type
    ,d.managernumber  -- 发运主管工号
    ,d.managername
    ,IF(c.complaint_code IS NOT NULL, '发起错误', '') AS has_error
FROM csx_analyse_tmp.csx_analyse_tmp_jf  a
JOIN csx_analyse_tmp.complaint_code_list_use b ON a.complaint_code=b.complaint_code   -- 与唐哥清单保持一致
LEFT JOIN csx_analyse_tmp.complaint_error c ON a.complaint_code = c.complaint_code    -- 是否发起错误
LEFT JOIN csx_analyse_tmp.complaint_shipper_manager d ON a.performance_city_name = d.performance_city_name  -- 发运主管信息
-- 新增条件关联表f
LEFT JOIN csx_analyse_tmp.complaint_code_order f ON  a.complaint_code=f.complaint_code  -- 订单稽核后需要扣分的单子，其他的不要

WHERE (a.department_name = '订单共享' AND f.complaint_code IS NOT NULL)
   OR (a.department_name <> '订单共享' AND a.username IS NOT NULL)	
	
	
	
	
	
	
	
	
	
	
	
	
	
select * from  csx_analyse_tmp.csx_analyse_tmp_jf
where has_error <> '发起错误'


select distinct 
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
	,goods_remarks_name
	,purchase_qty_unit_name
	,send_qty
	,complaint_describe_name
	,refund_qty
	,complaint_qty_unit_name
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
    ,driver_type
	,if(driver_type='承运商司机',managernumber,'') usernumber
	,if(driver_type='承运商司机',managername,driver_name) username
from  csx_analyse_tmp.csx_analyse_tmp_jf
where has_error = '发起错误'
	
	
	


	

(select 
	distinct 
	complaint_code
	,complaint_date_time
	,delivery_type_name
	,department_name
	,reason
	,classify_large_name
	,complaint_level_name
	,carrier_name
	,driver_name
	,supplier_info
	,if(has_error='发起错误', 0,usernumber) as usernumber
	,if(has_error='发起错误', 0,username) as username	
	,driver_type
	,managernumber
	,managername
	,has_error
	,complaint_level_score
	,classify_large_discount_score
	,delivery_discount_score
	case when has_error='发起错误' and driver_type='自营司机' then '发起错误-自营司机'
		 when has_error='发起错误' and driver_type='承运商司机' then '发起错误-承运商司机'
	else '个人' end complaint_locate,  -- 扣分定位最终责任方	
	
	case when has_error='发起错误' and driver_type='自营司机' then ''
	     when has_error='发起错误' and driver_type='承运商司机' then managernumber
	else usernumber end complaint_locate_number,  -- 扣分人员工号

	case when has_error='发起错误' and driver_type='自营司机' then driver_name
	     when has_error='发起错误' and driver_type='承运商司机' then managername
	else username end complaint_locate_name,      -- 扣分人员名称

	case 
		when has_error='发起错误' and driver_type='自营司机' then 1
		when has_error='发起错误' and driver_type='承运商司机' then 0.5
	else complaint_level_score*classify_large_discount_score*delivery_discount_score 
	end null as final_score		
from csx_analyse_tmp.csx_analyse_tmp_jf


-- 员工信息
select 
   user_number,
   user_name,
   user_position,
   user_channel,
    province_name,
    city_name
from csx_dim.csx_dim_uc_user
where sdt ='current' -- and status=0
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
	position_name,-- 职位
	employee_org_name -- 人员组织名称
from csx_dim.csx_dim_basic_employee
where sdt='current' and employee_status =3
)a
left join 
	(select * from csx_dim.csx_dim_uc_user 
	where sdt ='current'
	) b on a.employee_code =b.user_number
	
	
	