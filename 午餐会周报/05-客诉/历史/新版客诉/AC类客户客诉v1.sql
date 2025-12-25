drop table csx_analyse_tmp.csx_analyse_ks; 
create table csx_analyse_tmp.csx_analyse_ks 
as 
select 
complaint_code
,performance_region_name
,performance_province_name
,performance_city_name
,require_delivery_date
,customer_code
,customer_name
,sub_customer_code
,sub_customer_name
,goods_code
,goods_name
,classify_large_code
,classify_large_name
,classify_middle_code
,classify_middle_name
,classify_small_code
,classify_small_name
,complaint_qty
,unit_name
,purchase_qty
,purchase_unit_name
,complaint_amt
,complaint_type_name
,main_category_code
,main_category_name
,sub_category_code
,sub_category_name
,complaint_status_code
,complaint_deal_status
,need_process
,complaint_level
,complaint_source_code
,complaint_source_name
,first_level_department_code
,first_level_department_name
,second_level_department_code
,second_level_department_name
,cancel_reason
,customer_large_level_name
from csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
where 
	sdt>='${sdt_7dago}' and sdt<='${sdt_yes}'
	and performance_province_name not in ('东北','平台-B')
	-- and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
	and complaint_deal_status in(40) -- 客诉部门状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	-- and complaint_source = 1  --客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	and complaint_amt <> 0
	and main_category_code != '001'  -- 剔除一级退货原因编码 001送货后调整数量
	and customer_name not like '%XM%'

	
insert overwrite table csx_analyse.csx_analyse_fr_ks_aclevel_top
select 
	performance_province_name
	,customer_code	
	,customer_name
	,customer_large_level
	,ks_num
from 
	(select
			*,
			row_number() over(partition by performance_province_name, customer_large_level order by ks_num desc) w_r_num	
		from
		(select 
			performance_province_name
			,customer_code	
			,customer_name
			,customer_large_level
			,count(complaint_code) ks_num
		from csx_analyse_tmp.csx_analyse_ks 
		where customer_large_level in ('A','C')
		group by
			performance_province_name
			,customer_code	
			,customer_name
			,customer_large_level
		)a
	)a where w_r_num <=2;
		
	
	
CREATE TABLE `csx_analyse.csx_analyse_fr_ks_aclevel_top`(
  `performance_province_name` string COMMENT '省区',   
  `customer_code` string COMMENT '市调地点编码', 
  `customer_name` string COMMENT '市调地点名称', 
  `customer_large_level` string COMMENT '商品编码', 
  `ks_num` decimal(20,6) COMMENT '市调价'    
 ) COMMENT '客诉_ac类top客户客诉量';