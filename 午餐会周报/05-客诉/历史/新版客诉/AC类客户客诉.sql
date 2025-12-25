drop table csx_analyse_tmp.csx_analyse_ks; 
create table csx_analyse_tmp.csx_analyse_ks 
as 
select 
	a.*,
	h.customer_large_level
from 	
(select
	complaint_code	-- 客诉单编码
	,performance_region_name
	,performance_province_name
	,performance_city_name
	,require_delivery_date -- 要求送货日期
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
	,complaint_qty	      -- 客诉数量
	,unit_name             -- 单位	
	,purchase_qty	      -- 下单数量
	,purchase_unit_name    -- 单位
	,complaint_amt    -- 客诉金额
	,complaint_type_name    -- 客诉类型
	,main_category_code   -- 客诉大类编码
	,main_category_name
	,sub_category_code	 -- 客诉小类编码
	,sub_category_name
	-- ,reason   -- 客诉部门产生原因
	,complaint_status_code	 -- 客诉状态: 10-待判责 20-处理中 21-待审核 30-已完成 -1-已取消
	,complaint_deal_status   -- 客诉部门状态 10-待处理 20-待修改 30-已处理待审 31-已驳回待审核 40-已完成 -1-已取消
	,need_process -- 是否判责(-1.待判责 0-无需判责 1-已判责)
	,complaint_level  -- 客诉等级：0-一级紧急 1-一级非紧急 2-二级 3-三级
	,complaint_source  -- 客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	,first_level_department_code  -- 一级部门	
	,first_level_department_name	  
	,second_level_department_code -- 二级部门
	,second_level_department_name
	,cost_department_code	-- 成本归属部门
	,cost_department_name
	,cancel_reason -- 取消原因
from  csx_dws.csx_dws_oms_complaint_detail_di
where 
	sdt>='${sdt_7dago}' and sdt<='${sdt_yes}'
	and performance_province_name not in ('东北','平台-B')
	-- and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
	and complaint_deal_status in(40) -- 客诉部门状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	-- and complaint_source = 1  --客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	and complaint_amt <> 0
	and main_category_code != '001'  -- 剔除一级退货原因编码 001送货后调整数量
	and customer_name not like '%XM%'	
)a
left join -- 客户等级
	(
	select 
		customer_no,customer_large_level,month
	from csx_analyse.csx_analyse_report_sale_customer_level_mf
	where month = substr('${sdt_yes}',1,6)
		and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
	) h on h.customer_no=a.customer_code;

	
insert overwrite table csx_analyse.csx_analyse_fr_ks_aclevel_top
select 
	performance_province_name
	,customer_code	
	,customer_name
	,customer_large_level
	,ks_num
	,w_r_num
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
	)a where w_r_num <=3;
		
	
	
CREATE TABLE `csx_analyse.csx_analyse_fr_ks_aclevel_top`(
  `performance_province_name` string COMMENT '省区',   
  `customer_code` string COMMENT '市调地点编码', 
  `customer_name` string COMMENT '市调地点名称', 
  `customer_large_level` string COMMENT '商品编码', 
  `ks_num` decimal(20,6) COMMENT '市调价'    
 ) COMMENT '客诉_ac类top客户客诉量';