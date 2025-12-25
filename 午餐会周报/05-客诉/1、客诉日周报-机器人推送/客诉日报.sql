	where 
		sdt>='${sdt_3m}' and sdt<='${sdt_tdm}'
		and performance_province_name not in ('东北','平台-B')
		and complaint_status_code not in(-1) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status not in(-1) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and main_category_name <> '送货后调整数量'
		and customer_name not like '%XM%' 
		and shipper_code='YHCSX'
		-- and complaint_code in( select complaint_code from csx_analyse_tmp.complaint_code_list_use) 
		-- 清单只适用于周月报，不用上面的条件
		-- 日报用上面的条件不用清单
		
	
insert overwrite table csx_analyse.csx_analyse_fr_complaint_day_report_mf partition(month)	
select 
	a.sdt,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	a.customer_name,
	a.goods_code,
	a.goods_name,
	a.classify_middle_code,
	a.classify_middle_name,		
	a.complaint_type_code,  -- 客诉类型
	a.complaint_type_name,
	a.supplier_info,-- 供应商
	a.first_level_department_code,
	a.first_level_department_name,
	a.second_level_department_code,
	a.second_level_department_name,
	0 as list_type,
	c.week_of_year,	
	c.week_begin,
	c.week_end,
	c.week_range,
	a.complaint_code,
	substr('${edate}',1,6) as month 
from 
	( select  *
	from  csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di 
	where sdt >= '${sdate}'	and sdt <='${edate}'   -- 当天到60天前
		and performance_province_name not in ('东北','平台-B')
		and complaint_status_code not in(-1) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status not in(-1) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and main_category_name <> '送货后调整数量'
		and customer_name not like '%XM%' 
	) a	
	left join -- 周信息
	(
	select
		calday,week_of_year,week_begin,week_end,concat(week_of_year,'(',week_begin,'-',week_end,')') as week_range
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt;	
	
	
create table csx_analyse.csx_analyse_fr_complaint_day_report_mf(
`sdt` string COMMENT '客诉日期',
`performance_region_name` string COMMENT '大区',
`performance_province_name` string COMMENT '省区',
`performance_city_name`  string COMMENT '城市',
`customer_code` string COMMENT '客户编码',
`customer_name` string COMMENT '客户名称',
`goods_code` string COMMENT '商品编码',
`goods_name` string COMMENT '商品名称',
`classify_middle_code` string COMMENT '管理中类编号',
`classify_middle_name` string COMMENT '管理中类名称',
`complaint_type_code` string COMMENT '客诉类型编码(小类)',
`complaint_type_name` string COMMENT '客诉类型',
`supplier_info` string COMMENT '供应商',
`first_level_department_code` string COMMENT '一级部门编码',
`first_level_department_name` string COMMENT '一级部门名称',
`second_level_department_code` string COMMENT '二级部门编码',
`second_level_department_name` string COMMENT '二级部门名称',
`list_type` int COMMENT '清单标识',
`week_of_year` string COMMENT '年周(自然周)', 
`week_begin` string COMMENT '自然周起始日期', 
`week_end` string COMMENT '自然周结束日期' ,
`week_range` string COMMENT '自然周结束日期'
) COMMENT '客诉日周月报数据'
PARTITIONED BY
 (`month` STRING  COMMENT '日期分区{"FORMAT":"yyyymm"}' );  	