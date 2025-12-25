客诉详情：增加字段的话
csx_analyse_fr_oms_complaint_detail_new_di ，1、表增加字段，2、改逻辑	
csx_analyse_fr_oms_complaint_detail_new_di_real，1、改逻辑
csx_analyse_fr_oms_complaint_detail_new_di_2mysql，1、表增加字段
job_hive2mysql_csx_analyse_fr_oms_complaint_detail_new_di ，analysis_prd.csx_analyse_fr_oms_complaint_detail_new_di 加字段，加新增字段的表对应关系



    csx_dws.csx_dws_oms_complaint_detail_di
	csx_dwd_oms_complaint_detail_df

-- 客诉清单		
drop table csx_analyse_tmp.complaint_code_list_use;
-- select * from csx_analyse_tmp.complaint_code_list_use


csx_analyse_fr_complaint_month_report_mf

	
推送时间：
	excel，
	日报：下午三点；
	周月报：根据唐哥的时间定
	
	
insert overwrite table csx_analyse.csx_analyse_fr_complaint_month_report_mf partition(month)	
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
	c.week_of_year,	
	c.week_begin,
	c.week_end,
	c.week_range,
	a.complaint_code,
	substr('${edate}',1,6) as month 
from 
	( 
	select *  		
	from  csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
	where sdt >= '${sdate}'	and sdt <='${edate}'
		and complaint_code in (select complaint_code from csx_analyse_tmp.complaint_code_list_use) 
	) a	
	left join -- 周信息
	(
	select
		calday,week_of_year,week_begin,week_end,concat(week_of_year,'(',week_begin,'-',week_end,')') as week_range
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt;	
	
	
create table csx_analyse.csx_analyse_fr_complaint_month_report_mf(
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
) COMMENT '客诉各维度月报数据'
PARTITIONED BY
 (`month` STRING  COMMENT '日期分区{"FORMAT":"yyyymm"}' );  	