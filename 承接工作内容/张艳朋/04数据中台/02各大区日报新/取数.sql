insert overwrite directory '/tmp/zhangyanpeng/20220823_01' row format delimited fields terminated by '\t'
select * from csx_tmp.sales_income_info_new
;



create table if not exists `csx_analyse_report_sales_income_info_new_mf` (
  `cust_type` STRING comment '销售员类别',
  `sales_name` STRING comment '业务员名称',
  `work_no` STRING comment '业务员工号',
  `income_type` STRING comment '业务员收入组类'
) comment '销售提成_销售员收入组'
partitioned by (sdt string comment '日期分区')
row format delimited fields terminated by ','


sales_income_info_new;


CREATE TABLE `sales_income_info_new` (
`id` 							 bigint(20)     	  NOT NULL AUTO_INCREMENT COMMENT '主键',
`cust_type`                      varchar(32)          DEFAULT NULL  COMMENT    '销售员类别',
`sales_name`                     varchar(32)          DEFAULT NULL  COMMENT    '业务员名称',
`work_no`                        varchar(32)          DEFAULT NULL  COMMENT    '业务员工号',
`income_type`                    varchar(32)          DEFAULT NULL  COMMENT    '业务员收入组类',
`sdt_date`                       varchar(32)          DEFAULT NULL  COMMENT    '日期分区',


  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='销售员收入组';

select * from sales_income_info_new limit 100;

select count(*) from sales_income_info_new ;


SELECT DISTINCT region_code,region_name
	 FROM csx_dw.dws_sale_w_a_area_belong 
	 where region_code in ('1','2','3','4') 
	 and province_code in ('${dist}')


SELECT 
	DISTINCT performance_region_code as region_code,performance_region_name as region_name
FROM 
	-- csx_dw.dws_sale_w_a_area_belong 
	csx_dim.csx_dim_sales_area_belong_mapping
where 
	sdt='current'
	and performance_region_code in ('1','2','3','4') 
	and performance_province_code in ('${dist}')
;

select 
	* 
from 
	-- csx_dw.report_wms_r_m_back_order
	csx_analyse.csx_analyse_fr_wms_back_order_mi
where 
	month  = '${SDATE}' and performance_region_code  = '${dq}' 
;

select * from csx_dw.report_sss_r_m_receivable_ovd_amt_pr
where month ='${SDATE}' and region_code  = '${dq}' ;

select 
	* 
from 
	-- csx_dw.report_sss_r_m_receivable_ovd_amt_pr
	csx_analyse.csx_analyse_fr_sss_receivable_ovd_amt_pr_mi
where 
	month ='${SDATE}' and performance_region_code  = '${dq}' 
;

select * from csx_dw.report_sss_r_m_receivable_overdue_amount
where month ='${SDATE}' and region_code  = '${dq}' 
---全国各省	${if(len(dqsq)==0,"","and province_code in ('"+substitute(dqsq,",","','")+"')")} 
;

select 
	* 
from 
	-- csx_dw.report_sss_r_m_receivable_overdue_amount
	csx_analyse.csx_analyse_fr_sss_receivable_overdue_amount_mi
where 
	month ='${SDATE}' and performance_region_code  = '${dq}' 
;

select * from  csx_dw.report_sss_r_m_overdue_customer_top10
where month ='${SDATE}' and region_code  = '${dq}' 
;

select 
	* 
from  
	-- csx_dw.report_sss_r_m_overdue_customer_top10
	csx_analyse.csx_analyse_fr_sss_overdue_customer_top10_mi
where 
	month ='${SDATE}' and performance_region_code  = '${dq}' 
;

select * from  csx_dw.report_sale_r_m_customer_new_regular
where month ='${SDATE}' and region_code  = '${dq}' 
---全国各省 ${if(len(dqsq)==0,"","and province_code in ('"+substitute(dqsq,",","','")+"')")} 
;

select 
	* 
from  
	-- csx_dw.report_sale_r_m_customer_new_regular
	csx_analyse.csx_analyse_fr_sale_customer_new_regular_mi
where 
	month ='${SDATE}' and performance_region_code  = '${dq}'
;


select 
	* 
from 
	csx_dw.report_sale_r_w_customer_new_regular
where 
	week ='${week}' and region_code  = '${dq}'
---${if(len(dqsq)==0,"","and province_code in ('"+substitute(dqsq,",","','")+"')")}	
;

select 
	* 
from 
	-- csx_dw.report_sale_r_w_customer_new_regular
	csx_analyse.csx_analyse_fr_sale_customer_new_regular_week_di
where 
	week ='${week}' and performance_region_code  = '${dq}'
;

select distinct week from csx_dw.report_sale_r_w_customer_new_regular
order by week desc
;

select 
	distinct week 
from 
	-- csx_dw.report_sale_r_w_customer_new_regular
	csx_analyse.csx_analyse_fr_sale_customer_new_regular_week_di
order by 
	week desc

