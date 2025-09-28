CREATE EXTERNAL TABLE csx_analyse.csx_analyse_fr_hr_red_black_sale_info
(
  sdt string comment '分区字段',
  `user_id` bigint comment '员工ID', 
  `user_number` string comment '员工工号', 
  `user_name` string comment '员工姓名', 
  `user_position` string comment '员工职位', 
  `user_position_name` string comment '员工职位名称' , 
  `sub_position_name` string comment '员工子职位名称', 
  `begin_date` string comment '入职日期', 
  `source_user_position` string comment '员工原职位', 
  `leader_user_id` bigint comment '员工 Leader ID', 
  `new_leader_user_id` string comment '新一上级ID', 
  `new_leader_user_number` string comment '新上一级工号', 
  `new_leader_user_name` string comment '新上一级姓名', 
  `province_id` string comment '省ID', 
  `province_name` string comment '省名称', 
  `city_code` string comment '市编码', 
  `city_name` string comment '市名称', 
  `leader_user_number` string comment '上一级工号', 
  `leader_user_name` string comment '上一级姓名', 
  `leader_user_position` string comment '上一级职位', 
  `leader_user_position_name` string comment '上一级职位名称', 
  `leader_source_user_position` string comment '上一级职位来源', 
  `leader_source_user_position_name` string     comment '上一级职位来源名称',
  update_time timestamp comment '更新时间'
)comment'红黑榜-销售员信息存档'
partitioned by (smt string comment '月分区，每月底执行')
STORED AS parquet

;


CREATE  TABLE IF NOT EXISTS csx_analyse.csx_analyse_fr_sales_red_black_sale_detail
( 

`sale_month` STRING  COMMENT '销售月',
`performance_region_name` STRING  COMMENT '大区名称',
`performance_province_name` STRING  COMMENT '省区名称',
`performance_city_name` STRING  COMMENT '城市名称',
`business_type_name` string   COMMENT '业务类型',
`customer_code` STRING  COMMENT '客户编码',
`customer_name` STRING  COMMENT '客户名称',
`sales_user_number` STRING  COMMENT '新销售员工号',
`sales_user_name` STRING  COMMENT '新销售员名称',
`user_position_name` STRING  COMMENT '销售员角色',
`sub_position_name` STRING  COMMENT '销售员子角色',
`begin_date` STRING  COMMENT '入职日期',
`leader_user_number` STRING  COMMENT '上级领导工号',
`leader_user_name` STRING  COMMENT '上级领导名称',
`leader_source_user_position_name` STRING  COMMENT '上级领导角色',
`new_leader_user_number` STRING  COMMENT '新上一级领导工号',
`new_leader_user_name` STRING  COMMENT '新上一级领导名称',
`new_customer_flag` INT  COMMENT '是否新客',
`sale_amt` DECIMAL (30,6) COMMENT '销售额',
`profit` DECIMAL (30,6) COMMENT '毛利额',
`profit_rate` DECIMAL (30,6) COMMENT '毛利率',
`update_time` TIMESTAMP  COMMENT '更新时间' ) 
 COMMENT '红黑榜-销售员&销售经理销售明细'
 STORED AS PARQUET
 partitioned by (smt string comment '月分区，每月底执行')
 ;



 CREATE  TABLE IF NOT EXISTS data_analysis_prd.csx_analyse_fr_sales_red_black_sale_detail
( 
id BIGINT   not null auto_increment COMMENT 'id',
`sale_month` VARCHAR(128)  COMMENT '销售月',
`performance_region_name` VARCHAR(128)   COMMENT '大区名称',
`performance_province_name` VARCHAR(128)   COMMENT '省区名称',
`performance_city_name` VARCHAR(128)   COMMENT '城市名称',
`business_type_name` VARCHAR(128)    COMMENT '业务类型',
`customer_code` VARCHAR(128)   COMMENT '客户编码',
`customer_name` VARCHAR(128)   COMMENT '客户名称',
`sales_user_number` VARCHAR(128)   COMMENT '新销售员工号',
`sales_user_name` VARCHAR(128)   COMMENT '新销售员名称',
`user_position_name` VARCHAR(128)   COMMENT '销售员角色',
`sub_position_name` VARCHAR(128)   COMMENT '销售员子角色',
`begin_date` VARCHAR(128)   COMMENT '入职日期',
`leader_user_number` VARCHAR(128)   COMMENT '上级领导工号',
`leader_user_name` VARCHAR(128)   COMMENT '上级领导名称',
`leader_source_user_position_name` VARCHAR(128)   COMMENT '上级领导角色',
`new_leader_user_number` VARCHAR(128)   COMMENT '新上一级领导工号',
`new_leader_user_name` VARCHAR(128)   COMMENT '新上一级领导名称',
`new_customer_flag` INT  COMMENT '是否新客',
`sale_amt` DECIMAL (30,6) COMMENT '销售额',
`profit` DECIMAL (30,6) COMMENT '毛利额',
`profit_rate` DECIMAL (30,6) COMMENT '毛利率',
`update_time` TIMESTAMP  COMMENT '更新时间' ,
PRIMARY key(id),
key index_month(sale_month,performance_province_name,performance_city_name)
) 
 COMMENT= 'csx_analyse_fr_hr_sales_red_black_sale_detail销售经理&销售员红黑榜'



CREATE   TABLE csx_analyse.csx_analyse_fr_hr_sales_red_black_over_detail (
  `sale_month` string COMMENT '销售月', 
  `performance_region_name` STRING  COMMENT '大区名称',
  `performance_province_name` STRING  COMMENT '省区名称',
  `performance_city_name` STRING  COMMENT '城市名称',
  `customer_code` STRING  COMMENT '客户编码',
  `customer_name` STRING  COMMENT '客户名称',
  `business_attribute_name` string COMMENT '商机属性', 
  `credit_business_attribute_name` string COMMENT '信控商机业务名称', 
  `channel_name` string comment '渠道名称', 
  `sales_user_number` STRING  COMMENT '新销售员工号',
  `sales_user_name` STRING  COMMENT '新销售员名称',
  `user_position_name` STRING  COMMENT '销售员角色',
  `sub_position_name` STRING  COMMENT '销售员子角色',
  `begin_date` STRING  COMMENT '入职日期',
  `leader_user_number` STRING  COMMENT '上级领导工号',
  `leader_user_name` STRING  COMMENT '上级领导名称',
  `leader_source_user_position_name` STRING  COMMENT '上级领导角色',
  `new_leader_user_number` STRING  COMMENT '新上一级领导工号',
  `new_leader_user_name` STRING  COMMENT '新上一级领导名称',
  `overdue_amount` decimal(38,6) comment '逾期金额', 
  `receivable_amount` decimal(38,6) comment '应收金额',
  update_time timestamp comment  '更新时间'
  )
comment'销售红黑榜-销售员逾期率'
partitioned by (smt string comment '月分区')
STORED AS parquet;



CREATE EXTERNAL TABLE data_analysis_prd.report_csx_analyse_fr_hr_sales_red_black_over_detail (
  id BIGINT   not null auto_increment COMMENT 'id',
  `sale_month` varchar(255) COMMENT '销售月', 
  `performance_region_name` varchar(255)  COMMENT '大区名称',
  `performance_province_name` varchar(255)  COMMENT '省区名称',
  `performance_city_name` varchar(255)  COMMENT '城市名称',
  `customer_code` varchar(255)  COMMENT '客户编码',
  `customer_name` varchar(255)  COMMENT '客户名称',
  `business_attribute_name` varchar(255) COMMENT '商机属性', 
  `credit_business_attribute_name` varchar(255) COMMENT '信控商机业务名称', 
  `channel_name` varchar(255) comment '渠道名称', 
  `sales_user_number` varchar(255)  COMMENT '新销售员工号',
  `sales_user_name` varchar(255)  COMMENT '新销售员名称',
  `user_position_name` varchar(255)  COMMENT '销售员角色',
  `sub_position_name` varchar(255)  COMMENT '销售员子角色',
  `begin_date` varchar(255)  COMMENT '入职日期',
  `leader_user_number` varchar(255)  COMMENT '上级领导工号',
  `leader_user_name` varchar(255)  COMMENT '上级领导名称',
  `leader_source_user_position_name` varchar(255)  COMMENT '上级领导角色',
  `new_leader_user_number` varchar(255)  COMMENT '新上一级领导工号',
  `new_leader_user_name` varchar(255)  COMMENT '新上一级领导名称',
  `overdue_amount` decimal(38,6) comment '逾期金额', 
  `receivable_amount` decimal(38,6) comment '应收金额',
  update_time timestamp comment  '更新时间',
  PRIMARY key(id),
  key index_month(sale_month,performance_province_name,performance_city_name)
  )
comment='销售红黑榜-销售员逾期率'
 
 ;

 -- 销售员商机
 
 -- 红黑榜销售员商机
 CREATE   TABLE csx_analyse.csx_analyse_fr_hr_sales_red_black_business_detail (
  `sale_month` string COMMENT '销售月', 
  days_note	string	comment '天数备注',
  business_number	string	comment '商机编号',
  customer_id	bigint	comment '客户ID',
  customer_code	string	comment '客户编码',
  customer_name	string	comment '客户名称',
  owner_user_number	string	comment '销售员工号',
  owner_user_name	string	comment   '销售员名称',
  -- owner_user_position	string	comment '销售员角色',
  user_position_name	string	comment '销售员子角色',
  -- sub_position_name	string	comment
  begin_date	string	comment  '入职日期',
  performance_region_name	string	comment '大区名称',
  performance_province_code	string	comment '省区编码',
  performance_province_name	string	comment '省区名称',
  performance_city_code	string	comment '城市编码',
  performance_city_name	string	comment '城市名称',
  business_type_code	int	comment '商机类型编码',
  business_type_name string comment '商机类型名称',
  business_stage	int	comment '商机阶段',
  business_sign_time	timestamp	comment '签约时间',
  estimate_contract_amount	decimal(26,6)	comment '预估合同金额',
  business_create_time	timestamp	comment '商机创建时间',
  contract_cycle_int	int	comment '合同周期',
  contract_cycle_desc	string	comment '合同周期描述',
  contract_number	string	comment '合同编号',
  tran_year	string	comment '合同年化',
  tran_contract_amount	decimal(26,6)	comment '合同年化金额',
  rn	int	comment '排名',
  sale_max_sdt	string	comment '销售最大日期',
  sale_after_date	string	comment '后置90天日期',
  update_time	timestamp	comment '数据同步更新时间'
)comment '销售红黑榜-销售员商机'
partitioned by (smt string comment '月分区')
STORED AS parquet;



 -- 销售员商机
 CREATE   TABLE  data_analysis_prd.report_csx_analyse_fr_hr_sales_red_black_business_detail (
  id bigint not null auto_increment COMMENT 'id',
  `sale_month` varchar(255) COMMENT '销售月', 
  days_note	varchar(255)	comment '天数备注',
  business_number	varchar(255)	comment '商机编号',
  customer_id	bigint	comment '客户ID',
  customer_code	varchar(255)	comment '客户编码',
  customer_name	varchar(255)	comment '客户名称',
  owner_user_number	varchar(255)	comment '销售员工号',
  owner_user_name	varchar(255)	comment   '销售员名称',
  -- owner_user_position	varchar(255)	comment '销售员角色',
  user_position_name	varchar(255)	comment '销售员子角色',
  -- sub_position_name	varchar(255)	comment
  begin_date	varchar(255)	comment  '入职日期',
  performance_region_name	varchar(255)	comment '大区名称',
  performance_province_code	varchar(255)	comment '省区编码',
  performance_province_name	varchar(255)	comment '省区名称',
  performance_city_code	varchar(255)	comment '城市编码',
  performance_city_name	varchar(255)	comment '城市名称',
  business_type_code	int	comment '商机类型编码',
  business_type_name varchar(255) comment '商机类型名称',
  business_stage	int	comment '商机阶段',
  business_sign_time	timestamp	comment '签约时间',
  estimate_contract_amount	varchar(255)	comment '预估合同金额',
  business_create_time	timestamp	comment '商机创建时间',
  contract_cycle_int	int	comment '合同周期',
  contract_cycle_desc	varchar(255)	comment '合同周期描述'
  contract_number	varchar(255)	comment '合同编号',
  tran_year	double	comment '合同年化',
  tran_contract_amount	double	comment '合同年化金额',
  rn	int	comment '排名',
  sale_max_sdt	varchar(255)	comment '销售最大日期',
  sale_after_date	varchar(255)	comment '后置90天日期',
  update_time	timestamp	comment '数据同步更新时间',
  PRIMARY key(id)
  key index_month(sale_month,performance_region_name,performance_province_name)
)comment= '销售红黑榜-销售员商机'
;


 -- 红黑榜销售经理应收周转
 CREATE   TABLE csx_analyse.csx_analyse_fr_hr_sales_red_black_receiveable_turnover_detail 
 (
  `sale_month` string COMMENT '销售月',
performance_region_name	string	comment '大区名称',
performance_province_name	string	comment '省区名称',
performance_city_name	string	comment   '城市名称',
sales_user_number	string	comment '销售员工号',
sales_user_name	string	comment '销售员名称',
sales_user_position	string	comment '销售员角色',
leader_user_number	string	comment '上级领导工号',
leader_user_name	string	comment '上级领导名称',
leader_user_position	string	comment '上级领导角色',
leader_source_user_position	string	comment '上级领导原始角色',
new_leader_user_number	string	comment '新上级领导工号',
new_leader_user_name	string	comment   '新上级领导名称',
accounting_cnt	int	comment '天数',
sale_amt	decimal(38,6)	comment '销售额',
excluding_tax_sales	decimal(38,6)	comment '不含税销售额',
qm_receivable_amount	decimal(38,6)	comment '期末应收金额',
qc_receivable_amount	decimal(38,6)	comment '期初应收金额',
receivable_amount	decimal(38,6)	comment '应收金额',
turnover_days	decimal(38,22)	comment   '周转天数',
update_time TIMESTAMP() comment '数据同步更新时间'
)comment '销售红黑榜-销售经理应收周转'
partitioned by (smt string comment '月分区')
STORED AS parquet;


 -- 红黑榜销售经理应收周转 同步mysql
 CREATE   TABLE data_analysis_prd.report_csx_analyse_fr_hr_sales_red_black_receiveable_turnover 
 (
  id bigint not null auto_increment COMMENT 'id',
  `sale_month` string COMMENT '销售月',
performance_region_name	string	comment '大区名称',
performance_province_name	string	comment '省区名称',
performance_city_name	string	comment   '城市名称',
sales_user_number	string	comment '销售员工号',
sales_user_name	string	comment '销售员名称',
sales_user_position	string	comment '销售员角色',
leader_user_number	string	comment '上级领导工号',
leader_user_name	string	comment '上级领导名称',
leader_user_position	string	comment '上级领导角色',
leader_source_user_position	string	comment '上级领导原始角色',
new_leader_user_number	string	comment '新上级领导工号',
new_leader_user_name	string	comment   '新上级领导名称',
accounting_cnt	int	comment '天数',
sale_amt	decimal(38,6)	comment '销售额',
excluding_tax_sales	decimal(38,6)	comment '不含税销售额',
qm_receivable_amount	decimal(38,6)	comment '期末应收金额',
qc_receivable_amount	decimal(38,6)	comment '期初应收金额',
receivable_amount	decimal(38,6)	comment '应收金额',
turnover_days	decimal(38,22)	comment   '周转天数',
update_time TIMESTAMP comment '数据同步更新时间',
PRIMARY key(id),
key index_month(sale_month,performance_region_name,performance_province_name)
)comment= '销售红黑榜-销售经理应收周转'
;


-- 红黑榜保证金逾期
 create table csx_analyse.csx_analyse_fr_hr_red_black_break_deposit_overdue ( 
sale_month	string	comment '销售月',
performance_region_name	string	comment '大区名称',
performance_province_code string comment '省区编码',
performance_province_name	string	comment '省区名称',
payment_company_code	string	comment '保证金公司编码',
credit_customer_code	string	comment '信控客户编码',
sign_customer_code	string	comment '签约客户编码',
customer_code	string	comment '实际客户编码',
customer_name	string	comment '实际客户名称',
create_time	timestamp	comment '创建时间',
follow_up_user_name	string	comment '跟进人名称',
follow_up_user_code	string	comment '跟进人编码',
sales_user_number	string	comment '销售员工号',
sales_user_name	string	comment '销售员名称',
sales_user_position	string	comment '销售员角色',
leader_user_number	string	comment '上级领导工号',
leader_user_name	string	comment '上级领导名称',
leader_source_user_position STRING comment '上级领导原始角色',
new_business_type_name	string	comment '新业务类型',
responsible_person	string	comment '负责人',
responsible_person_number	string	comment '负责人工号',
lave_write_off_amount	decimal(36,2)	comment '待销金额',
new_real_credit_code	string	comment '新实际信控客户编码',
receive_sdt	string	comment '最晚应收日期',
contract_end_date	string	comment '合同结束日期',
break_contract_date	string	comment '断约日期',
max_sdt	string	comment '应收日期&合同结束日期&断约日期中取最晚日期',
max_sale_sdt	string	comment '最晚销售日期',
receivable_amount	decimal(38,6)	comment '应收金额',
is_receive_oveder_flag	string	comment '是否应收逾期',
is_oveder_flag	string	comment '是否逾期',

update_time	timestamp	comment '数据同步更新时间'
 )  comment '销售红黑榜-保证金逾期'
 partitioned by (smt string comment '月分区')
  STORED AS parquet;


  
-- 红黑榜保证金逾期同步mysql
 create table data_analysis_prd.report_csx_analyse_fr_hr_red_black_break_deposit_overdue ( 

sale_month	varchar(255)	comment '销售月',
performance_region_name	varchar(255)	comment '大区名称',
performance_province_code varchar(255) comment '省区编码',
performance_province_name	varchar(255)	comment '省区名称',
payment_company_code	varchar(255)	comment '保证金公司编码',
credit_customer_code	varchar(255)	comment '信控客户编码',
sign_customer_code	varchar(255)	comment '签约客户编码',
customer_code	varchar(255)	comment '实际客户编码',
customer_name	varchar(255)	comment '实际客户名称',
create_time	timestamp	comment '创建时间',
follow_up_user_name	varchar(255)	comment '跟进人名称',
follow_up_user_code	varchar(255)	comment '跟进人编码',
sales_user_number	varchar(255)	comment '销售员工号',
sales_user_name	varchar(255)	comment '销售员名称',
sales_user_position	varchar(255)	comment '销售员角色',
leader_user_number	varchar(255)	comment '上级领导工号',
leader_user_name	varchar(255)	comment '上级领导名称',
leader_source_user_position varchar(255) comment '上级领导原始角色',
new_business_type_name	varchar(255)	comment '新业务类型',
responsible_person	varchar(255)	comment '负责人',
responsible_person_number	varchar(255)	comment '负责人工号',
lave_write_off_amount	decimal(36,2)	comment '待销金额',
new_real_credit_code	varchar(255)	comment '新实际信控客户编码',
receive_sdt	varchar(255)	comment '最晚应收日期',
contract_end_date	varchar(255)	comment '合同结束日期',
break_contract_date	varchar(255)	comment '断约日期',
max_sdt	varchar(255)	comment '应收日期&合同结束日期&断约日期中取最晚日期',
max_sale_sdt	varchar(255)	comment '最晚销售日期',
receivable_amount	decimal(38,6)	comment '应收金额',
is_receive_oveder_flag	varchar(255)	comment '是否应收逾期',
is_oveder_flag	varchar(255)	comment '是否逾期',

update_time	timestamp	comment '数据同步更新时间',
key index_month(sale_month,performance_region_name,performance_province_name)
 )  comment ='销售红黑榜-保证金逾期'
;


CREATE  TABLE IF NOT EXISTS data_analysis_prd.report_csx_analyse_fr_hr_red_black_break_deposit_overdue( 
  id BIGINT not null auto_increment PRIMARY key,
`sale_month` varchar(255)  COMMENT '销售月',
`performance_region_name` varchar(255)  COMMENT '大区名称',
`performance_province_code` varchar(255)  COMMENT '省区编码',
`performance_province_name` varchar(255)  COMMENT '省区名称',
`payment_company_code` varchar(255)  COMMENT '保证金公司编码',
`credit_customer_code` varchar(255)  COMMENT '信控客户编码',
`sign_customer_code` varchar(255)  COMMENT '签约客户编码',
`customer_code` varchar(255)  COMMENT '实际客户编码',
`customer_name` varchar(255)  COMMENT '实际客户名称',
`create_time` TIMESTAMP  COMMENT '创建时间',
`follow_up_user_name` varchar(255)  COMMENT '跟进人名称',
`follow_up_user_code` varchar(255)  COMMENT '跟进人编码',
`follow_up_position` varchar(255)  COMMENT '跟进人角色',
`leader_user_number` varchar(255)  COMMENT '上级领导工号',
`leader_user_name` varchar(255)  COMMENT '上级领导名称',
`leader_source_user_position` varchar(255)  COMMENT '上级领导原始角色',
`new_leader_user_number` varchar(255)  COMMENT '新上一级工号',
`new_leader_user_name` varchar(255)  COMMENT '新上一级名称',
`new_business_type_name` varchar(255)  COMMENT '新业务类型',
`responsible_person` varchar(255)  COMMENT '负责人',
`responsible_person_number` varchar(255)  COMMENT '负责人工号',
`lave_write_off_amount` DECIMAL (36,2) COMMENT '待销金额',
`new_real_credit_code` varchar(255)  COMMENT '新实际信控客户编码',
`receive_sdt` varchar(255)  COMMENT '最晚应收日期',
`contract_end_date` varchar(255)  COMMENT '合同结束日期',
`break_contract_date` varchar(255)  COMMENT '断约日期',
`max_sdt` varchar(255)  COMMENT '应收日期&合同结束日期&断约日期中取最晚日期',
`max_sale_sdt` varchar(255)  COMMENT '最晚销售日期',
`receivable_amount` DECIMAL (38,6) COMMENT '应收金额',
`is_receive_oveder_flag` varchar(255)  COMMENT '是否应收逾期',
`is_oveder_flag` varchar(255)  COMMENT '是否逾期',
`update_time` TIMESTAMP  COMMENT '数据同步更新时间' ,
key index_month(sale_month,performance_region_name,performance_province_name)
 )  comment ='销售红黑榜-保证金逾期'
; 
 



 -- 销售员评分结果表
CREATE  TABLE IF NOT EXISTS csx_analyse.csx_analyse_fr_hr_red_balck_sales_score_result_mf( 

`sale_month` STRING  COMMENT '销售月',
`performance_region_name` STRING  COMMENT '大区',
`sales_user_number` STRING  COMMENT '销售员工号',
`sales_user_name` STRING  COMMENT '销售员名称',
`user_position` STRING  COMMENT '销售员角色',
`sub_position_name` STRING  COMMENT '销售员子角色',
`begin_date` STRING  COMMENT '入职日期',
`leader_user_name` STRING  COMMENT '上级领导名称',
`top_rank` string  COMMENT '考核结果',
`total_rank` INT  COMMENT '总排名',
`total_score` DECIMAL (26,6) COMMENT '总得分',
`plan_sales_amt` DECIMAL (26,6) COMMENT '计划销售额',
`sale_amt` DECIMAL (26,6) COMMENT '销售额',
`sale_achieve_rate` DECIMAL (26,6) COMMENT '销售额达成',
`sale_rank` INT  COMMENT '销售排名',
`sale_weight` DECIMAL (26,6) COMMENT '销售权重',
`sale_score` DECIMAL (26,6) COMMENT '销售得分',
`plan_profit` DECIMAL (26,6) COMMENT '计划毛利额',
`profit` DECIMAL (26,6) COMMENT '毛利额',
`profit_achieve_rate` DECIMAL (26,6) COMMENT '毛利额达成',
`profit_rank` INT  COMMENT '毛利额排名',
`profit_weight` DECIMAL (26,6) COMMENT '毛利额权重',
`profit_score` DECIMAL (26,6) COMMENT '毛利额得分',
`new_customer_sale_amt` DECIMAL (26,6) COMMENT '新客销售额',
`new_cust_rank` INT  COMMENT '新客销售额排名',
`new_customer_weight` DECIMAL (26,6) COMMENT '新客销售额权重',
`new_cust_score` DECIMAL (26,6) COMMENT '新客销售额得分',
`new_customer_profit` DECIMAL (26,6) COMMENT '新客毛利额',
`overdue_amount` DECIMAL (26,6) COMMENT '逾期金额',
`receivable_amount` DECIMAL (26,6) COMMENT '应收金额',
`overdue_rate` DECIMAL (26,6) COMMENT '逾期率',
`overdue_rank` INT  COMMENT '逾期排名',
`overdue_weight` DECIMAL (26,6) COMMENT '逾期权重',
`overdue_score` DECIMAL (26,6) COMMENT '逾期得分',
`begin_month_customer_cn` INT  COMMENT '商机月初中旬商机数',
`end_month_customer_cn` INT  COMMENT '商机月末商机数',
`avg_customer_cn` INT  COMMENT '平均商机数=(商机月末商机数+商机月初中旬商机数)/2',
`business_cnt_rnk` INT  COMMENT '商机数量排名',
`business_cnt_weight` DECIMAL (26,6) COMMENT '商机数量权重',
`business_cnt_score` DECIMAL (26,6) COMMENT '商机数量得分',
`begin_month_contract_amt` DECIMAL (26,6) COMMENT '商机月初中旬商机金额',
`end_month_contract_amt` DECIMAL (26,6) COMMENT '商机月末商机金额',
`avg_customer_contract_amt` DECIMAL (26,6) COMMENT '平均商机金额=(商机月末商机金额+商机月初中旬商机金额)/2',
`business_amt_rnk` INT  COMMENT '商机金额排名',
`business_amt_weight` DECIMAL (26,6) COMMENT '商机金额权重',
`business_amt_score` DECIMAL (26,6) COMMENT '商机金额得分',
`lave_customer_cn` INT  COMMENT '保证金逾期客户数',
`lave_score` DECIMAL (26,6) COMMENT '保证金逾期客户得分',
`update_time` TIMESTAMP  COMMENT '数据同步更新时间' ) 
 COMMENT '销售红黑榜-销售评分结果表csx_analyse_fr_hr_red_balck_sales_score_result_mf' 
 PARTITIONED BY
 (
`smt` STRING  COMMENT '月分区，每月底执行{"FORMAT":"yyyymm"}' )
 STORED AS PARQUET



 -- 销售员评分结果表 同步mysql
create table data_analysis_prd.report_csx_analyse_fr_hr_red_balck_sales_score_result_mf (
id  BIGINT not NULL auto_increment PRIMARY KEY COMMENT '主键',
sale_month	varchar(255) COMMENT '销售月',
performance_region_name	varchar(255) COMMENT '大区',
sales_user_number	varchar(255) COMMENT '销售员工号',
sales_user_name	varchar(255) COMMENT '销售员名称',
user_position	varchar(255) COMMENT '销售员角色',
sub_position_name	varchar(255) COMMENT '销售员子角色',
begin_date	varchar(255) COMMENT '入职日期',
leader_user_name	varchar(255) COMMENT '上级领导名称',
top_rank	int COMMENT '总排名',
total_rank	int COMMENT '总排名',
total_score	DECIMAL(26,6) COMMENT '总得分',
plan_sales_amt	DECIMAL(26,6) COMMENT '计划销售额',
sale_amt	DECIMAL(26,6) COMMENT '销售额',
sale_achieve_rate	DECIMAL(26,6) COMMENT '销售额达成',
sale_rank	int COMMENT '销售排名',
sale_weight	DECIMAL(26,6) COMMENT '销售权重',
sale_score	DECIMAL(26,6) COMMENT '销售得分',
plan_profit	DECIMAL(26,6) COMMENT '计划毛利额',
profit	DECIMAL(26,6) COMMENT '毛利额',
profit_achieve_rate	DECIMAL(26,6) COMMENT '毛利额达成',
profit_rank	int COMMENT '毛利额排名',
profit_weight	DECIMAL(26,6) COMMENT '毛利额权重',
profit_score	DECIMAL(26,6) COMMENT '毛利额得分',
new_customer_sale_amt	DECIMAL(26,6) COMMENT '新客销售额',
new_cust_rank	int COMMENT '新客销售额排名',
new_customer_weight	DECIMAL(26,6) COMMENT '新客销售额权重',
new_cust_score	DECIMAL(26,6) COMMENT '新客销售额得分',
new_customer_profit	DECIMAL(26,6) COMMENT '新客毛利额',
overdue_amount	DECIMAL(26,6) COMMENT '逾期金额',
receivable_amount	DECIMAL(26,6) COMMENT '应收金额',
overdue_rate	DECIMAL(26,6) COMMENT '逾期率',
overdue_rank	int COMMENT '逾期排名',
overdue_weight	DECIMAL(26,6) COMMENT '逾期权重',
overdue_score	DECIMAL(26,6) COMMENT  '逾期得分',
begin_month_customer_cn	int COMMENT '商机月初中旬商机数',
end_month_customer_cn	int COMMENT '商机月末商机数',
avg_customer_cn	int COMMENT '平均商机数=(商机月末商机数+商机月初中旬商机数)/2',
business_cnt_rnk	int COMMENT '商机数量排名',
business_cnt_weight	DECIMAL(26,6) COMMENT '商机数量权重',
business_cnt_score	DECIMAL(26,6) COMMENT '商机数量得分',
begin_month_contract_amt	DECIMAL(26,6) COMMENT '商机月初中旬商机金额',
end_month_contract_amt	DECIMAL(26,6) COMMENT '商机月末商机金额',
avg_customer_contract_amt	DECIMAL(26,6) COMMENT '平均商机金额=(商机月末商机金额+商机月初中旬商机金额)/2',
business_amt_rnk	int COMMENT '商机金额排名',
business_amt_weight	DECIMAL(26,6) COMMENT '商机金额权重',
business_amt_score	DECIMAL(26,6) COMMENT '商机金额得分',
lave_customer_rnk	int COMMENT '保证金客户排名',
lave_customer_cn	int COMMENT '保证金客户数 无计算 0',
lave_score	DECIMAL(26,6) COMMENT '保证金得分',
update_time timestamp COMMENT '数据同步更新时间',
KEY index_month (sale_month,performance_region_name)
) comment ='红黑榜-销售员评分结果表'
;

-- 销售经理红黑榜评分结果表
create table csx_analyse.csx_analyse_fr_hr_red_balck_sales_manager_score_result_mf
(
`sale_month` STRING  COMMENT '销售月',
`performance_region_name` STRING  COMMENT '大区',
`sales_user_number` STRING  COMMENT '销售员工号',
`sales_user_name` STRING  COMMENT '销售员名称',
`user_position` STRING  COMMENT '销售员角色',
`sub_position_name` STRING  COMMENT '销售员子角色',
begin_date string COMMENT '入职日期',
top_rank string comment '考核结果',
`total_rank` INT  COMMENT '总排名',
 last_total_rank int COMMENT '末位排名',
`total_score` DECIMAL (26,6) COMMENT '总得分',
`plan_sales_amt` DECIMAL (26,6) COMMENT '计划销售额',
`sale_amt` DECIMAL (26,6) COMMENT '销售额',
`sale_achieve_rate` DECIMAL (26,6) COMMENT '销售额达成',
`sale_rank` INT  COMMENT '销售排名',
`sale_weight` DECIMAL (26,6) COMMENT '销售权重',
`sale_score` DECIMAL (26,6) COMMENT '销售得分',
`plan_profit` DECIMAL (26,6) COMMENT '计划毛利额',
`profit` DECIMAL (26,6) COMMENT '毛利额',
`profit_achieve_rate` DECIMAL (26,6) COMMENT '毛利额达成',
`profit_rank` INT  COMMENT '毛利额排名',
`profit_weight` DECIMAL (26,6) COMMENT '毛利额权重',
`profit_score` DECIMAL (26,6) COMMENT '毛利额得分',
`new_customer_sale_amt` DECIMAL (26,6) COMMENT '新客销售额',
sales_cnt int COMMENT '销售员人数',
avg_new_customer_amt DECIMAL (26,6) COMMENT '平均新客销售额',
`new_cust_rank` INT  COMMENT '新客销售额排名',
`new_customer_weight` DECIMAL (26,6) COMMENT '新客销售额权重',
`new_cust_score` DECIMAL (26,6) COMMENT '新客销售额得分',
receivable_turnover_days DECIMAL(26,6) COMMENT '应收周转天数',
receivable_turnover_rank int COMMENT '应收周转天数排名',
receivable_turnover_weight DECIMAL(26,6) COMMENT '应收周转天数权重',
receivable_turnover_score DECIMAL(26,6) COMMENT '应收周转天数得分',
receivable_trunover_sale_amt DECIMAL(26,6) COMMENT '应收周转天数销售额',
qc_receivable_amount DECIMAL(26,6) COMMENT '期初应收金额',
qm_receivable_amount DECIMAL(26,6) COMMENT '期末应收金额',
lave_customer_cn int COMMENT '保证金逾期客户数',
lave_score decimal(26,6) '保证金加减分',
lave_write_off_amount decimal(26,6) '保证金金额',
update_time timestamp COMMENT '数据同步更新时间'
)comment '红黑榜-销售经理评分结果表'
partitioned by (smt string comment '月分区')
STORED AS parquet;



-- 销售经理红黑榜评分结果表
create table data_analysis_prd.report_csx_analyse_fr_hr_red_balck_sales_manager_score_result_mf
(
  id BIGINT not null auto_increment PRIMARY key comment   'id',
`sale_month` varchar(255)  COMMENT '销售月',
`performance_region_name` varchar(255)  COMMENT '大区',
`sales_user_number` varchar(255)  COMMENT '销售员工号',
`sales_user_name` varchar(255)  COMMENT '销售员名称',
`user_position` varchar(255)  COMMENT '销售员角色',
`sub_position_name` varchar(255)  COMMENT '销售员子角色',
begin_date varchar(255) COMMENT '入职日期',
top_rank varchar(255) comment '考核结果',
`total_rank` INT  COMMENT '总排名',
last_total_rank int COMMENT '末位排名',
`total_score` DECIMAL (26,6) COMMENT '总得分',
`plan_sales_amt` DECIMAL (26,6) COMMENT '计划销售额',
`sale_amt` DECIMAL (26,6) COMMENT '销售额',
`sale_achieve_rate` DECIMAL (26,6) COMMENT '销售额达成',
`sale_rank` INT  COMMENT '销售排名',
`sale_weight` DECIMAL (26,6) COMMENT '销售权重',
`sale_score` DECIMAL (26,6) COMMENT '销售得分',
`plan_profit` DECIMAL (26,6) COMMENT '计划毛利额',
`profit` DECIMAL (26,6) COMMENT '毛利额',
`profit_achieve_rate` DECIMAL (26,6) COMMENT '毛利额达成',
`profit_rank` INT  COMMENT '毛利额排名',
`profit_weight` DECIMAL (26,6) COMMENT '毛利额权重',
`profit_score` DECIMAL (26,6) COMMENT '毛利额得分',
`new_customer_sale_amt` DECIMAL (26,6) COMMENT '新客销售额',
sales_cnt int COMMENT '销售员人数',
avg_new_customer_amt DECIMAL (26,6) COMMENT '平均新客销售额',
`new_cust_rank` INT  COMMENT '新客销售额排名',
`new_customer_weight` DECIMAL (26,6) COMMENT '新客销售额权重',
`new_cust_score` DECIMAL (26,6) COMMENT '新客销售额得分',
receivable_turnover_days DECIMAL(26,6) COMMENT '应收周转天数',
receivable_turnover_rank int COMMENT '应收周转天数排名',
receivable_turnover_weight DECIMAL(26,6) COMMENT '应收周转天数权重',
receivable_turnover_score DECIMAL(26,6) COMMENT '应收周转天数得分',
receivable_trunover_sale_amt DECIMAL(26,6) COMMENT '应收周转天数销售额',
qc_receivable_amount DECIMAL(26,6) COMMENT '期初应收金额',
qm_receivable_amount DECIMAL(26,6) COMMENT '期末应收金额',
lave_customer_cn int COMMENT '保证金逾期客户数',
lave_score decimal(26,6) '保证金加减分',
lave_write_off_amount decimal(26,6) '保证金金额',
update_time timestamp COMMENT '数据同步更新时间',
key index_month (sale_month,performance_region_name)
)comment= '红黑榜-销售经理评分结果表' 
;

-- 销售红黑榜服务管家信息

create table  csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_info
(
sdt	string COMMENT '管家信息分区', 
performance_region_code	string COMMENT '大区编码',
performance_region_name	string COMMENT '大区名称',
performance_province_code	string COMMENT '省份编码',
performance_province_name	string COMMENT '省份名称',
performance_city_code	string COMMENT  '城市编码',
performance_city_name	string COMMENT '城市名称',
customer_no	string COMMENT '客户编号',
customer_name	string COMMENT '客户名称',
service_user_work_no	string COMMENT '服务管家员工号',
service_user_name	string COMMENT '服务管家名称',
attribute_code	int COMMENT '属性编码',
attribute_name	string COMMENT  '属性名称',
business_type_code	int COMMENT '业务类型编码',
service_manager_user_position	string COMMENT '服务管家职位',
sales_user_name	string COMMENT '销售员名称',
sales_user_number	string COMMENT '销售员工号',
sales_user_position	string COMMENT '销售员职位',
cnt	bigint COMMENT '数量',
ranks	int COMMENT '排名',
update_time	timestamp COMMENT '数据同步更新时间'
)comment '红黑榜-服务管家信息表'
partitioned by (smt string comment '月分区')
stored as parquet;

-- 销售红黑榜榜-服务管家销售明细
create table csx_analyse.csx_analyse_fr_hr_red_balck_service_manager_sale_detail
 (
sale_month	string COMMENT '月份',
performance_province_name	string COMMENT '省份名称',
performance_region_name	string COMMENT '大区名称',
performance_city_name	string COMMENT '城市名称',
new_business_type_code	string COMMENT '业务类型编码',
new_business_type_name string COMMENT '业务类型名称',
customer_code	string COMMENT '客户编号',
customer_name	string COMMENT '客户名称',
sales_user_name	string COMMENT'销售员名称',
sales_user_number	string COMMENT '销售员工号',
sales_user_position	string COMMENT'销售员职位',
service_user_work_no	string COMMENT'服务管家员工号',
service_user_name	string COMMENT'服务管家名称',
service_manager_user_position	string COMMENT'服务管家职位',
new_service_user_work_no	string COMMENT '新服务管家员工号',
new_service_user_name	string COMMENT '新服务管家名称',
new_service_manager_user_position	string COMMENT'新服务管家职位',
new_customer_flag	int COMMENT'是否新客户',
sale_amt	decimal(30,6) COMMENT'销售额',
profit	decimal(30,6) COMMENT'毛利额',
avg_sale_amt	decimal(38,14) COMMENT'平均销售额=客户销售额/管家数',
avg_profit	decimal(38,14) COMMENT'平均毛利额=客户毛利额/管家数',
manager_num 	bigint COMMENT'管家数量' ,
update_time	timestamp COMMENT '数据同步更新时间'
 ) comment '销售红黑榜-服务管家销售明细表'
 partitioned by (smt string comment '月分区')
 stored as parquet;




-- 销售红黑榜榜-服务管家销售明细同步mysql
create table data_analysis_prd.report_csx_analyse_fr_hr_red_balck_service_manager_sale_detail
 (
  id BIGINT not null auto_increment PRIMARY key comment   'id',
sale_month	varchar(64) COMMENT '月份',
performance_province_name	varchar(64) COMMENT '省份名称',
performance_region_name	varchar(64) COMMENT '大区名称',
performance_city_name	varchar(64) COMMENT '城市名称',
new_business_type_code	varchar(64) COMMENT '业务类型编码',
new_business_type_name varchar(64) COMMENT '业务类型名称',
customer_code	varchar(64) COMMENT '客户编号',
customer_name	varchar(64) COMMENT '客户名称',
sales_user_name	varchar(64) COMMENT'销售员名称',
sales_user_number	varchar(64) COMMENT '销售员工号',
sales_user_position	varchar(64) COMMENT'销售员职位',
service_user_work_no	varchar(64) COMMENT'服务管家员工号',
service_user_name	varchar(64) COMMENT'服务管家名称',
service_manager_user_position	varchar(64) COMMENT'服务管家职位',
new_service_user_work_no	varchar(64) COMMENT '新服务管家员工号',
new_service_user_name	varchar(64) COMMENT '新服务管家名称',
new_service_manager_user_position	varchar(64) COMMENT'新服务管家职位',
new_customer_flag	int COMMENT'是否新客户',
sale_amt	decimal(30,6) COMMENT'销售额',
profit	decimal(30,6) COMMENT'毛利额',
avg_sale_amt	decimal(38,14) COMMENT'平均销售额=客户销售额/管家数',
avg_profit	decimal(38,14) COMMENT'平均毛利额=客户毛利额/管家数',
manager_num 	bigint COMMENT'管家数量' ,
update_time	timestamp COMMENT '数据同步更新时间',
key index_month (sale_month,performance_region_name)
 ) comment= '销售红黑榜-服务管家销售明细表'
;


-- 销售红黑榜榜-服务管家应收逾期明细
create table csx_analyse.csx_analyse_fr_hr_red_black_service_manager_receivable_overdue
(
sale_month	string COMMENT '月份',
performance_region_name	string COMMENT '业绩大区名称',
performance_province_name	string COMMENT '省区名称',
performance_city_name	string COMMENT '城市群名称',
customer_code	string COMMENT '客户编号',
customer_name	string COMMENT '客户名称', 
customer_attribute_name	string COMMENT '客户属性编码',
channel_name	string COMMENT    '渠道名称',
sales_employee_code	string COMMENT '销售员工号',
sales_employee_name	string COMMENT '销售员姓名',
user_position	string COMMENT '销售员职位',
service_user_work_no	string COMMENT '服务管家员工号',
service_user_name	string COMMENT '服务管家名称',
service_manager_user_position	string COMMENT '服务管家职位',
new_service_user_work_no	string COMMENT '新服务管家员工号',
new_service_user_name	string COMMENT '新服务管家名称',
new_service_manager_user_position	string COMMENT '新服务管家职位',
overdue_amount	decimal(38,6) COMMENT '超期应收金额',
receivable_amount	decimal(38,6) COMMENT '应收金额',
update_time	timestamp COMMENT '数据同步更新时间'
)comment '销售红黑榜-服务管家应收逾期明细表'
partitioned by (smt string comment '月分区')
stored as parquet;





-- 销售红黑榜榜-服务管家应收逾期明细
create table data_analysis_prd.report_csx_analyse_fr_hr_red_black_service_receivable_overdue
(
  id BIGINT not null auto_increment PRIMARY key comment   'id',
sale_month	varchar(255) COMMENT '月份',
performance_region_name	varchar(255) COMMENT '业绩大区名称',
performance_province_name	varchar(255) COMMENT '省区名称',
performance_city_name	varchar(255) COMMENT '城市群名称',
customer_code	varchar(255) COMMENT '客户编号',
customer_name	varchar(255) COMMENT '客户名称', 
customer_attribute_name	varchar(255) COMMENT '客户属性编码',
channel_name	varchar(255) COMMENT    '渠道名称',
sales_employee_code	varchar(255) COMMENT '销售员工号',
sales_employee_name	varchar(255) COMMENT '销售员姓名',
user_position	varchar(255) COMMENT '销售员职位',
service_user_work_no	varchar(255) COMMENT '服务管家员工号',
service_user_name	varchar(255) COMMENT '服务管家名称',
service_manager_user_position	varchar(255) COMMENT '服务管家职位',
new_service_user_work_no	varchar(255) COMMENT '新服务管家员工号',
new_service_user_name	varchar(255) COMMENT '新服务管家名称',
new_service_manager_user_position	varchar(255) COMMENT '新服务管家职位',
overdue_amount	decimal(38,6) COMMENT '超期应收金额',
receivable_amount	decimal(38,6) COMMENT '应收金额',
update_time	timestamp COMMENT '数据同步更新时间'
key index_month (sale_month,performance_region_name)
)comment ='销售红黑榜-服务管家应收逾期明细表'


-- 客户评价基础表

CREATE  TABLE IF NOT EXISTS csx_analyse.csx_analyse_fr_hr_customer_evaluation_detail_mf( 

`performance_region_name` STRING  COMMENT '大区',
`performance_province_name` STRING  COMMENT '省区',
`service_user_work_no` STRING  COMMENT '管家工号',
`service_user_name` STRING  COMMENT '管家名称',
`label` STRING  COMMENT '问题',
`answer` STRING  COMMENT '回答',
`answer_score` BIGINT  COMMENT '得分',
`customer_code` STRING  COMMENT '客户编码',
`customer_name` STRING  COMMENT '客户名称',
`user_id` BIGINT  COMMENT '用户id',
`s_month` STRING  COMMENT '月份',
`create_time` TIMESTAMP  COMMENT '创建时间',
`rn` INT  COMMENT '排序' ) 
 COMMENT 'csx_analyse_fr_hr_customer_evaluation_detail_mf' 
 PARTITIONED BY
 (
`smt` STRING  COMMENT '分区字段{"FORMAT":"yyyymm"}' )
 STORED AS PARQUET


 CREATE  TABLE IF NOT EXISTS data_analysis_prd.report_csx_analyse_fr_hr_customer_evaluation_detail_mf( 
id bigint not null auto_increment PRIMARY key comment   'id',
`performance_region_name` varchar(128)  COMMENT '大区',
`performance_province_name` varchar(128)  COMMENT '省区',
`service_user_work_no` varchar(128)  COMMENT '管家工号',
`service_user_name` varchar(128)  COMMENT '管家名称',
`label` varchar(128)  COMMENT '问题',
`answer` varchar(128)  COMMENT '回答',
`answer_score` BIGINT  COMMENT '得分',
`customer_code` varchar(128)  COMMENT '客户编码',
`customer_name` varchar(128)  COMMENT '客户名称',
`user_id` BIGINT  COMMENT '用户id',
`s_month` varchar(128)  COMMENT '月份',
`create_time` TIMESTAMP  COMMENT '创建时间',
`rn` INT  COMMENT '排序',
 key index_month (s_month,performance_region_name)
 ) 
 COMMENT ='管家客户评价基础表' 
 
 


-- 销售红黑榜榜-服务管家考核结果明细
create table csx_analyse.csx_analyse_fr_hr_red_black_service_manager_result_mf 
(
sale_month	string	comment '月份',
performance_region_name	string	comment '大区名称',
service_user_work_no	string	comment '管家工号',
sales_user_name	string	comment '管家名称',
user_position	string	comment '岗位角色',
sub_position_name	string	comment '岗位名称',
begin_date	string	comment '入职日期',
top_rank	string	comment '考核结果（红榜or黑榜）',
total_rank	int	comment '总排名',
last_total_rank	int	comment '末位排名',
total_score	double	comment '总分数',
plan_sales_amt	decimal(36,6)	comment '销售额目标',
sale_amt	decimal(36,6)	comment '实际销售额',
sale_achieve_rate	decimal(36,6)	comment '销售额达成率',
sale_rank	int	comment '销售额排名',
sale_weight	decimal(36,6)	comment '销售客权重',
sale_score	decimal(36,6)	comment '销售额得分',
plan_profit	decimal(36,6)	comment '毛利额目标',
profit	decimal(36,6)	comment '毛利额实际',
profit_achieve_rate	decimal(36,6)	comment '毛利额达成率',
profit_rank	int	comment '毛利额排名',
profit_weight	decimal(36,6)	comment '毛利额权重',
profit_score	decimal(36,6)	comment '毛利额得分',
overdue_rate	decimal(36,6)	comment '应收逾期率',
overdue_rank	int	comment '应收排名',
overdue_amount	decimal(36,6)	comment '逾期金额',
receivable_amount	decimal(38,6)	comment '应收金额',
overdue_weight	decimal(36,6)	comment '应收权重',
overdue_score	double	comment '应收逾期得分',
answer_score	decimal(36,6)	comment '客户评价得分',
answer_rank	int	comment '客户评价排名',
answer_weight	decimal(36,6)	comment '客户评价权重',
new_answer_score	decimal(36,6)	comment '客户评价权重得分',
customer_cnt	bigint	comment '客户评价客户数',
all_answer_score	decimal(36,6)	comment '客户评价总分数',
lave_customer_cn	bigint	comment '保证金逾期客户数',
lave_score	decimal(36,6)	comment '保证金加减分',
lave_write_off_amount	decimal(36,6)	comment '保证金逾期金额',
update_time	timestamp	comment '更新时间'
)comment '销售红黑榜-服务管家考核结果明细表'
partitioned by (smt string comment '分区字段')
stored as parquet;




-- 销售红黑榜榜-服务管家考核结果明细 同步mysql
create table data_analysis_prd.report_csx_analyse_fr_hr_red_black_service_manager_result_mf 
(id BIGINT not null auto_increment PRIMARY key comment   'id',
sale_month	varchar(128)	comment '月份',
performance_region_name	varchar(128)	comment '大区名称',
service_user_work_no	varchar(128)	comment '管家工号',
sales_user_name	varchar(128)	comment '管家名称',
user_position	varchar(128)	comment '岗位角色',
sub_position_name	varchar(128)	comment '岗位名称',
begin_date	varchar(128)	comment '入职日期',
top_rank	varchar(128)	comment '考核结果（红榜or黑榜）',
total_rank	int	comment '总排名',
last_total_rank	int	comment '末位排名',
total_score	double	comment '总分数',
plan_sales_amt	decimal(36,6)	comment '销售额目标',
sale_amt	decimal(36,6)	comment '实际销售额',
sale_achieve_rate	decimal(36,6)	comment '销售额达成率',
sale_rank	int	comment '销售额排名',
sale_weight	decimal(36,6)	comment '销售客权重',
sale_score	decimal(36,6)	comment '销售额得分',
plan_profit	decimal(36,6)	comment '毛利额目标',
profit	decimal(36,6)	comment '毛利额实际',
profit_achieve_rate	decimal(36,6)	comment '毛利额达成率',
profit_rank	int	comment '毛利额排名',
profit_weight	decimal(36,6)	comment '毛利额权重',
profit_score	decimal(36,6)	comment '毛利额得分',
overdue_rate	decimal(36,6)	comment '应收逾期率',
overdue_rank	int	comment '应收排名',
overdue_amount	decimal(36,6)	comment '逾期金额',
receivable_amount	decimal(38,6)	comment '应收金额',
overdue_weight	decimal(36,6)	comment '应收权重',
overdue_score	double	comment '应收逾期得分',
answer_score	decimal(36,6)	comment '客户评价得分',
answer_rank	int	comment '客户评价排名',
answer_weight	decimal(36,6)	comment '客户评价权重',
new_answer_score	decimal(36,6)	comment '客户评价权重得分',
customer_cnt	bigint	comment '客户评价客户数',
all_answer_score	decimal(36,6)	comment '客户评价总分数',
lave_customer_cn	bigint	comment '保证金逾期客户数',
lave_score	decimal(36,6)	comment '保证金加减分',
lave_write_off_amount	decimal(36,6)	comment '保证金逾期金额',
update_time	timestamp	comment '更新时间',
key idx_sale_month(sale_month) using btree
)comment= '销售红黑榜-服务管家考核结果明细表'

;

-- 客户对公司建议
create table data_analysis_prd.report_csx_analyse_fr_hr_red_black_customer_suggestion
(id BIGINT not null auto_increment PRIMARY key comment   'id',
performance_region_name	VARCHAR(255)	comment '大区名称',
performance_province_name	VARCHAR(255)	comment '省区名称',
type	VARCHAR(255)	comment '类型',
label	VARCHAR(255)	comment '问题',
answer	VARCHAR(255)	comment '回答',
customer_code	VARCHAR(255)	comment '客户编码',
customer_name	VARCHAR(255)	comment '客户名称',
service_user_work_no	VARCHAR(255)	comment '服务管家工号',
service_user_name	VARCHAR(255)	comment  '服务管家名称',
rn	int	comment '排序',
create_time	timestamp	comment '创建时间',
smt	VARCHAR(255)	comment '月份分区字段',
key idx_smt(smt) using btree
)comment '客户对公司建议';



-- 红黑榜目标导入
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=HR%252FHR_%25E6%2595%25B0%25E6%258D%25AE%25E9%2587%2587%25E9%259B%2586%252F%25E9%2594%2580%25E5%2594%25AE%25E7%25BA%25A2%25E9%25BB%2591%25E6%25A6%259C%25E7%259B%25AE%25E6%25A0%2587%25E5%25AF%25BC%25E5%2585%25A5.cpt&ref_t=design&op=write&ref_c=ed2672b3-0339-40f6-93ae-0110eec6c052



https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=f236869b-3e2e-487c-b327-a1d4dc568861