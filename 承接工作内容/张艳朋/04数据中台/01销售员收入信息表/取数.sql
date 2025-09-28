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