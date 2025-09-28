--  动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

--  中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
--  启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;

insert overwrite table csx_analyse.csx_analyse_fr_sss_money_back_head_df partition (sdt)

select * from csx_ods.csx_ods_csx_b2b_sss_sss_money_back_head_df where sdt='${ytd}'

;



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sss_money_back_head_df  回款表

drop table if exists csx_analyse.csx_analyse_fr_sss_money_back_head_df;
create table csx_analyse.csx_analyse_fr_sss_money_back_head_df(
`id`                             bigint              COMMENT    '主键',
`claim_bill_no`                  string              COMMENT    '认领单号',
`serial_no`                      string              COMMENT    '收款单号',
`remedy_no`                      string              COMMENT    '原认领单号（补救单）',
`claim_employee_code`            string              COMMENT    '认领人/申请人工号',
`claim_employee_name`            string              COMMENT    '认领人/申请人姓名',
`auditor_account`                string              COMMENT    '审核人工号',
`auditor_name`                   string              COMMENT    '审核人姓名',
`bank_acc`                       string              COMMENT    '本方账号',
`acc_name`                       string              COMMENT    '本方户名',
`opp_acc_no`                     string              COMMENT    '对方账号',
`opp_acc_name`                   string              COMMENT    '对方户名',
`amount`                         string              COMMENT    '交易金额',
`trans_time`                     timestamp           COMMENT    '交易日期',
`claim_time`                     timestamp           COMMENT    '申请日期',
`fd_abs`                         string              COMMENT    '摘要信息',
`business_scene`                 string              COMMENT    '业务类型',
`cd_sign`                        string              COMMENT    '收付款标记(区分是招投标的退款，还是客户的回款，还是供应商的打款)',
`adjust_reason`                  string              COMMENT    '备注',
`posting_time`                   timestamp           COMMENT    '过账日期',
`sync_status`                    string              COMMENT    '同步状态',
`create_by`                      string              COMMENT    '创建人',
`create_time`                    timestamp           COMMENT    '创建时间',
`update_by`                      string              COMMENT    '更新人',
`update_time`                    timestamp           COMMENT    '更新时间',
`version_no`                     int                 COMMENT    '乐观锁',
`is_deleted`                     int                 COMMENT    '是否删除 0：否 1：是',
`voucher_no`                     string              COMMENT    'SAP凭证编号',
`data_source`                    int                 COMMENT    '状态：1:下发、2:导入'

) COMMENT '回款头'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/