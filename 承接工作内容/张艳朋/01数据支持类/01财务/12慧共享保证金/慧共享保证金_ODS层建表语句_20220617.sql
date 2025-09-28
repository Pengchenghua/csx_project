-- DROP TABLE IF EXISTS `source_sss_r_a_sss_incidental_write_off` ;
CREATE TABLE `source_sss_r_a_sss_incidental_write_off` (
`id` string COMMENT '主键ID',
`incidental_expenses_no` string COMMENT '杂项用款单号',
`purchase_code` string COMMENT '所属大区代码',
`purchase_name` string COMMENT '所属大区名称',
`company_code` string COMMENT '公司编码',
`company_name` string COMMENT '公司名称',
`payment_unit_name` string COMMENT '签约主体',
`payment_company_code` string COMMENT '实际付款公司编码',
`payment_company_name` string COMMENT '实际付款公司名称',
`receiving_customer_code` string COMMENT '收款客户编码',
`receiving_customer_name` string COMMENT '收款客户名称',
`business_scene` string COMMENT '业务场景名称',
`business_scene_code` string COMMENT '业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约',
`payment_amount` string COMMENT '付款金额',
`write_off_amount` string COMMENT '核销金额',
`lave_write_off_amount` string COMMENT '剩余待核销金额',
`payment_status` string COMMENT '付款状态  S:付款成功 T:已退汇',
`payment_method` string COMMENT '支付方式  1:外付网银 2:线下',
`apply_user` string COMMENT '申请人',
`responsible_person` string COMMENT '负责人',
`responsible_person_phone` string COMMENT '负责人电话',
`write_off_status` string COMMENT '核销状态  0:未核销  1:已核销 2:部分核销',
`write_off_type` string COMMENT '核销类型  1:慧共享核销  2:手工核销',
`apply_date` string COMMENT '单据申请日期',
`apply_reason` string COMMENT '申请事由',
`entry_company_code` string COMMENT '入账单位编码',
`entry_company_name` string COMMENT '入账单位名称',
`profit_center_code` string COMMENT '利润中心编码',
`profit_center_name` string COMMENT '利润中心名称',
`cost_center_code` string COMMENT '成本中心编码',
`cost_center_name` string COMMENT '成本中心名称',
`assignment_number` string COMMENT '分配号',
`currency` string COMMENT '币种',
`audit_status` string COMMENT '审批状态',
`approved_date` string COMMENT '单据审批通过日期',
`voucher_code` string COMMENT '凭证编码',
`receiving_account` string COMMENT '收款账号',
`unpaid_progress` string COMMENT '未回款进度 1:合同未签署 2:退款流程中 3:合同已丢失 4:诉讼中 5:收据已丢失 6:其他',
`form_todo_progress` string COMMENT '表单待办进度  0:未填写  1:已完成 2:待办中',
`finance_form_todo_progress` string COMMENT '财务表单待办进度  0:未填写  1:已完成',
`sale_form_todo_progress` string COMMENT '销售表单待办进度  0:未填写  1:已完成',
`tender_form_todo_progress` string COMMENT '投标表单待办进度  0:未填写  1:已完成',
`attachment_todo_progress` string COMMENT '附件待办进度  0:未上传  1:已完成 2:待办中',
`finance_attachment_todo_progress` string COMMENT '财务附件待办进度  0:未上传  1:已完成',
`tender_attachment_todo_progress` string COMMENT '投标附件待办进度  0:未上传  1:已完成',
`create_by` string COMMENT '创建人',
`create_time` string COMMENT '创建时间',
`update_by` string COMMENT '更新人',
`update_time` string COMMENT '更新时间',
`version_no` string COMMENT '乐观锁',
`is_deleted` string COMMENT '状态：0:正常、1:删除'
  )  COMMENT '杂项用款核销表' 
PARTITIONED BY (
`sdt` string COMMENT '日期分区')
STORED AS textfile tblproperties ("author"="wangkuiming"); 


-- DROP TABLE IF EXISTS `source_sss_r_a_sss_incidental_write_off_finance` ;
CREATE TABLE `source_sss_r_a_sss_incidental_write_off_finance` (
`id` string COMMENT '主键ID',
`incidental_expenses_no` string COMMENT '杂项用款单号',
`self_employed` string COMMENT '是否自营  0:否  1:是',
`cooperation_deposit_recovery` string COMMENT '合作保证金是否已收回  0:否  1:是',
`money_back_no_write_off` string COMMENT '是否已回款未核销  0:否  1:是',
`change_business_scene` string COMMENT '转其他业务场景',
`change_business_scene_code` string COMMENT '转其他业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约',
`change_performance_offline_voucher` string COMMENT '转履约线下凭证号',
`receipt_recover` string COMMENT '收据是否回收  0:否  1:是',
`contract_recover` string COMMENT '合同是否回收  0:否  1:是',
`finance_remark` string COMMENT '财务备注',
`sign_off_attachment_url` string COMMENT '签呈文件附件',
`receipt_attachment_url` string COMMENT '收据文件附件',
`sales_contract_attachment_url` string COMMENT '销售合同文件附件',
`create_by` string COMMENT '创建人',
`create_time` string COMMENT '创建时间',
`update_by` string COMMENT '更新人',
`update_time` string COMMENT '更新时间',
`version_no` string COMMENT '乐观锁',
`is_deleted` string COMMENT '状态：0:正常、1:删除'
  )  COMMENT '杂项用款核销财务待办表' 
PARTITIONED BY (
`sdt` string COMMENT '日期分区')
STORED AS textfile tblproperties ("author"="wangkuiming"); 


-- DROP TABLE IF EXISTS `source_sss_r_a_sss_incidental_write_off_sale` ;
CREATE TABLE `source_sss_r_a_sss_incidental_write_off_sale` (
`id` string COMMENT '主键ID',
`incidental_expenses_no` string COMMENT '杂项用款单号',
`break_contract` string COMMENT '是否已经断约  0:否  1:是',
`sale_remark` string COMMENT '销售备注',
`create_by` string COMMENT '创建人',
`create_time` string COMMENT '创建时间',
`update_by` string COMMENT '更新人',
`update_time` string COMMENT '更新时间',
`version_no` string COMMENT '乐观锁',
`is_deleted` string COMMENT '状态：0:正常、1:删除'
  )  COMMENT '杂项用款核销销售待办表' 
PARTITIONED BY (
`sdt` string COMMENT '日期分区')
STORED AS textfile tblproperties ("author"="wangkuiming"); 


-- DROP TABLE IF EXISTS `source_sss_r_a_sss_incidental_write_off_tender` ;
CREATE TABLE `source_sss_r_a_sss_incidental_write_off_tender` (
`id` string COMMENT '主键ID',
`incidental_expenses_no` string COMMENT '杂项用款单号',
`won_bid` string COMMENT '是否中标 0:否  1:是',
`won_bid_date` string COMMENT '中标日期',
`target_payment_time` string COMMENT '目标回款时间',
`tender_attachment_url` string COMMENT '招标文件附件',
`tender_remark` string COMMENT '投标备注',
`create_by` string COMMENT '创建人',
`create_time` string COMMENT '创建时间',
`update_by` string COMMENT '更新人',
`update_time` string COMMENT '更新时间',
`version_no` string COMMENT '乐观锁',
`is_deleted` string COMMENT '状态：0:正常、1:删除'
  )  COMMENT '杂项用款核销投标待办表' 
PARTITIONED BY (
`sdt` string COMMENT '日期分区')
STORED AS textfile tblproperties ("author"="wangkuiming");