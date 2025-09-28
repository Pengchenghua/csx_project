-- 切换tez计算引擎
SET hive.execution.engine=mr;
-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;
-- 启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;


--昨天
set last_1day = regexp_replace(date_sub(current_date,1),'-','');
set last_1day_2 = date_sub(current_date,1);

set created_time = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');
set created_by='zhangyanpeng';

insert overwrite table csx_tmp.report_sss_r_a_sss_incidental_write_off_info partition (sdt)

select
	concat(a.incidental_expenses_no,${hiveconf:last_1day}) as biz_id, --业务主键
	coalesce(g.region_code,'') as region_code, --大区编码
	coalesce(g.region_name,'') as region_name, --大区名称
	coalesce(g.province_code,'') as province_code, --省份编码
	coalesce(g.province_name,'') as province_name, --省份名称
	a.incidental_expenses_no, --杂项用款单号
	a.purchase_code, --所属城市代码
	a.purchase_name, --所属城市名称
	a.company_code, --公司编码
	a.company_name, --公司名称
	a.payment_unit_name, --签约主体
	a.payment_company_code, --实际付款公司编码
	a.payment_company_name, --实际付款公司名称
	a.receiving_customer_code, --收款客户编码
	a.receiving_customer_name, --收款客户名称
	a.business_scene, --业务场景名称
	a.business_scene_code, --业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约
	a.payment_amount, --付款金额
	a.write_off_amount, --核销金额
	a.lave_write_off_amount, --剩余待核销金额
	a.payment_status, --付款状态  S:付款成功 T:已退汇
	case when a.payment_status='S' then '付款成功'
		when a.payment_status='T' then '已退汇'
		else '' 
	end as payment_status_name, --付款状态名称
	a.payment_method, --支付方式  1:外付网银 2:线下
	case when a.payment_method='1' then '外付网银'
		when a.payment_method='T' then '线下'
		else '' 
	end as payment_method_name, --支付方式名称
	a.apply_user, --申请人
	a.responsible_person, --负责人
	a.write_off_status, --核销状态  0:未核销  1:已核销 2:部分核销
	case when a.write_off_status='0' then '未核销'
		when a.write_off_status='1' then '已核销'
		when a.write_off_status='2' then '部分核销'
		else '' 
	end as write_off_status_name, --核销状态名称
	a.write_off_type, --核销类型  1:慧共享核销  2:手工核销
	case when a.write_off_type='1' then '慧共享核销'
		when a.write_off_type='2' then '手工核销'
		else '' 
	end as write_off_type_name, --核销类型名称
	to_date(a.apply_date) as apply_date, --单据申请日期
	a.apply_reason, --申请事由
	a.entry_company_code, --入账单位编码
	a.entry_company_name, --入账单位名称
	a.assignment_number, --分配号
	a.audit_status, --审批状态
	a.approved_date, --单据审批通过日期
	regexp_replace(to_date(a.approved_date),'-','') as approved_date2,
	a.voucher_code, --凭证编码
	a.receiving_account, --客户收款账号
	coalesce(a.unpaid_progress,'') as unpaid_progress, --未回款进度 1:合同未签署 2:退款流程中 3:合同已丢失 4:诉讼中 5:收据已丢失 6:其他
	case when a.unpaid_progress='1' then '合同未签署'
		when a.unpaid_progress='2' then '退款流程中'
		when a.unpaid_progress='3' then '合同已丢失'
		when a.unpaid_progress='4' then '诉讼中'
		when a.unpaid_progress='5' then '收据已丢失'
		when a.unpaid_progress='6' then '其他'
		else '' 
	end as unpaid_progress_name, --未回款进度名称
	a.form_todo_progress, --表单待办进度  0:未填写  1:已完成 2:待办中
	case when a.form_todo_progress='0' then '未填写'
		when a.form_todo_progress='1' then '已完成'
		when a.form_todo_progress='2' then '待办中'
		else ''
	end as form_todo_progress_name, --表单待办进度名称
	a.finance_form_todo_progress, --财务表单待办进度  0:未填写  1:已完成
	case when a.finance_form_todo_progress='0' then '未填写'
		when a.finance_form_todo_progress='1' then '已完成'
		else ''
	end as finance_form_todo_progress_name, --财务表单待办进度名称
	a.sale_form_todo_progress, --销售表单待办进度  0:未填写  1:已完成
	case when a.sale_form_todo_progress='0' then '未填写'
		when a.sale_form_todo_progress='1' then '已完成'
		else ''	
	end as sale_form_todo_progress_name, --销售表单待办进度名称
	a.tender_form_todo_progress, --投标表单待办进度  0:未填写  1:已完成
	case when a.tender_form_todo_progress='0' then '未填写'
		when a.tender_form_todo_progress='1' then '已完成'
		else ''	
	end as tender_form_todo_progress_name, --投标表单待办进度名称
	a.attachment_todo_progress, --附件待办进度  0:未上传  1:已完成 2:待办中
	case when a.attachment_todo_progress='0' then '未上传'
		when a.attachment_todo_progress='1' then '已完成'
		when a.attachment_todo_progress='2' then '待办中'
		else ''	
	end as attachment_todo_progress_name, --附件待办进度名称
	a.finance_attachment_todo_progress, --财务附件待办进度  0:未上传  1:已完成
	case when a.finance_attachment_todo_progress='0' then '未上传'
		when a.finance_attachment_todo_progress='1' then '已完成'
		else ''	
	end as finance_attachment_todo_progress_name, --财务附件待办进度名称
	a.tender_attachment_todo_progress, --投标附件待办进度  0:未上传  1:已完成
	case when a.tender_attachment_todo_progress='0' then '未上传'
		when a.tender_attachment_todo_progress='1' then '已完成'
		else ''	
	end as tender_attachment_todo_progress_name, --投标附件待办进名称
	a.update_by, --中台更新人
	a.update_time, --中台更新时间
	a.is_deleted, --状态：0:正常、1:删除
	coalesce(b.self_employed,'') as self_employed, --是否自营  0:否  1:是
	case when b.self_employed='0' then '否'
		when b.self_employed='1' then '是'
		else ''	
	end as self_employed_name, --是否自营
	coalesce(b.cooperation_deposit_recovery,'') as cooperation_deposit_recovery, --合作保证金是否已收回  0:否  1:是
	case when b.cooperation_deposit_recovery='0' then '否'
		when b.cooperation_deposit_recovery='1' then '是'
		else ''	
	end as cooperation_deposit_recovery_name, --合作保证金是否已收回
	coalesce(b.money_back_no_write_off,'') as money_back_no_write_off, --是否已回款未核销  0:否  1:是
	case when b.money_back_no_write_off='0' then '否'
		when b.money_back_no_write_off='1' then '是'
		else ''	
	end as money_back_no_write_off_name, --是否已回款未核销
	coalesce(b.change_business_scene,'') as change_business_scene, --转其他业务场景
	coalesce(b.change_business_scene_code,'') as change_business_scene_code, --转其他业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约
	coalesce(b.change_performance_offline_voucher,'') as change_performance_offline_voucher, --转履约线下凭证号
	coalesce(b.receipt_recover,'') as receipt_recover, --收据是否回收  0:否  1:是
	case when b.receipt_recover='0' then '否'
		when b.receipt_recover='1' then '是'
		else ''	
	end as receipt_recover_name, --收据是否回收
	coalesce(b.contract_recover,'') as contract_recover, --合同是否回收  0:否  1:是
	case when b.contract_recover='0' then '否'
		when b.contract_recover='1' then '是'
		else ''	
	end as contract_recover_name, --合同是否回收
	coalesce(b.finance_remark,'') as finance_remark, --财务备注
	coalesce(b.sign_off_attachment_url,'') as sign_off_attachment_url, --签呈文件附件
	coalesce(b.receipt_attachment_url,'') as receipt_attachment_url, --收据文件附件
	coalesce(b.sales_contract_attachment_url,'') as sales_contract_attachment_url, --销售合同文件附件
	coalesce(c.break_contract,'') as break_contract, --是否已经断约  0:否  1:是
	case when c.break_contract='0' then '否'
		when c.break_contract='1' then '是'
		else ''	
	end as break_contract_name, --是否已经断约
	coalesce(regexp_replace(c.sale_remark,'\n|\t|\r|\,|\"|\\\\n',''),'') as sale_remark, --销售备注
	coalesce(d.won_bid,'') as won_bid, --是否中标
	coalesce(d.won_bid_date,'') as won_bid_date, --中标日期
	coalesce(d.target_payment_time,'') as target_payment_time, --目标回款时间
	coalesce(d.tender_attachment_url,'') as tender_attachment_url, --招标文件附件
	coalesce(d.tender_remark,'') as tender_remark, --投标备注
	coalesce(e.customer_code,'') as customer_code, --客户编码
	coalesce(e.customer_name,'') as customer_name, --客户名称
	coalesce(e.operate_no,'') as operate_no, --核销单号
	coalesce(e.voucher_no,'') as voucher_no, --sap凭证单号
	coalesce(e.operate_amount,'') as operate_amount, --操作金额
	coalesce(e.trade_time,'') as trade_time, --交易时间
	coalesce(e.write_off_source,'') as write_off_source, --核销来源：1:认领、2:冲抵
	case when e.write_off_source='1' then '认领'
		when e.write_off_source='2' then '冲抵'
		else ''	
	end as write_off_source_name, --核销来源名称
	coalesce(e.all_write_off,'') as all_write_off, --是否全部核销：0:否、1:是
	case when e.all_write_off='0' then '否'
		when e.all_write_off='1' then '是'
		else ''	
	end as all_write_off_name, --是否全部核销
	coalesce(e.operate_type,'') as operate_type, --操作类型 0:冲抵中 1:取消占用 2:已执行 3:已释放
	case when e.operate_type='0' then '冲抵中'
		when e.operate_type='1' then '取消占用'
		when e.operate_type='2' then '已执行'
		when e.operate_type='3' then '已释放'
		else ''	
	end as operate_type_name, --操作类型名称
	coalesce(a.break_contract_date,'') as break_contract_date, --断约时间
	a.receiving_account_name, --收款账号名称
	coalesce(a.account_diff,'') as account_diff, --账期天数差值
	case when account_diff>=0 and account_diff<=45 then '[0,45天]'	
		when account_diff>45 and account_diff<=90 then '(45,90天]'
		when account_diff>90 and account_diff<=180 then '(90,180天]'
		when account_diff>180 and account_diff<=365 then '(180,365天]'
		when account_diff>365 then '>365天'
		else '' 
	end as account_type, --账期类型
	case when f.table_type='1' then '否'
		when f.table_type='2' then '是'
		else ''
	end as is_borrow_zizhi, --是否借用资质
	${hiveconf:created_by} as created_by, --数据创建人
	${hiveconf:created_time} as created_time, --数据创建时间
	${hiveconf:last_1day} as sdt
from
	(
	select 
		*,
		case when business_scene_code='1' then datediff(${hiveconf:last_1day_2},to_date(approved_date))
		when business_scene_code in ('2','3') then datediff(${hiveconf:last_1day_2},coalesce(to_date(break_contract_date),to_date(approved_date)))
		else '' end as account_diff
	from csx_ods.source_sss_r_a_sss_incidental_write_off where sdt=${hiveconf:last_1day}
	) a 
	left join (select * from csx_ods.source_sss_r_a_sss_incidental_write_off_finance where sdt=${hiveconf:last_1day}) b on b.incidental_expenses_no=a.incidental_expenses_no
	left join (select * from csx_ods.source_sss_r_a_sss_incidental_write_off_sale where sdt=${hiveconf:last_1day}) c on c.incidental_expenses_no=a.incidental_expenses_no
	left join (select * from csx_ods.source_sss_r_a_sss_incidental_write_off_tender where sdt=${hiveconf:last_1day}) d on d.incidental_expenses_no=a.incidental_expenses_no
	left join (select * from csx_ods.source_sss_r_a_sss_incidental_occupy_record where sdt=${hiveconf:last_1day}) e on e.incidental_expenses_no=a.incidental_expenses_no
	left join (select * from csx_dw.dws_basic_w_a_company_code where sdt='current') f on f.code=a.payment_company_code
	left join (select * from csx_dw.dws_sale_w_a_area_belong) g on g.city_code=a.purchase_code