

-- 增加创建时间更新时间字段

alter table csx_data_config.tc_service_level add   `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间';
alter table csx_data_config.tc_customer_person_rate_special_rules add   `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

-- 服务管家导出处理
SELECT x.* FROM csx_data_config.tc_service_level x
WHERE tc_sdt ='20240731'

-- 签呈导入处理 单客特殊规则维护表 将上个月数据失效的删除后导入当前月份
tc_customer_special_rules_2023

-- INSERT	into csx_data_config.tc_customer_special_rules_2023
	(biz_id,
	province_name,
	customer_code,
	smt_date,
	qc_yearmonth,
	category_first,
	category_second,
	adjust_business_type,
	effective_period,
	remark,
	adjust_amount,
	adjust_rate,
	service_fee,
	back_amt_tc_rate,
	company_code,
	credit_code,
	date_star,
	date_end,
	hk_dff_rate,
	paid_date_new,
	data_source)
SELECT	
	CONCAT('202407', '_', customer_code) biz_id,
	province_name,
	customer_code,
	'202407' smt_date,
	qc_yearmonth,
	category_first,
	category_second,
	adjust_business_type,
	effective_period,
	remark,
	adjust_amount,
	adjust_rate,
	service_fee,
	back_amt_tc_rate,
	company_code,
	credit_code,
	date_star,
	date_end,
	hk_dff_rate,
	paid_date_new,
	data_source
FROM
	csx_data_config.tc_customer_special_rules_2023 x
where
	smt_date = '202406'
	and (effective_period != '当月'
		and effective_period not like '%202406');
-- 验证数据		
select *
FROM
	csx_data_config.tc_customer_special_rules_2023 x	
	where
	smt_date = '202407'
	

;

-- 人员比例根据有需求
-- INSERT into csx_data_config.tc_customer_person_rate_special_rules  
-- (biz_id,
-- 	customer_id,
-- 	customer_no,
-- 	customer_name,
-- 	channel_code,
-- 	channel_name,
-- 	region_code,
-- 	region_name,
-- 	province_code,
-- 	province_name,
-- 	city_group_code,
-- 	city_group_name,
-- 	sales_id,
-- 	work_no,
-- 	sales_name,
-- 	rp_service_user_id,
-- 	rp_service_user_work_no,
-- 	rp_service_user_name,
-- 	fl_service_user_id,
-- 	fl_service_user_work_no,
-- 	fl_service_user_name,
-- 	bbc_service_user_id,
-- 	bbc_service_user_work_no,
-- 	bbc_service_user_name,
-- 	rp_sales_sale_fp_rate,
-- 	rp_sales_profit_fp_rate,
-- 	fl_sales_sale_fp_rate,
-- 	fl_sales_profit_fp_rate,
-- 	bbc_sales_sale_fp_rate,
-- 	bbc_sales_profit_fp_rate,
-- 	rp_service_user_sale_fp_rate,
-- 	rp_service_user_profit_fp_rate,
-- 	fl_service_user_sale_fp_rate,
-- 	fl_service_user_profit_fp_rate,
-- 	bbc_service_user_sale_fp_rate,
-- 	bbc_service_user_profit_fp_rate,
-- 	smt_date)
SELECT
	concat('202407','_',customer_no )biz_id,
	customer_id,
	customer_no,
	customer_name,
	channel_code,
	channel_name,
	region_code,
	region_name,
	province_code,
	province_name,
	city_group_code,
	city_group_name,
	sales_id,
	work_no,
	sales_name,
	rp_service_user_id,
	rp_service_user_work_no,
	rp_service_user_name,
	fl_service_user_id,
	fl_service_user_work_no,
	fl_service_user_name,
	bbc_service_user_id,
	bbc_service_user_work_no,
	bbc_service_user_name,
	rp_sales_sale_fp_rate,
	rp_sales_profit_fp_rate,
	fl_sales_sale_fp_rate,
	fl_sales_profit_fp_rate,
	bbc_sales_sale_fp_rate,
	bbc_sales_profit_fp_rate,
	rp_service_user_sale_fp_rate,
	rp_service_user_profit_fp_rate,
	fl_service_user_sale_fp_rate,
	fl_service_user_profit_fp_rate,
	bbc_service_user_sale_fp_rate,
	bbc_service_user_profit_fp_rate,
	'202407' smt_date 
FROM
	csx_data_config.tc_customer_person_rate_special_rules x
where
	smt_date = '202406' 


	;


	