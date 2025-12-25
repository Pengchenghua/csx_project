-- 大客户提成MYSQL导入

-- 销售员、管家系数查询上月导入
INSERT into csx_data_config.tc_customer_person_rate_special_rules  
(biz_id,
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
rp_sales_number,
rp_sales_name,
fl_sales_number,
fl_sales_name,
bbc_sales_number,
bbc_sales_name,
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
smt_date)
SELECT
	concat('202510','_',customer_no )biz_id,
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
rp_sales_number,
rp_sales_name,
fl_sales_number,
fl_sales_name,
bbc_sales_number,
bbc_sales_name,
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
	'202510' smt_date 					-- 更改日期
-- 	b.category_first,
-- 	effective_period 
from 
(SELECT  x.* FROM csx_data_config.tc_customer_person_rate_special_rules x
WHERE smt_date ='202509' 	
)a 
join 
(SELECT DISTINCT x.customer_code,x.category_first,x.effective_period 
FROM csx_data_config.tc_customer_special_rules_2023 x
	WHERE smt_date ='202509'
and category_first='大客户提成-调整对应人员比例' 
and (effective_period != '当月'
		and effective_period not like '%202509'
	)) b on a.customer_no=b.customer_code 
;
-- 签呈导入处理 单客特殊规则维护表 将上个月数据失效的删除后导入当前月份
--   tc_customer_special_rules_2023
 INSERT	into csx_data_config.tc_customer_special_rules_2023
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
	CONCAT('202510', '_', customer_code) biz_id, -- 签呈月份更新比如计算10月签呈，则修改202510
	province_name,
	customer_code,
	'202510' smt_date,      -- 签呈月份更新比如计算10月签呈，则修改202510
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
	smt_date = '202509'  -- 取上个月的签呈数据
-- 	and  category_first ='大客户提成-调整对应人员比例'
-- 删除失效或过期的签呈
	and (effective_period != '当月'
		and effective_period not like '%202509'
		)
	;
	
-- 基准毛利导入与查询
select * from tc_sales_service_profit_basic where smt_c ='202509'  and work_no in ('81296577',
'81119588',
'81305527',
'81293430',
'81259842',
'81213388',
'81170574');

-- 签呈检查机制：1、tc_customer_person_rate_special_rules、tc_customer_special_rules_2023、tc_sales_service_profit_basic 三个月是否同步，
-- 2、注意新增字段的同步映射，检查系数
-- 管家等级导入(已取消-新方案不使用)
-- INSERT
-- 	into
-- 	csx_data_config.tc_service_level
-- (region_name,
-- 	province_name,
-- 	city_group_name,
-- 	service_user_work_no,
-- 	service_user_name,
-- 	begin_date,
-- 	position_detail,
-- 	service_user_position,
-- 	s_level,
-- 	salary_level,
-- 	level_sale_rate,
-- 	level_profit_rate,
-- 	tc_sdt)
-- select
-- 	region_name,
-- 	province_name,
-- 	city_group_name,
-- 	service_user_work_no,
-- 	service_user_name,
-- 	begin_date,
-- 	position_detail,
-- 	service_user_position,
-- 	s_level,
-- 	salary_level,
-- 	level_sale_rate,
-- 	level_profit_rate,
-- 	'20250831' tc_sdt
-- from
-- 	csx_data_config.tc_service_level x
-- WHERE
-- 	tc_sdt = '20250930'

	
-- 	;

-- select * 	from
-- 	csx_data_config.tc_service_level x
-- WHERE
-- 	tc_sdt >= '20250730'
-- 	and service_user_work_no like '%80907460%'
	
-- 	;
-- 调整人员比例-删除过期签呈
select a.*,b.category_first,effective_period
 from 
(SELECT x.* FROM csx_data_config.tc_customer_person_rate_special_rules x
WHERE smt_date ='202411')a 
left join 
(SELECT x.* FROM csx_data_config.tc_customer_special_rules_2023 x
WHERE smt_date ='202508'
and category_first='大客户提成-调整对应人员比例') b on a.customer_no=b.customer_code 
;


-- 删除签呈表
DELETE  from csx_data_config.tc_customer_special_rules_2023  where  smt_date='202508';

-- 删除管家销售系数
delete   from csx_data_config.tc_customer_person_rate_special_rules  where  smt_date='202510' and biz_id  like '202510_%';

-- 销售系数
select *
from csx_data_config.tc_customer_person_rate_special_rules a where  smt_date='202510'  
;
-- 签呈
select *
from csx_data_config.tc_customer_special_rules_2023 a where  smt_date='202509'   
and  a.category_second  like '%跨%'
and province_name='北京市'
;



SELECT  x.* FROM csx_data_config.tc_customer_special_rules_2023 x
WHERE 
smt_date ='202510'   
-- and x.customer_code ='PF1205'
and   x.category_second  like '%扣减回款%'
-- and x.customer_code in ('170509',
-- '251021',
-- '126319',
-- '119990'
-- )

 and x.province_name ='浙江省'

;
select * from tc_customer_person_rate_special_rules where smt_date ='202510' and customer_no ='257837'
;
-- 签呈导入处理 单客特殊规则维护表 将上个月数据失效的删除后导入当前月份
--   tc_customer_special_rules_2023
 INSERT	into csx_data_config.tc_customer_special_rules_2023
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
	CONCAT('202510', '_', customer_code) biz_id,
	province_name,
	customer_code,
	'202510' smt_date,
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
	smt_date = '202509'
-- 	and  category_first ='大客户提成-调整对应人员比例'
	and (effective_period != '当月'
		and effective_period not like '%202509'
		)
	;
	

delete   from csx_data_config.tc_customer_special_rules_2023 where 	smt_date = '202510'
	and  category_first ='大客户提成-调整对应人员比例'
;
 -- 查询上月导入
INSERT into csx_data_config.tc_customer_person_rate_special_rules  
(biz_id,
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
rp_sales_number,
rp_sales_name,
fl_sales_number,
fl_sales_name,
bbc_sales_number,
bbc_sales_name,
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
	smt_date)
SELECT
	concat('202510','_',customer_no )biz_id,
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
rp_sales_number,
rp_sales_name,
fl_sales_number,
fl_sales_name,
bbc_sales_number,
bbc_sales_name,
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
	'202510' smt_date 					-- 更改日期
-- 	b.category_first,
-- 	effective_period 
from 
(SELECT  x.* FROM csx_data_config.tc_customer_person_rate_special_rules x
WHERE smt_date ='202509' 	
)a 
join 
(SELECT DISTINCT x.customer_code,x.category_first,x.effective_period 
FROM csx_data_config.tc_customer_special_rules_2023 x
	WHERE smt_date ='202509'
and category_first='大客户提成-调整对应人员比例' 
and (effective_period != '当月'
		and effective_period not like '%202509'
	)) b on a.customer_no=b.customer_code 
;

DELETE  FROM csx_data_config.tc_customer_person_rate_special_rules 
WHERE smt_date ='202510' 
-- and  rp_sales_name='张艳京'
and customer_no = '248454'
;

SELECT *
FROM csx_data_config.tc_customer_special_rules_2023 x
	WHERE smt_date ='202510'
	
	and customer_code in ('248454','117721','126145','191154'
	)
and category_first like '%大客户提成-调整对应人员比例%' 
 and create_time >='2025-10-27'
	;
-- create table csx_data_config.tc_customer_person_rate_special_rules_20261024
-- as 
-- SELECT x.* FROM csx_data_config.tc_customer_person_rate_special_rules x
--  ;
-- 
-- create table  csx_data_config.tc_customer_special_rules_2023_bak_20251024
-- as 
-- select * from csx_data_config.tc_customer_special_rules_2023
