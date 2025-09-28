-- 切换tez计算引擎
SET hive.execution.engine=mr;
-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =10000;
set hive.exec.max.dynamic.partitions.pernode =10000;


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

insert overwrite table csx_tmp.csx_tmp_report_sss_incidental_write_off_detail_di partition (sdt)

select
	concat(a.incidental_expenses_no,'&',${hiveconf:last_1day}) as biz_id, --业务主键
	
	if(a.purchase_code='','10',d2.belong_region_code) as region_code, --大区编码
	if(a.purchase_code='','平台',d2.belong_region_name) as region_name, --大区名称
	if(a.purchase_code='','211',d2.belong_province_code) as province_code, --省份编码
	if(a.purchase_code='','平台-其他',d2.belong_province_name) as province_name, --省份名称
	if(a.purchase_code='','211001',a.purchase_code) as city_group_code, --城市组编码
	if(a.purchase_code='','平台-其他',a.purchase_name) as city_group_name, --城市组名称	
	
	a.incidental_expenses_no, --杂项用款单号
	a.receiving_customer_code, --收款客户编码
	a.receiving_customer_name, --收款客户名称
	coalesce(e.first_category_code,'') as first_category_code, --一级分类编码
	coalesce(e.first_category_name,'') as first_category_name, --一级分类名称
	coalesce(e.second_category_code,'') as second_category_code, --二级分类编码
	coalesce(e.second_category_name,'') as second_category_name, --二级分类名称
	coalesce(e.third_category_code,'') as third_category_code, --三级分类编码
	coalesce(e.third_category_name,'') as third_category_name, --三级分类名称
	coalesce(f.custom_category,'') as custom_category, --自定义分类名称
	a.business_scene, --业务场景名称
	a.business_scene_code, --业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约
	--a.company_code, --公司编码
	--a.company_name, --公司名称
	a.payment_unit_name, --签约主体
	a.payment_company_code, --实际付款公司编码
	a.payment_company_name, --实际付款公司名称
	substr(a.approved_date,1,10) as approved_date, --单据审批通过日期
	coalesce(substr(a.break_contract_date,1,10),'') as break_contract_date, --断约时间
	coalesce(substr(c.target_payment_time,1,10),'') as target_payment_time, --目标回款时间
	a.account_diff, --账期天数 1、投标：当期时间-单据审核通过时间 2、履约、投标转履约：当期时间-断约时间，若断约时间为空时当期时间-单据审核通过时间
	case when account_diff>=0 and account_diff<=60 then '0'	
		when account_diff>60 and account_diff<=90 then '1'
		when account_diff>90 and account_diff<=180 then '2'
		when account_diff>180 and account_diff<=365 then '3'
		when account_diff>365 then '4'
		else null 
	end as account_type, --账期类型
	a.account_diff2, --账期天数 1、投标：当期时间-单据审核通过时间 2、履约、投标转履约：当期时间-断约时间，若断约时间为空时不统计
	case when account_diff2>=0 and account_diff2<=60 then '0'	
		when account_diff2>60 and account_diff2<=90 then '1'
		when account_diff2>90 and account_diff2<=180 then '2'
		when account_diff2>180 and account_diff2<=365 then '3'
		when account_diff2>365 then '4'
		else null 
	end as account_type2, --账期类型		
	a.payment_amount, --付款金额
	a.write_off_amount, --核销金额
	if(b.money_back_no_write_off='1',a.payment_amount,0.0) as money_back_no_write_off_amount, --已回款未核销金额
	a.lave_write_off_amount, --剩余待核销金额
	if(a.account_diff2>90,a.lave_write_off_amount,0.0) as lave_write_off_amount_90,
	if(a.account_diff2>365,a.lave_write_off_amount,0.0) as lave_write_off_amount_365,	
	${hiveconf:created_by} as created_by, --创建人
	${hiveconf:created_time} as created_time, --创建时间
	regexp_replace(substr(a.approved_date,1,10),'-','') as sdt
from
	(
	select 
		*,
		case when business_scene_code='1' then datediff(${hiveconf:last_1day_2},to_date(approved_date))
			when business_scene_code in ('2','3') then datediff(${hiveconf:last_1day_2},coalesce(to_date(break_contract_date),to_date(approved_date)))
			else null end as account_diff,
		case when business_scene_code='1' then datediff(${hiveconf:last_1day_2},to_date(approved_date))
			when business_scene_code in ('2','3') then datediff(${hiveconf:last_1day_2},to_date(break_contract_date))
			else null end as account_diff2
	from csx_ods.source_sss_r_a_sss_incidental_write_off where sdt=${hiveconf:last_1day}
	) a 
	left join (select * from csx_ods.source_sss_r_a_sss_incidental_write_off_finance where sdt=${hiveconf:last_1day}) b on b.incidental_expenses_no=a.incidental_expenses_no
	left join (select * from csx_ods.source_sss_r_a_sss_incidental_write_off_tender where sdt=${hiveconf:last_1day}) c on c.incidental_expenses_no=a.incidental_expenses_no
	left join (select * from csx_dw.dws_sale_w_a_area_belong) d on d.city_code=a.purchase_code
	left join (select distinct belong_region_code,belong_region_name,belong_province_code,belong_province_name,performance_province_code,performance_province_name,province_code,province_name
				from csx_dw.dws_basic_w_a_performance_attribution) d2 on d2.province_code=d.province_code
	left join (select * from csx_dw.dws_crm_w_a_customer where sdt=${hiveconf:last_1day})e on e.customer_no=a.receiving_customer_code
	left join csx_tmp.tmp_crm_customer_custom_category f on f.second_category_code=e.second_category_code
;

