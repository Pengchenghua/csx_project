-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =10000;
set hive.exec.max.dynamic.partitions.pernode =10000;


insert overwrite table csx_analyse.csx_analyse_report_sss_incidental_write_off_detail_di partition (sdt)

select
	concat(a.incidental_expenses_no,'&','${ytd}') as biz_id, --业务主键
	
	d.belong_region_code as region_code, --大区编码
	d.belong_region_name as region_name, --大区名称
	a.purchase_code as province_code, --省份编码
	a.purchase_name as province_name, --省份名称
	a.incidental_expenses_no, --杂项用款单号
	a.receiving_customer_code, --收款客户编码
	regexp_replace(a.receiving_customer_name,'\n|\t|\r|\,|\"|\\\\n','') as receiving_customer_name, -- 收款客户名称
	e.first_category_code, --一级分类编码
	e.first_category_name, --一级分类名称
	e.second_category_code, --二级分类编码
	e.second_category_name, --二级分类名称
	e.third_category_code, --三级分类编码
	e.third_category_name, --三级分类名称
	f.custom_category, --自定义分类名称
	a.business_scene, --业务场景名称
	a.business_scene_code, --业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约
	a.payment_unit_name, --签约主体
	a.payment_company_code, --实际付款公司编码
	regexp_replace(a.payment_company_name,'\n|\t|\r|\,|\"|\\\\n','') as payment_company_name, 	
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
	'zhangyanpeng' as created_by, --创建人
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as created_time, --创建时间
	regexp_replace(substr(a.approved_date,1,10),'-','') as tb_sdt,
	regexp_replace(substr(a.approved_date,1,10),'-','') as sdt
from
	(
	select 
		*,
		case when business_scene_code='1' then datediff('${ytd_date}',to_date(approved_date))
			when business_scene_code in ('2','3') then datediff('${ytd_date}',coalesce(to_date(break_contract_date),to_date(approved_date)))
			else null end as account_diff,
		case when business_scene_code='1' then datediff('${ytd_date}',to_date(approved_date))
			when business_scene_code in ('2','3') then datediff('${ytd_date}',to_date(break_contract_date))
			else null end as account_diff2
	from csx_ods.csx_ods_csx_b2b_sss_sss_incidental_write_off_df  where sdt='${ytd}'
	) a 
	left join (select * from csx_ods.csx_ods_csx_b2b_sss_sss_incidental_write_off_finance_df where sdt='${ytd}') b on b.incidental_expenses_no=a.incidental_expenses_no
	left join (select * from csx_ods.csx_ods_csx_b2b_sss_sss_incidental_write_off_tender_df where sdt='${ytd}') c on c.incidental_expenses_no=a.incidental_expenses_no
	left join (select distinct belong_region_code,belong_region_name,performance_province_code,performance_province_name
				from csx_dim.csx_dim_basic_performance_attribution) d on d.performance_province_code=a.purchase_code
	left join (select * from csx_dim.csx_dim_crm_customer_info where sdt='${ytd}')e on e.customer_code=a.receiving_customer_code
	left join csx_analyse.csx_analyse_report_crm_customer_custom_category_yf f on f.second_category_code=e.second_category_code
;

