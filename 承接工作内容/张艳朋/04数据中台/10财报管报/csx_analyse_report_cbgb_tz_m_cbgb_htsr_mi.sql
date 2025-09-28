-- 数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


-- 后台收入明细
with cbgb_tz_m_cbgb_htsr as 
(
select 
	case when a.settlement_dc_code='W0H4' then '-' else b.performance_province_code end performance_province_code,
	case when a.settlement_dc_code='W0H4' then '供应链' else b.performance_province_name end performance_province_name,
	case when a.settlement_dc_code='W0H4' then '-' else b.performance_city_code end performance_city_code,
	case when a.settlement_dc_code='W0H4' then '供应链' else b.performance_city_name end performance_city_name,
	a.settle_code,a.agreement_code,a.settle_date,a.purchase_org_code,a.purchase_org_name,a.purchase_org_code dept_id,a.purchase_org_name dept_name,a.fee_code,a.fee_name,
	a.supplier_code,a.supplier_name,a.settlement_dc_code,a.settlement_dc_name,a.company_code,a.company_name,
	a.net_value,a.tax_amt,a.value_tax_total,a.total_amount,a.invoice_type_code,a.invoice_type_name,
	substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6) as sdt
from 
	( 
	select 
		* 
	from 
		-- csx_dw.dwd_gss_r_d_settle_bill 
		-- dwd_pss_r_d_settle_settle_bill
		csx_dwd.csx_dwd_pss_settle_settle_bill_di
	where 
		to_date(belong_date) >= add_months(trunc('${ytd_date}','MM'),-1) 
		and to_date(belong_date) < trunc('${ytd_date}','MM')
	)a
	join 
		(
		select 
			shop_code,shop_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
		from 
			-- csx_dw.dws_basic_w_a_csx_shop_m 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current'
			and purpose<>'09'
		) b on b.shop_code=a.settlement_dc_code
),

cbgb_tz_m_cbgb_htsr_2 as 
(
select 
	case when fee_name like '目标返利%' then '目标返利'
		when fee_name like '仓储服务费%' then '仓储服务费'  
		else fee_name end fee_name ,
	performance_province_code,performance_province_name,performance_city_code,performance_city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settlement_dc_code,settlement_dc_name,sdt,
	sum(net_value) net_value,sum( value_tax_total) value_tax_total
from cbgb_tz_m_cbgb_htsr
group by 
	case when fee_name like '目标返利%' then '目标返利'
		when fee_name like '仓储服务费%' then '仓储服务费'  
		else fee_name end,
	performance_province_code,performance_province_name,performance_city_code,performance_city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settlement_dc_code,settlement_dc_name,sdt
)
	

insert overwrite table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_htsr_mi partition(sdt) 

select
	concat_ws('&',performance_city_code,fee_name,dept_id,supplier_code,settlement_dc_code,sdt) as biz_id,
	fee_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,dept_id,dept_name,	
	supplier_code,supplier_name,settlement_dc_code,settlement_dc_name,net_value,value_tax_total,sdt
from cbgb_tz_m_cbgb_htsr_2;


/*
create table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_htsr_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`fee_name`                       string              COMMENT    '费用名称',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`dept_id`                        string              COMMENT    '课组编号',
`dept_name`                      string              COMMENT    '课组名称',
`supplier_code`                  string              COMMENT    '供应商编码',
`supplier_name`                  string              COMMENT    '供应商名称',
`settlement_dc_code`             string              COMMENT    '结算dc编码',
`settlement_dc_name`             string              COMMENT    '结算dc名称',
`net_value`                      decimal(26,6)       COMMENT    '净价值',
`value_tax_total`                decimal(26,6)       COMMENT    '价税合计'

) COMMENT '财报管报-调整项-后台收入'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	
