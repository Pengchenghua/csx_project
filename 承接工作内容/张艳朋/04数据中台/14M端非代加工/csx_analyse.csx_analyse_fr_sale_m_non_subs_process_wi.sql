
insert overwrite table csx_analyse.csx_analyse_fr_sale_m_non_subs_process_wi partition(sdt)
								
select
	performance_province_name as key_name,
	if(purchase_group_code in ('U01','H03','H05','H01','H02','H04','104'),purchase_group_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	if(sdt>='${7days_ago}','本周','上周') as week_type,
	sum(sale_amt) as sales_value, 
	sum(profit) as profit,
	if(sum(sale_amt)=0,0,sum(profit)/abs(sum(sale_amt))) as profit_rate,
	'${ytd}' as update_time,
	'${ytd}' as sdt	
from 
	-- csx_dw.dws_sale_r_d_detail
	csx_dws.csx_dws_sale_detail_di
where 
	sdt between '${14days_ago}' and '${ytd}'
	and channel_code = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
	and (inventory_dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5','W0X5') or (inventory_dc_code in ('W0M4') and purchase_group_code not in ('H03','H01')))--数据不含代加工DC 
	and performance_province_name not like '平台%'
group by
	performance_province_name,
	if(purchase_group_code in ('U01','H03','H05','H01','H02','H04','104'),purchase_group_name,'其他'),--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	if(sdt>='${7days_ago}','本周','上周')
	
union all

select
	a.performance_region_name as key_name,
	a.department_type,
	a.week_type,				
	sum(a.sales_value) as sales_value, 
	sum(a.profit) as profit,
	if(sum(a.sales_value)=0,0,sum(a.profit)/abs(sum(a.sales_value))) as profit_rate,
	'${ytd}' as update_time,
	'${ytd}' as sdt
from	
	(	
	select
		performance_region_name,
		performance_province_name,
		if(purchase_group_code in ('U01','H03','H05','H01','H02','H04','104'),purchase_group_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
		if(sdt>='${7days_ago}','本周','上周') as week_type,
		sum(sale_amt) as sales_value, 
		sum(profit) as profit
	from 
		-- csx_dw.dws_sale_r_d_detail
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '${14days_ago}' and '${ytd}'
		and channel_code = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and (inventory_dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5','W0X5') or (inventory_dc_code in ('W0M4') and purchase_group_code not in ('H03','H01')))--数据不含代加工DC
		and performance_province_name not like '平台%'
	group by
		performance_region_name,
		performance_province_name,
		if(purchase_group_code in ('U01','H03','H05','H01','H02','H04','104'),purchase_group_name,'其他'),--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
		if(sdt>='${7days_ago}','本周','上周')
	) a	
group by
	a.performance_region_name,
	department_type,
	a.week_type	
;


	
/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sale_m_non_subs_process_wi  M端非代加工周报数据

drop table if exists csx_analyse.csx_analyse_fr_sale_m_non_subs_process_wi;
create table csx_analyse.csx_analyse_fr_sale_m_non_subs_process_wi(
`key_name`                 string              COMMENT    '指标名称',
`department_type`          string              COMMENT    '课组类型',
`week_type`                string              COMMENT    '周期类型',
`sales_value`              decimal(26,6)       COMMENT    '销售额',
`profit`                   decimal(26,6)       COMMENT    '毛利额',
`profit_rate`              decimal(26,6)       COMMENT    '毛利率',
`update_time`              string              COMMENT    '更新时间'
) COMMENT 'M端非代加工周报数据'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	


