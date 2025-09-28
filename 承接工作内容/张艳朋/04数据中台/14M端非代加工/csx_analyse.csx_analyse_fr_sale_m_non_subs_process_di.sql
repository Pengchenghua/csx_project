
insert overwrite table csx_analyse.csx_analyse_fr_sale_m_non_subs_process_di partition(sdt)
				
select
	a.province_name, 
	a.region_name,
	a.department_type,
	case when b.sales_belong_flag like '%云超%' then '云超'
		when b.sales_belong_flag like '%云创%' then '云创'
		when a.customer_name rlike '红旗|中百' then '关联方'
		when b.sales_belong_flag='' or b.sales_belong_flag is null then '外部'
		else '其他'
	end as sales_belong_type,
	a.month_type,
	sum(a.sale_amt) as sales_value,
	sum(a.profit) as profit,
	coalesce(sum(a.profit)/abs(sum(a.sale_amt)),0) as profit_rate,
	regexp_replace('${ytd_date}','-','') as update_time,
	regexp_replace('${ytd_date}','-','') as sdt
from	
	(	
	select
		performance_region_name as region_name,performance_province_name as province_name,customer_code,customer_name,
		if(purchase_group_code in ('U01','H03','H05','H01','H02','H04','104'),purchase_group_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
		case when sdt between regexp_replace(add_months(trunc('${ytd_date}','MM'),0),'-','') and regexp_replace('${ytd_date}','-','') then '本月'
			when sdt between regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and regexp_replace(add_months('${ytd_date}',-1),'-','') then '上月'
			else '其他'
		end as month_type,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit
	from 
		-- csx_dw.dws_sale_r_d_detail
		csx_dws.csx_dws_sale_detail_di
	where 
		(sdt between regexp_replace(add_months(trunc('${ytd_date}','MM'),0),'-','') and regexp_replace('${ytd_date}','-','') or sdt between regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and regexp_replace(add_months('${ytd_date}',-1),'-',''))
		and channel_code = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and (inventory_dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5','W0X5') or (inventory_dc_code in ('W0M4') and purchase_group_code not in ('H03','H01')))--数据不含代加工DC
		and performance_province_name not like '平台%'				
	group by
		performance_region_name,performance_province_name,customer_code,customer_name,
		if(purchase_group_code in ('U01','H03','H05','H01','H02','H04','104'),purchase_group_name,'其他'), --熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他		
		case when sdt between regexp_replace(add_months(trunc('${ytd_date}','MM'),0),'-','') and regexp_replace('${ytd_date}','-','') then '本月'
			when sdt between regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and regexp_replace(add_months('${ytd_date}',-1),'-','') then '上月'
			else '其他'
		end
	) a 
	left join
		(
		select 
			shop_code,company_code,sales_belong_flag
		from 
			-- csx_dw.dws_basic_w_a_csx_shop_m
			csx_dim.csx_dim_shop
		where 
			sdt = 'current'
		group by
			shop_code,company_code,sales_belong_flag
		) b on a.customer_code = concat('S', b.shop_code)
group by
	a.province_name,
	a.region_name,
	a.department_type,
	case when b.sales_belong_flag like '%云超%' then '云超'
		when b.sales_belong_flag like '%云创%' then '云创'
		when a.customer_name rlike '红旗|中百' then '关联方'
		when b.sales_belong_flag='' or b.sales_belong_flag is null then '外部'
		else '其他'
	end,
	a.month_type
;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_analyse.csx_analyse_fr_sale_m_non_subs_process_di  M端非代加工月环比数据

drop table if exists csx_analyse.csx_analyse_fr_sale_m_non_subs_process_di;
create table csx_analyse.csx_analyse_fr_sale_m_non_subs_process_di(
`province_name`            string              COMMENT    '省份名称',
`region_name`              string              COMMENT    '大区名称',
`department_type`          string              COMMENT    '课组类型',
`sales_belong_type`        string              COMMENT    '业态类型',
`month_type`               string              COMMENT    '月份类型',
`sales_value`              decimal(26,6)       COMMENT    '销售额',
`profit`                   decimal(26,6)       COMMENT    '毛利额',
`profit_rate`              decimal(26,6)       COMMENT    '毛利率',
`update_time`              string              COMMENT    '更新时间'
) COMMENT 'M端非代加工月环比数据'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/		