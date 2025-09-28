--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


with tmp_cbgb_tz_htzh as 
(
select 
	a.adjust_reason,a.dc_code,a.dc_name,
	case when a.dc_code='W0H4' then '-' else a.performance_province_code end performance_province_code,
	case when a.dc_code='W0H4' then '供应链' else a.performance_province_name end performance_province_name,
	case when a.dc_code='W0H4' then '-' else a.performance_city_code end performance_city_code,
	case when a.dc_code='W0H4' then '供应链' else a.performance_city_name end performance_city_name,
	c.channel_name,a.goods_code as product_code,a.goods_name as product_name,e.dept_id,e.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(sales_value/(1+tax_rate/100)) amt_no_tax,
	sum(sales_value) amt

from
	(
	select 
		a.adjust_reason,a.dc_code,a.dc_name,a.customer_code,a.customer_name,
		a.goods_code,a.goods_name,a.sales_value,a.tax_rate,
		b.performance_province_code,b.performance_province_name,b.performance_city_code,b.performance_city_name
	from
		(
		select
			a.adjust_reason,a.dc_code,a.dc_name,a.customer_code,a.customer_name,
			a.goods_code,a.goods_name,a.sales_value,a.tax_rate
		from
			(
			select
				'Z68' as adjust_reason,rebate_order_code as no_type,dc_code,dc_name,customer_code,customer_name,goods_code,goods_name,total_rebate_amount as sales_value,tax_rate
			from
				-- csx_dw.dwd_sss_r_d_customer_rebate_detail -- 客户返利单明细表
				csx_dwd.csx_dwd_sss_customer_rebate_detail_di
			where
				sdt>='20220317' and sdt<='${ytd}' 
			union all
			select
				'Z69' as adjust_reason,adjust_price_order_code as no_type,dc_code,dc_name,customer_code,customer_name,goods_code,goods_name,sales_value,tax_rate
			from
				-- csx_dw.dwd_sss_r_d_customer_adjust_price_detail -- 客户调价单明细表
				csx_dwd.csx_dwd_sss_customer_adjust_price_detail_di
			where
				sdt>='20220317' and sdt<='${ytd}' 			
			) a 
			join(select distinct order_code from csx_dws.csx_dws_sale_detail_di where sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and sdt<regexp_replace(trunc('${ytd_date}','MM'),'-','')) b on b.order_code=a.no_type
		) a 
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
			) b on b.shop_code=a.dc_code
	)a	
	left join -- 渠道
		(
		select 
			* 
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt ='current'
		) c on a.customer_code = c.customer_code
	left join -- 是否工厂商品
		(
		select
			workshop_code, province_code, goods_code
		from 
			-- csx_dw.dws_mms_w_a_factory_setting_craft_once_all
			csx_dws.csx_dws_mms_factory_setting_craft_once_all_df
		where 
			sdt='current' 
			and new_or_old=1
		) d on a.performance_province_code=d.province_code and a.goods_code=d.goods_code
	left join -- 课组
		(
		select 
			goods_code,goods_name,purchase_group_code dept_id,purchase_group_name dept_name,
			category_small_code small_category_code,category_small_name small_category_name
		from 
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where 
			sdt = 'current' 
		)e on a.goods_code=e.goods_code
group by 
	a.adjust_reason,a.dc_code,a.dc_name,
	case when a.dc_code='W0H4' then '-' else a.performance_province_code end,
	case when a.dc_code='W0H4' then '供应链' else a.performance_province_name end,
	case when a.dc_code='W0H4' then '-' else a.performance_city_code end,
	case when a.dc_code='W0H4' then '供应链' else a.performance_city_name end,
	c.channel_name,a.goods_code,a.goods_name,e.dept_id,e.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品')
)


insert overwrite table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_htzc_mi partition(sdt)

select
	concat_ws('&',performance_city_code,adjust_reason,dc_code,product_code,substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6)) as biz_id,
	adjust_reason,dc_code as inventory_dc_code,dc_name as inventory_dc_name,
	performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	channel_name,product_code,product_name,dept_id,dept_name,is_factory_goods_name,
	amt_no_tax,amt,
	substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6) as sdt
from tmp_cbgb_tz_htzh;


/*
create table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_htzc_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`adjust_reason`                  string              COMMENT    '调整原因',
`inventory_dc_code`              string              COMMENT    'DC编码',
`inventory_dc_name`              string              COMMENT    'DC名称',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`channel_name`                   string              COMMENT    '渠道名称',
`product_code`                   string              COMMENT    '产品编码',
`product_name`                   string              COMMENT    '产品名称',
`dept_id`                        string              COMMENT    '课组编号',
`dept_name`                      string              COMMENT    '课组名称',
`is_factory_goods_name`          string              COMMENT    '是否工厂商品',
`amt_no_tax`                     decimal(26,6)       COMMENT    '不含税金额',
`amt`                            decimal(26,6)       COMMENT    '含税金额'

) COMMENT '财报管报-调整项-后台支出'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	
