-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr = true;

with tmp_cbgb_tz_bs as 
(
select 
	a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,a.location_code,a.location_name,a.posting_time,a.wms_order_no,a.wms_biz_type,a.wms_biz_type_name,a.credential_no,
	a.purchase_group_code,a.purchase_group_name,b.dept_id,b.dept_name,a.product_code,a.product_name,a.unit_name,a.qty,a.price_no_tax,a.amt_no_tax,a.amt,
	'' fac_adjust_amt_no_tax,
	'' negative_adjust_amt_no_tax,
	'' remedy_adjust_amt_no_tax,
	'' manual_adjust_amt_no_tax,
	'' cost_amt_no_tax,
	a.company_code,a.company_name,a.cost_center_code,a.cost_center_name,b.small_category_code,b.small_category_name,a.reservoir_area_code,a.reservoir_area_name,a.reservoir_area_prop        
from
	(
	select 
		a.*,
		case when a.location_code='W0H4' then '-' else b.performance_province_code end performance_province_code,
		case when a.location_code='W0H4' then '供应链' else b.performance_province_name end performance_province_name,
		case when a.location_code='W0H4' then '-' else b.performance_city_code end performance_city_code,
		case when a.location_code='W0H4' then '供应链' else b.performance_city_name end performance_city_name,
		b.shop_code,b.shop_name
	from 
		(
		select
			a.*,b.reservoir_area_name,b.reservoir_area_attribute,b.reservoir_area_attribute reservoir_area_prop
		from
			(
			select
				location_code,location_name,company_code,company_name,goods_code product_code,goods_name product_name,unit_name,price_no_tax,
				credential_no,posting_time,purchase_group_code,purchase_group_name,move_type_code,reservoir_area_code,wms_biz_type_code,
				wms_order_no,wms_biz_type_code wms_biz_type,wms_biz_type_name,cost_center_code,cost_center_name,
				if(move_type_code in ('117B','118B'),-1*qty,qty) qty,
				if(move_type_code in ('117B','118B'),-1*amt_no_tax,amt_no_tax) amt_no_tax,
				if(move_type_code in ('117B','118B'),-1*amt,amt) amt
			from 
				-- csx_dw.dws_cas_r_d_account_credential_detail
				csx_dws.csx_dws_cas_credential_detail_di
			where 
				sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and sdt<regexp_replace(trunc('${ytd_date}','MM'),'-','')
				and wms_biz_type_code in (35, 36, 37, 38, 39, 40, 41, 64, 66, 76, 77, 78)
			) a
			left join
				(
				select 
					* 
				from 
					-- csx_ods.source_wms_w_a_wms_reservoir_area
					csx_ods.csx_ods_csx_b2b_wms_wms_reservoir_area_df
				where
					sdt='${ytd}'
				)b on a.location_code=b.warehouse_code and a.reservoir_area_code=b.reservoir_area_code
		where 
			(reservoir_area_attribute='C' or reservoir_area_attribute='Y')
			and (( a.wms_biz_type_code <>'64' and b.reservoir_area_attribute = 'C' and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) ) 
			or a.wms_biz_type_code = '64' )
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
			) b on b.shop_code=a.location_code
	) a
	left join 
		(
		select 
			goods_code,goods_name,purchase_group_code dept_id,purchase_group_name dept_name,
			category_small_code small_category_code,category_small_name small_category_name
		from 
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where 
			sdt = 'current' 
		)b on a.product_code=b.goods_code
)

insert overwrite table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_bs_mi partition(sdt) 

select 
	concat_ws('&',location_code,wms_order_no,credential_no,product_code,substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6)) as biz_id,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	location_code,
	location_name,
	posting_time,
	wms_order_no,
	wms_biz_type,
	wms_biz_type_name,
	credential_no,
	purchase_group_code,
	purchase_group_name,	
	dept_id,
	dept_name,	
	product_code,
	product_name,
	unit_name,
	qty,
	price_no_tax,
	amt_no_tax,
	amt,
	fac_adjust_amt_no_tax,
	negative_adjust_amt_no_tax,
	remedy_adjust_amt_no_tax,
	manual_adjust_amt_no_tax,
	cost_amt_no_tax,
	company_code,
	company_name,
	cost_center_code,
	cost_center_name,
	small_category_code,
	small_category_name,
	reservoir_area_code,
	reservoir_area_name,
	reservoir_area_prop,
	substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6) as sdt
from tmp_cbgb_tz_bs;


/*
create table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_bs_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`location_code`                  string              COMMENT    '地点编码',
`location_name`                  string              COMMENT    '地点名称',
`posting_time`                   string              COMMENT    '过账时间',
`wms_order_no`                   string              COMMENT    '订单号',
`wms_biz_type`                   string              COMMENT    '成本类型编码',
`wms_biz_type_name`              string              COMMENT    '成本类型',
`credential_no`                  string              COMMENT    '凭证号',
`purchase_group_code`            string              COMMENT    '商品采购组编码',
`purchase_group_name`            string              COMMENT    '商品采购组名称',
`dept_id`                        string              COMMENT    '课组编号',
`dept_name`                      string              COMMENT    '课组名称',
`product_code`                   string              COMMENT    '商品编码',
`product_name`                   string              COMMENT    '商品名称',
`unit_name`                      string              COMMENT    '单位',
`qty`                            decimal(26,6)       COMMENT    '数量',
`price_no_tax`                   decimal(26,6)       COMMENT    '不含税单价',
`amt_no_tax`                     decimal(26,6)       COMMENT    '不含税金额',
`amt`                            decimal(26,6)       COMMENT    '含税金额',
`fac_adjust_amt_no_tax`          decimal(26,6)       COMMENT    '工厂倒杂调整成本（不含税）',
`negative_adjust_amt_no_tax`     decimal(26,6)       COMMENT    '负库存调整成本（不含税）',
`remedy_adjust_amt_no_tax`       decimal(26,6)       COMMENT    '价格补救调整成本（不含税）',
`manual_adjust_amt_no_tax`       decimal(26,6)       COMMENT    '手工调整成本（不含税）',
`cost_amt_no_tax`                decimal(26,6)       COMMENT    '成本合计（不含税）',
`company_code`                   string              COMMENT    '公司编码',
`company_name`                   string              COMMENT    '公司名称',
`cost_center_code`               string              COMMENT    '成本中心编码',
`cost_center_name`               string              COMMENT    '成本中心名称',
`small_category_code`            string              COMMENT    '小类编码',
`small_category_name`            string              COMMENT    '小类名称',
`reservoir_area_code`            string              COMMENT    '库区编码',
`reservoir_area_name`            string              COMMENT    '库区名称',
`reservoir_area_prop`            string              COMMENT    '库区属性'

) COMMENT '财报管报-调整项-报损'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	
