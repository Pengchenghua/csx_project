--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


with tmp_cbgb_tz_pd as 
(
select 
	a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,
	a.location_code,a.location_name,a.company_code,a.company_name,a.product_code,
	regexp_replace(regexp_replace(a.product_name,'\n',''),'\r','') product_name,
	b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品') as is_factory_goods_name,
	sum(case when amt_no_tax>=0 then -amt_no_tax end )  inventory_p_no, --盘盈  
	sum(case when amt_no_tax<0 then -amt_no_tax end )  inventory_l_no, --盘亏
	sum(case when amt>=0 then -amt end )  inventory_p, --盘盈  
	sum(case when amt<0 then -amt end ) inventory_l --盘亏
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
			location_code,location_name,company_code,company_name,goods_code product_code,goods_name product_name,
			credential_no,posting_time,purchase_group_code,move_type_code,reservoir_area_code,
			if(move_type_code in ('115B','116A'),-1*qty,qty) qty,
			if(move_type_code in ('115B','116A'),-1*amt_no_tax,amt_no_tax) amt_no_tax,   --不含税金额
			if(move_type_code in ('115B','116A'),-1*amt,amt) amt   --含税金额
        from 
			-- csx_dw.dws_cas_r_d_account_credential_detail
			csx_dws.csx_dws_cas_credential_detail_di
        where 
			sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') 
			and sdt<regexp_replace(trunc('${ytd_date}','MM'),'-','')
			and wms_biz_type_code = 34
			and reservoir_area_code = 'PD01' 
			and (purchase_group_code like 'H%' or purchase_group_code like 'U%')		
		)a 
		left join 
			(
			select 
				shop_code,shop_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
			from 
				csx_dim.csx_dim_shop
			where 
				sdt = 'current'
			) b on b.shop_code=a.location_code
	)a
	left join 
		(
		select 
			regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') goods_name,
			goods_code,purchase_group_code dept_id,purchase_group_name dept_name
		from 
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where 
			sdt = 'current' 
		)b on a.product_code=b.goods_code
	left join
		(
		select
			workshop_code,province_code,goods_code
		from 
			-- csx_dw.dws_mms_w_a_factory_setting_craft_once_all
			csx_dws.csx_dws_mms_factory_setting_craft_once_all_df
		where 
			sdt='current' and new_or_old=1
		)d on a.performance_province_code=d.province_code and a.product_code=d.goods_code
group by 
	a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,
	a.location_code,a.location_name,a.company_code,a.company_name,a.product_code,product_name,
	b.dept_id,b.dept_name,
	if(d.workshop_code is null,'不是工厂商品','是工厂商品')
)

insert overwrite table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_pd_mi partition(sdt)

select 
	concat_ws('&',performance_city_code,location_code,product_code,substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6)) as biz_id,
	performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	location_code,location_name,company_code,company_name,product_code,product_name,
	dept_id,dept_name,is_factory_goods_name,
	inventory_p_no,inventory_l_no,
	inventory_p,inventory_l,
	substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6) as sdt
from tmp_cbgb_tz_pd;


/*
create table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_pd_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`location_code`                  string              COMMENT    '地点编码',
`location_name`                  string              COMMENT    '地点名称',
`company_code`                   string              COMMENT    '公司编码',
`company_name`                   string              COMMENT    '公司名称',
`product_code`                   string              COMMENT    '商品编码',
`product_name`                   string              COMMENT    '商品名称',
`dept_id`                        string              COMMENT    '课组编号',
`dept_name`                      string              COMMENT    '课组名称',
`is_factory_goods_name`          string              COMMENT    '是否工厂商品',
`inventory_p_no`                 decimal(26,6)       COMMENT    '盘盈不含税',
`inventory_l_no`                 decimal(26,6)       COMMENT    '盘亏不含税',
`inventory_p`                    decimal(26,6)       COMMENT    '盘盈含税',
`inventory_l`                    decimal(26,6)       COMMENT    '盘亏含税'

) COMMENT '财报管报-调整项-盘点'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	

