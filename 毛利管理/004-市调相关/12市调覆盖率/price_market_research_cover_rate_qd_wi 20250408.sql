	select distinct
		csx_week, -- 彩食鲜业务周(上周六开始本周五结束)
		concat_ws('-',csx_week_begin,csx_week_end) as csx_week_range		
	from csx_dim.csx_dim_basic_date
	where calday>='20240729' 
	and calday<=regexp_replace(cast(to_date(date_sub(now(),1)) as string),'-','')
	order by csx_week
	
	


import_table_price_market_research_list
-- drop table if exists import_table_price_market_research_list;
CREATE TABLE `import_table_price_market_research_list` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
`flag` varchar(32) DEFAULT NULL COMMENT '类别',
`province_name` varchar(32) DEFAULT NULL COMMENT '省区',
`city_name` varchar(32) DEFAULT NULL COMMENT '城市',
`location_code` varchar(32) DEFAULT NULL COMMENT 'DC编码',
`classify_large_name` varchar(32) DEFAULT NULL COMMENT '一级分类',
`classify_middle_name` varchar(32) DEFAULT NULL COMMENT '二级分类',
`goods_code` varchar(32) DEFAULT NULL COMMENT '商品编码',
`goods_name` varchar(256) DEFAULT NULL COMMENT '商品名称',
`regionalized_goods_name` varchar(256) DEFAULT NULL COMMENT '区域化名称',
`unit_name` varchar(32) DEFAULT NULL COMMENT '单位',
`standard` varchar(64) DEFAULT NULL COMMENT '规格',
`sale_amt` decimal(20,6) DEFAULT NULL COMMENT '销售额',
`sale_qty` decimal(20,6)  DEFAULT NULL COMMENT '销售数量',
`profit_rate` decimal(20,6)  DEFAULT NULL COMMENT '毛利率',
`customer_cnt` decimal(20,6)  DEFAULT NULL COMMENT '下单客户数',
`day_cnt` int(4) DEFAULT NULL COMMENT '动销天数',
`goods_cnt` int(4) DEFAULT NULL COMMENT '下单次数',
`sdt_import` varchar(32) DEFAULT NULL COMMENT '导入日期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='导入表-市调清单-标品非标品';

create table csx_ods.csx_ods_data_analysis_prd_import_table_price_market_research_list_df(
`id` bigint COMMENT '主键',
`flag` string COMMENT '类别',
`province_name` string COMMENT '省区',
`city_name` string COMMENT '城市',
`location_code` string COMMENT 'DC编码',
`classify_large_name` string COMMENT '一级分类',
`classify_middle_name` string COMMENT '二级分类',
`goods_code` string COMMENT '商品编码',
`goods_name` string COMMENT '商品名称',
`regionalized_goods_name` string COMMENT '区域化名称',
`unit_name` string COMMENT '单位',
`standard` string COMMENT '规格',
`sale_amt` decimal(20,6) COMMENT '销售额',
`sale_qty` decimal(20,6) COMMENT '销售数量',
`profit_rate` decimal(20,6) COMMENT '毛利率',
`customer_cnt` decimal(20,6) COMMENT '下单客户数',
`day_cnt` int COMMENT '动销天数',
`goods_cnt` int COMMENT '下单次数',
`sdt_import` string COMMENT '导入日期'
) COMMENT '导入表-市调清单-标品非标品'
;




-- ds1
select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,	
	flag,	
	swt,
	count(1) as count_sku,
	count(case when is_market_research_price_all='是' then goods_code end) as sd_count_sku,
	count(case when is_market_research_price_ty='是' then goods_code end) as sd_count_sku_ty,
	count(case when is_market_research_price_yc='是' then goods_code end) as sd_count_sku_yc,
	count(case when is_market_research_price_ty_online='是' then goods_code end) as sd_count_sku_ty_online,
	count(case when is_market_research_price_ty_offline='是' then goods_code end) as sd_count_sku_ty_offline,
	
	sum(sale_amt)/10000 as sale_amt,
	sum(case when is_market_research_price_all='是' then sale_amt end)/10000 as sd_sale_amt,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/10000 as sd_sale_amt_ty,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/10000 as sd_sale_amt_yc
from csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,	
	flag,	
	swt
	
-- 明细	
select *
from csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
${if(len(is_market_research_price_all)==0,"","and is_market_research_price_all in( '"+is_market_research_price_all+"')")}


-- 周趋势 如何定位到近8周 更新时间近2个月？
select 	
	swt as `周`,
	count(1) as `需市调数-sku`,
	count(case when is_market_research_price_all='是' then goods_code end)/count(1) as `市调覆盖率-sku`,
	count(case when is_market_research_price_ty='是' then goods_code end)/count(1) as `通用覆盖率-sku`,
	count(case when is_market_research_price_yc='是' then goods_code end)/count(1) as `云超覆盖率-sku`,
	
	sum(sale_amt) as `需市调数-销售额`,
	sum(case when is_market_research_price_all='是' then sale_amt end)/sum(sale_amt) as `市调覆盖率-销售额`,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/sum(sale_amt) as `通用覆盖率-销售额`,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/sum(sale_amt) as `云超覆盖率-销售额`
from csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi
where update_time>date_sub(now(),60)
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by swt


substr(week_range,10,8)

regexp_replace(cast(to_date(add_months(now(),-1)) as string),'-','')




-- 周趋势
select 	
	swt as `周`,
	count(1) as `需市调数-sku`,
	count(case when is_market_research_price_all='是' then goods_code end)/count(1) as `市调覆盖率-sku`,
	count(case when is_market_research_price_ty='是' then goods_code end)/count(1) as `通用覆盖率-sku`,
	count(case when is_market_research_price_yc='是' then goods_code end)/count(1) as `云超覆盖率-sku`,
	
	sum(sale_amt) as `需市调数-销售额`,
	sum(case when is_market_research_price_all='是' then sale_amt end)/sum(sale_amt) as `市调覆盖率-销售额`,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/sum(sale_amt) as `通用覆盖率-销售额`,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/sum(sale_amt) as `云超覆盖率-销售额`
from csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by swt

union all	
select 	
	'202430' as `周`,
	count(1) as `需市调数-sku`,
	count(case when is_market_research_price_all='是' then goods_code end)/count(1) as `市调覆盖率-sku`,
	count(case when is_market_research_price_ty='是' then goods_code end)/count(1) as `通用覆盖率-sku`,
	count(case when is_market_research_price_yc='是' then goods_code end)/count(1) as `云超覆盖率-sku`,
	
	sum(sale_amt) as `需市调数-销售额`,
	sum(case when is_market_research_price_all='是' then sale_amt end)/sum(sale_amt) as `市调覆盖率-销售额`,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/sum(sale_amt) as `通用覆盖率-销售额`,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/sum(sale_amt) as `云超覆盖率-销售额`
from csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by swt

union all	
select 	
	'202429' as `周`,
	count(1) as `需市调数-sku`,
	count(case when is_market_research_price_all='是' then goods_code end)/count(1) as `市调覆盖率-sku`,
	count(case when is_market_research_price_ty='是' then goods_code end)/count(1) as `通用覆盖率-sku`,
	count(case when is_market_research_price_yc='是' then goods_code end)/count(1) as `云超覆盖率-sku`,
	
	sum(sale_amt) as `需市调数-销售额`,
	sum(case when is_market_research_price_all='是' then sale_amt end)/sum(sale_amt) as `市调覆盖率-销售额`,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/sum(sale_amt) as `通用覆盖率-销售额`,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/sum(sale_amt) as `云超覆盖率-销售额`
from csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by swt
	






-- drop table csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi;
-- create table csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi as
with
-- 通用市调价
market_research_ty as
(
	select location_code,product_code,
	if(sum(if(substr(shop_code,2,1)='W',1,0))>0,'是','否') as is_ty_online,  --线上通用市调
	if(sum(if(substr(shop_code,2,1)<>'W',1,0))>0,'是','否') as is_ty_offline,  --线下通用市调	
	concat_ws(',',collect_list(concat(shop_code,shop_name))) as shop_code_name,
	concat_ws(',',collect_list(concat(shop_code,shop_name,round(market_research_price,2)))) as market_research_price  -- 市调价格
		-- price_end_time,  -- 生效结束时间
		-- row_number() over(partition by location_code,product_code order by create_date desc) as rum
	from csx_analyse.csx_analyse_fr_price_market_research_price_detail_df
	where substr(price_end_time,1,10)>=cast(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),2) as string)  -- 生效结束时间>=T-2
	and (change_type_name<>'手动失效' or change_type_name is NULL)  -- 页面修改价格类型<>手动失效
	and shop_code not in('ZD273','ZD319','ZD41','ZD527') -- 剔除4个市调地点（ZD273、ZD319、ZD41、ZD527）
	group by location_code,product_code	
),
-- 云超价，通用市调-永辉门店
market_research_yc as
(
select *
from 
(
	select location_code,product_code,
		market_research_price,  -- 市调价格
		market_research_date,
		row_number() over(partition by location_code,product_code order by market_research_date desc) as rum
	from 
	(	
		select 
			b.location_code,
			b.product_code,
			-- regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 		
			a.market_research_date,
			min(a.market_research_price) as market_research_price
		from 
			(
				select * 
				from csx_dwd.csx_dwd_price_market_research_price_di 
				-- where sdt='${sdt_yes}'  -- regexp_replace(date_sub(current_date,1),'-','')
				where sdt>=regexp_replace(date_sub('${sdt_date}',if(dayofweek('${sdt_date}')=1,9,dayofweek('${sdt_date}')-2+3)),'-','') -- 上周五
				and market_source_type_code='1' -- 市调来源类型编码：1-永辉门店,2-网站,3-批发市场,4-一批,5-二批,6-终端
				and shipper_code='YHCSX'
			) a 
			left join 
			(
				select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
				where sdt='${sdt_yes}'  -- regexp_replace(date_sub(current_date,1),'-','')
			) b on a.market_goods_id=b.id 
		group by 
			b.location_code,
			b.product_code,
			-- regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n',''), 		
			a.market_research_date
	)a
)a where rum=1
)

insert overwrite table csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi partition(swt)
select
	concat_ws('-',a.week_of_year,a.flag,a.location_code,a.goods_code) as biz_id,
	e.performance_region_code,
	e.performance_region_name,
	e.performance_province_code,
	e.performance_province_name,
	e.performance_city_code,
	e.performance_city_name,
	a.flag,
	a.province_name,
	a.city_name,
	a.location_code,
	g.classify_large_code,g.classify_large_name,
	g.classify_middle_code,g.classify_middle_name,
	g.classify_small_code,g.classify_small_name,	
	a.goods_code,
	a.goods_name,
	a.regionalized_goods_name,
	a.unit_name,
	a.standard,
	cast(a.sale_amt as decimal(20,6)) as sale_amt,
	cast(a.sale_qty as decimal(20,6)) as sale_qty,
	cast(a.profit_rate as decimal(20,6)) as profit_rate,
	cast(a.customer_cnt as decimal(20,6)) as customer_cnt,
	a.day_cnt,
	a.goods_cnt,
	a.sdt_import,
	a.product_code,  -- 原料编码
	a.product_name,  -- 原料名称
	a.product_source,  -- 原料来源	
	c.market_research_price as market_research_price_ty,  -- 市调价格
	c2.market_research_price as market_research_price_ty_bom,  -- 市调价格
	d.market_research_price as market_research_price_yc,
	d2.market_research_price as market_research_price_yc_bom,
	if(coalesce(c.market_research_price,c2.market_research_price) is not null,'是','否') as is_market_research_price_ty,
	if(coalesce(d.market_research_price,d2.market_research_price) is not null,'是','否') as is_market_research_price_yc,
	if(coalesce(c.market_research_price,c2.market_research_price,d.market_research_price,d2.market_research_price) is not null,'是','否') as is_market_research_price_all,
	a.week_range,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	c.shop_code_name as shop_code_name_ty,  -- 通用市调地点_商品
	c2.shop_code_name as shop_code_name_bom,  -- 通用市调地点_bom
	h.base_product_status_name,
	if(c.is_ty_online='是' or c2.is_ty_online='是','是','否') as is_market_research_price_ty_online,
	if(c.is_ty_offline='是' or c2.is_ty_offline='是','是','否') as is_market_research_price_ty_offline,
	a.week_of_year as swt
from 
(
	select
		a.*,
		b.product_code,  -- 原料编码
		b.product_name,  -- 原料名称
		b.flag as product_source  -- 原料来源
	from 
	(
	select a.*,
	-- b.csx_week, -- 彩食鲜业务周(上周六开始本周五结束)
	b.week_of_year, -- 年周(自然周)
	b.week_end,
	concat_ws('-',week_begin,week_end) as week_range
	from 
	(
	select * 
	from csx_analyse.csx_analyse_import_table_price_market_research_list_di
	where swt=concat(substr('${sdt_yes}',1,4),lpad(weekofyear(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),-0)),2,'0'))
	)a
	join csx_dim.csx_dim_basic_date b on a.sdt_import=b.calday
	-- 跨年的时候这里可能年周出现问题，需要注释掉,用导入最大日期
	-- where b.week_of_year=concat(substr('${sdt_yes}',1,4),lpad(weekofyear(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),-0)),2,'0'))
	)a
-- 匹配bom看是否有原料码
	left join 
	(
	select *
	from 
	(
		select *,
			row_number() over(partition by location_code,goods_code order by product_num desc) as rum
		from csx_analyse.csx_analyse_fr_ts_bom_man_made_factory_setting_df
	)a where rum=1
	)b on a.location_code=b.location_code and a.goods_code=b.goods_code
)a
-- 是否有通用市调价
left join market_research_ty c on a.location_code=c.location_code and a.goods_code=c.product_code
left join market_research_ty c2 on a.location_code=c2.location_code and a.product_code=c2.product_code
-- 是否有云超价，通用市调-永辉门店
left join market_research_yc d on a.location_code=d.location_code and a.goods_code=d.product_code
left join market_research_yc d2 on a.location_code=d2.location_code and a.product_code=d2.product_code
left join (select * from csx_dim.csx_dim_shop where sdt='current') e on e.shop_code=a.location_code
left join 
(
	select 
		classify_large_code,classify_large_name,
		classify_middle_code,classify_middle_name,
		classify_small_code,classify_small_name,
		goods_code,goods_name,unit_name,brand_name,standard,category_small_name,spu_goods_name,goods_bar_code,business_division_name
	from csx_dim.csx_dim_basic_goods
	where sdt ='current'
) g on a.goods_code=g.goods_code
left join 
(
	select dc_code,goods_code,goods_status_name,
 	goods_status_name as base_product_status_name	 -- 商品状态
from csx_dim.csx_dim_basic_dc_goods 
where sdt='current' 
) h on a.goods_code=h.goods_code and a.location_code=h.dc_code 		
;





drop table if exists csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi;
create table csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi(
`biz_id`	string	COMMENT	'业务主键',
`performance_region_code` string COMMENT '大区编码',
`performance_region_name` string COMMENT '大区名称',
`performance_province_code` string COMMENT '省区编码',
`performance_province_name` string COMMENT '省区名称',
`performance_city_code` string COMMENT '城市编码',
`performance_city_name` string COMMENT '城市名称',
`flag` string COMMENT '类别标品非标品',
`province_name` string COMMENT '导入省区',
`city_name` string COMMENT '导入城市',
`location_code` string COMMENT 'DC编码',
`classify_large_code` string COMMENT '管理大类编号',
`classify_large_name` string COMMENT '管理大类名称',
`classify_middle_code` string COMMENT '管理中类编号',
`classify_middle_name` string COMMENT '管理中类名称',
`classify_small_code` string COMMENT '管理小类编号',
`classify_small_name` string COMMENT '管理小类名称',
`goods_code` string COMMENT '商品编码',
`goods_name` string COMMENT '商品名称',
`regionalized_goods_name` string COMMENT '区域化名称',
`unit_name` string COMMENT '单位',
`standard` string COMMENT '规格',
`sale_amt` decimal(20,6) COMMENT '销售额',
`sale_qty` decimal(20,6) COMMENT '销售数量',
`profit_rate` decimal(20,6) COMMENT '毛利率',
`customer_cnt` decimal(20,6) COMMENT '下单客户数',
`day_cnt` int COMMENT '动销天数',
`goods_cnt` int COMMENT '下单次数',
`sdt_import` string COMMENT '导入日期',
`product_code` string COMMENT '原料编码',
`product_name` string COMMENT '原料名称',
`product_source` string COMMENT '原料来源',
`market_research_price_ty` string COMMENT '通用市调价格_商品',
`market_research_price_ty_bom` string COMMENT '通用市调价格_bom',
`market_research_price_yc` string COMMENT '云超价格_商品',
`market_research_price_yc_bom` string COMMENT '云超价格_bom',
`is_market_research_price_ty` string COMMENT '是否有通用市调价格',
`is_market_research_price_yc` string COMMENT '是否有云超价格',
`is_market_research_price_all` string COMMENT '是否有市调价格',
`csx_week_range` string COMMENT '周区间',
`update_time`	timestamp	COMMENT    '更新时间'
) COMMENT '市调清单与覆盖率-清单'
PARTITIONED BY (swt string COMMENT '日期分区')
;


insert overwrite table csx_analyse.csx_analyse_import_table_price_market_research_list_di partition(swt)
select a.*,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	concat(substr(a.sdt_import,1,4),lpad(weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(a.sdt_import,'yyyyMMdd'))),-0)),2,'0')) as swt
	from 
	(
	select 
	  flag,
	  province_name,
	  city_name,
	  location_code,
	  classify_large_name,
	  classify_middle_name,
	  goods_code,
	  goods_name,
	  regionalized_goods_name,
	  unit_name,
	  standard,
	  cast(sale_amt as decimal(20,6)) as sale_amt,
	  cast(sale_qty as decimal(20,6)) as sale_qty,
	  cast(profit_rate as decimal(20,6)) as profit_rate,
	  cast(customer_cnt as decimal(20,6)) as customer_cnt,
	  cast(day_cnt as decimal(20,6)) as day_cnt,
	  cast(goods_cnt as decimal(20,6)) as goods_cnt,
	  regexp_replace(date_sub('${sdt_date}',if(dayofweek('${sdt_date}')=1,-1-7,if(dayofweek('${sdt_date}')>=6,dayofweek('${sdt_date}')-2-14,dayofweek('${sdt_date}')-2-7))),'-','') as sdt_import -- 周一
	from csx_ods.csx_ods_data_analysis_prd_import_table_price_market_research_list_df a
	-- 导入最大日期
	join (select max(sdt_import) sdt_import from csx_ods.csx_ods_data_analysis_prd_import_table_price_market_research_list_df )a1 on a.sdt_import=a1.sdt_import
	where a.flag='标品'
	union all 
	select 
	  '非标品' as flag,
	  performance_province_name as province_name,
	  performance_city_name as city_name,
	  inventory_dc_code as location_code,
	  classify_large_name,
	  classify_middle_name,
	  goods_code,
	  goods_name,
	  regionalized_goods_name,
	  unit_name,
	  standard,
	  sale_amt,
	  sale_qty,
	  profit_rate,
	  customer_cnt,
	  day_cnt,
	  goods_cnt,
	  -- 非标品用每周五早推出来的数据，周四及前是下周一，周五六日为下下周一
	  regexp_replace(date_sub('${sdt_date}',if(dayofweek('${sdt_date}')=1,-1-7,if(dayofweek('${sdt_date}')>=6,dayofweek('${sdt_date}')-2-14,dayofweek('${sdt_date}')-2-7))),'-','') as sdt_import -- 周一
	from csx_analyse.csx_analyse_fr_no_standard_product_sales_df
	where stype='保留'
	)a
;	
	
drop table if exists csx_analyse.csx_analyse_import_table_price_market_research_list_di;
create table csx_analyse.csx_analyse_import_table_price_market_research_list_di(
`flag` string COMMENT '类别',
`province_name` string COMMENT '省区',
`city_name` string COMMENT '城市',
`location_code` string COMMENT 'DC编码',
`classify_large_name` string COMMENT '一级分类',
`classify_middle_name` string COMMENT '二级分类',
`goods_code` string COMMENT '商品编码',
`goods_name` string COMMENT '商品名称',
`regionalized_goods_name` string COMMENT '区域化名称',
`unit_name` string COMMENT '单位',
`standard` string COMMENT '规格',
`sale_amt` decimal(20,6) COMMENT '销售额',
`sale_qty` decimal(20,6) COMMENT '销售数量',
`profit_rate` decimal(20,6) COMMENT '毛利率',
`customer_cnt` decimal(20,6) COMMENT '下单客户数',
`day_cnt` decimal(20,6) COMMENT '动销天数',
`goods_cnt` decimal(20,6) COMMENT '下单次数',
`sdt_import` string COMMENT '导入日期',
`update_time`	timestamp	COMMENT    '更新时间'
) COMMENT '市调清单商品-推送非标品加清单标品'
PARTITIONED BY (swt string COMMENT '日期分区')
;


-- 查数
select 
	concat_ws('-',swt,flag,location_code,goods_code) abc,
	swt,
	flag,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,		
	goods_code,
	count(1) aa
from csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi
group by 
	concat_ws('-',swt,flag,location_code,goods_code),
	swt,
	flag,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,		
	goods_code
having aa>1

select *
from csx_analyse.csx_analyse_price_market_research_cover_rate_qd_wi
where concat_ws('-',swt,flag,location_code,goods_code)=''


select * from csx_analyse.csx_analyse_fr_ts_bom_man_made_factory_setting_df where location_code='W0A2' and goods_code='1028534'

	select location_code,product_code,
		market_research_price,  -- 市调价格
		price_end_time  -- 生效结束时间
	from csx_analyse.csx_analyse_fr_price_market_research_price_detail_df
	where substr(price_end_time,1,10)>=cast(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),-2) as string)  -- 生效结束时间>=T-2
	and (change_type_name<>'手动失效' or change_type_name is NULL)  -- 页面修改价格类型<>手动失效
	and shop_code not in('ZD273','ZD319','ZD41') -- 剔除3个市调地点（ZD273、ZD319、ZD41）
select * from csx_analyse.csx_analyse_fr_price_market_research_price_detail_df where location_code='W0A2' and product_code='1028534'








市调清单明细-清单
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E6%25B8%2585%25E5%258D%2595%25E6%2598%258E%25E7%25BB%2586-%25E6%25B8%2585%25E5%258D%2595.cpt&ref_t=design&ref_c=a7d05a63-1ec9-4346-951b-86fa3a3d18b7

市调覆盖率-清单
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E8%25A6%2586%25E7%259B%2596%25E7%258E%2587-%25E6%25B8%2585%25E5%258D%2595.cpt&ref_t=design&ref_c=a7d05a63-1ec9-4346-951b-86fa3a3d18b7


市调清单明细-非清单
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E6%25B8%2585%25E5%258D%2595%25E6%2598%258E%25E7%25BB%2586-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595.cpt&ref_t=design&ref_c=a7d05a63-1ec9-4346-951b-86fa3a3d18b7

市调覆盖率-非清单
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E8%25A6%2586%25E7%259B%2596%25E7%258E%2587-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595.cpt&ref_t=design&ref_c=a7d05a63-1ec9-4346-951b-86fa3a3d18b7








