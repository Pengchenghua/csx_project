drop table if exists csx_analyse_tmp.feibiaopin; 
create table if not exists csx_analyse_tmp.feibiaopin as 
select
	e.performance_province_name,
	e.performance_city_name,
	a.inventory_dc_code,
	c.business_division_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.category_small_name,
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	regexp_replace(d.regionalized_goods_name,'\n|\t|\r|\,|\"|\\\\n','') as regionalized_goods_name, 
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end as is_beihuo_goods,
	c.goods_bar_code,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	count(distinct a.customer_code) customer_cnt,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) as goods_cnt
from 
	(
	select 
		sdt,goods_code,customer_code,inventory_dc_code,order_code,sale_amt,sale_qty,profit
	from 
		csx_dws.csx_dws_sale_detail_di
	-- where sdt between regexp_replace(cast(to_date(add_months(now(),-1)) as string),'-','') and regexp_replace(cast(to_date(date_sub(now(),1)) as string),'-','')   -- impala
	where sdt between regexp_replace(add_months(current_date,-1),'-','') 
		and regexp_replace(date_sub(current_date,1),'-','')   -- hive
		and channel_code in ('1','7','9')
		and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and delivery_type_code in (1) -- 剔除直送和自提 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		--and order_channel_code not in (5) -- 剔除调价返利和价格补救 订单来源渠道: 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		--and order_channel_detail_code in (11,12) -- 11系统手工单 12小程序大宗单 25客户返利 26价格补救 27客户调价
		and inventory_dc_code in('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95','WC53')
	) a 	
	join 
		(
		select 
			goods_code,goods_name,unit_name,classify_large_name,classify_middle_name,classify_small_name,brand_name,standard,category_small_name,spu_goods_name,goods_bar_code,business_division_name
		from 
			csx_dim.csx_dim_basic_goods
		where 
			sdt ='current'
			and (
				(classify_large_name='干货加工' and (classify_middle_name='蛋' or unit_name='KG'))
				or (classify_large_name='肉禽水产' and classify_middle_name!='预制菜')	
				or classify_large_name='蔬菜水果'			
				) --  标品
		) c on a.goods_code=c.goods_code
	join
		(
		select
			dc_code,goods_code,shop_special_goods_status,goods_status_name,stock_attribute_code,regionalized_goods_name,stock_attribute_name --1存储 2货到即配
		from
			csx_dim.csx_dim_basic_dc_goods
		where 
			sdt = 'current'
			and shop_special_goods_status in('0','7') -- 0：B 正常商品；3：H 停售；6：L 退场；7：K 永久停购；
		) d on d.dc_code=a.inventory_dc_code and d.goods_code=a.goods_code	
	left join 
		(
		select 
			shop_code,shop_name,performance_province_name,performance_city_name
		from 
			csx_dim.csx_dim_shop 
		where 
			sdt='current'
		) e on e.shop_code=a.inventory_dc_code
group by 
	e.performance_province_name,
	e.performance_city_name,
	a.inventory_dc_code,
	c.business_division_name,
	c.classify_large_name,
	c.classify_middle_name,
	c.category_small_name,
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	regexp_replace(d.regionalized_goods_name,'\n|\t|\r|\,|\"|\\\\n',''),
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end,
	c.goods_bar_code
having
	sum(a.sale_amt)>0
;


insert overwrite table csx_analyse.csx_analyse_fr_no_standard_product_sales_df	
select 
a.*,
case
    when change_goods_name like '%TS%'
    or change_goods_name like '%DHM%'
    or change_goods_name like '%ZD%'
    or change_goods_name like '%WJ%'
    or change_goods_name like '%XB%'
    or change_goods_name like '%FJ%'
    or change_goods_name like '%EJ%'
    or change_goods_name like '%YF%'
    or change_goods_name like '%HDL%'
    or change_goods_name like '%PY%'
    or change_goods_name like '%LY%'
    or change_goods_name like '%SJQ%'
    or change_goods_name like '%CY%'
    or change_goods_name like '%HC%'
    or change_goods_name like '%SH%'
    or change_goods_name like '%YY%'
    or change_goods_name like '%YT%'
    or change_goods_name like '%KJ%'
    or change_goods_name like '%YEY%'
    or change_goods_name like '%JP%'
    or change_goods_name like '%JY%'
    or change_goods_name like '%SE%'
    or change_goods_name like '%GT%'
    or change_goods_name like '%LG%'
    or change_goods_name like '%TD%'
    or change_goods_name like '%HW%'
    or change_goods_name like '%TH%'
    or change_goods_name like '%XB%'
    or change_goods_name like '%TL%'
    or change_goods_name like '%JV%'
    or change_goods_name like '%RT%'
    or change_goods_name like '%CSX%'
    or change_goods_name like '%TJ%'
    or change_goods_name like '%GJ%'
    or change_goods_name like '%JG%'
    or change_goods_name like '%HJ%'
    or change_goods_name like '%SWDX%'
    or change_goods_name like '%JC%'
    or change_goods_name like '%ts%'
    or change_goods_name like '%dhm%'
    or change_goods_name like '%zd%'
    or change_goods_name like '%wj%'
    or change_goods_name like '%xb%'
    or change_goods_name like '%fj%'
    or change_goods_name like '%ej%'
    or change_goods_name like '%yf%'
    or change_goods_name like '%hdl%'
    or change_goods_name like '%py%'
    or change_goods_name like '%ly%'
    or change_goods_name like '%sjq%'
    or change_goods_name like '%cy%'
    or change_goods_name like '%hc%'
    or change_goods_name like '%sh%'
    or change_goods_name like '%yy%'
    or change_goods_name like '%yt%'
    or change_goods_name like '%kj%'
    or change_goods_name like '%yey%'
    or change_goods_name like '%jp%'
    or change_goods_name like '%jy%'
    or change_goods_name like '%se%'
    or change_goods_name like '%gt%'
    or change_goods_name like '%lg%'
    or change_goods_name like '%td%'
    or change_goods_name like '%hw%'
    or change_goods_name like '%th%'
    or change_goods_name like '%xb%'
    or change_goods_name like '%tl%'
    or change_goods_name like '%jv%'
    or change_goods_name like '%rt%'
    or change_goods_name like '%csx%'
    or change_goods_name like '%tj%'
    or change_goods_name like '%gj%'
    or change_goods_name like '%jg%'
    or change_goods_name like '%hj%'
    or change_goods_name like '%swdx%'
    or change_goods_name like '%jc%'
    or change_goods_name like '%礼盒%'
    or change_goods_name like '%五得利%'
    or change_goods_name like '%清洁蛋%'
    or change_goods_name like '%航天直属%'
    or change_goods_name like '%比格%'
    or change_goods_name like '%景戈塔%'
    or change_goods_name like '%直送%'
    or change_goods_name like '%定制%'
    or change_goods_name like '%幼儿园%'
    or change_goods_name like '%团购%'
    or change_goods_name like '%专用%'
    or change_goods_name like '%B端%'
    or change_goods_name like '%星辉%'
    or change_goods_name like '%员工%'
    or change_goods_name like '%玉蝴蝶%'
    or change_goods_name like '%青田上%'
    or change_goods_name like '%圣农%'
    or change_goods_name like '%外销%'
    or change_goods_name like '%招待所%'
    or change_goods_name like '%交大%'
    or change_goods_name like '%高超%'
    or change_goods_name like '%融通%'
    or change_goods_name like '%联%'
    or change_goods_name like '%石人山%'
    or change_goods_name like '%索迪斯%'
    or change_goods_name like '%工厂子码%'
    or change_goods_name like '%公交%'
    or change_goods_name like '%去杂%'
    or change_goods_name like '%专供%'
    or change_goods_name like '%鲜伊品%'
    or change_goods_name like '%丰众%'
    or change_goods_name like '%豆承%'
    or change_goods_name like '%渔沧海%'
    or change_goods_name like '%恒都%'
    or change_goods_name like '%鸿光%'
    or change_goods_name like '%聚行家%'
    or change_goods_name like '%风车%'
    or change_goods_name like '%牛德幅%'
    or change_goods_name like '%太合%'
    or change_goods_name like '%百香顺%'
    or change_goods_name like '%馔玉%'
    or change_goods_name like '%美佳达%'
    or change_goods_name like '%佳丰%'
    or change_goods_name like '%大庄园%'
    or change_goods_name like '%双汇%'
    or change_goods_name like '%雨润%' then '定制'
	
	when   -- 精品：一级 精品 特级 A（除蛋、猪肉）
	(change_goods_name like '%一级%'
	or change_goods_name like '%精品%' 
	or change_goods_name like '%特级%'
	or change_goods_name like '%a%'
	or change_goods_name like '%A%'
	) and classify_middle_name not in ('蛋', '猪肉') 
	then '精品'
	
	when   -- 二级：b 二级（不含福建省、广东）、贵阳:蔬果非彩食鲜开头为二级品
	(change_goods_name like '%b%'
	or change_goods_name like '%B%'
	or change_goods_name like '%二级%'
	) and performance_province_name not in ('福建省', '广东深圳','广东广州')
	then '二级'
	
	when   
	performance_province_name ='贵州省' 
	and classify_large_name='蔬菜水果' 
	and change_goods_name not like '彩食鲜%' 
	then '二级'

	when   -- 热鲜：肉禽分类包含热（不含南平、三明、深圳），直接用一级分类肉禽水产判断，因为水产没有热
	classify_large_name='肉禽水产' 
	and change_goods_name like '%热%' 
	and performance_city_name not in ('南平市', '三明市','深圳市')
	then '热鲜'	
	
	when customer_cnt=1 then '下单客户数1'   -- 所有仓，下单客户数=1
	
	when   -- 低销：①指定14个仓下单客户小于等于5，金额小于,3000；②全部仓：再剔除一遍：金额小于,1000
	inventory_dc_code in ('W0A6','W0Q2','W0BK','W0N0','W0AS','W0A2','W0P8','W0A8','W0A3','W0Q9','W0R9','W0A7','W0A5','W0BH')
	and customer_cnt<=5 and sale_amt< 3000 then '低销'
	
	when sale_amt<1000 then '低销'
	
	else '保留' end stype,
	from_utc_timestamp(current_timestamp(),'GMT') update_time
from 
	(select 
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.business_division_name,
		a.classify_large_name,
		a.classify_middle_name,
		a.category_small_name,
		a.spu_goods_name,
		a.brand_name,
		a.goods_code,
		a.goods_name,
		a.regionalized_goods_name, 
		a.unit_name,
		a.standard,	
		-- 重庆主城、四川、福州、贵阳,工厂bom这几列内容显示为空,南京蔬菜工厂bom这几列内容显示为空
		case when b.flag='工厂bom' and (b.location_code in ('W0A8','W0A6','W0Q2','W0A7') or (b.location_code ='W0R9' and a.classify_middle_name='蔬菜')) then null else b.product_code end as product_code,
		case when b.flag='工厂bom' and (b.location_code in ('W0A8','W0A6','W0Q2','W0A7') or (b.location_code ='W0R9' and a.classify_middle_name='蔬菜')) then null else b.product_name end as product_name ,
		case when b.flag='工厂bom' and (b.location_code in ('W0A8','W0A6','W0Q2','W0A7') or (b.location_code ='W0R9' and a.classify_middle_name='蔬菜')) then null else b.product_unit end as product_unit,
		case when b.flag='工厂bom' and (b.location_code in ('W0A8','W0A6','W0Q2','W0A7') or (b.location_code ='W0R9' and a.classify_middle_name='蔬菜')) then null else b.product_spec end as product_spec,
		case when b.flag='工厂bom' and (b.location_code in ('W0A8','W0A6','W0Q2','W0A7') or (b.location_code ='W0R9' and a.classify_middle_name='蔬菜')) then null else b.flag end as flag,
		a.goods_status_name,
		a.is_beihuo_goods,
		a.goods_bar_code,
		a.sale_amt,
		a.sale_qty,
		a.profit_rate,
		a.customer_cnt,
		a.day_cnt,
		a.goods_cnt,
		concat(a.goods_name,'-',a.regionalized_goods_name) change_goods_name
	from csx_analyse_tmp.feibiaopin a
	left join 
		(select * from 
			(select  
					location_code,
					goods_code,
					product_code,
					regexp_replace(product_name,'\n|\t|\r|\,|\"|\\\\n','') product_name, 
					product_unit,
					product_spec,
					flag,
					row_number()over(partition by location_code,goods_code) as srank
				from csx_analyse.csx_analyse_fr_ts_bom_man_made_factory_setting_df
			)b where srank=1
		)b on a.inventory_dc_code=b.location_code and a.goods_code=b.goods_code
	)a;
	


insert overwrite table csx_analyse.csx_analyse_fr_no_standard_product_market_survey_list_df	
select a.* from 
(select 
	a.*,
	row_number()over(partition by inventory_dc_code,goods_code) as srank
from 
	(select 
		performance_province_name,
		performance_city_name,
		inventory_dc_code,
		classify_large_name,
		classify_middle_name,
		goods_code,
		goods_name,
		regionalized_goods_name,
		unit_name,
		standard
	from csx_analyse.csx_analyse_fr_no_standard_product_sales_df
	where stype='保留' and flag is null
	union all
	select 
		performance_province_name,
		performance_city_name,
		inventory_dc_code,
		classify_large_name,
		classify_middle_name,
		product_code as goods_code,
		product_name as goods_name,
		'' as regionalized_goods_name,
		product_unit as unit_name,
		product_spec as standard
	from csx_analyse.csx_analyse_fr_no_standard_product_sales_df
	where stype='保留' and flag is not null
	) a
)a where srank =1


	
--hive 非标品商品近一月销售数据：
drop table if exists csx_analyse.csx_analyse_fr_no_standard_product_sales_df;
create table csx_analyse.csx_analyse_fr_no_standard_product_sales_df(
`performance_province_name` string COMMENT '省区',
`performance_city_name` string COMMENT '城市',
`inventory_dc_code` string COMMENT 'DC编码',
`business_division_name`  string COMMENT  '业务部名称',
`classify_large_name` string COMMENT '管理大类',
`classify_middle_name` string COMMENT '管理中类',
`category_small_name` string COMMENT  '管理小类',
`spu_goods_name` string COMMENT 'spu名称',
`brand_name` string COMMENT  '品牌',
`goods_code` string COMMENT  '商品编码',
`goods_name` string COMMENT  '商品名称',
`regionalized_goods_name` string COMMENT  '区域化名称', 
`unit_name` string COMMENT    '单位',
`standard` string COMMENT  '规格',
`product_code` string COMMENT '原料编码',
`product_name` string COMMENT '原料名称', 
`product_unit` string COMMENT '原料单位', 
`product_spec` string COMMENT '原料规格',
`flag` string COMMENT '来源', 
`goods_status_name` string COMMENT '商品状态', 
`is_beihuo_goods` string COMMENT  '是否备货商品',
`goods_bar_code` string COMMENT  '商品条码',
`sale_amt` decimal(20,6) COMMENT '销售额',
`sale_qty` decimal(20,6) COMMENT '销售数量',
`profit_rate` decimal(20,6) COMMENT '毛利率',
`customer_cnt` decimal(20,6) COMMENT '下单客户数',
`day_cnt` decimal(20,6) COMMENT '动销天数',
`goods_cnt` decimal(20,6) COMMENT '下单次数',
`change_goods_name` string COMMENT  '商品名称&区域化名称',
`stype` string COMMENT  '剔除标签',
`update_time` string  COMMENT '报表更新时间' 
) COMMENT '非标品商品近一月销售明细'
 -- PARTITIONED BY
 -- (`month` STRING  COMMENT '日期分区{"FORMAT":"yyyymm"}' );

 
 drop table if exists csx_analyse.csx_analyse_fr_no_standard_product_market_survey_list_df;
create table csx_analyse.csx_analyse_fr_no_standard_product_market_survey_list_df(
`performance_province_name` string COMMENT '省区',
`performance_city_name` string COMMENT '城市',
`inventory_dc_code` string COMMENT 'DC编码',
`classify_large_name` string COMMENT '管理大类',
`classify_middle_name` string COMMENT '管理中类',
`goods_code` string COMMENT  '商品编码',
`goods_name` string COMMENT  '商品名称',
`regionalized_goods_name` string COMMENT  '区域化名称', 
`unit_name` string COMMENT    '单位',
`standard` string COMMENT  '规格'
) COMMENT '非标品市调商品清单'