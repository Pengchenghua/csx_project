--进价全球眼平均价  
改：只要贵州省，，所有品类取近30天维度 ，，对标地点加入W0Q1。 20200922分区 

规则 ：对标生鲜W 对标食百9
价格对标：
1.各省区各品类“销售额占比前80%+工厂领用前80%”并集的商品，剔除BBC、合伙人、M端数据
2.彩食鲜价格--各品类近一周入库平均价格
3.对标云超价格--生鲜对标物流仓，食百对标门店；生鲜中大米类、粉类、调味类、家禽，食百近一周有入库取周平均价格，
				--近7天无入库取近30天平均价格；
				生鲜中其余品类取近一周入库平均价格
4.进价指数--可对比到商品的彩食鲜平均进价/云超平均进价*100
5.品类映射：
		--按课组分：
			蔬菜-蔬菜课（课组）；家禽-家禽课（课组）；猪肉-猪肉课（课组）；水果-水果课（课组）；
		--水产三个课组合并为水产	
			水产-冰鲜课（课组）&活鲜课（课组）&贝类课（课组）；
		--按中类分
			蛋类-蛋类（中类）；大米类-大米类（中类）；粉类-粉类（中类）；调味类-调味类（中类）；
		--按大类分
			食用油-食用油类（大类）；调味品类-调味品类（大类）；乳品饮料-常温乳品饮料（大类）
			
--20200618 新增条件			
6.要剔除四川W0B1   四川企业购红旗配送仓的数据

--20200618 敲定计算逻辑 
7.周/月平均价格计算加权平均价 即  总入库金额/总入库数量

--20200619新增规则
8.猪肉课组   家禽课组下的 鲜杀家禽 对标门店 （确认之后这里只对标门店、不对标物流仓）
	--说明：给定的仓有些省区没有、目前按给定做、后续确认之后再加门店	
--20200620新增规则
9.浙江云超对标物流地点更换为W0R6  （需要确认哪个城市）	
--20200624 新增逻辑 
10.云超剔除名称含有 彩食鲜 的商品 
--20200624新改逻辑
10.进价指数=可对标到商品的 彩食鲜加权平均进价/云超加权平均进价*100 
   商品周/月平均进价=总入库金额/总入库数量 
   加权平均进价=商品周/月平均进价*权重
   权重=商品入库金额/商品对应品类可对标所有商品的入库金额
  
-- --20200628新改逻辑
11.权重计算以彩食鲜为基准计算 云超也使用彩食鲜的为计算基准 

--20200701 新增逻辑
12.进价页面加入 有对标的权重进去  
	逻辑确认：有对标的商品在有对标的品类下的占比 、其他没有对标的为空
进价指数 ：
13.结果页要增加生鲜、食百汇总（汇总重点品类） 按目前指定对标的品类进行汇总
14.结果页增加高价支数、高价率 （各省区高价商品汇总）
细节确认：	有对标的 商品  为分母 、 彩食鲜平均进价高于云超为分子、全国不去重（所有省区的和）、

--20200801 --新增逻辑 
15、猪肉课单独做拆解 推算云超   
使用工厂分解型 价值占比、加权因子等推算、



--彩食鲜 分解型商品bom   分解 的商品 和对应的基数

--相同编码不同DC的名称不一致、得出的 商品页不一样、所以 对标做到DC

--猪肉课逻辑单独做、业务原因： 云超进价都是片肉、整猪、 所以对标很少、

drop table csx_tmp.temp_hsale_bom;
CREATE  table csx_tmp.temp_hsale_bom
as
select 
	a.id,
	province_name,
	city_name,
	a.location_code,
	a.product_code,
	b.product_code as goods_code,
	b.product_name as goods_name,
	b.number,
	c.calc_factor,   			--计算因子     使用工厂bom计算因子 获取得到 例如片肉拆解数量(商品分解数量占比)  
	(b.number / c.calc_factor) as goods_number,--分解出来的数量/计算因子（总的）=占比
	b.value_share		--价值占比、  使用彩食鲜工厂bom分解的部位价值占比 推算云超的单价   
from 
(
	select
		location_code,
		product_code,
		max(id) as id 		--因为一个仓会有相同bom分为不同部位、取最新的一条、
	from 
		csx_ods.source_mms_w_a_factory_setting_bom
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','') 
		and 
		bom_type =2 
		and 
		status =1 
	group by 
		location_code,
		product_code
) as a 
join
(
	select distinct
		id ,
		calc_factor			--bom 计算因子
	from 
		csx_ods.source_mms_w_a_factory_setting_bom  	--取计算因子
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','') 
		and 
		bom_type =2 
		and 
		status =1 
)as c on c.id=a.id	
join 
(
	select 
		*
	from 
		csx_ods.source_mms_w_a_factory_setting_bom_ability   --为了取价值占比 value_share
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','') 
) as b  on  a.id =b.bom_id 
join
(
	select distinct
		shop_id as shop_id, 
		city_name as city_name,
		province_name as province_name,
		dept_id_channel,
		case when shop_channel ='csx' then '彩食鲜' else '云超' end stype
	from 
		csx_dw.ads_sale_r_d_purprice_globaleye_shop_copy
	where 
		sdt='current' 
		and
		shop_id <> 'W0B1'		--运营 ： 剔除四川W0B1仓的数据  
) as d on a.location_code=d.shop_id--只要彩食鲜的
;



--云超猪肉课价值占比推算
drop table csx_tmp.temp_hsale_bom_yc;
CREATE  table csx_tmp.temp_hsale_bom_yc
as
select distinct
	a.shop_id_in,
	case when  b.product_code is null  then a.goodsid   --如果分解后的商品编码是空的，说明没有这个bom.不是拆解的
		else b.goods_code end  goodsid,
	a.pur_doc_id,
	a.sdt,	
	case when  b.product_code is null  then  a.pur_qty_in 
		else   (a.pur_qty_in*value_share /100)  end pur_qty_in,   			
		case when  b.product_code is null  then  (tax_pur_val_in/pur_qty_in )  --金额/数量=单价
		else  (tax_pur_val_in/pur_qty_in *value_share/100) /goods_number  end tax_pur_val_in	  --单价推荐计算逻辑    单价*价值占比  / 分解数量占比
from 
(
	select
		shop_id_in,
		goodsid,
		pur_doc_id,
		sdt,
		pur_qty_in,
		tax_pur_val_in  
	from 
		b2b.ord_orderflow_t --云超订单表
	where 
		sdt>=regexp_replace(date_sub(current_date,7),'-','') 	
		and 		
		pur_grp = 'H05'		--指定课组猪肉
		and  
		pur_qty_in>0 
		and
		tax_pur_val_in >0  --剔除入库金额为 0 的商品
		and 
		ordertype not in ('返配','退货') 
		and 
		regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'--供应商
        and 
        pur_doc_id not in ('4057186322','4057210905')

) as a 
join
(
	select distinct
		shop_id as shop_id, 
		city_name as city_name,
		province_name as province_name,
		dept_id_channel,
		case when shop_channel ='csx' then '彩食鲜' else '云超' end stype
	from 
		csx_dw.ads_sale_r_d_purprice_globaleye_shop_copy
	where 
		sdt='current' 
		and
		shop_id <> 'W0B1'		--运营 ： 剔除四川W0B1仓的数据  
) as d on a.shop_id_in=d.shop_id--拿到云超的数据
left join 
csx_tmp.temp_hsale_bom as b on d.province_name =b.province_name and  d.city_name =b.city_name and a.goodsid= b.product_code  --b.product_code 分解后的商品
;

--以上是猪肉
-------

--指定品类 近30天有销售的商品
drop table csx_tmp.temp_hsale;
CREATE  table csx_tmp.temp_hsale
as
select 
	'sale' gtype,
	dc_province_name as province_name,
	department_code as dept_id,	--课组编码
	goods_code,
	sum(sales_qty) as qty,
	sum(excluding_tax_sales) as untax_sale,
	sum(sales_value) as sale
from  
	csx_dw.dws_sale_r_d_customer_sale  
where 
	sdt>=regexp_replace(date_sub(current_date,30),'-','')
	and
	channel<>'2'    --、剔除M端数据
	and 
	sales_type <> 'bbc'  --剔除BBC 
	and
	attribute_code <>'5'  --剔除合伙人
	
--	and 
--	is_factory_goods_code ='1'				--工厂商品 (不含食百)  目前这个字段不准确
	and 
	(
		department_code in ('H08','H07','H06','H05','H04' ,'H03','H02')	-- 	生鲜  指定课组 
		or 
		category_middle_code in ('110130','110132')		--生鲜对标 30天 对标仓
		or 
		category_large_code in ('1250','1257','1241')  				--食百对标30天 对标门店
	--	or
	--	department_code in ('A04','A03')		--指定食百		
	)	
group by 
	dc_province_name,		--库存DC所属编码
	department_code,
	goods_code
;

--工厂生产订单领用  业务要求加入工厂领用  
insert into 
	csx_tmp.temp_hsale
select 
	'recip'gtype,
	c.province_name as province_name,
	b.dept_id as dept_id,	--课组编码
	a.goodsid  as goods_code,
	qty,
	untax_sale,
	untax_sale*(1+coalesce(b.taxrate,0)/100) as sale
from 
(
	select 
		location_code as  shop_id,
		product_code as goodsid,
		sum(case when status=0 then qty else -1*qty end) qty,
		sum(case when status=0 then qty*unit_price else -1*qty*unit_price end) as untax_sale
	from 
		csx_ods.source_mms_r_a_factory_mr_receive_return  --新系统工厂领用表
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','')
		and to_date(order_time)>=date_sub(current_date,30)
		and to_date(order_time)<=date_sub(current_date,1)
	group by 
		location_code,product_code
)a
join 
(
	select distinct 
		department_id	as dept_id,	--课组编码,
		goods_id as goodsid ,
		tax_rate as taxrate
	from 
		csx_dw.dws_basic_w_a_csx_product_m
	where 
		sdt='current' 
		and 
		(
			department_id in ('H08','H07','H06','H05','H04' ,'H03','H02')	-- 	生鲜  指定课组 
			or 
			category_middle_code in  ('110130','110132')		--生鲜对标 30天 对标仓
			or 
			category_large_code in ('1250','1257','1241')  				--食百对标30天 对标门店
	--		or
	--		department_code in ('A04','A03')		--指定食百		
		)	
)b on a.goodsid=b.goodsid 
left join 
(
	SELECT 
		shop_id,
		province_name
	from 
		 csx_dw.dws_basic_w_a_csx_shop_m 
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','') and shop_id like 'W%'--取彩食鲜的
)c on a.shop_id=c.shop_id
;	

--销售占比处理 业务要求 要是占比为前百分之80的商品 （因为数据量比较大、对标会很多为空的、需要再限定每个省区每个课组最多取80个）

drop table csx_tmp.temp_hsale1;
CREATE  table csx_tmp.temp_hsale1   --这一段是为了做销售占比的处理逻辑、
as
select 
	a.province_name,
	a.goods_code,
	x.goodsname,
	x.dept_id,
	x.dept_name,
	x.catg_m_id,
	x.catg_m_name,
	qty,
	untax_sale,
	sale,
	row_number() OVER(PARTITION BY x.dept_id ORDER BY sale desc) as rno,	
	sale/sale_t  as zb_sale					--销售占比
--	sum(sale)over(partition by 	a.province_name,x.dept_id,a.goods_code order by sale desc)/sale_t  as zb_sale	--销售占比
from 
(
	select 
		province_name,
		dept_id,
		goods_code,
		sum(qty) as qty,
		sum(untax_sale) as untax_sale,
		sum(sale) as sale 
	from 
		csx_tmp.temp_hsale 
	group by
		province_name,
		dept_id,		
		goods_code
) a
join 
(
	select 
		goods_id as goodsid,
		goods_name as goodsname,
		department_id as dept_id,
		department_name as dept_name,
		category_middle_code as catg_m_id,
		category_middle_name as catg_m_name 
	from 
		csx_dw.dws_basic_w_a_csx_product_m
	where 
		sdt='current'
)x on a.goods_code=x.goodsid 
join 
(
	select
		province_name,
		dept_id,
		sum(sale) as sale_t 		--按省区和课组汇总   取 该省区课组下的销售占比
	from 
		csx_tmp.temp_hsale
	group by 
		province_name,
		dept_id
)b on a.province_name =b.province_name and a.dept_id=b.dept_id
;--每个省区下每个商品占课组的比例


--进价明细   需求中彩食鲜取近一周、对标中不是、那么这边 限制 彩食鲜入库时间为近7天的明细、云超为近30天的明细
drop table csx_tmp.temp_purprice;
CREATE  table csx_tmp.temp_purprice
as
select DISTINCT
	stype,
	d.dept_id_channel,
	d.province_name,
	d.city_name,
	shop_id_in,
--	shop_name,
	goodsid,
	goodsname,
	b.dept_id,
	b.dept_name,
	b.catg_m_id,
	b.catg_m_name,
	pur_doc_id,
	sdt,
	pur_qty_in,
	tax_pur_val_in,	--单价
	tax_pur_val_in as  pur_price,
	(pur_qty_in * tax_pur_val_in) as  amount
from 
(	--永辉云超 金额/数量
	select
		shop_id_in,
		goodsid,
		pur_doc_id,
		sdt,
		pur_qty_in,
		tax_pur_val_in/pur_qty_in  as tax_pur_val_in 
	from 
		b2b.ord_orderflow_t 
	where 
		sdt>=regexp_replace(date_sub(current_date,30),'-','') 	
		and 
		(
		(substr(goods_catg,1,2) ='11' or goods_catg is null and pur_grp <> 'H05')		--生鲜  H05猪肉，前面单独做了，先去掉，后面加上
		or
		(substr(goods_catg,1,2) ='12' and pur_grp in ('A04','A03'))		--指定课组食百
		)
		and  
		pur_qty_in>0 
		and
		tax_pur_val_in >0  --剔除入库金额为 0 的商品		
		and 
		ordertype not in ('返配','退货') 
		and 
		regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
        and 
		pur_doc_id not in ('4057186322','4057210905')
	union all 
--加上 云超猪肉课单独推算处理	
	select
		shop_id_in,
		goodsid,
		pur_doc_id,
		sdt,
		pur_qty_in,
		tax_pur_val_in 
	from 
		csx_tmp.temp_hsale_bom_yc		
	union all 
	select 
		location_code as shop_id_in,
		product_code as goodsid,
		a.order_code as pur_doc_id,
		a.sdate as  sdt,
		receive_qty  as pur_qty_in,
		price as  tax_pur_val_in
		
	from 
	(
		select distinct 
			order_code,
			regexp_replace(to_date(receive_time),'-','')  as sdate 
		from 
			csx_ods.source_wms_r_d_entry_order_header a 
		where 
			sdt>=regexp_replace(date_sub(current_date,90),'-','') 
			and 
			(
			entry_type LIKE 'P%'
			and 
			entry_type not in ('P03','P06')
			)
			and 
			to_date(receive_time)>=date_sub(current_date,30) 	--需求：彩食鲜只取入库近一周的平均价 所以限制入库时间为七天、  
			and 
			return_flag<>'Y' and receive_status<>0
	)a 
	join 
	(
		select distinct 
			order_code,
			product_code,
			location_code,
			receive_qty,
			price,
			amount 
		from 
			csx_ods.source_wms_r_d_entry_order_item 
		where 
			sdt>=regexp_replace(date_sub(current_date,90),'-','') 
			and 
			receive_qty>0
			and 
			to_date(update_time)>=date_sub(current_date,30)
	)b on a.order_code=b.order_code

 )a 
join 
(
	select 
		* 
	from  
		csx_tmp.temp_hsale1  
	where 
		zb_sale<=0.8
--		and 
--	(rno=80 or rno is null)
)b on  a.goodsid=b.goods_code
join
(
	select distinct
		shop_id as shop_id, 
		city_name as city_name,
		province_name as province_name,
		dept_id_channel,
		case when shop_channel ='csx' then '彩食鲜' else '云超' end stype
	from 
		csx_dw.ads_sale_r_d_purprice_globaleye_shop_copy		--自己建的码表
	where 
		sdt='current' 
		and
		shop_id <> 'W0B1'		--运营 ： 剔除四川W0B1仓的数据  
) as d on a.shop_id_in=d.shop_id
;



-- 对标食百
-- H05	猪肉课
--H04	家禽课  110404 鲜杀家禽类

--省区下的进价  最大金额
--获取最近日期的进价(细分到城市)

--品类不为大米 粉类 调味类 的	省区下的平均价  '110119','110132','110125'

--品类为   department_id in ('H02','H03','H05') 
--H04	家禽课  H05	猪肉课  H06	冰鲜课 H07	贝类课  H08	活鲜课   H03	蔬菜课  H02	水果课

--H01	干货课	110130	蛋类

--只给 那三个给30天的   、其他干货类 都给 7天   

--要求近7天的 平均价  生鲜进价

--校验语句
--SELECT count(DISTINCT goodsid) as a  from  csx_tmp.temp_purprice_dept where stype='云超' and  shop_id_in not like 'W%' and (dept_id ='H05' or catg_m_id ='110404')  ;


drop table csx_tmp.temp_purprice_dept;
CREATE  table csx_tmp.temp_purprice_dept
as
select distinct
		stype,
		dept_id_channel,	--区分生鲜食百对标
		province_name,
		city_name,
		shop_id_in,
		shop_name,
		goodsid,
		goodsname,
		dept_id,
		dept_name,
		catg_m_id,
		catg_m_name,
		sdt,
		pur_qty_in,
		pur_price,
		amount
from 
(
	select 
		stype,
		dept_id_channel,
		province_name,
		city_name,
		shop_id_in,
--		shop_name,
		goodsid,
		goodsname,
		dept_id,
		dept_name,
		catg_m_id,
		catg_m_name,
		sdt,
		pur_qty_in,
		pur_price,
		amount				--金额明细
	--	pur_doc_id,
	--	avg(pur_price) as avg_pur_price   --平均进价
	from 
		csx_tmp.temp_purprice
	where  sdt>=regexp_replace(date_sub(current_date,30),'-','')
		--(
		--	sdt>=regexp_replace(date_sub(current_date,30),'-','')
		--		
		--	and 		
		--	dept_id_channel ='sx' 		--生鲜对标用 	生鲜中要求取30天
		--	and
		--	(dept_id ='H01' and catg_m_id  in ('110132')
		--)
		--)
		--or 
		--(
		--	sdt>=regexp_replace(date_sub(current_date,7),'-','')
		--	and 
		--	dept_id_channel ='sx' 		--生鲜取近7天   （猪肉课  和鲜杀家禽 另外给逻辑）'H05','H04' ,
		--	and	
        --   (
		--	dept_id	 in ('H08','H07','H06','H03','H02')    
		--	or 
		--	(dept_id ='H01' and catg_m_id = '110130')		--干货课 蛋类 对标 7天 
		--	or
		--	(stype ='彩食鲜' and dept_id in('H04','H05')  )   --剔除鲜杀家禽  --》确认只对标门店  取彩食鲜 猪肉 家禽
		--	or 
		--	(stype ='云超' and  dept_id='H04' and  catg_m_id <> '110404') 	--云超 生鲜不含 鲜杀家禽类
        --   )	
		--)
		--or
		--(		--只对标食百  （猪肉  鲜杀家禽类） 
		--	sdt>=regexp_replace(date_sub(current_date,7),'-','')
		--	and 
		--	dept_id_channel='sb'
		--	and 
		--	shop_id_in not like 'W%' 	--取对标门店 
		--	and 
		--	(dept_id = 'H05' or catg_m_id ='110404')   ---- 对标食百 H05	猪肉课    H04	家禽课  110404 鲜杀家禽类
		--)
		--
		--or
		--(
		--	sdt>=regexp_replace(date_sub(current_date,30),'-','')
		--	and 		
		--	dept_id_channel ='sb' 			--食百对标用 
		--	and
		--	--dept_id in ('A04','A03')
		--	substr(catg_m_id,1,4) in ('1250','1257','1241') 		--指定大类 食用油-食用油类（大类）；调味品类-调味品类（大类）；乳品饮料-常温乳品饮料（大类）
		--)

) as a 	
join
(
	select distinct
		shop_id ,
		shop_name 
	from 
		 csx_dw.dws_basic_w_a_csx_shop_m 
	where 
		sdt='current'
) as  b on b.shop_id =a.shop_id_in		
where province_name='贵州省'
;


--明细 
--校验语句
--SELECT count(DISTINCT goodsid) as a  from  csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye_detail where stype='云超' and  shop_id_in not like 'W%' and (dept_id ='H05' or catg_m_id ='110404')  ;


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye_detail partition (sdt) 
select distinct 
	stype,
	province_name ,
	city_name,
	goodsid,
	goodsname,
	dept_id,
	dept_name ,
	catg_m_id,
	catg_m_name,
	shop_id_in,
	shop_name,
	sdt as sdt_c,
	pur_qty_in ,
	pur_price,
--	amount,
	regexp_replace(date_sub(current_date,1),'-','') as sdt
from 
	csx_tmp.temp_purprice_dept
where 
	dept_id_channel ='sx'
	and 
	(
	(
		stype ='彩食鲜' 
		and
		dept_id not in ('A04','A03')
	)	
	or 	
	(
		stype ='云超' 
		and
		dept_id not in ('A04','A03')
		and 
		(dept_id <> 'H05' or catg_m_id<> '110404')
		and
		goodsname not like '%彩食鲜%'   --剔除商品中含有彩食鲜的
	)	
	)											-- 生鲜 剔除云超 猪肉  鲜杀家禽类
union all
select distinct 
	stype,
	province_name ,
	city_name,
	goodsid,
	goodsname,
	dept_id,
	dept_name ,
	catg_m_id,
	catg_m_name,
	shop_id_in,
	shop_name,
	sdt as sdt_c,
	pur_qty_in ,
	pur_price,
--	amount,
	regexp_replace(date_sub(current_date,1),'-','') as sdt
from 
	csx_tmp.temp_purprice_dept
where 
	dept_id_channel ='sb'
	and 
	(
	dept_id in ('A04','A03')
	or
	(
		stype ='云超' and (dept_id = 'H05' or catg_m_id = '110404')  
		and
		goodsname not like '%彩食鲜%'   --剔除商品中含有彩食鲜的
	)				--对标食百猪肉课 鲜杀家禽类
	)
;


--食百 和大米 有7天求七天、没有取 30天、


drop table csx_tmp.temp_purprice_dept1;
CREATE  table csx_tmp.temp_purprice_dept1
as
select
	a.stype,
	a.province_name ,
	a.city_name,
	a.goodsid,
	a.goodsname,
	a.dept_id,
	a.dept_name ,
	a.catg_m_id,
	a.catg_m_name,
	--a.shop_id_in,
	--a.shop_name,
--	if(b.goodsid is null or b.goodsid ='',a.sdt_c,b.sdt_c) as sdt_c,
	a.amount,
	a.qty,
	a.pur_price  --周/月平均价格计算加权平均价 即总入库金额/总入库数量
from 
(
	select
		stype,
		province_name ,
		city_name,
		goodsid,
		goodsname,
		dept_id,
		dept_name ,
		catg_m_id,
		catg_m_name,
		--shop_id_in,
		--shop_name,
		sum(pur_price*pur_qty_in) as amount,
		sum(pur_qty_in) as qty,
		sum(pur_price*pur_qty_in)/sum(pur_qty_in) as pur_price		--月平均价格
	from 
		csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye_detail
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','') 
		and 
		sdt_c >=regexp_replace(date_sub(current_date,30),'-','')
	group by 
		stype,
		province_name ,
		city_name,
		goodsid,
		goodsname,
		dept_id,
		dept_name ,
		catg_m_id,
		catg_m_name
		--shop_id_in,
		--shop_name
) a
;

--进价    --指标规则 总入库金额/总入库数量 (因为要加入权重、暂时赋值为空、临时使用作为跳板表--后面加入权重需要再重新插入)
insert overwrite table csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye partition (sdt) 
select 
	a.stype,
	a.province_name,
	a.city_name,
	a.goodsid,
	goodsname,
	catg_m_id,
	catg_m_name,
	a.dept_id,
	dept_name,
	sum(amount)/sum(qty) as  pur_price,
	'' as  amt_rno,--权重
	'' as rno_pur_price,--加权平均价
	regexp_replace(date_sub(current_date,1),'-','')sdt
from
	csx_tmp.temp_purprice_dept1 as a 
group by 
	a.stype,
	a.province_name,
	a.city_name,
	a.goodsid,
	goodsname,
	catg_m_id,
	catg_m_name,
	a.dept_id,
	dept_name
order by
	a.dept_id
;


--进价指数  可比商品数：省区城市下商品的总数，去重。全国的不去重。

--获取每个省区进价有对标的商品（彩食鲜有，云超也有）
drop table csx_tmp.temp_purprice_dept_duibiao;
CREATE  table csx_tmp.temp_purprice_dept_duibiao 
as 
select distinct
	a.province_name ,
	a.city_name,
	a.dept_id,
	c.category_large_code,
	c.category_large_name,
	a.catg_m_id,
	a.catg_m_name,
	a.goodsid,
	c.goodsname,
	a.pur_price as pur_price_csx,
	b.pur_price as pur_price_yc,
	if((a.pur_price - b.pur_price)>0, 1,0) as pur_price_hight		--如果 彩食鲜进价高于 云超进价 标记为1 
from 
(
	select
		province_name ,
		city_name,
		dept_id,
		dept_name ,
		catg_m_id,
		catg_m_name,
		goodsid,
		pur_price
	from 
		csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye	
	where 
		sdt=regexp_replace(date_sub(current_date,1),'-','')
		and
		stype ='彩食鲜'	
) as a 
join
(
	select
		province_name ,
		city_name,
		dept_id,
		dept_name ,
		catg_m_id,
		catg_m_name,
		goodsid,
		pur_price
	from 
		csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye	
	where
		sdt=regexp_replace(date_sub(current_date,1),'-','')
		and		
		stype ='云超'
) as b	 on  a.province_name=b.province_name and a.city_name=b.city_name and a.dept_id =b.dept_id and a.catg_m_id=b.catg_m_id  and a.goodsid=b.goodsid
join
(
	select 
		goods_id as goodsid,
		goods_name as goodsname,
		department_id as dept_id,
		department_name as dept_name,
		category_large_code,
		category_large_name,
		category_middle_code as catg_m_id,
		category_middle_name as catg_m_name 
	from 
		csx_dw.dws_basic_w_a_csx_product_m
	where 
		sdt='current'	
) as c on a.goodsid  =c.goodsid
;



--获取明细中(周平均--月平均) 有对标的商品的总金额 、总数量、  校验 1003379
--食百 和大米 有7天求七天、没有取 30天
drop table csx_tmp.temp_purprice_dept_duibiao_amt;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_amt 
as 
select 
	a.stype,
	a.province_name,
	a.city_name,
	a.goodsid,
	a.goodsname,
	a.dept_id,
	a.dept_name,
	b.category_large_code,
	b.category_large_name,
	a.catg_m_id,
	a.catg_m_name,
	--a.shop_id_in,
	--a.shop_name,
	a.amount,
	a.qty,
	a.pur_price,
	b.pur_price_hight				--如果 彩食鲜进价高于 云超进价 标记为1 
from 
	csx_tmp.temp_purprice_dept1  as a 		----食百 和大米 有7天求七天、没有取 30天
join 
	csx_tmp.temp_purprice_dept_duibiao  as b  on a.province_name=b.province_name and a.city_name=b.city_name and  a.dept_id =b.dept_id and a.catg_m_id=b.catg_m_id and a.goodsid=b.goodsid
;


drop table csx_tmp.temp_purprice_dept_duibiao_amt_middle;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_amt_middle 
as 
select
	x.stype,
	x.province_name ,
	x.city_name,
	x.dept_id,
	x.dept_name ,
	x.catg_m_id,
	x.catg_m_name,
	x.goodsid,
	sum(amount) as goods_amount,		--省市下 -品类汇总金额 数量
	sum(qty) as goods_qty,
	x.pur_price_hight 		--如果 彩食鲜进价高于 云超进价 标记为1
from 
(

	select
		a.stype,
		a.province_name ,
		a.city_name,
		if(a.dept_id in ('H08','H07','H06'),'11', a.dept_id ) as dept_id,

		
		if(a.dept_id in ('H08','H07','H06'),'冰鲜&活鲜&贝类', a.dept_name ) as dept_name,	
		
		case when 	a.category_large_code in ('1250','1257','1241') then a.category_large_code
			when  a.dept_id in ('H05','H04' ,'H03','H02') then a.dept_id
			when  a.dept_id in ('H08','H07','H06') then  '11'
			else    a.catg_m_id 
			end  as catg_m_id,
		case when 	a.category_large_code in ('1250','1257','1241') then a.category_large_name
			when  a.dept_id in ('H05','H04' ,'H03','H02') then a.dept_name
			when  a.dept_id in ('H08','H07','H06') then  '水产'
			else    a.catg_m_name 
			end  as catg_m_name,	
		a.goodsid,
		a.amount,
		a.qty,
		a.pur_price,
		a.pur_price_hight 		--如果 彩食鲜进价高于 云超进价 标记为1
from 
	csx_tmp.temp_purprice_dept_duibiao_amt    as a 
union all 
	select
		b.stype,
		b.province_name ,
		b.city_name,
		if(b.dept_id like 'H%','生鲜', '食百') as dept_id,

		
		if(b.dept_id like 'H%','生鲜', '食百') as dept_name,	
		
		if(b.dept_id like 'H%','生鲜', '食百')  as catg_m_id,
		if(b.dept_id like 'H%','生鲜', '食百') as catg_m_name,	
		b.goodsid,
		b.amount,
		b.qty,
		b.pur_price,
		b.pur_price_hight 		--如果 彩食鲜进价高于 云超进价 标记为1
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt    as b 
union all 
	select
		c.stype,
		c.province_name ,
		c.city_name,
		'小计' as dept_id,

		
		'小计' as dept_name,	
		
		'小计'  as catg_m_id,
		'小计' as catg_m_name,	
		c.goodsid,
		c.amount,
		c.qty,
		c.pur_price,
		c.pur_price_hight 		--如果 彩食鲜进价高于 云超进价 标记为1
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt  as c    		
) as x 	
group by 
	x.stype,
	x.province_name ,
	x.city_name,
	x.dept_id,
	x.dept_name ,
	x.catg_m_id,
	x.catg_m_name,
	x.goodsid,
	x.pur_price_hight
--全国  对标的商品  校验 1003379
union all 
select
	x.stype,
	'全国' as province_name ,
	'全国' as city_name,
	x.dept_id,
	x.dept_name ,
	x.catg_m_id,
	x.catg_m_name,
	x.goodsid,
	sum(amount) as goods_amount,		--省市下 -品类汇总金额 数量
	sum(qty) as goods_qty,
	'' as pur_price_hight		--有些城市商品有对标、而其他城市没有、所以为空
from 
(

	select
		a.stype,
		a.province_name ,
		a.city_name,		
		if(a.dept_id in ('H08','H07','H06'),'11', a.dept_id ) as dept_id,
	
		if(a.dept_id in ('H08','H07','H06'),'冰鲜&活鲜&贝类', a.dept_name ) as dept_name,	
		
		case when 	a.category_large_code in ('1250','1257','1241') then a.category_large_code
			when  a.dept_id in ('H05','H04' ,'H03','H02') then a.dept_id
			when  a.dept_id in ('H08','H07','H06') then  '11'
			else    a.catg_m_id 
			end  as catg_m_id,
		case when 	a.category_large_code in ('1250','1257','1241') then a.category_large_name
			when  a.dept_id in ('H05','H04' ,'H03','H02') then a.dept_name
			when  a.dept_id in ('H08','H07','H06') then  '水产'
			else    a.catg_m_name 
			end  as catg_m_name,	
		a.goodsid,
		a.amount,
		a.qty,
		a.pur_price
	--	a.pur_price_hight
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt    as a 
	union all 
		
		select
			b.stype,
			b.province_name ,
			b.city_name,
			if(b.dept_id like 'H%','生鲜', '食百') as dept_id,
	
			
			if(b.dept_id like 'H%','生鲜', '食百') as dept_name,	
			
			if(b.dept_id like 'H%','生鲜', '食百')  as catg_m_id,
			if(b.dept_id like 'H%','生鲜', '食百') as catg_m_name,	
			b.goodsid,
			b.amount,
			b.qty,
			b.pur_price
		--	b.pur_price_hight 		--如果 彩食鲜进价高于 云超进价 标记为1
		from 
			csx_tmp.temp_purprice_dept_duibiao_amt    as b 
	union all 
		select
			c.stype,
			c.province_name ,
			c.city_name,
			'小计' as dept_id,
	
			
			'小计' as dept_name,	
			
			'小计'  as catg_m_id,
			'小计' as catg_m_name,	
			c.goodsid,
			c.amount,
			c.qty,
			c.pur_price
		--	c.pur_price_hight 		--如果 彩食鲜进价高于 云超进价 标记为1
		from 
			csx_tmp.temp_purprice_dept_duibiao_amt  as c 
) as x 	
group by 
		x.stype,
	--	x.province_name ,
	--	x.city_name,
		x.dept_id,
		x.dept_name ,
		x.catg_m_id,
		x.catg_m_name,
		x.goodsid
	--	x.pur_price_hight
		
;


--品类维度（中类）     --作为分母
drop table csx_tmp.temp_purprice_dept_duibiao_amt_all;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_amt_all 
as 
select
	x.stype,
	x.province_name ,
	x.city_name,
	x.dept_id,
	x.dept_name ,
	x.catg_m_id,
	x.catg_m_name,
	sum(amount) as middle_amount,		--省市下 -品类汇总金额 数量
	sum(qty) as middle_qty
from 
(

	select
		a.stype,
		a.province_name ,
		a.city_name,
		if(a.dept_id in ('H08','H07','H06'),'11', a.dept_id ) as dept_id,

		
		if(a.dept_id in ('H08','H07','H06'),'冰鲜&活鲜&贝类', a.dept_name ) as dept_name,	
		
		case when 	a.category_large_code in ('1250','1257','1241') then a.category_large_code
			when  a.dept_id in ('H05','H04' ,'H03','H02') then a.dept_id
			when  a.dept_id in ('H08','H07','H06') then  '11'
			else    a.catg_m_id 
			end  as catg_m_id,
		case when 	a.category_large_code in ('1250','1257','1241') then a.category_large_name
			when  a.dept_id in ('H05','H04' ,'H03','H02') then a.dept_name
			when  a.dept_id in ('H08','H07','H06') then  '水产'
			else    a.catg_m_name 
			end  as catg_m_name,	
		a.goodsid,
		a.amount,
		a.qty,
		a.pur_price
from 
	csx_tmp.temp_purprice_dept_duibiao_amt    as a 

union all 
	select
		b.stype,
		b.province_name ,
		b.city_name,
		if(b.dept_id like 'H%','生鲜', '食百') as dept_id,

		
		if(b.dept_id like 'H%','生鲜', '食百') as dept_name,	
		
		if(b.dept_id like 'H%','生鲜', '食百')  as catg_m_id,
		if(b.dept_id like 'H%','生鲜', '食百') as catg_m_name,	
		b.goodsid,
		b.amount,
		b.qty,
		b.pur_price
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt    as b 
union all 
	select
		c.stype,
		c.province_name ,
		c.city_name,
		'小计' as dept_id,

		
		'小计' as dept_name,	
		
		'小计'  as catg_m_id,
		'小计' as catg_m_name,	
		c.goodsid,
		c.amount,
		c.qty,
		c.pur_price
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt  as c   	
) as x 	
group by 
	x.stype,
	x.province_name ,
	x.city_name,
	x.dept_id,
	x.dept_name ,
	x.catg_m_id,
	x.catg_m_name
union all 	
--全国
select
	x.stype,
	'全国' as province_name ,
	'全国' as city_name,
	x.dept_id,
	x.dept_name ,
	x.catg_m_id,
	x.catg_m_name,
	sum(amount) as middle_amount,		--省市下 -品类汇总金额 数量
	sum(qty) as middle_qty
from 
(

	select
		a.stype,
		a.province_name ,
		a.city_name,
		if(a.dept_id in ('H08','H07','H06'),'11', a.dept_id ) as dept_id,

		
		if(a.dept_id in ('H08','H07','H06'),'冰鲜&活鲜&贝类', a.dept_name ) as dept_name,	
		
		case when 	a.category_large_code in ('1250','1257','1241') then a.category_large_code
			when  a.dept_id in ('H05','H04' ,'H03','H02') then a.dept_id
			when  a.dept_id in ('H08','H07','H06') then  '11'
			else    a.catg_m_id 
			end  as catg_m_id,
		case when 	a.category_large_code in ('1250','1257','1241') then a.category_large_name
			when  a.dept_id in ('H05','H04' ,'H03','H02') then a.dept_name
			when  a.dept_id in ('H08','H07','H06') then  '水产'
			else    a.catg_m_name 
			end  as catg_m_name,	
		a.goodsid,
		a.amount,
		a.qty,
		a.pur_price
from 
	csx_tmp.temp_purprice_dept_duibiao_amt    as a 
	
union all 
		
		select
			b.stype,
			b.province_name ,
			b.city_name,
			if(b.dept_id like 'H%','生鲜', '食百') as dept_id,
	
			
			if(b.dept_id like 'H%','生鲜', '食百') as dept_name,	
			
			if(b.dept_id like 'H%','生鲜', '食百')  as catg_m_id,
			if(b.dept_id like 'H%','生鲜', '食百') as catg_m_name,	
			b.goodsid,
			b.amount,
			b.qty,
			b.pur_price
		--	b.pur_price_hight 		--如果 彩食鲜进价高于 云超进价 标记为1
		from 
			csx_tmp.temp_purprice_dept_duibiao_amt    as b 
	union all 
		select
			c.stype,
			c.province_name ,
			c.city_name,
			'小计' as dept_id,
	
			
			'小计' as dept_name,	
			
			'小计'  as catg_m_id,
			'小计' as catg_m_name,	
			c.goodsid,
			c.amount,
			c.qty,
			c.pur_price
		--	c.pur_price_hight 		--如果 彩食鲜进价高于 云超进价 标记为1
		from 
			csx_tmp.temp_purprice_dept_duibiao_amt  as c 	
) as x 	
group by 
	x.stype,
--	x.province_name ,
--	x.city_name,
	x.dept_id,
	x.dept_name ,
	x.catg_m_id,
	x.catg_m_name	
;

--计算 商品在该品类下的商品金额占比 权重  -->外层 计算 平均价权重  (加权平均价) = 商品金额/商品数量  * 权重   最后再求平均
drop table csx_tmp.temp_purprice_dept_duibiao_amt_rno_goods;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_amt_rno_goods
as 
select
	a.stype,
	a.province_name ,
	a.city_name,
	a.dept_id,
	a.dept_name ,
	a.catg_m_id,
	a.catg_m_name,
	a.goodsid,
	a.goods_amount,			--商品金额汇总
	b.middle_amount,		--品类金额汇总
	a.goods_qty,
	b.middle_qty,
	(a.goods_amount / b.middle_amount) as amt_rno    --金额占比  权重
from 
	csx_tmp.temp_purprice_dept_duibiao_amt_middle  as a 
join
	csx_tmp.temp_purprice_dept_duibiao_amt_all as b  on a.stype=b.stype and a.province_name=b.province_name and a.city_name=b.city_name  and a.dept_id =b.dept_id and a.catg_m_id=b.catg_m_id 
where
	a.stype ='彩食鲜' and  b.stype='彩食鲜'
;

--
--根据权重获取加权平均价   商品 （总金额/ 总数量）  * 权重   云超也使用彩食鲜的对标权重 

--根据占比 算出彩食鲜权重     云超和彩食鲜都使用彩食鲜权重 算出 平均价的加权平均价

drop table csx_tmp.temp_purprice_dept_duibiao_amt_rno_pur_price;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_amt_rno_pur_price 
as 
select	
	a.stype,
	a.province_name ,
	a.city_name,
	a.dept_id,
	a.dept_name ,
	a.catg_m_id,
	a.catg_m_name,
	a.goodsid,
	a.goods_amount,			--商品金额汇总
--	b.middle_amount,		--品类金额汇总
	a.goods_qty,
--	b.middle_qty,
--	(a.goods_amount/ a.goods_qty)  as  goods_avg,		--商品进价平均价格
--	(a.goods_amount / b.middle_amount) as amt_rno,   --金额占比
	(cast((a.goods_amount/ a.goods_qty) as decimal(32,10)) *  cast((amt_rno) as decimal(32,10))) as  pur_price   --加权平均价    
from 
	csx_tmp.temp_purprice_dept_duibiao_amt_middle  as a 	
join
	csx_tmp.temp_purprice_dept_duibiao_amt_rno_goods as b  --彩食鲜的权重
on 
	a.province_name=b.province_name and a.city_name=b.city_name  and a.dept_id =b.dept_id and a.catg_m_id=b.catg_m_id and a.goodsid=b.goodsid
;	

--进价页面加入权重
--进价    --指标规则 总入库金额/总入库数量
insert overwrite table csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye partition (sdt) 
select
	a.stype,
	a.province_name,
	a.city_name,
	a.goodsid,
	a.goodsname,
	a.catg_m_id,
	a.catg_m_name,
	a.dept_id,
	a.dept_name,
	a.pur_price,
	b.amt_rno,
	if(b.amt_rno is not null ,  a.pur_price*b.amt_rno, '')  as  rno_pur_price,
	regexp_replace(date_sub(current_date,1),'-','')sdt
from 
(
	select 
		a.stype,
		a.province_name,
		a.city_name,
		a.goodsid,
		goodsname,
		catg_m_id,
		catg_m_name,
		a.dept_id,
		dept_name,
		sum(amount)/sum(qty) as  pur_price,
		regexp_replace(date_sub(current_date,1),'-','')sdt
	from
		csx_tmp.temp_purprice_dept1 as a 
		where province_name='贵州省'
	group by 
		a.stype,
		a.province_name,
		a.city_name,
		a.goodsid,
		goodsname,
		catg_m_id,
		catg_m_name,
		a.dept_id,
		dept_name

) as a 	
left join 
(
	select 
		province_name,
		city_name,
		goodsid,
		amt_rno
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt_rno_goods 
	where
		dept_id not  in ('小计','食百','生鲜')  	--进价页面剔除 自己添加的、因为这个页面是实际商品进价对标页面情况、加入会产生一个商品行专列扩张为三个的现象、
	
) as b  on a.province_name =b.province_name and a.city_name=b.city_name and a.goodsid=b.goodsid
order by
	a.dept_id
;



-- 获取城市下有对标的商品  彩食鲜和云超  的加权平均价
drop table csx_tmp.temp_purprice_dept_sku_avg_price;
CREATE  table csx_tmp.temp_purprice_dept_sku_avg_price
as
select
	a.province_name ,
	a.city_name,
	a.dept_id as dept_id,	
	a.dept_name  as dept_name,		
	a.catg_m_id,
	a.catg_m_name ,	
	a.goodsid,
	cast(a.pur_price  as  decimal(26,10) )  as csx_pur_price,		--彩食鲜'
	cast(b.pur_price  as  decimal(26,10) )	as yc_pur_price	 	--'云超'
from 
(
	select
		province_name ,
		city_name,
		dept_id,
		dept_name ,
		catg_m_id,
		catg_m_name,
		goodsid,
		pur_price
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt_rno_pur_price			--加权平均价
	where 
		stype ='彩食鲜' 
		and
		(pur_price is not null or pur_price<>'')
) as a 
join
(
	select
		province_name ,
		city_name,
		dept_id,
		dept_name ,
		catg_m_id,
		catg_m_name,
		goodsid,
		pur_price		--平均价 
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt_rno_pur_price			--加权平均价
	where 
		stype ='云超'
		and
		(pur_price is not null or pur_price<>'')
) as b	 on  a.province_name=b.province_name and a.city_name=b.city_name and a.dept_id =b.dept_id and a.catg_m_id=b.catg_m_id and a.goodsid=b.goodsid
		
;

--有对标的 商品 平均sku数  分母----省区+全国（不去重）
--进价指数 ：
--13.结果页要增加生鲜、食百汇总（汇总重点品类） 按目前指定对标的品类进行汇总 和 小计一致
--14.结果页增加高价支数、高价率 （各省区高价商品汇总）
--细节确认：	有对标的 商品  为分母 、 彩食鲜平均进价高于云超为分子、全国不去重（所有省区的和）


drop table csx_tmp.temp_purprice_dept_sku_tar;
CREATE  table csx_tmp.temp_purprice_dept_sku_tar
as
select
	x.province_name ,--省区+全国（去重）
	x.city_name,
	x.dept_id,
	x.dept_name,
	x.catg_m_id,
	x.catg_m_name,
	count( distinct x.goodsid) as goods_sku,		--去重的sku数
	(avg(x.csx_pur_price) /avg(x.yc_pur_price))*100 as pur_price_tar 	--彩食鲜/云超  说明：正常是商品维度唯一、不需要使用avg函数、此处为保险使用 进价指数'价格指数
from 
	csx_tmp.temp_purprice_dept_sku_avg_price as x 	
group by 
	x.province_name ,
	x.city_name,
	x.dept_id,
	x.dept_name ,
	x.catg_m_id,
	x.catg_m_name
	
;	


--有对标的 商品 sku数  分母----省区+全国（不去重）。所有省区的和
drop table csx_tmp.temp_purprice_dept_duibiao_hight_fm;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_hight_fm 
as 
select
	'全国' as province_name,
	'全国' as city_name,
	dept_id,
	dept_name,
	catg_m_id,
	catg_m_name,
	sum(goods_sku)  as goods_sku 	--使用所有省区累计 得到全国
from	
	csx_tmp.temp_purprice_dept_sku_tar 
where 
	province_name <> '全国'			--源数据之前做过全国的进去、所以这边需要只取原始省区数据、全国逻辑需要单独处理、因为是两套逻辑、一个指标要全国去重、一个不要去重
group by 
	dept_id,
	dept_name,
	catg_m_id,
	catg_m_name
union all
select
	province_name,
	city_name,
	dept_id,
	dept_name,
	catg_m_id,
	catg_m_name,
	sum(goods_sku)  as goods_sku 		
from	
	csx_tmp.temp_purprice_dept_sku_tar 
where 
	province_name <> '全国'
group by 
	province_name,
	city_name,
	dept_id,
	dept_name,
	catg_m_id,
	catg_m_name
;


--有对标的 商品 且 平均进价》云超平均进价  sku数  分子----省区+全国（不去重）

--高价指数
drop table csx_tmp.temp_purprice_dept_duibiao_hight_fz_city;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_hight_fz_city
as
select 
	province_name,
	city_name,
	dept_id,
	dept_name,
	catg_m_id,
	catg_m_name,
	count(distinct goodsid) as goods_sku
from 
	csx_tmp.temp_purprice_dept_duibiao_amt_middle 
where 
	 province_name <> '全国'
	 and 
	 pur_price_hight=1			--之前标识使用  如果 彩食鲜进价高于 云超进价 标记为1
group by 
	province_name,
	city_name,
	dept_id,
	dept_name,
	catg_m_id,
	catg_m_name
	
union all
select
	'全国' as province_name,
	'全国' as	city_name,
	a.dept_id,
	a.dept_name,
	a.catg_m_id,
	a.catg_m_name,
	sum(goods_sku) as goods_sku
from 
(--省区去重后，算全国
	select 
		province_name,
		city_name,
		dept_id,
		dept_name,
		catg_m_id,
		catg_m_name,
		count(distinct goodsid) as goods_sku
	from 
		csx_tmp.temp_purprice_dept_duibiao_amt_middle 
	where 
		province_name <> '全国'
		and 
		pur_price_hight=1		--使用标识 获取 不用再区分是彩食鲜还是云超
	group by 
		province_name,
		city_name,
		dept_id,
		dept_name,
		catg_m_id,
		catg_m_name	
) as  a 
group by 
	a.dept_id,
	a.dept_name,
	a.catg_m_id,
	a.catg_m_name
;


--分子 
drop table csx_tmp.temp_purprice_dept_duibiao_hight_fz;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_hight_fz
as	
select
	province_name,
	city_name,	
	dept_id as dept_id,
	dept_name as dept_name,
	catg_m_id as catg_m_id,
	catg_m_name as catg_m_name,
	sum(goods_sku) as goods_sku
from 
	csx_tmp.temp_purprice_dept_duibiao_hight_fz_city 
group by 
	province_name,
	city_name,
	dept_id ,
	dept_name ,
	catg_m_id ,
	catg_m_name 
;	


--求  高价率   高价商品sku数/ 可比对商品数
drop table csx_tmp.temp_purprice_dept_duibiao_hight_tar;
CREATE  table csx_tmp.temp_purprice_dept_duibiao_hight_tar
as
select 
	province_name,
	city_name,
	dept_id,
	dept_name,
	catg_m_id,
	catg_m_name, 
	sum(goods_sku_fz) as goods_sku_fz,
	sum(goods_sku_fm) as goods_sku_fm,
	concat(sum(x.goods_sku_fz)/sum(x.goods_sku_fm) *100,'%') as goods_sku_hight
from 
(
select
	a.province_name,
	a.city_name,
	a.dept_id,
	a.dept_name,
	a.catg_m_id,
	a.catg_m_name,
	a.goods_sku as goods_sku_fz,
	0 as  goods_sku_fm

from 
	 csx_tmp.temp_purprice_dept_duibiao_hight_fz  as  a   --分子 --高价的
	 
union all 	 
	 
select 
	b.province_name,
	b.city_name,
	b.dept_id,
	b.dept_name,
	b.catg_m_id,
	b.catg_m_name,
	0 as  goods_sku_fz,
	b.goods_sku as goods_sku_fm
from 
	csx_tmp.temp_purprice_dept_duibiao_hight_fm as  b   --分母 --有对标的商品
) as x
group by 
	province_name,
	city_name,
	dept_id,
	dept_name,
	catg_m_id,
	catg_m_name
;
	
--全国不去重
	
--进价指数	
	
insert overwrite table csx_dw.ads_wms_r_d_fineReport_city_sku_dept_purprice_globaleye partition (sdt) 
select
	a.province_name,
	a.city_name,
	a.dept_id,
	a.dept_name,
	a.catg_m_id,
	a.catg_m_name,
	b.goods_sku_fz,
	a.goods_sku,
	b.goods_sku_hight,	
	a.pur_price_tar,
--	b.goods_sku_fm,
	regexp_replace(date_sub(current_date,1),'-','') as sdt
	
from 
	csx_tmp.temp_purprice_dept_sku_tar as a ,
	csx_tmp.temp_purprice_dept_duibiao_hight_tar as b --join
where 
	a.province_name=b.province_name
	and
	a.city_name=b.city_name 
	and
	a.dept_id=b.dept_id
	and
	a.catg_m_id=b.catg_m_id
	and a.province_name='贵州省'
;	



INVALIDATE METADATA   csx_dw.ads_wms_r_d_fineReport_city_sku_dept_purprice_globaleye ; 

INVALIDATE METADATA   csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye ;  
INVALIDATE METADATA   csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye_detail ; 



insert overwrite directory '/tmp/zhaoxiaomin/purprice_mingxi' row format delimited fields terminated by '\t' 
select
	*
from 
	csx_dw.ads_wms_r_d_fineReport_city_dept_purprice_globaleye_detail
where 
	sdt=regexp_replace(date_sub(current_date,1),'-','')