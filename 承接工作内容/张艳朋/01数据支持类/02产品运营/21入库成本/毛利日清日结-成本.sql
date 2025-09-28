
CREATE TABLE `dc_logistics_factory_list` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `province_name` varchar(64) NOT NULL DEFAULT '' COMMENT '省区',
  `city_name` varchar(64) NOT NULL DEFAULT '' COMMENT '城市',
  `dc_type` varchar(64) NOT NULL DEFAULT '' COMMENT '仓类型',
  `dc_code` varchar(64) NOT NULL COMMENT 'DC编码',
  `dc_name` varchar(64) NOT NULL DEFAULT '' COMMENT 'DC名称',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=4954 DEFAULT CHARSET=utf8mb4 COMMENT='物流仓与工厂仓清单';

-- hive 物流仓与工厂仓清单
drop table if exists csx_analyse.csx_analyse_wh_dc_logistics_factory_list_mf;
create table csx_analyse.csx_analyse_wh_dc_logistics_factory_list_mf(
`province_name`	string	COMMENT	'省区',
`city_name`	string	COMMENT	'城市',
`dc_type`	string	COMMENT	'仓类型',
`dc_code`	string	COMMENT	'DC编码',
`dc_name`	string	COMMENT	'DC名称'
) COMMENT '物流仓与工厂仓清单'
PARTITIONED BY (smt string COMMENT '日期分区');

-- 毛利日清日结-成本侧
with sale_detail_negative_profit as -- 昨日负毛利
(
    select *
	from 
	(
		select
			split(a.id, '&')[0] as credential_no,
			a.performance_region_code,
			a.performance_region_name,
			a.performance_province_code,
			a.performance_province_name,
			a.performance_city_code,
			a.performance_city_name,
			a.inventory_dc_code,
			a.inventory_dc_name,
			a.delivery_type_code,
			a.delivery_type_name,
			a.customer_code,
			a.customer_name,
			a.order_code,
			a.goods_code,
			a.goods_name,
			a.sale_qty,
			a.sale_amt,
			a.sale_cost,
			a.profit,
			a.cost_price,
			a.sale_price,
			a.refund_order_flag,
			-- 如果当日有退货、价格补救等看综合后业绩毛利
			sum(a.sale_amt) over(partition by coalesce(a.original_order_code,a.order_code),a.goods_code) as order_goods_sale_amt,
			sum(a.profit) over(partition by coalesce(a.original_order_code,a.order_code),a.goods_code) as order_goods_profit
		from
		(
			select * from csx_dws.csx_dws_sale_detail_di
			where sdt = '${sdt_yes}' 
			and channel_code in ('1','7','9') 
			and business_type_code='1'
			and order_channel_code not in('4','6') -- 剔除调价返利
		)a join (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag =0) b
		on a.inventory_dc_code = b.shop_code
	)a 
	where refund_order_flag=0 
	and order_goods_sale_amt>0 and order_goods_profit<0
 ),
 
 -- 物流仓工厂仓 
 dc_logistics_factory_list
 as 
 (
select dc_code 
from csx_analyse.csx_analyse_wh_dc_logistics_factory_list_mf
where smt=substr('${sdt_yes}',1,6)
 ),
 
 -- 品类成本阈值
 classify_middle_threshold_list
 as 
 (
select 'B0101' as classify_middle_code,0.08 as classify_middle_threshold
union all select 'B0102' as classify_middle_code,0.15 as classify_middle_threshold
union all select 'B0103' as classify_middle_code,0.1 as classify_middle_threshold
union all select 'B0104' as classify_middle_code,0.15 as classify_middle_threshold
union all select 'B0201' as classify_middle_code,0.1 as classify_middle_threshold
union all select 'B0202' as classify_middle_code,0.05 as classify_middle_threshold
union all select 'B0301' as classify_middle_code,0.06 as classify_middle_threshold
union all select 'B0302' as classify_middle_code,0.03 as classify_middle_threshold
union all select 'B0303' as classify_middle_code,0.08 as classify_middle_threshold
union all select 'B0305' as classify_middle_code,0.1 as classify_middle_threshold
union all select 'B0306' as classify_middle_code,0.06 as classify_middle_threshold
union all select 'B0401' as classify_middle_code,0.25 as classify_middle_threshold
union all select 'B0402' as classify_middle_code,0.25 as classify_middle_threshold
union all select 'B0501' as classify_middle_code,0.25 as classify_middle_threshold
union all select 'B0601' as classify_middle_code,0.11 as classify_middle_threshold
union all select 'B0602' as classify_middle_code,0.1 as classify_middle_threshold
union all select 'B0603' as classify_middle_code,0.08 as classify_middle_threshold
union all select 'B0604' as classify_middle_code,0.15 as classify_middle_threshold
union all select 'B0605' as classify_middle_code,0.13 as classify_middle_threshold
union all select 'B0701' as classify_middle_code,0.15 as classify_middle_threshold
union all select 'B0702' as classify_middle_code,0.15 as classify_middle_threshold
union all select 'B0801' as classify_middle_code,0.25 as classify_middle_threshold
union all select 'B0802' as classify_middle_code,0.15 as classify_middle_threshold
union all select 'B0803' as classify_middle_code,0.25 as classify_middle_threshold
union all select 'B0804' as classify_middle_code,0.25 as classify_middle_threshold
union all select 'B0805' as classify_middle_code,0.25 as classify_middle_threshold
union all select 'B0901' as classify_middle_code,0.25 as classify_middle_threshold
union all select 'B0902' as classify_middle_code,0.25 as classify_middle_threshold
 ),
 
-- 商品转码
 wms_product_change
 as 
 (
select
    t2.goods_code,
    t2.credential_no,
    sum(t2.qty) as qty,
    sum(t3.target_goods_qty) as target_goods_qty,
	concat_ws('、',collect_list(t3.source_goods_code)) as source_goods_code,
	concat_ws('、',collect_list(t3.source_goods_name)) as source_goods_name
from
	(
    select
      goods_code,
      credential_no,
      -- source_order_no,
	  wms_order_no,
      sum(qty) as qty
    from csx_dws.csx_dws_wms_batch_detail_di
    where sdt >= regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','')
    and move_type_code in ('107A', '108A')
    group by goods_code,credential_no,wms_order_no
    )t2
join -- 关联商品转码
    (
      select
		order_code,  -- 转码单号 规则zm+年（2位）+月（2位）+日（2位）+6位流水
		dc_code,
		source_goods_code,  -- 源商品编号
		source_goods_name,  -- 源商品名称
		source_goods_qty,  -- 消耗数量
		target_goods_code,  -- 目标商品编号
		target_goods_name,  -- 目标商品名称
		target_goods_qty,  -- 转换数量
		change_proportion  -- 转换比例
      from csx_dwd.csx_dwd_wms_product_change_order_detail_di
      where sdt >= regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','')
    )t3 on t2.wms_order_no = t3.order_code and t2.goods_code = t3.target_goods_code
  group by t2.goods_code,t2.credential_no 
  ),
  
 dc_goods_received -- 商品入库成本
 as 
 (
	select 
		b.*,
		case when (c.business_division_name like '%生鲜%' and c.classify_middle_code='B0101') or  c.business_division_name like '%食百%' then '食百' else '生鲜' end as division_name
	from dc_logistics_factory_list a
	join	
	(-- 入库减供应商退货作为最终入库
		select *,
			cast(received_amount_all/received_qty_all as decimal(20,6)) as received_price_avg,
			cast(received_amount_1/received_qty_1 as decimal(20,6)) as received_price_1
		from 
		(select 
			b1.target_location_code,
			b1.goods_code,
			-- sum((case when b1.received_amount<0 then 0 else b1.received_amount end)) as all_not_t_received_amount,
			-- sum(nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0)) as all_gys_shipped_amount,
			-- sum((case when b1.received_qty<0 then 0 else b1.received_qty end)) as all_not_t_received_qty,
			-- sum(nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as all_gys_shipped_qty,
			-- 入库-供应商退货，如果有价格补救则取价格补救后的值,若同时有价格补救与退货则只看退货
			-- 近30天/近7天
			sum((case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
				-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0)) as received_amount_all,
				
			sum((case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
				-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0)) as received_qty_all,
				
			cast(max(case when b1.received_amount<0 then 0 
				else if(b2.received_amount is not null,b2.received_price2,
						if(coalesce(b1.received_price2,0)=0,b1.received_price1,b1.received_price2)) end)
			as decimal(20,6)) as received_price_max,
				
			-- 昨日
		 	sum(if(b1.sdt=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-1),'-',''),
					(case when b1.received_amount<0 then 0 else if(b2.received_amount is not null and b3.shipped_amount is null,b2.received_amount,b1.received_amount) end)
						-nvl((case when b3.shipped_amount<0 then 0 else b3.shipped_amount end),0),0))
		 	 as received_amount_1,

		 	sum(if(b1.sdt=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-1),'-',''),
					(case when b1.received_qty<0 then 0 else if(b2.received_qty is not null and b3.shipped_amount is null,b2.received_qty,b1.received_qty) end)
						-nvl((case when b3.shipped_qty<0 then 0 else b3.shipped_qty end),0),0))
		 	 as received_qty_1			
		from 
			-- 入库数据
			(
				select target_location_code, 
					order_code,goods_code,sdt,
					sum(received_amount) received_amount,
					sum(received_qty) received_qty,
					sum(received_amount)/sum(received_qty) as received_price1,
					max(received_price2) received_price2
				from csx_dws.csx_dws_scm_order_received_di 
				where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
				and sdt<='${sdt_yes}' 
				and super_class in (1,3) -- 加上调拨入库的数据 供应商订单
				and header_status=4 
				and source_type not in (2,3,4,11,15,16)  -- 剔除项目合伙人
				and local_purchase_flag='0' -- 剔除地采，是否地采(0-否、1-是)
				and direct_delivery_type='0' -- 直送类型 0-P(普通) 1-R(融单)、2-Z(过账)
				-- and target_location_code in ('W0BK')
				group by target_location_code,order_code,goods_code,sdt
			) b1 
			-- 关联价格补救订单数据，如果有价格补救则成本取补救单中的价格
			left join 
			(
				select * 
				from csx_dws.csx_dws_scm_order_received_di 
				where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
				and sdt<='${sdt_yes}' 
				-- and target_location_code in ('W0BK') 
				and price_remedy_flag=1 
			) b2 on b1.order_code=b2.original_order_code and b1.goods_code=b2.goods_code 
			-- 关联供应商退货订单
			left join 
			(
				select * 
				from csx_dws.csx_dws_scm_order_shipped_di   
				where sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') 
				and super_class in (2) 
				-- and target_location_code in ('W0BK') 
			) b3 on b1.order_code=b3.original_order_code and b1.goods_code=b3.goods_code 
			left join 
			(
				select * 
				from csx_dim.csx_dim_basic_goods 
				where sdt='current' 
			) b4 on b1.goods_code=b4.goods_code 
		-- where b2.original_order_code is null 
		where (
		(((b4.business_division_name like '%生鲜%' and b4.classify_middle_code='B0101') or  b4.business_division_name like '%食百%') and b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') and b1.sdt<='${sdt_yes}' )
		or 
		(b4.business_division_name like '%生鲜%' and (b4.classify_middle_code<>'B0101' or b4.classify_middle_code is null) and b1.sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','') and b1.sdt<='${sdt_yes}')
		)
		group by b1.target_location_code,b1.goods_code
		) b 
	) b on a.dc_code=b.target_location_code
	left join 
	(
		select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current' 
	) c on b.goods_code=c.goods_code 
)

select 
    a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
	a.inventory_dc_code,
	a.inventory_dc_name,
	a.delivery_type_code,
	a.delivery_type_name,
    a.customer_code,
    a.customer_name,
	a.order_code,
	e.classify_middle_code,
	e.classify_middle_name,
    a.goods_code,
    a.goods_name,
    a.sale_qty,
	a.sale_cost,
    a.sale_amt,
    a.profit,
    a.order_goods_sale_amt,
	a.order_goods_profit,	
	a.cost_price,
	a.sale_price,	
	b.received_amount_all,
	b.received_qty_all,
	b.received_price_avg,
	b.received_price_max,
	b.received_amount_1,
	b.received_qty_1,
	b.received_price_1,
	-- 近期指生鲜近7天食百近30天
	case 
		 when a.delivery_type_name='直送' and a.cost_price>b.received_price_max then '直送单，成本价高于配送近期入库最高价'
		 when a.delivery_type_name='直送' and a.cost_price-b.received_price_max >f.classify_middle_threshold then '直送单，成本价高于配送近期入库阈值'
		 when a.cost_price>b.received_price_max and c.target_goods_qty is not null then '商品转码' 
		 when a.cost_price>b.received_price_max then '成本价高于近期入库最高价，疑似入库价错误'
		 when a.cost_price-b.received_price_max >f.classify_middle_threshold then '成本价波动超过品类近期入库阈值，疑似入库价错误'
		 when a.cost_price-b.received_price_max <=f.classify_middle_threshold then '未知原因，成本价波动在品类近期入库阈值范围内'
		 when d.dc_code is null then '无入库价参考，非物流工厂仓'
		 when coalesce(b.received_price_max,0)=0 and d.dc_code is not null and a.delivery_type_name='直送' then '无入库价参考，直送单，近期无入库'
		 when coalesce(b.received_price_max,0)=0 and d.dc_code is not null then '无入库价参考，近期无入库'
		 end as cost_price_type,

	case when c.target_goods_qty is not null then '是' else '否' end as is_product_change,
	c.source_goods_code,
	c.source_goods_name,
	if(d.dc_code is null,'否','是') is_target_dc
from sale_detail_negative_profit a
left join dc_goods_received b on a.inventory_dc_code=b.target_location_code and a.goods_code=b.goods_code
left join wms_product_change c on a.credential_no=c.credential_no and a.goods_code=c.goods_code
left join dc_logistics_factory_list d on d.dc_code=a.inventory_dc_code
left join 
(
	select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current' 
) e on a.goods_code=e.goods_code 
left join classify_middle_threshold_list f on f.classify_middle_code=e.classify_middle_code
;



 
 
/* 
疑问待确认：
采购入库数量大于0但金额与价格均为0的是什么场景，是否需要剔除
因 非清单内物流工厂仓而没获取到入库成本，二次确认是否加入清单


-- --------------------------其他不重要的 -----------------------------------------------------------------
-- 品类成本价波动幅度
 
select 
sdt,
classify_middle_code,
classify_middle_name,
sum(sale_cost)/sum(sale_qty)
from csx_dws.csx_dws_sale_detail_di
where sdt >= '20230801' 
and channel_code in ('1','7','9') 
and business_type_code='1'
and order_channel_code not in('4','6','5') -- 剔除调价返利 
and refund_order_flag=0
group by sdt,
classify_middle_code,
classify_middle_name;


select 
classify_middle_code,
classify_middle_name,
sum(sale_cost)/sum(sale_qty)
from csx_dws.csx_dws_sale_detail_di
where sdt >= '20230801' 
and channel_code in ('1','7','9') 
and business_type_code='1'
and order_channel_code not in('4','6','5') -- 剔除调价返利 
and refund_order_flag=0
and (
		(((business_division_name like '%生鲜%' and classify_middle_code='B0101') or  business_division_name like '%食百%') and sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-30),'-','') and sdt<='${sdt_yes}' )
		or 
		(business_division_name like '%生鲜%' and (classify_middle_code<>'B0101' or classify_middle_code is null) and sdt>=regexp_replace(date_add(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'),'yyyy-MM-dd'),-6),'-','') and sdt<='${sdt_yes}')
		)
group by 
classify_middle_code,
classify_middle_name;

 
select source_type,category_name,sdt,goods_code,
case when (category_name like '%生鲜%' and classify_middle_code='B0101') or  category_name like '%食百%' then '食百' else '生鲜' end as division_name,
* 
from csx_dws.csx_dws_scm_order_received_di 
where sdt>='20230831'
and target_location_code in ('W0A3') 
and super_class in (1,3) -- 加上调拨入库的数据
and header_status=4 
and goods_code in('320')
-- and source_type in (1,10,22,23) -- 订单类型 
 
 select * 
from csx_dws.csx_dws_scm_order_received_di 
where sdt>='20230831'
and sdt<='20230906' 
and target_location_code in ('W0A3') 
and price_remedy_flag=1 
-- and original_order_code='POW0Q9230905005933'
and goods_code in('320')

select * 
from csx_dws.csx_dws_scm_order_shipped_di   
where sdt>='20230831'
and super_class in (2) 
and target_location_code in ('W0A3') 
and goods_code in('320')
