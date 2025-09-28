-- 蔬菜SPU进价对比
select * from 
(select *,
    dense_rank() over(order by all_received_amount desc ) ranks
from 
	(select *,
	case when performance_province_name='河南省' then '华北大区' 
	      when performance_province_name in ('安徽省','湖北省') then '华东大区' 
	 else performance_region_name end as city_group,
	 sum(received_qty) over(partition by spu_product_id) as all_received_qty,
     sum(received_amount) over(partition by spu_product_id) as all_received_amount
	 from 
		(
		select 
			d.performance_region_name,
			d.performance_province_name,
			d.performance_city_name,
			b.spu_product_id,
			c.name,
			sum(received_qty) received_qty,--入库数量
			sum(received_amount) received_amount --入库金额
		from 
			(
			select 	sdt,
				target_location_code,--收货地点编码
				goods_code,
				sum(received_qty) received_qty,--入库数量
				sum(received_amount) received_amount --入库金额
			from csx_dws.csx_dws_scm_order_received_di
			where sdt >='${sdate}'  -- 近一年的日期
				and classify_middle_code='B0202' -- 蔬菜
				-- and classify_middle_name ='水产'
				and header_status=4  -- 已完成
				-- and source_type in (1,10,19,23,2,14,20,21) -- 含非正常入库
				and source_type in (1,10,19,23) -- 不含非正常入库
				and target_location_code in ('WA93','WB04','WB03','W0F4','W0L3','W0K1','WB11','W0G9','WA96','W0AU','W0K6','W0F7','W0AH','WB56','WB67','W0BK','W0Q2','W0S9','W0Q9','W0Q8','W0AT','W0BH','W0BR','W0BT','W0BZ','WB00','W0P8','W0AR','W079','W0N0','W0P3','WB01','W039','W0AZ','W0T7','W0P6') -- 仓商品规则中的仓 -- 物流仓、工厂仓、粗加工仓
			group by 
				target_location_code,--收货地点编码
				goods_code,sdt
			)a 
			 join 
			(select distinct spu_product_id,product_code
			 from  csx_ods.csx_ods_csx_basic_data_md_product_spu_base_link_df
			 where sdt='${yester}' and status= 1
			) b  on a.goods_code = b.product_code 
			left join 
			(select distinct id,name 
			 from  csx_ods.csx_ods_csx_basic_data_md_product_spu_info_df
			 where sdt='${yester}' and status= 1
			)c on b.spu_product_id=c.id
			 join
			(select 
				performance_region_code,performance_region_name,
				performance_province_code,performance_province_name,
				performance_city_code,performance_city_name,
				shop_code,shop_name
			from csx_dim.csx_dim_shop
			where sdt = 'current'	
			)d on d.shop_code=a.target_location_code 
		where c.name is not null 
		group by 
			d.performance_region_name,
			d.performance_province_name,
			d.performance_city_name,
			b.spu_product_id,
			c.name
		)a
	)a
)a 
where ranks<= 50 and spu_product_id is not null
-- ${if(len(pro)=0,"","and city_group in ('"+pro+"')")}