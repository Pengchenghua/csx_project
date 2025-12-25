select * from 
	(select 
		a.*,
		 COALESCE(b.purchase_qty_new,0) as purchase_qty_new_21,   --  21点时商品A的下单量
		 COALESCE(b.purchase_qty_new,0)-a.purchase_qty_new as diff_qty_new,
		 if(COALESCE(b.purchase_qty_new,0) -a.purchase_qty_new <0,'是','') types
	from 
	(select * from  csx_analyse.csx_analyse_fr_order_change_goods_hf
	where substr(shf,9,2)='17' and sdt>='20251029'
	)a 
	left join 
	(select * from csx_analyse.csx_analyse_fr_order_change_goods_hf
	where substr(shf,9,2)='21'  and sdt>='20251029'
	) b on a.sdt=b.sdt and a.order_code =b.order_code and a.goods_code=b.goods_code
    )a 
where purchase_qty_new >=10 and profit_rate_diff>0 
and sale_price_new is not null 
and sale_price_new <> 0
 and customer_price_b is not null and customer_price_b <> 0 






-- 成本数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_cb;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_cb as 
select 
    a.* 
from 
    (select 
        t1.*,
        row_number()over(partition by t1.performance_city_name,t1.goods_code order by t1.create_time desc) as pm
    from 
          (select 
              c.performance_city_name,
              a.target_location_code as dc_code,
              a.goods_code,
              cast(a.price_include_tax as decimal(20,6)) as price,
              substr(a.create_time,1,19) as create_time 
          from 
                (select 
                    *  
                from csx_dws.csx_dws_scm_order_detail_di 
                where sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-3),'-','') 
                and sdt<=regexp_replace('${yes_date}','-','')  
                and super_class in (1,3)  
                and assign_type<>1 -- 剔除客户指定数据 
                and (direct_delivery_type not in (1,2) or direct_delivery_type is null) -- 剔除RD/ZZ的数据
                and source_type not in (20,21) -- 剔除紧急补货及临时加单数据
                and delivery_to_direct_flag<>1 -- 剔除配转直（发车前缺货数据）
                and price_remedy_flag<>1 -- 剔除价格补救单 
                and header_status<>5 -- 剔除“已取消”订单 
                and shipper_code='YHCSX' 
                and target_location_code not in ('W0BD','WC51') 
                and price_type<>2 
                and source_type in ('1','10','19', '23','9') 
                and price_include_tax>0.1
                ) a 
                left join 
                (select * 
                from csx_dim.csx_dim_basic_goods 
                where sdt='current' 
                ) b 
                on a.goods_code=b.goods_code 
                left join 
                (select 
                    performance_region_name,
                    performance_province_name,
                    performance_city_name,
                    shop_code as dc_code,
                    shop_name as dc_name,
                    warehouse_purpose_name,
                    (case when warehouse_status=1 then '开启' 
                          when warehouse_status=2 then '禁用' 
                    end) as warehouse_status_name 
                from csx_dim.csx_dim_shop 
                where sdt='current' 
                and warehouse_purpose_name in ('大客户物流','工厂') 
                ) c 
                on a.target_location_code=c.dc_code 
          where 
          c.dc_code is not null 
          and 
          (
              (b.division_code in (10,11) and date(a.create_time)>=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-14) and date(a.create_time)<=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-1)) -- 生鲜取近7天最后一次入库数据
              or 
              (b.division_code not in (10,11) and date(a.create_time)>=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-60) and date(a.create_time)<=date_add(from_unixtime(unix_timestamp('${yes_sdt}','yyyyMMdd'),'yyyy-MM-dd'),-1)) -- 食百取近30天最后一次入库数据
          ) 
        ) t1 
    ) a 
where a.pm=1;