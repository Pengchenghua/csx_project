针对100万以上，干货、预制菜、食百毛利率≤5%，按照管理中类销售额top20商品推送换品
要按照子客户维度
drop table if exists csx_analyse_tmp.link_customercode01;
create table if not exists csx_analyse_tmp.link_customercode01 as 
select a.* 
from 
	(select 
		a.*,
		ROW_NUMBER() OVER (PARTITION BY inventory_dc_code,classify_middle_name ORDER BY sale_amt DESC) as sales_rank
	from 
	(select
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,		
		a.goods_code,
		c.goods_name,
		c.classify_middle_name,
		c.classify_small_name,
		sum(sale_amt) as sale_amt,
		sum(profit) as profit,
		sum(profit)/abs(sum(sale_amt)) as profit_rate
	from 
		(select * from csx_dws.csx_dws_sale_detail_di
		where 
			sdt>=regexp_replace(add_months(trunc('${yester_day}','MM'),0),'-','') 
			and sdt<='${yester}'
			and business_type_code=1  
			and order_channel_code not in ('4','6','5') -- 剔除所有异常
			and refund_order_flag<>1 
            and delivery_type_code<>2 
			and shipper_code='YHCSX'
			and (classify_large_name not in ('肉禽水产','干货加工','蔬菜水果') or classify_middle_name in ('预制菜','干货') or classify_small_name in('冻鸭副','冻鸡副'))
			and performance_city_name in ('北京市','郑州市','合肥市','南京主城','江苏盐城','江苏苏州','上海松江','福州市','深圳市','贵阳市','成都市','重庆主城','永川区')
		) a	
		left join  -- 商品信息
		(select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current' 
		) c 
		on a.goods_code=c.goods_code 
		-- 客户数据
		left join 
		(select * 
			from csx_dim.csx_dim_crm_customer_info 
			where sdt='current' 
			and shipper_code='YHCSX'
		) d on a.customer_code=d.customer_code 
		-- 关联客户
		left join 
		(select a.* ,b.customer_name as link_customername
		from 
			(select
				customer_code as link_customercode,  
				get_json_object(item_json, '$.customerCode') as customer_code -- 这个字段是主客户信息
			FROM csx_ods.csx_ods_csx_price_prod_customer_config_df
			LATERAL VIEW explode(split(regexp_replace(substr(customer_link, 2, length(customer_link) - 2), '\\}\\,', '\\}\\|\\|'), '\\|\\|')) r1 AS item_json
			where sdt='${yester}'
			)a
			left join -- 取关联客户名称
			(select customer_code,customer_name from csx_dim.csx_dim_crm_customer_info where sdt='current') b on a.link_customercode=b.customer_code	
		) e on a.customer_code=e.customer_code		
		-- 客户管家、销售、经理
		left join  	
		(select
			customer_no,
			coalesce(work_no,'') work_no,
			coalesce(sales_name,'') sales_name, 
			coalesce(second_supervisor_work_no,'') second_supervisor_work_no,
			coalesce(second_supervisor_name,'') second_supervisor_name,
			coalesce(rp_service_user_work_no_new, '') as rp_service_user_work_no_new, -- 管家
			coalesce(rp_service_user_name_new, '') as rp_service_user_name_new		
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
		where sdt = '${yester}'
		) f on a.customer_code = f.customer_no
		-- 采购参与/销售管理
		left join 
		(select
			code as type,
			max(name) as name,
			max(extra) as extra 
		from csx_dim.csx_dim_basic_topic_dict_df
		where parent_code = 'direct_delivery_type' 
		group by code 
		) h on a.direct_delivery_type=h.type 
		-- 100w关联客户清单
		-- join 
		-- (select  		
		-- 	customer_code,
		-- 	target_rate,
		-- 	price_name
		-- from csx_analyse_tmp.cstop30_target_rate
		-- ) i on a.customer_code=i.customer_code		
	where h.extra='采购参与'		
	group by 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,		
		a.goods_code,
		c.goods_name,
		c.classify_middle_name,
		c.classify_small_name
	)a where profit_rate< 0.05

)a where sales_rank  <= 20;
 
