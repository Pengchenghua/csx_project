-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 启用引号识别
set hive.support.quoted.identifiers=none;


-- 换品复盘看板（省区内个人维度）

with tmp_sale_detail as 
(
select
	sdt,inventory_dc_code,customer_code,goods_code,
	sum(sale_amt) as sale_amt,
	sum(profit) as profit
from
	csx_dws.csx_dws_sale_detail_di
where
	sdt>='20220101'
	and sdt<='${ytd}'
	and channel_code in ('1','7','9')
	and business_type_code in (1)
	and order_channel_code !=4
group by 
	sdt,inventory_dc_code,customer_code,goods_code
)	

insert overwrite table csx_analyse.csx_analyse_fr_product_pool_change_wi partition(sdt)		
select
	a.performance_region_code,a.performance_region_name,a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,
	a.inventory_dc_code,
	a.operator_by_id,
	'' as operator_by_user_number,
	a.operator_by,
	coalesce(a.sales_user_id,'') as sales_user_id,
	coalesce(a.sales_user_number,'') as sales_user_number,
	coalesce(a.sales_user_name,'') as sales_user_name,
	coalesce(a.supervisor_user_id,'') as supervisor_user_id,
	coalesce(a.supervisor_user_number,'') as supervisor_user_number,
	coalesce(a.supervisor_user_name,'') as supervisor_user_name,
	coalesce(a.sales_manager_user_id,'') as sales_manager_user_id,
	coalesce(a.sales_manager_user_number,'') as sales_manager_user_number,
	coalesce(a.sales_manager_user_name,'') as sales_manager_user_name,
	coalesce(a.city_manager_user_id,'') as city_manager_user_id,
	coalesce(a.city_manager_user_number,'') as city_manager_user_number,
	coalesce(a.city_manager_user_name,'') as city_manager_user_name,
	coalesce(a.province_manager_user_id,'') as province_manager_user_id,
	coalesce(a.province_manager_user_number,'') as province_manager_user_number,
	coalesce(a.province_manager_user_name,'') as province_manager_user_name,
	count(case when a.status=2 then a.id else null end) as total_change_cnt, -- 整体换品数 客户+商品
	count(case when a.status=2 and a.finish_after_sale_amt !=0 then a.id else null end) as sale_goods_cnt, -- 动销商品数
	sum(case when a.status=2 and a.finish_after_sale_amt !=0 then a.finish_after_sale_amt else 0 end) as finish_after_sale_amt, -- 销售额
	sum(case when a.status=2 then a.increase_profit else 0 end) as increase_profit, -- 毛利额增量
	count(case when a.release_time !='' then a.main_product_code else null end) as need_th_product_cnt, -- 需换品商品数
	count(case when a.release_time !='' and a.status='1' then a.main_product_code else null end) as pending_product_cnt, -- 待处理商品数
	count(case when a.release_time !='' and a.status='2' then a.main_product_code else null end) as finish_product_cnt, -- 完成商品数
	count(case when a.release_time !='' and a.status='3' then a.main_product_code else null end) as refuse_product_cnt, -- 拒绝商品数	
	count(case when a.release_time !='' and a.status='2' and a.finish_after_sale_amt !=0 then a.main_product_code else null end) as change_after_sale_product_cnt, -- 换后下单商品数
	sum(case when release_time !='' and a.status=2 then a.sale_amt else 0 end ) as finish_sale_amt, -- 完成商品销售额
	sum(case when release_time !='' and a.status=3 then a.sale_amt else 0 end ) as refuse_sale_amt, -- 拒绝商品销售额
	concat(i.week_of_year,'（',regexp_replace(date_sub('${td_date}',if(pmod(datediff('${td_date}','2012-01-06'),7)=0,7,pmod(datediff('${td_date}','2012-01-06'), 7))),'-',''),'-','${ytd}','）') as week_interval, -- 周区间
	i.week_of_year as sdt
from
	(
	select
		'${ytd}' as sdt,c.performance_region_code,c.performance_region_name,c.performance_province_code,c.performance_province_name,c.performance_city_code,c.performance_city_name,
		d.sales_user_id,d.sales_user_number,d.sales_user_name,
		d.supervisor_user_id,d.supervisor_user_number,d.supervisor_user_name,
		d.sales_manager_user_id,d.sales_manager_user_number,d.sales_manager_user_name,
		d.city_manager_user_id,d.city_manager_user_number,d.city_manager_user_name,
		d.province_manager_user_id,d.province_manager_user_number,d.province_manager_user_name,
		a.id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,a.status_name,a.operator_by,a.operator_by_id,
		a.change_product_code,a.change_product_name,a.change_unit,
		a.release_time, -- 操作时间 发布时间
		a.create_time, -- 映射到客户的时间
		a.update_time, -- 完成时间
		a.sale_amt, -- 往前追一个月 如没有则再往前追一个月 如没有再往前追一个月 还没有就是0
		a.profit, -- 往前追一个月 如没有则再往前追一个月 如没有再往前追一个月 还没有就是0
		coalesce(a.profit_rate,0) as profit_rate,
		coalesce(b.finish_after_sale_amt,0) as finish_after_sale_amt, -- 完成后销售额
		coalesce(b.finish_after_profit,0) as finish_after_profit, -- 完成后毛利额
		coalesce(b.finish_after_profit,0)-coalesce(a.profit_rate,0)*coalesce(b.finish_after_sale_amt,0) as increase_profit
	from
		(
		select
			a.id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
			a.update_by,a.update_by_id,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,
			coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0) as sale_amt,
			coalesce(a.month_ago_1_profit,a.month_ago_2_profit,a.month_ago_3_profit,0) as profit,
			coalesce(a.month_ago_1_profit,a.month_ago_2_profit,a.month_ago_3_profit,0)/abs(coalesce(a.month_ago_1_sale_amt,a.month_ago_2_sale_amt,a.month_ago_3_sale_amt,0)) as profit_rate,
			case status when 1 then '待处理' when 2 then '已完成' when 3 then '已拒绝' end as status_name,
			coalesce(b.create_time,'') as release_time, -- 操作时间 发布时间
			if(a.status=1,coalesce(c.rp_service_user_name_new,c.fl_service_user_name_new,c.bbc_service_user_name_new,c.sales_name_new),a.update_by) as operator_by, -- 操作人
			if(a.status=1,coalesce(c.rp_service_user_id_new,c.fl_service_user_id_new,c.bbc_service_user_id_new,c.sales_id_new),a.update_by_id) as operator_by_id -- 操作人id
		from
			(
			select
				a.id,a.config_id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
				a.update_by,a.update_by_id,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3,
				sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_1_sale_amt,
				sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_2_sale_amt,
				sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.sale_amt else null end) as month_ago_3_sale_amt,
				
				sum(case when b.sdt>=a.month_ago_1 and b.sdt<=a.create_date then b.profit else null end) as month_ago_1_profit,
				sum(case when b.sdt>=a.month_ago_2 and b.sdt<=a.create_date then b.profit else null end) as month_ago_2_profit,
				sum(case when b.sdt>=a.month_ago_3 and b.sdt<=a.create_date then b.profit else null end) as month_ago_3_profit
			from	
				(
				select
					id,config_id,inventory_dc_code,inventory_dc_name,main_product_code,main_product_name,unit,sales_amount,profit_margin,sap_cus_code,sap_cus_name,status,
					update_by,update_by_id,change_product_code,change_product_name,change_unit,create_time,update_time,
					regexp_replace(to_date(create_time),'-','') as create_date,
					regexp_replace(add_months(to_date(create_time),-1),'-','') as month_ago_1,
					regexp_replace(add_months(to_date(create_time),-2),'-','') as month_ago_2,
					regexp_replace(add_months(to_date(create_time),-3),'-','') as month_ago_3
					-- row_number()over(partition by inventory_dc_code,main_product_code,sap_cus_code order by update_time desc) as rn
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
				where
					sdt='${ytd}'
				) a 
				left join
					(
					select
						sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
					from
						tmp_sale_detail
					group by 
						sdt,inventory_dc_code,customer_code,goods_code
					) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.main_product_code
			group by 
				a.id,a.config_id,a.inventory_dc_code,a.inventory_dc_name,a.main_product_code,a.main_product_name,a.unit,a.sales_amount,a.profit_margin,a.sap_cus_code,a.sap_cus_name,a.status,
				a.update_by,a.update_by_id,a.change_product_code,a.change_product_name,a.change_unit,a.create_time,a.update_time,a.create_date,a.month_ago_1,a.month_ago_2,a.month_ago_3	
			) a 
			left join
				(
				select
					id,inventory_dc_code,main_product_code,update_time,create_time
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_config_df
				where
					sdt='${ytd}'
				) b on b.id=a.config_id
			left join
				(
				select
					customer_no,sales_id_new,work_no_new,sales_name_new,
					rp_service_user_id_new,rp_service_user_work_no_new,rp_service_user_name_new,
					fl_service_user_id_new,fl_service_user_work_no_new,fl_service_user_name_new,
					bbc_service_user_id_new,bbc_service_user_work_no_new,bbc_service_user_name_new
				from
					csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df
				where
					sdt='${ytd}'
				) c on c.customer_no=a.sap_cus_code
		) a 
		left join
			(
			select
				a.id,a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,
				a.change_product_code,a.create_time,a.update_time,					
				sum(case when b.sdt>=a.finish_date and b.sdt<a.next_finish_date then b.sale_amt else 0 end) as finish_after_sale_amt,
				sum(case when b.sdt>=a.finish_date and b.sdt<a.next_finish_date then b.profit else 0 end) as finish_after_profit
			from
				(
				select
					id,inventory_dc_code,main_product_code,sap_cus_code,status,
					change_product_code,create_time,update_time,
					regexp_replace(to_date(update_time),'-','') as finish_date,
					-- row_number()over(partition by inventory_dc_code,change_product_code,sap_cus_code order by update_time desc) as rn
					regexp_replace(to_date(lead(update_time,1,'9999-12-31')over(partition by inventory_dc_code,change_product_code,sap_cus_code order by update_time)),'-','') as next_finish_date
				from
					csx_ods.csx_ods_b2b_mall_prod_yszx_change_product_task_df
				where
					sdt='${ytd}'	
					and status=2 -- 已完成
				) a 
				left join
					(
					select
						sdt,inventory_dc_code,customer_code,goods_code,sum(sale_amt) as sale_amt,sum(profit) as profit
					from
						tmp_sale_detail
					where
						sdt>=regexp_replace(date_sub('${td_date}',if(pmod(datediff('${td_date}','2012-01-06'),7)=0,7,pmod(datediff('${td_date}','2012-01-06'), 7))),'-','')
						and sdt<='${ytd}'
					group by 
						sdt,inventory_dc_code,customer_code,goods_code
					) b on b.inventory_dc_code=a.inventory_dc_code and b.customer_code=a.sap_cus_code and b.goods_code=a.change_product_code
			where
				1=1
				-- rn=1
			group by 
				a.id,a.inventory_dc_code,a.main_product_code,a.sap_cus_code,a.status,
				a.change_product_code,a.create_time,a.update_time				
			) b on b.id=a.id
		left join
			(
			select
				shop_code,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
			from
				csx_dim.csx_dim_shop
			where
				sdt='current'
			) c on c.shop_code=a.inventory_dc_code
		left join
			(
			select
				customer_code,customer_name,
				-- 业务员
				sales_user_id,sales_user_number,sales_user_name,
				-- 销售主管
				supervisor_user_id,supervisor_user_number,supervisor_user_name,
				-- 销售总监
				sales_manager_user_id,sales_manager_user_number,sales_manager_user_name,
				-- 城市经理
				city_manager_user_id,city_manager_user_number,city_manager_user_name,
				-- 省区总
				province_manager_user_id,province_manager_user_number,province_manager_user_name,
				performance_province_name,performance_city_name
			from
				csx_dim.csx_dim_crm_customer_info
			where
				sdt='current'
			) d on d.customer_code=a.sap_cus_code
	) a 
	left join
		(
		select 
			calday,day_of_week,week_number,week_of_year,csx_week,csx_week_begin,csx_week_end,week_begin,week_end,lag(calday,3) over(order by calday) as lag_day
		from 
			csx_dim.csx_dim_basic_date
		) i on i.lag_day=a.sdt
group by 
	a.performance_region_code,a.performance_region_name,a.performance_province_code,a.performance_province_name,a.performance_city_code,a.performance_city_name,
	a.inventory_dc_code,a.operator_by,a.operator_by_id,
	coalesce(a.sales_user_id,''),
	coalesce(a.sales_user_number,''),
	coalesce(a.sales_user_name,''),
	coalesce(a.supervisor_user_id,''),
	coalesce(a.supervisor_user_number,''),
	coalesce(a.supervisor_user_name,''),
	coalesce(a.sales_manager_user_id,'') ,
	coalesce(a.sales_manager_user_number,''),
	coalesce(a.sales_manager_user_name,''),
	coalesce(a.city_manager_user_id,''),
	coalesce(a.city_manager_user_number,''),
	coalesce(a.city_manager_user_name,''),
	coalesce(a.province_manager_user_id,''),
	coalesce(a.province_manager_user_number,''),
	coalesce(a.province_manager_user_name,''),
	concat(i.week_of_year,'（',regexp_replace(date_sub('${td_date}',if(pmod(datediff('${td_date}','2012-01-06'),7)=0,7,pmod(datediff('${td_date}','2012-01-06'), 7))),'-',''),'-','${ytd}','）') , -- 周区间
	i.week_of_year
;


/*

create table csx_analyse.csx_analyse_fr_product_pool_change_wi(
`performance_region_code`               string              COMMENT    '大区编码',
`performance_region_name`               string              COMMENT    '大区名称',
`performance_province_code`             string              COMMENT    '省份编码',
`performance_province_name`             string              COMMENT    '省份名称',
`performance_city_code`                 string              COMMENT    '城市编码',
`performance_city_name`                 string              COMMENT    '城市名称',
`inventory_dc_code`                     string              COMMENT    'dc编码',
`operator_by_id`                        string              COMMENT    '操作人id',
`operator_by_user_number`               string              COMMENT    '操作人工号',
`operator_by`                           string              COMMENT    '操作人名称',
`sales_user_id`                         string              COMMENT    '业务员id',
`sales_user_number`                     string              COMMENT    '业务员工号',
`sales_user_name`                       string              COMMENT    '业务员名称',
`supervisor_user_id`                    string              COMMENT    '销售主管id',
`supervisor_user_number`                string              COMMENT    '销售主管工号',
`supervisor_user_name`                  string              COMMENT    '销售主管姓名',
`sales_manager_user_id`                 string              COMMENT    '销售总监id',
`sales_manager_user_number`             string              COMMENT    '销售总监工号',
`sales_manager_user_name`               string              COMMENT    '销售总监名称',
`city_manager_user_id`                  string              COMMENT    '城市经理id',
`city_manager_user_number`              string              COMMENT    '城市经理工号',
`city_manager_user_name`                string              COMMENT    '城市经理名称',
`province_manager_user_id`              string              COMMENT    '省区总id',
`province_manager_user_number`          string              COMMENT    '省区总工号',
`province_manager_user_name`            string              COMMENT    '省区总姓名',
`total_change_cnt`                      decimal(26,6)       COMMENT    '整体换品数 客户&商品',
`sale_goods_cnt`                        decimal(26,6)       COMMENT    '动销商品数',
`finish_after_sale_amt`                 decimal(26,6)       COMMENT    '销售额',
`increase_profit`                       decimal(26,6)       COMMENT    '毛利额增量',
`need_th_product_cnt`                   decimal(26,6)       COMMENT    '需换品商品数',
`pending_product_cnt`                   decimal(26,6)       COMMENT    '待处理商品数',
`finish_product_cnt`                    decimal(26,6)       COMMENT    '完成商品数',
`refuse_product_cnt`                    decimal(26,6)       COMMENT    '拒绝商品数',
`change_after_sale_product_cnt`         decimal(26,6)       COMMENT    '换后下单商品数',
`finish_sale_amt`                       decimal(26,6)       COMMENT    '完成商品销售额',
`refuse_sale_amt`                       decimal(26,6)       COMMENT    '拒绝商品销售额',
`week_interval`                         string       		COMMENT    '周区间'

) COMMENT '换品复盘看板（省区内个人维度-周维度）'
PARTITIONED BY (sdt string COMMENT '周分区')
STORED AS PARQUET;

*/	
