-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

with current_back_order as 	
(
	select
		a.send_date, -- 出库日期
		e.performance_region_code,
		e.performance_region_name,
		e.performance_province_code,
		e.performance_province_name,
		e.performance_city_code,
		e.performance_city_name,
		a.order_code,-- 订单号
		case when b.back_time = '2000-01-01 00:00:00' then null else from_unixtime(unix_timestamp(b.back_time), "yyyyMMdd") end back_time,-- 回单日期
		case when a.sign_date = '2000-01-01 00:00:00' then null else from_unixtime(unix_timestamp(a.sign_date), "yyyyMMdd") end sign_time,-- 签收日期
		case when b.depart_date = '2000-01-01 00:00:00' then null else from_unixtime(unix_timestamp(b.depart_date), "yyyyMMdd") end depart_date,  -- 发货时间
		case when a.sale_channel in ('1','2','3','4') or (a.sale_channel in ('6','7') and c.channel_code='2') then 'M端商超'
			when a.sale_channel in ('6','7') and c.channel_code!='2' and d.is_partner_order = 1 and d.is_partner_dc = 1 then 'B端联营' 
			else 'B端自营' 
		end as business_flag
	from
		(
		select
			from_unixtime(unix_timestamp(send_time), "yyyyMMdd") as send_date,
			order_code,
			date_format(sign_date, 'yyyy-MM-dd HH:mm:ss') as sign_date,
			customer_code,
			sale_channel, -- 销售渠道 1-云超 2-云创 3-寄售 4-自营小店 5.BBC,6.红旗,7.B端
			send_dc_code
		from 
			-- csx_dw.dwd_wms_r_d_ship_order_header  -- 2.销售渠道 B段
			csx_dwd.csx_dwd_wms_shipped_order_header_di			
		where 
			from_unixtime(unix_timestamp(send_time), "yyyyMMdd")>=regexp_replace(trunc('${ytd_date}', 'MM'), '-', '')
			and from_unixtime(unix_timestamp(send_time), "yyyyMMdd")<='${ytd}'
			and shipped_type in ('S01', 'S18') -- 销售出库(S01)
			and price_fix_flag = 0 -- 价格补单不要
			and status >= 6 -- 状态为 已发货 
			and status != 9 -- 去除已取消
		group by 
			from_unixtime(unix_timestamp(send_time), "yyyyMMdd"),order_code,date_format(sign_date, 'yyyy-MM-dd HH:mm:ss'),customer_code,sale_channel,send_dc_code
		) a
		left join 
			(
			select 
				shipped_order_code,back_time,depart_date
			from 
				-- csx_dw.dws_tms_r_d_entrucking_order_detail
				csx_dws.csx_dws_tms_entrucking_order_detail_di
			where 
				from_unixtime(unix_timestamp(coalesce(back_time,depart_date)), "yyyyMMdd")>=regexp_replace(trunc('${ytd_date}', 'MM'), '-', '')
				and from_unixtime(unix_timestamp(coalesce(back_time,depart_date)), "yyyyMMdd")<='${ytd}'
				and instr(shipped_order_code, 'OT') = 0
				and shipped_type_code in ('S01', 'S18')
			group by 
				shipped_order_code,back_time,depart_date
			) b on a.order_code = b.shipped_order_code
		left join --关联客户信息 判断商超业绩
			(
			select 
				customer_code,customer_name,channel_code,channel_name
			from 
				csx_dim.csx_dim_crm_customer_info
			where 
				sdt = '${ytd}'
			group by 
				customer_code,customer_name,channel_code,channel_name
			) c on a.customer_code = c.customer_code
		left join -- 关联订单表 判断城市服务商订单
			(
			select 
				order_code,is_partner_order,is_partner_dc
			from 
				-- csx_dw.dws_csms_r_d_yszx_order_m_new
				csx_dwd.csx_dwd_csms_yszx_order_detail_di
			where 
				sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-6),'-','')
				and sdt<='${ytd}'
			group by 
				order_code,is_partner_order,is_partner_dc
			) d on a.order_code = d.order_code
		left join
			(
			select 
				shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
			from 
				-- csx_dw.dws_basic_w_a_csx_shop_m
				csx_dim.csx_dim_shop
			where 
				sdt = 'current'
			) e on a.send_dc_code = e.shop_code
)

insert overwrite table csx_analyse.csx_analyse_fr_wms_back_order_mi partition(month)		
select
    concat_ws('-',performance_city_code,business_flag,substr('${ytd}', 1, 6)) as biz_id,
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	business_flag,
	coalesce(count(distinct case when send_date >=regexp_replace(trunc('${ytd_date}', 'MM'), '-', '') and send_date<='${ytd}' then order_code else null end),0) as order_num,
	coalesce(count(distinct case when coalesce(back_time,sign_time) >=regexp_replace(trunc('${ytd_date}', 'MM'), '-', '') and coalesce(back_time,sign_time) <='${ytd}' then order_code else null end),0) as back_order_num,
    coalesce(count(distinct case when coalesce(back_time,sign_time) is null or coalesce(back_time,sign_time)>'${ytd}' then order_code else null end),0) as zt_order_num,
    coalesce(count(distinct case when coalesce(back_time,sign_time) >=regexp_replace(trunc('${ytd_date}', 'MM'), '-', '') and coalesce(back_time,sign_time) <='${ytd}' then order_code else null end) / 
	count(distinct case when send_date >=regexp_replace(trunc('${ytd_date}', 'MM'), '-', '') and send_date<='${ytd}' then order_code else null end),0) as back_order_rate,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr('${ytd}', 1, 6) as month
from
	current_back_order
group by 
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	business_flag
;



/*
create table csx_analyse.csx_analyse_fr_wms_back_order_mi(
`biz_id`                   string              COMMENT    '业务主键',
`performance_region_code`  string              COMMENT    '大区编码',
`performance_region_name`  string              COMMENT    '大区名称',
`performance_province_code`string              COMMENT    '省份编码',
`performance_province_name`string              COMMENT    '省份名称',
`performance_city_code`    string              COMMENT    '城市编码',
`performance_city_name`    string              COMMENT    '城市名称',
`business_flag`            string              COMMENT    '业务类型',
`order_num`                decimal(26,6)       COMMENT    '出库订单数',
`back_order_num`           decimal(26,6)       COMMENT    '回单数',
`zt_order_num`             decimal(26,6)       COMMENT    '在途数',
`back_order_rate`          decimal(26,6)       COMMENT    '回单率',
`update_time`              string              COMMENT    '数据更新时间'
) COMMENT '回单统计'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	
	
