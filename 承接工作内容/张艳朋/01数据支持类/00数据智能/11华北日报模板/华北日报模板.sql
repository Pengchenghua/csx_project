select
	send_date,
	sales_region_name,
	province_name,
	business_flag,
	count(distinct order_code) as order_num,
	sum(case when coalesce(back_time,sign_time) is not null then 1 else 0 end) as back_order_num,
    sum(case when coalesce(back_time,sign_time) is null then 1 else 0 end) as zt_order_num,
    sum(case when coalesce(back_time,sign_time) is not null then 1 else 0 end) / count(distinct order_code) as back_order_rate
from
	(
	select
		a.send_date, -- 出库日期
		e.sales_region_name, -- 大区
		e.province_name,-- 省区名称
		a.order_code,-- 订单号
		case when b.back_time = '2000-01-01 00:00:00' then null else b.back_time end back_time,-- 回单日期
		case when a.sign_date = '2000-01-01 00:00:00' then null else a.sign_date end sign_time,-- 签收日期
		case when b.depart_date = '2000-01-01 00:00:00' then null else b.depart_date end depart_date,  -- 发货时间
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
			send_location_code
		from 
			csx_dw.dwd_wms_r_d_shipped_order_header -- 1.销售出库(S01) -- 2.销售渠道 B段 -- 3.价格补单不要 -- 4.状态为 已发货 去除已取消
		where 
			from_unixtime(unix_timestamp(send_time), "yyyyMMdd")='20210221'
			and shipped_type in ('S01', 'S18')
			--and sale_channel = 7
			and price_fix_flag = 0
			and status >= 6
			and status != 9
		group by 
			from_unixtime(unix_timestamp(send_time), "yyyyMMdd"),order_code,date_format(sign_date, 'yyyy-MM-dd HH:mm:ss'),customer_code,sale_channel,send_location_code
		) a
		left join 
			(
			select 
				shipped_order_code,back_time,depart_date
			from 
				csx_dw.dws_tms_r_d_entrucking_order_detail
			where 
				instr(shipped_order_code, 'OT') = 0
				and shipped_type_code in ('S01', 'S18')
			group by 
				shipped_order_code,back_time,depart_date
			) b on a.order_code = b.shipped_order_code
		left join --关联客户信息 判断商超业绩
			(
			select 
				customer_no,customer_name,channel_code,channel_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt='current'
			group by 
				customer_no,customer_name,channel_code,channel_name
			) c on a.customer_code = c.customer_no
		left join --关联订单表 判断城市服务商订单
			(
			select 
				order_no,is_partner_order,is_partner_dc
			from 
				csx_dw.dws_csms_r_d_yszx_order_m_new
			where 
				sdt>='20201231'
			group by 
				order_no,is_partner_order,is_partner_dc
			) d on a.order_code = d.order_no
		left join
			(
			select 
				shop_id,shop_name,province_code,province_name,city_code,city_name,town_code,town_name,sales_region_code,sales_region_name
			from 
				csx_dw.dws_basic_w_a_csx_shop_m
			where 
				sdt = 'current'
			group by 
				shop_id,shop_name,province_code,province_name,city_code,city_name,town_code,town_name,sales_region_code,sales_region_name
			) e on a.send_location_code = e.shop_id
	) as t1	
group by 
	send_date,
	sales_region_name,
	province_name,
	business_flag
;