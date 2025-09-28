-- 华北日报 
-- 核心逻辑： 按业务类型统计出库、回单、在途情况

-- 切换tez计算引擎
set mapred.job.name=report_wms_r_m_back_order;
SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;

-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 计算日期
set current_day = regexp_replace(date_sub(current_date, 1), '-', '');

-- 当月第一天
set current_start_day = regexp_replace(trunc(date_sub(current_date, 1), 'MM'), '-', '');

--近三个月第一天
set three_months_before_day =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-3),'-','');

-- 目标表
set target_table=csx_tmp.report_wms_r_m_back_order;
	
with current_back_order as 	
(
	select
		a.send_date, -- 出库日期
		e.sales_region_code as region_code,
		e.sales_region_name as region_name, -- 大区
		e.sales_province_code as province_code,
		e.sales_province_name as province_name,-- 省区名称
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
			send_location_code
		from 
			csx_dw.dwd_wms_r_d_shipped_order_header  -- 2.销售渠道 B段  
		where 
			from_unixtime(unix_timestamp(send_time), "yyyyMMdd")>=${hiveconf:current_start_day}
			and from_unixtime(unix_timestamp(send_time), "yyyyMMdd")<=${hiveconf:current_day}
			and shipped_type in ('S01', 'S18') -- 销售出库(S01)
			and price_fix_flag = 0 -- 价格补单不要
			and status >= 6 -- 状态为 已发货 
			and status != 9 -- 去除已取消
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
				from_unixtime(unix_timestamp(coalesce(back_time,depart_date)), "yyyyMMdd")>=${hiveconf:current_start_day}
				and from_unixtime(unix_timestamp(coalesce(back_time,depart_date)), "yyyyMMdd")<=${hiveconf:current_day}
				and instr(shipped_order_code, 'OT') = 0
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
				sdt>=${hiveconf:three_months_before_day}
				and sdt<=${hiveconf:current_day}
			group by 
				order_no,is_partner_order,is_partner_dc
			) d on a.order_code = d.order_no
		left join
			(
			select 
				shop_id,shop_name,sales_province_code,sales_province_name,city_code,city_name,town_code,town_name,sales_region_code,sales_region_name
			from 
				csx_dw.dws_basic_w_a_csx_shop_m
			where 
				sdt = 'current'
			group by 
				shop_id,shop_name,sales_province_code,sales_province_name,city_code,city_name,town_code,town_name,sales_region_code,sales_region_name
			) e on a.send_location_code = e.shop_id
)

insert overwrite table ${hiveconf:target_table} partition(month)			
			
select
	region_code,
	region_name,
	province_code,
	province_name,
	business_flag,
	coalesce(count(distinct case when send_date >=${hiveconf:current_start_day} and send_date<=${hiveconf:current_day} then order_code else null end),0) as order_num,
	coalesce(count(distinct case when coalesce(back_time,sign_time) >=${hiveconf:current_start_day} and coalesce(back_time,sign_time) <=${hiveconf:current_day} then order_code else null end),0) as back_order_num,
    coalesce(count(distinct case when coalesce(back_time,sign_time) is null or coalesce(back_time,sign_time)>${hiveconf:current_day} then order_code else null end),0) as zt_order_num,
    coalesce(count(distinct case when coalesce(back_time,sign_time) >=${hiveconf:current_start_day} and coalesce(back_time,sign_time) <=${hiveconf:current_day} then order_code else null end) / 
	count(distinct case when send_date >=${hiveconf:current_start_day} and send_date<=${hiveconf:current_day} then order_code else null end),0) as back_order_rate,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr(${hiveconf:current_day}, 1, 6) as month
from
	current_back_order
group by 
	region_code,
	region_name,
	province_code,
	province_name,
	business_flag
;



INVALIDATE METADATA csx_tmp.report_wms_r_m_back_order;	



/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.report_wms_r_m_back_order  华北日报 回单统计

drop table if exists csx_tmp.report_wms_r_m_back_order;
create table csx_tmp.report_wms_r_m_back_order(
`region_code`              string              COMMENT    '大区编码',
`region_name`              string              COMMENT    '大区名称',
`province_code`            string              COMMENT    '省份编码',
`province_name`            string              COMMENT    '省份名称',
`business_flag`            string              COMMENT    '业务类型',
`order_num`                decimal(26,6)       COMMENT    '出库订单数',
`back_order_num`           decimal(26,6)       COMMENT    '回单数',
`zt_order_num`             decimal(26,6)       COMMENT    '在途数',
`back_order_rate`          decimal(26,6)       COMMENT    '回单率',
`update_time`              string              COMMENT    '数据更新时间'
) COMMENT 'zhangyanpeng:华北日报-回单统计'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	