--昨日
set i_sdate =date_sub(current_date,1);
--昨日分区样式
set i_sdate_1 =regexp_replace(date_sub(current_date,1),'-','');
-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_tmp.ads_fr_inventory_day partition(sdt)
 
select
	b.province_name,   --省区
	b.city_name,   --城市
	a.wastage_type, --损耗分类
	b.location_uses_name, --地点分类
	a.location_code,   --DC编码
	a.division_name,   --部类
	sum(post_total_amt) as post_total_amt,   --盘点金额（业务盘点含税金额+财务盘点不含税金额+报损金额）
	${hiveconf:i_sdate_1} as update_time,
	substr(${hiveconf:i_sdate_1},1,6) as sdt
from
	(
	select
		location_code,
		wastage_type,
		division_name,
		sum(no_post_amt+post_amt_no_tax+bs_amt) as post_total_amt
	from
		(
		select
			location_code, --DC编码
			if(substr(purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(purchase_group_code,1,1)='A','食百','易耗品')) as division_name, --部类
			if(substr(move_type,1,3) in ('110','111','115','116') and substr(reservoir_area_code,1,2)='PD','盘盈亏',if(substr(move_type,1,3) in ('117'),'报损',null)) as wastage_type,	--损耗分类	
			if(substr(move_type,1,3) in ('110','111') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction='+',-1*amt,amt),0) as no_post_amt,   --业务盘点未过账含税金额，注意direction条件'+'
			if(substr(move_type,1,3) in ('115','116') and amt_no_tax<>'0',if(direction='-',-1*amt_no_tax,amt_no_tax),0) as post_amt_no_tax,   --财务盘点过账不含税金额
			if(substr(move_type,1,3) in ('117'),if(direction='-',-1*amt,amt),0) as bs_amt   --报损含税金额
		from 
			csx_ods.source_cas_r_d_accounting_credential_item 
		where 
			sdt='19990101' 
			and to_date(posting_time) between ${hiveconf:i_sdate_11} and ${hiveconf:i_sdate}
		) t1
	where
		t1.wastage_type is not null
	group by
		location_code,
		wastage_type,
		division_name
	) a
	left join 
		(
		select 
			shop_id,shop_name,province_code,province_name,city_code,city_name,location_uses_name
		from 
			csx_dw.dws_basic_w_a_csx_shop_m 
		where 
			sdt = 'current' 
		) b on b.shop_id=a.location_code
group by
	b.province_name,   --省区
	b.city_name,       --城市
	a.wastage_type,    --损耗分类
	b.location_uses_name,--地点分类
	a.location_code,   --DC编码
	a.division_name    --部类
;	
	

INVALIDATE METADATA csx_tmp.ads_fr_inventory_day;


/*
--------------------------------- hive建表语句 -------------------------------
-- csx_tmp.ads_fr_inventory_day  盘点报损-计算

drop table if exists csx_tmp.ads_fr_inventory_day;
create table csx_tmp.ads_fr_inventory_day(
`province_name`       string              COMMENT    '省份',
`city_name`           string              COMMENT    '城市',
`wastage_type`        string              COMMENT    '损耗分类',
`location_uses_name`  string              COMMENT    '地点分类',
`location_code`       string              COMMENT    'DC编码',
`division_name`       string              COMMENT    '部类',
`post_total_amt`      decimal(26,6)       COMMENT    '盘点金额',
`update_time`         string              COMMENT    '更新时间'
) COMMENT '盘点报损-计算'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/


