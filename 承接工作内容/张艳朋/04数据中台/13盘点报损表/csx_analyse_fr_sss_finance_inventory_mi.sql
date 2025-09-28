set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_analyse.csx_analyse_fr_sss_finance_inventory_mi partition(sdt)
 
select
	concat_ws('-',b.performance_city_name,a.location_code,a.wastage_type,a.division_name,substr('${ytd}', 1, 6)) as biz_id,
	case when a.location_code in ('W0G1','W0H1') then '大宗一'
		when a.location_code in ('W0H4','W0S1') then '大宗二'
		else b.performance_province_name
	end as performance_province_name,   --省区
	b.performance_city_name,   --城市
	a.wastage_type, --损耗分类
	b.warehouse_purpose_name, --地点分类
	a.location_code,   --DC编码
	a.division_name,   --部类
	sum(post_total_amt) as post_total_amt,   --盘点金额（业务盘点含税金额+财务盘点不含税金额+报损金额）
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr('${ytd}',1,6) as sdt
from
	(
	select
		location_code,
		wastage_type,
		division_name,
		sum(no_post_amt+post_amt_no_tax+bs_amt_no_tax) as post_total_amt
	from
		(
		select
			location_code, --DC编码
			if(substr(purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(purchase_group_code,1,1)='A','食百','易耗品')) as division_name, --部类
			if(substr(move_type_code,1,3) in ('110','111','115','116') and substr(reservoir_area_code,1,2)='PD','盘盈亏',if(substr(move_type_code,1,3) in ('117'),'报损',null)) as wastage_type,	--损耗分类	
			if(substr(move_type_code,1,3) in ('110','111') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction_flag='+',-1*amt,amt),0) as no_post_amt,   --业务盘点未过账含税金额，注意direction_flag条件'+'
			if(substr(move_type_code,1,3) in ('115','116') and amt_no_tax<>'0',if(direction_flag='-',-1*amt_no_tax,amt_no_tax),0) as post_amt_no_tax,   --财务盘点过账不含税金额
			if(substr(move_type_code,1,3) in ('117'),if(direction_flag='-',-1*amt_no_tax,amt_no_tax),0) as bs_amt_no_tax   --报损不含税金额
		from 
			-- csx_ods.source_cas_r_d_accounting_credential_item 
			csx_dwd.csx_dwd_cas_credential_item_di
		where 
			-- sdt='19990101' 
			to_date(credential_item_posting_time) between trunc('${ytd_date}','MM') and '${ytd_date}'
			and location_code not in ('W098','W0B7')
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
			shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,warehouse_purpose_name
		from 
			csx_dim.csx_dim_shop
		where 
			sdt = '${ytd}'
		) b on b.shop_code=a.location_code
group by
	case when a.location_code in ('W0G1','W0H1') then '大宗一'
		when a.location_code in ('W0H4','W0S1') then '大宗二'
		else b.performance_province_name
	end,
	b.performance_city_name,       -- 城市
	a.wastage_type,    -- 损耗分类
	b.warehouse_purpose_name,-- 地点分类
	a.location_code,   -- DC编码
	a.division_name    -- 部类
;	


/*
create table csx_analyse.csx_analyse_fr_sss_finance_inventory_mi(
`biz_id`                   string              COMMENT    '业务主键',
`performance_province_name`string              COMMENT    '省份',
`performance_city_name`    string              COMMENT    '城市',
`wastage_type`             string              COMMENT    '损耗分类',
`warehouse_purpose_name`   string              COMMENT    '地点分类',
`location_code`            string              COMMENT    'DC编码',
`division_name`            string              COMMENT    '部类',
`post_total_amt`           decimal(26,6)       COMMENT    '盘点金额',
`update_time`              string              COMMENT    '更新时间'
) COMMENT '盘点报损表'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/


