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
	'20200331' as sdt
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
			csx_tmp.source_cas_r_d_accounting_credential_item 
		where 
			sdt='19990101' 
			and to_date(posting_time) between '2020-03-01' and '2020-03-31'
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