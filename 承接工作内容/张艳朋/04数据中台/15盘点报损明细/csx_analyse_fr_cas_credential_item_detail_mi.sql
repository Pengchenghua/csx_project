
insert overwrite table csx_analyse.csx_analyse_fr_cas_credential_item_detail_mi partition(sdt)
 
select
	a.credential_item_id as biz_id,
	a.credential_no,
	b.basic_performance_province_name as province_name,   --省区
	b.basic_performance_city_name as city_name,   --城市
	if(substr(a.purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(a.purchase_group_code,1,1)='A','食百','易耗品')) division_name,   --部类
	a.create_time,   --创建时间
	a.wms_order_time,   --订单时间
	a.credential_item_posting_time,   --记账日期
	a.company_code,   --公司代码
	a.company_name,   --公司名称
	a.location_code,   --DC编码
	a.location_name,   --DC名称
	a.reservoir_area_code,   --库区编码
	a.reservoir_area_name,   --库区名称
	a.purchase_group_code,   --课组编码
	a.purchase_group_name,   --课组名称
	a.goods_code,   --商品编码
	c.goods_name,   --商品名称
	c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,c.classify_small_name,
	a.move_type_code,   --移动类型编码
	a.move_type_name,   --移动类型名称
	coalesce(d.wms_biz_type_code,'') as wms_biz_type_code, -- 报损类型
	coalesce(d.wms_biz_type_name,'') as wms_biz_type_name, -- 报损名称
	a.cost_center_code,
	a.cost_center_name,
	coalesce(e.reservoir_area_attribute,'') as reservoir_area_attribute, -- 库区属性

	-- 盘点未过账
	(case when a.direction_flag='+' then -1*a.qty else a.qty end) as pd_no_qty,   -- 盘点未过账数量
	(case when a.direction_flag='+' then -1*a.amt_no_tax else a.amt_no_tax end) as pd_no_amt_no_tax,   -- 盘点未过账不含税金额
	(case when a.direction_flag='+' then -1*a.amt else a.amt end) as pd_no_amt,   -- 盘点未过账含税金额
	-- 盘点已过账
	(case when a.direction_flag='-' then -1*a.qty else a.qty end) as pd_yes_qty,   --盘点过账数量
	(case when a.direction_flag='-' then -1*a.amt_no_tax else a.amt_no_tax end) as pd_yes_amt_no_tax,   --盘点过账不含税金额
	(case when a.direction_flag='-' then -1*a.amt else a.amt end) as pd_yes_amt,   -- 盘点过账含税金额	
	-- 报损
	(case when a.direction_flag='-' then -1*a.qty else a.qty end) as bs_qty,
	(case when a.direction_flag='-' then -1*a.amt_no_tax else a.amt_no_tax end) as bs_amt_no_tax,
	(case when a.direction_flag='-' then -1*a.amt else a.amt end) as bs_amt,
	
	case when substr(a.move_type_code,1,3) in ('110','111') and substr(a.reservoir_area_code,1,2)='PD' then '盘点未过账'
		when substr(a.move_type_code,1,3) in ('115','116') then '盘点已过账'
		when substr(a.move_type_code,1,3) in ('117') then '报损'
		else '其他' end as biz_type,
	case when a.move_type_code in('110A','116A') then '盘亏' else '盘盈' end as pd_type,
	'${last_month}' as sdt_month,
	'${last_month}' as sdt
from
	(
	select 
		credential_item_id,credential_no,purchase_group_code,purchase_group_name,create_time,wms_order_time,credential_item_posting_time,company_code,company_name,
		location_code,location_name,reservoir_area_code,reservoir_area_name,purchase_group_code,purchase_group_name,goods_code,move_type_code,move_type_name,
		direction_flag,qty,amt_no_tax,amt,cost_center_code,cost_center_name
	from 
		csx_dwd.csx_dwd_cas_credential_item_di
	where 
		substr(sdt,1,6) ='${last_month}'
		and substr(move_type_code,1,3) in ('110','111','115','116','117') 
		and substr(company_code,1,1)<>'C' -- 20221101新加 公司代码 限制不为城市服务商
	) a
	left join 
		(
		select 
			shop_code,shop_name,basic_performance_province_name,basic_performance_city_name
		from 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current' 
		) b on b.shop_code=a.location_code
	left join 
		(
		select 
			goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name
		from  
			csx_dim.csx_dim_basic_goods
		where 
			sdt = 'current'
		)c on c.goods_code = a.goods_code
	left join 
		(
		select 
			credential_no,wms_biz_type_code,wms_biz_type_name
		from 
			csx_dwd.csx_dwd_cas_credential_header_di
		where 
			substr(sdt,1,6) ='${last_month}'
		group by 
			credential_no,wms_biz_type_code,wms_biz_type_name
		)d on a.credential_no=d.credential_no
	left join 
		(
        select 
            b.code as warehouse_code,a.code as reservoir_area_code,a.reservoir_area_attribute
        from 
			(
			select
				code,name,level,parent_code,reservoir_area_attribute
			from
				csx_dim.csx_dim_wms_reservoir_area
			where
				level='3'
				and (reservoir_area_attribute='C' or reservoir_area_attribute='Y')
			) a 
			left join
				(
				select
					code,name,level,parent_code
				from
					csx_dim.csx_dim_wms_reservoir_area
				where
					level='2'
				) b on b.code=a.parent_code	
        ) e on a.location_code=e.warehouse_code and a.reservoir_area_code=e.reservoir_area_code  
;



