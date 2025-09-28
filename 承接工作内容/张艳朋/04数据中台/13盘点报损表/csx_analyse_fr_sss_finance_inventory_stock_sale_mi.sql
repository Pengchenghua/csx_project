set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_analyse.csx_analyse_fr_sss_finance_inventory_stock_sale_mi partition(sdt)

select
	coalesce(b.performance_province_name,c.performance_province_name,a.performance_province_name) as performance_province_name,
	coalesce(b.performance_city_name,c.performance_city_name,a.performance_city_name) as performance_city_name,
	coalesce(b.dc_code,c.inventory_dc_code,a.location_code) as location_code,
	coalesce(b.division_name,c.division_name,a.division_name) as division_name,
	coalesce(a.surplus_qty,'0') as surplus_qty,
	coalesce(a.surplus_amt,'0') as surplus_amt,
	coalesce(a.loss_qty,'0') as loss_qty,
	coalesce(a.loss_amt,'0') as loss_amt,
	coalesce(b.qty,'0') as qty,
	coalesce(b.amt,'0') as amt,
	coalesce(a.surplus_qty+a.loss_qty,'0') as surplus_loss_total_qty,
	coalesce(a.surplus_amt+a.loss_amt,'0') as surplus_loss_total_amt,
	coalesce(c.sale_amt_no_tax,'0') as sale_amt_no_tax,
	coalesce(c.sale_cost_no_tax,'0') as sale_cost_no_tax,
	coalesce(c.profit_no_tax,'0') as profit_no_tax,
	coalesce(c.profit_no_tax/c.sale_amt_no_tax,'0') as profit_no_tax_rate,
	coalesce((a.surplus_amt+a.loss_amt)/c.sale_amt_no_tax,'0') as loss_rate,
	coalesce((a.surplus_amt+a.loss_amt+c.profit_no_tax)/c.sale_amt_no_tax,'0') as inv_after_profit_no_tax_rate,
	from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time, -- 更新时间
	substr('${ytd}',1,6) as sdt
from
	--盘盈亏
	(
	select
		case when location_code in ('W0G1','W0H1') then '大宗一'
			when location_code in ('W0H4','W0S1') then '大宗二'
			else tmp1.performance_province_name
		end as performance_province_name,
		performance_city_name,
		location_code,
		division_name,
		sum(no_post_surplus_qty+post_surplus_qty) as surplus_qty,
		sum(no_post_surplus_amt+post_surplus_amt_no_tax) as surplus_amt,
		sum(no_post_loss_qty+post_loss_qty) as loss_qty,
		sum(no_post_loss_amt+post_loss_amt_no_tax) as loss_amt
	from
		(
		select
			location_code,
			if(substr(purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(purchase_group_code,1,1)='A','食百','易耗品')) as division_name, --部类
			--业务盘点-盘盈
			if(substr(move_type_code,1,3) in ('111') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction_flag='+',-1*qty,qty),0) as no_post_surplus_qty,   --业务盘点未过账 盘盈 数量，注意direction_flag条件'+'
			if(substr(move_type_code,1,3) in ('111') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction_flag='+',-1*amt,amt),0) as no_post_surplus_amt,   --业务盘点未过账 盘盈 含税金额，注意direction_flag条件'+'
			--业务盘点-盘亏
			if(substr(move_type_code,1,3) in ('110') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction_flag='+',-1*qty,qty),0) as no_post_loss_qty,   --业务盘点未过账 盘盈 数量，注意direction_flag条件'+'
			if(substr(move_type_code,1,3) in ('110') and reservoir_area_code='PD01' and substr(purchase_group_code,1,1) not in('U','H'),if(direction_flag='+',-1*amt,amt),0) as no_post_loss_amt,   --业务盘点未过账 盘盈 含税金额，注意direction_flag条件'+'
			--财务盘点-盘盈
			if(substr(move_type_code,1,3) in ('115') and amt_no_tax<>'0',if(direction_flag='-',-1*qty,qty),0) as post_surplus_qty,   --财务盘点过账不含税数量	
			if(substr(move_type_code,1,3) in ('115') and amt_no_tax<>'0',if(direction_flag='-',-1*amt_no_tax,amt_no_tax),0) as post_surplus_amt_no_tax,   --财务盘点过账不含税金额
			--财务盘点-盘亏
			if(substr(move_type_code,1,3) in ('116') and amt_no_tax<>'0',if(direction_flag='-',-1*qty,qty),0) as post_loss_qty,   --财务盘点过账不含税数量	
			if(substr(move_type_code,1,3) in ('116') and amt_no_tax<>'0',if(direction_flag='-',-1*amt_no_tax,amt_no_tax),0) as post_loss_amt_no_tax  --财务盘点过账不含税金额	
		from 
			-- csx_ods.source_cas_r_d_accounting_credential_item 
			csx_dwd.csx_dwd_cas_credential_item_di
		where 
			-- sdt='19990101' 
			to_date(credential_item_posting_time) between trunc('${ytd_date}','MM') and '${ytd_date}'
			and substr(move_type_code,1,3) in ('110','111','115','116')
			and location_code not in ('W098','W0B7') -- 仓库不属于彩食鲜
		) t1
		left join
			(
			select 
				shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,warehouse_purpose_name
			from 
				csx_dim.csx_dim_shop
			where 
				sdt = '${ytd}'
			group by
				shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,warehouse_purpose_name
			) tmp1 on tmp1.shop_code=t1.location_code
	group by
		case when location_code in ('W0G1','W0H1') then '大宗一'
			when location_code in ('W0H4','W0S1') then '大宗二'
			else tmp1.performance_province_name
		end,
		performance_city_name,
		location_code,
		division_name
	) a
	-- 结存信息
	full join 
		(
		select
			case when dc_code in ('W0G1','W0H1') then '大宗一'
				when dc_code in ('W0H4','W0S1') then '大宗二'
				else tmp2.performance_province_name
			end as performance_province_name,
			performance_city_name,
			dc_code,
			division_name,
			qty,
			amt
		from
			(
			select
				dc_code,
				if(substr(purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(purchase_group_code,1,1)='A','食百','易耗品')) division_name,   --部类
				sum(qty) as qty,
				sum(amt_no_tax) as amt --不含税金额 20201118更新
			from
				-- csx_dw.dws_wms_r_d_accounting_stock_m
				csx_dws.csx_dws_cas_accounting_stock_m_df
			where
				sdt='${ytd}'
				and substr(reservoir_area_code,1,2) not in ('PD','TS') --盘点 'TS'：在途
				and dc_code not in ('W098','W0B7')
			group by
				dc_code,
				if(substr(purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(purchase_group_code,1,1)='A','食百','易耗品'))
			) t2 
			left join
				(
				select 
					shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,warehouse_purpose_name
				from 
					csx_dim.csx_dim_shop
				where 
					sdt = '${ytd}'
				group by
					shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,warehouse_purpose_name
				) tmp2 on tmp2.shop_code=t2.dc_code
		) b on b.dc_code=a.location_code and b.division_name=a.division_name and b.performance_province_name=a.performance_province_name and b.performance_city_name=a.performance_city_name
	-- 销售信息
	full join 
		(
		select
			t3.performance_province_name,
			tmp3.performance_city_name as performance_city_name,
			inventory_dc_code,
			division_name,
			sale_amt_no_tax,
			sale_cost_no_tax,
			profit_no_tax
		from
			(
			select
				case when performance_province_name like '%大宗%' then '大宗一'
					when performance_province_name like '%采购%' then '大宗二'
					else performance_province_name
				end as performance_province_name,
				inventory_dc_code,
				if(substr(purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(purchase_group_code,1,1)='A','食百','易耗品')) division_name,   --部类
				sum(sale_amt_no_tax) as sale_amt_no_tax, --不含税
				sum(sale_cost_no_tax) as sale_cost_no_tax, --不含税
				sum(profit_no_tax) as profit_no_tax --不含税
			from 
				-- csx_dw.dws_sale_r_d_detail
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt between regexp_replace(trunc('${ytd_date}','MM'),'-','') and '${ytd}'
				and inventory_dc_code not in ('W098','W0B7')
			group by
				case when performance_province_name like '%大宗%' then '大宗一'
					when performance_province_name like '%采购%' then '大宗二'
					else performance_province_name
				end,	
				inventory_dc_code,
				if(substr(purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(purchase_group_code,1,1)='A','食百','易耗品'))  --部类
			)t3
			left join
				(
				select 
					shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,warehouse_purpose_name
				from 
					csx_dim.csx_dim_shop
				where 
					sdt = '${ytd}'
				group by
					shop_code,shop_name,performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,warehouse_purpose_name
				) tmp3 on tmp3.shop_code=t3.inventory_dc_code
		) c on c.inventory_dc_code=b.dc_code and c.division_name=b.division_name and c.performance_province_name=b.performance_province_name and c.performance_city_name=b.performance_city_name
;	
	


/*
create table csx_analyse.csx_analyse_fr_sss_finance_inventory_stock_sale_mi(
`performance_province_name`   string              COMMENT    '省份',
`performance_city_name`       string              COMMENT    '城市',
`location_code`               string              COMMENT    'DC编码',
`division_name`               string              COMMENT    '课组',
`surplus_qty`                 decimal(26,6)       COMMENT    '盘盈数量',
`surplus_amt`                 decimal(26,6)       COMMENT    '盘盈金额',
`loss_qty`                    decimal(26,6)       COMMENT    '盘亏数量',
`loss_amt`                    decimal(26,6)       COMMENT    '盘亏金额',
`qty`                         decimal(26,6)       COMMENT    '结存总量',
`amt`                         decimal(26,6)       COMMENT    '结存金额',
`surplus_loss_total_qty`      decimal(26,6)       COMMENT    '盈亏合计数量',
`surplus_loss_total_amt`      decimal(26,6)       COMMENT    '盈亏合计金额',
`sale_amt_no_tax`             decimal(26,6)       COMMENT    '销售额',
`sale_cost_no_tax`            decimal(26,6)       COMMENT    '定价成本',
`profit_no_tax`               decimal(26,6)       COMMENT    '毛利额',
`profit_no_tax_rate`          decimal(26,6)       COMMENT    '定价毛利率',
`loss_rate`                   decimal(26,6)       COMMENT    '损耗率',
`inv_after_profit_no_tax_rate`decimal(26,6)       COMMENT    '盘点后毛利率',
`update_time`                 string              COMMENT    '更新时间'
) COMMENT '盘点&结存&销售数据'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/


