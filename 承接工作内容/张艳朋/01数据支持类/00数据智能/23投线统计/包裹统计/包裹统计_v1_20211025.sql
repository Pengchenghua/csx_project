select 
	replace(xx.plan_date, '-', '') as sdt,
	xx.warehouse_code,
	xx.split_group_code,
	xx.split_group_name,
	count(*) as '总包裹数量',
	sum(case when xx.zhuangtai = '0' then 1 else 0 end) as '未拣包裹数',
	sum(case when xx.zhuangtai = '1' then 1 else 0 end) as '已拣包裹数',
	(sum(case when xx.zhuangtai = '1' then 1 else 0 end))/count(*) as '拣货进度',
	sum(case when xx.touxianzhuangtai = '0' then 1 else 0 end) as '未投包裹数',
	sum(case when xx.touxianzhuangtai = '1' then 1 else 0 end) as '已投包裹数',
	(sum(case when xx.touxianzhuangtai = '1' then 1 else 0 end))/count(*) as '投线进度'
from 
	(
	select 
		a.plan_date,a.warehouse_code,c.split_group_code,c.split_group_name,a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) as touxianzhuangtai
	from  
		csx_b2b_wms.wms_task_picking a 
		left join csx_b2b_wms.wms_container_detail b  on b.ticket_code = a.order_code
		left join csx_b2b_wms.wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
	where 
		a.plan_date>= '${sdate}' 
		and a.plan_date<= '${edate}'   
		and a.`status`='0'
		-- where a.warehouse_code = 'W0A7' and a.plan_date = '2021-09-29'  and a.`status`='0'
	union all
	select 
		a.plan_date,a.warehouse_code,c.split_group_code,c.split_group_name,a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) as touxianzhuangtai
	from  
		csx_b2b_wms.wms_task_picking a 
		left join csx_b2b_wms.wms_container_detail b  on b.ticket_code = a.order_code
		left join csx_b2b_wms.wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
	where 
		a.plan_date >= '${sdate}' 
		and a.plan_date<= '${edate}'  
		and a.picking_qty > '0' 
		and a.`status`='1'
		-- where a.warehouse_code = 'W0A7' and a.plan_date = '2021-09-29' and a.picking_qty > '0' and a.`status`='1'
	union all
	select 
		d.plan_date,a.warehouse_code,c.split_group_code,c.split_group_name,a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) as touxianzhuangtai
	from 
		csx_b2b_wms.wms_task a 
		left join csx_b2b_wms.wms_container_detail b  on b.ticket_code = a.order_code
		left join csx_b2b_wms.wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
		left join csx_b2b_wms.wms_shipped_order_header d on a.source_code = d.order_code
	where 
		d.plan_date >= '${sdate}'  
		and d.plan_date<= '${edate}'  
		and a.`status`!='2' and a.task_type = '2'
		-- where a.warehouse_code = 'W0A7' and d.plan_date = '2021-09-29'  and a.`status`!='2' and a.task_type = '2'
	) as xx
where 
	xx.plan_date >= '${sdate}'  
	and xx.plan_date <= '${edate}' 
	${if(len(dc)==0,"","AND xx.warehouse_code in( '"+dc+"') ")}
	${if(len(split_group)==0,"","AND xx.split_group_name in( '"+split_group+"') ")}
group by 
	replace(xx.plan_date, '-', ''),xx.warehouse_code,xx.split_group_code,xx.split_group_name
order by 
	sdt,xx.warehouse_code,xx.split_group_name;
	
--==============================================================================================================================================================================

select 
	xx.chaidanfenzu as '拆单分组',
	count(*) as '总包裹数量',
	sum(case when xx.zhuangtai = '0' then 1 else 0 end) as 未拣包裹数,
	sum(case when xx.zhuangtai = '1' then 1 else 0 end) as 已拣包裹数,
	CONCAT(ROUND(((sum(case when xx.zhuangtai = '1' then 1 else 0 end))/count(*))*100,2),'%') as 拣货进度,
	sum(case when xx.touxianzhuangtai = '0' then 1 else 0 end) as 未投包裹数,
	sum(case when xx.touxianzhuangtai = '1' then 1 else 0 end) as 已投包裹数,
	CONCAT(ROUND(((sum(case when xx.touxianzhuangtai = '1' then 1 else 0 end))/count(*))*100,2),'%') as 投线进度
from 
	(
	select 
		c.split_group_name as chaidanfenzu,a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) AS touxianzhuangtai
	from  
		wms_task_picking a 
		LEFT JOIN wms_container_detail b  on b.ticket_code = a.order_code
		LEFT JOIN wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
	where 
		a.warehouse_code = 'W0A8' 
		AND a.plan_date = '2021-10-25'  
		and a.`status`='0' 
		and a.local_purchase_flag = '0'
	union all
	select 
		c.split_group_name as chaidanfenzu,a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) AS touxianzhuangtai
	from  
		wms_task_picking a 
		LEFT JOIN wms_container_detail b  on b.ticket_code = a.order_code
		LEFT JOIN wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
	where 
		a.warehouse_code = 'W0A8' 
		AND a.plan_date = '2021-10-25' 
		and a.picking_qty > '0' 
		and a.`status`='1' 
		and a.local_purchase_flag = '0'
	union all
	select 
		c.split_group_name as chaidanfenzu,a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) AS touxianzhuangtai
	from 
		wms_task a 
		LEFT JOIN wms_container_detail b  on b.ticket_code = a.order_code
		LEFT JOIN wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
		LEFT JOIN wms_shipped_order_header d on a.source_code = d.order_code
	where 
		a.warehouse_code = 'W0A8' 
		AND d.plan_date = '2021-10-25'  
		and a.`status`!='2' 
		and a.task_type in ('2','7')
	) as xx
GROUP BY 
	xx.chaidanfenzu
ORDER BY 
	xx.chaidanfenzu;	
	
	
