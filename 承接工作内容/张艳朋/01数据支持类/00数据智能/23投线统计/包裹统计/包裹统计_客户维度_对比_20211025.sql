select 
	xx.fahuoquyu as '发货区域',
	xx.kehumingcheng as '客户名称',
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
		c.split_group_name as chaidanfenzu,a.picking_sort_number as fahuoquyu,a.sub_custom_name as kehumingcheng,a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) AS touxianzhuangtai
	from  
		csx_b2b_wms.wms_task_picking a 
		LEFT JOIN csx_b2b_wms.wms_container_detail b  on b.ticket_code = a.order_code
		LEFT JOIN csx_b2b_wms.wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
	where 
		a.warehouse_code = 'W0A8' 
		AND a.plan_date = '2021-10-25'  
		and a.`status`='0' 
		and a.local_purchase_flag = '0'
	union all
	select 
		c.split_group_name as chaidanfenzu,a.picking_sort_number as fahuoquyu,a.sub_custom_name as kehumingcheng,a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) AS touxianzhuangtai
	from  
		csx_b2b_wms.wms_task_picking a 
		LEFT JOIN csx_b2b_wms.wms_container_detail b  on b.ticket_code = a.order_code
		LEFT JOIN csx_b2b_wms.wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
	where 
		a.warehouse_code = 'W0A8' 
		AND a.plan_date = '2021-10-25' 
		and a.picking_qty > '0' 
		and a.`status`='1' 
		and a.local_purchase_flag = '0'
	union all
	select 
		c.split_group_name as chaidanfenzu,d.picking_sort_number as fahuoquyu,a.sub_custom_name as kehumingcheng,
		a.`status` as zhuangtai,
		(case when container_id is null then '0'  else '1' end) AS touxianzhuangtai
	from 
		csx_b2b_wms.wms_task a 
		LEFT JOIN csx_b2b_wms.wms_container_detail b  on b.ticket_code = a.order_code
		LEFT JOIN csx_b2b_wms.wms_shipped_order_item c on c.order_code = a.source_code and c.product_code = a.product_code
		LEFT JOIN csx_b2b_wms.wms_shipped_order_header d on a.source_code = d.order_code
	where 
		a.warehouse_code = 'W0A8' 
		AND d.plan_date = '2021-10-25'  
		and a.`status`!='2' 
		and a.task_type in ('2','7')
	) as xx
GROUP BY 
	xx.fahuoquyu,xx.kehumingcheng
ORDER BY 
	xx.fahuoquyu,xx.kehumingcheng;