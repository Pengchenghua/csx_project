select 
	* 
from
	(
	select 
		a.warehouse_code,replace(a.plan_date,'-','') as sdt,a.update_by,count(*) as counts,
		sum(case when a.shifouzidong ='0' then '1' else 0 end) as touxian_normal,
		sum(case when a.shifouzidong ='1' then '1' else 0 end) as touxian_auto
	from 
		(
		select 
			a.warehouse_code,a.plan_date,b.container_code,c.update_by,
			(case when LEFT(b.container_code,2) ='ZD' then 1 else 0 end) as shifouzidong
		from 
			csx_b2b_wms.wms_task_picking a 
			LEFT JOIN csx_b2b_wms.wms_container_detail b on a.order_code = b.ticket_code
			LEFT JOIN csx_b2b_wms.wms_task_picking_log c on a.order_code = c.order_code and c.operate_type = '10'
		where 
			a.plan_date >='${sdate}'  and a.plan_date<='${edate}'
			${if(len(dc)==0,"","AND a.warehouse_code in( '"+dc+"') ")}
			and a.`status` = '1' 
			and  b.container_code is not NULL
		UNION ALL
		select 
			a.warehouse_code,d.plan_date,b.container_code,c.update_by,
			(case when LEFT(b.container_code,2) ='ZD' then 1 else 0 end) as shifouzidong
		from 
			csx_b2b_wms.wms_task a 
			LEFT JOIN csx_b2b_wms.wms_container_detail b  on b.ticket_code = a.order_code
			LEFT JOIN csx_b2b_wms.wms_shipped_order_header d on a.source_code = d.order_code
			LEFT JOIN csx_b2b_wms.wms_task_log c on c.order_code = a.order_code and c.operate_type = '6'
		where 
			d.plan_date >='${sdate}' and d.plan_date <='${edate}'
			${if(len(dc)==0,"","AND a.warehouse_code in( '"+dc+"') ")}
			and a.`status`='1' 
			and a.task_type = '2' 
			and b.container_code is not null
		) as a
	GROUP BY 
		a.warehouse_code,a.plan_date,a.update_by
	) as a
ORDER BY 
	a.warehouse_code,a.sdt,a.counts DESC;
	
	
	
--select 
--	xx.warehouse_code,
--	xx.plan_date,
--	xx.update_by,
--	count(*),
--	sum(case when xx.shifouzidong ='0' then '1' else 0 end) as 正常投线,
--	sum(case when xx.shifouzidong ='1' then '1' else 0 end) as 自动投线
--from 
--	(
--	select 
--		a.warehouse_code,a.plan_date,b.container_code,c.update_by,
--		(case when LEFT(b.container_code,2) ='ZD' then 1 else 0 end) as shifouzidong
--	from 
--		wms_task_picking a 
--		LEFT JOIN wms_container_detail b on a.order_code = b.ticket_code
--		LEFT JOIN wms_task_picking_log c on a.order_code = c.order_code and c.operate_type = '10'
--	where 
--		a.plan_date >= '2021-10-07' and a.plan_date <= '2021-10-20'
--		and a.warehouse_code in ('W0A5','W0R9','W0N0','W0BK','W0A7','W0X2','W0Z9','W0Q2','W0Q9','W0P8','W0A3','W0A2','W0A6')
--		and a.`status` = '1' and  b.container_code is not NULL
--	UNION ALL
--	select 
--		a.warehouse_code,d.plan_date,b.container_code,c.update_by,
--		(case when LEFT(b.container_code,2) ='ZD' then 1 else 0 end) as shifouzidong
--	from 
--		wms_task a 
--		LEFT JOIN wms_container_detail b  on b.ticket_code = a.order_code
--		LEFT JOIN wms_shipped_order_header d on a.source_code = d.order_code
--		LEFT JOIN wms_task_log c on c.order_code = a.order_code and c.operate_type = '6'
--	where 
--		a.warehouse_code IN ('W0A5','W0R9','W0N0','W0BK','W0A7','W0X2','W0Z9','W0Q2','W0Q9','W0P8','W0A3','W0A2','W0A6')
--		AND d.plan_date >= '2021-10-07'  and d.plan_date <= '2021-10-20'
--		and a.`status`='1' and a.task_type = '2' 
--		and b.container_code is not null
--	) as xx
--GROUP BY 
--	xx.warehouse_code,xx.plan_date,xx.update_by;