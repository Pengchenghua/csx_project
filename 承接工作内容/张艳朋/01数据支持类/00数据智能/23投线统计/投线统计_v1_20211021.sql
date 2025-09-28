	select * from
	(
	select 
		a.warehouse_code,replace(a.plan_date,'-','') as sdt,a.update_by,count(*) as counts
	from 
		(
		select 
			a.warehouse_code,a.plan_date,b.container_code,c.update_by 
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
			a.warehouse_code,d.plan_date,b.container_code,c.update_by
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