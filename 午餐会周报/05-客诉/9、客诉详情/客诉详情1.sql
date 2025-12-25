明细增加：
1、电子称有无分拣记录-有，取货判责物流；无，缺货判责采购；是否有分拣记录，是，否；根据拣货数量判断
2、分拣人员
3、有无物流投线记录-有，缺货判责物流；无，缺货判责采购 ；是否有投线记录，是，否；根据投线数量判断
4、投线人员;
5、是否有库存（含子母码）库存与可用库存；待定；



select * from csx_report.csx_report_wms_task_piecework_detail_di where sdt>='${sdate}' and sdt<='${edate}' ${if(len(dc)==0,"","and dc_code in ('"+replace(dc,",","','")+"')")}	


-- 电子称任务表
select * from csx_dws.csx_dws_wms_task_picking_di 
where order_code ='TK240606167202';

select * from csx_report.csx_report_wms_task_piecework_detail_di 
where sdt>='${sdate}' and sdt<='${edate}';

with
SELECT 
    sale_order_no,
    goods_code,
    MAX(COALESCE(pick_employee_num, '')) AS pick_employee_num,
    MAX(COALESCE(pick_by, '')) AS pick_by,
    MAX(COALESCE(touxian_employee_num, '')) AS touxian_employee_num,
    MAX(COALESCE(touxian_by, '')) AS touxian_by,
    SUM(picking_qty) AS picking_qty,
    SUM(touxian_qty) AS touxian_qty,
    case when SUM(picking_qty) =0 then '否' else '是' end as picking_type,
	case when SUM(touxian_qty) =0 then '否' else '是' end as touxian_type	
FROM csx_report.csx_report_wms_task_piecework_detail_di
WHERE sdt >= '20250601' and dc_code='W0A3'
GROUP BY 
    sale_order_no,
    goods_code