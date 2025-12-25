select 
	a.region_name as `大区`,
	a.province_name as `省区`,
	b.performance_city_name as `城市`,
	a.employee_num as `员工工号`,
	a.employee_name as `员工姓名`,
	a.process_type_name as `工序`,
	count(distinct a.task_code) as `工作量`  
from 
    (select * 
    from csx_report.csx_report_wms_piece_price_details  
    where substr(piece_data,1,7)='2025-10' 
    ) a 
    left join 
    (select * 
    from csx_dim.csx_dim_shop 
    where sdt='current'
    ) b 
    on a.dc_code=b.shop_code 
where a.region_name='华东大区'
group by 
	a.region_name,
	a.province_name,
	b.performance_city_name,
	a.employee_num,
	a.employee_name,
	a.process_type_name 