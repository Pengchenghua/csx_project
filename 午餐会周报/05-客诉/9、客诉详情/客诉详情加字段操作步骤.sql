客诉详情：增加字段的话
csx_analyse_fr_oms_complaint_detail_new_di ，1、表增加字段√，2、改逻辑	
csx_analyse_fr_oms_complaint_detail_new_di_real，1、改逻辑 
csx_analyse_fr_oms_complaint_detail_new_di_2mysql，1、表增加字段
mysql_增加字段
job_hive2mysql_csx_analyse_fr_oms_complaint_detail_new_di ，加新增字段的表对应关系

dc_goods as 
 -- 彩食鲜地点商品信息
 (select 
	dc_code,
	goods_code,
	regionalized_goods_name,
	goods_status_name
from csx_dim.csx_dim_basic_dc_goods
where sdt = 'current'
)





-- 责任人一人一行:JSON数组转换为了结构化的表格数据

SELECT 
    a.*,
    get_json_object(person_json, '$.userId') as `用户ID`,
    get_json_object(person_json, '$.userName') as `责任人姓名`,
    get_json_object(person_json, '$.userNumber') as `工号`,
    get_json_object(person_json, '$.personType') as `人员类型`
FROM csx_dws.csx_dws_oms_complaint_detail_di a
LATERAL VIEW posexplode(
    split(
        regexp_replace(
            regexp_replace(responsible_person, '^\\[|\\]$', ''),
            '\\}\\,\\s*\\{',
            '\\}\\|\\|\\{'
        ),
        '\\|\\|'
    )
) persons as pos, person_json
WHERE a.responsible_person is not null;











-- 责任人JSON数组中的信息提取并格式化为工号/姓名的形式

SELECT 
    complaint_code,
    concat_ws(', ', 
        collect_list(
            concat(
                get_json_object(person_json, '$.userNumber'), 
                '/', 
                get_json_object(person_json, '$.userName')
            )
        )
    ) as responsible_person_formatted
FROM csx_dws.csx_dws_oms_complaint_detail_di
LATERAL VIEW explode(
    split(
        regexp_replace(
            regexp_replace(responsible_person, '^\\[|\\]$', ''),
            '\\}\\,\\s*\\{',
            '\\}\\#\\{'
        ),
        '\\#'
    )
) persons as person_json
GROUP BY complaint_code;