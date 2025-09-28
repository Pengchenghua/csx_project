-- csx_b2b_scm.scm_code_confirm 采购发言-黄钊
select c.confirm_no,
       c.submit_time,
       c.process_time,
       c.confirm_status,
			 c.location_code, c.location_name,
       CASE WHEN c.confirm_status = 1 THEN '待处理' WHEN c.confirm_status = 2 THEN '已处理' ELSE '已取消' END,
       count(if(m.message_type = 1, 1, null)) as "采购发言次数",
       count(if(m.message_type = 2, 1, null)) as "接单发言次数"	 

from csx_b2b_scm.scm_code_confirm c,
     csx_b2b_scm.scm_code_confirm_chat_message m
where c.confirm_no = m.confirm_code
  and c.create_time >= '2023-12-18 00:00:00'
  and c.create_time < '2023-12-23 00:00:00'
group by c.confirm_no;