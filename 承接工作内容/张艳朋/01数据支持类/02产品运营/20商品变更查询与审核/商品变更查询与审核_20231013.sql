-- 2022/4/18-5/15
#csx_b2b_master_data  10.0.74.154
select 
	l.code as '地点编码',pc.product_code as '商品编码',
	replace(pc.product_name,char(9),'') as '商品名称',
	pc.review_id as '申请单号',pc.review_status as '状态编码',
	case when pc.review_status ='10' then '暂存' 
		when pc.review_status ='20' then '待审核'
		when pc.review_status ='30' then '驳回'
		when pc.review_status ='40' then '审核通过'
		when pc.review_status ='50' then '丢弃'
	end as '状态名称',
	pc.create_time as '提交时间', 
	pc.submit_by as '提交人',
	replace(rd.submit_note,char(9),'') as '提交备注',
	replace(rd.review_note,char(9),'') as '审核备注',
	rd.flow_id as 'flow_id'
from 
	md_product_change_review_view pc 
	left join md_base_location l on pc.location_id = l.id 
	left join md_review_detail_info rd on pc.review_detail_id = rd.id
where 
	pc.review_type = 'PRODUCT_CHANGE_STATUS' 
	and date(pc.create_time)>='2023-10-09' 
	and date(pc.create_time)<='2023-10-13' 
	-- and pc.review_id='160103186994'
;

-- #csx_bsf_flow3   10.0.74.80
-- 2022/4/18-5/15
select 
	flow_id as 'flow_id',user_by as '审批人',result as '审批结果' -- ,remark as '审批意见'  
from 
	t_approve_log 
where 
	id in (
		select 
			max(a.id) 
		from 
			t_approve_log a 
			left join t_flow f on a.flow_id = f.id 
		where 
			f.model_name= 'PRODUCT_CHANGE_STATUS' 
			and date(a.create_time)>='2023-10-09' 
			and date(a.create_time)<='2023-10-13' 
			and a.node_show = '产品运营' 
			and result!='自动同意' 
			-- and f.id='24344911'
		group by a.flow_id
			)
;
-- ===================================================================================================================================================================
-- #csx_b2b_master_data  10.0.74.154
select 
	d.operation_by as '操作人',
	d.create_time as '操作时间',
	p.product_code as '商品编码',
	p.product_name as '商品名称',
	bl.code as '地点编码',
	JSON_EXTRACT(d.old,'$.product_status') as '变更前',
	JSON_EXTRACT(d.data,'$.product_status') as '变更后'
from 
	md_data_log d 
	left join md_product_location_info pl on d.pk = pl.id 
	left join md_product_info p on pl.product_id = p.id
	left join md_base_location bl on pl.location_id = bl.id
where 
	d.table_name = 'md_product_location_info'
	and (d.old like '%"product_status": "0"%' or d.old like '%"product_status": "7"%')   
	and (d.data like '%"product_status": "3"%' or d.data like '%"product_status": "6"%') 
	and d.type = 'UPDATE' 
	and (date(d.create_time) between '2022-04-18' and '2022-05-15' 
		or date(d.create_time) between '2023-04-18' and '2023-05-15' 
		or date(d.create_time) between '2023-03-18' and '2023-04-15')