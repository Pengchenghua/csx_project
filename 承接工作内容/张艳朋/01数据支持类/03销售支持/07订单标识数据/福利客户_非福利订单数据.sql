insert overwrite directory '/tmp/zhangyanpeng/linshi_1127' row format delimited fields terminated by '\t' 

select
	a.sdt,
	a.province_name,
	a.city_name,
	a.dc_code,
	a.customer_no,
	a.customer_name,
	concat('a',a.order_no) as order_no,
	a.order_kind,
	a.goods_code,
	a.goods_name,
	a.unit,
	a.sales_qty,
	a.sales_value,
	a.profit,
	a.is_factory_goods_name,
	b.attribute,
	to_date(b.sign_time) as sign_time,
	b.first_category,
	b.second_category,
	b.third_category,
	b.source,
	b.customer_level,
	a.channel,
	a.channel_name
from 
	(
	select 
		sdt,province_name,city_name,dc_code,order_no,order_kind,is_factory_goods_name,channel,channel_name,
		goods_code,goods_name,unit,customer_no,customer_name,sales_value,sales_qty,profit
	from 
		csx_dw.dws_sale_r_d_customer_sale 
	where 
		sdt between '20201101' and '20201126'
		and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
		--and channel != '7' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and province_name not like '平台%'
		and (order_kind<>'WELFARE' or order_kind is null or order_kind='')
	)a  
	left join   --CRM客户信息取每月最后一天 剔除合伙人
		(
		select 
			customer_no,customer_name,attribute,attribute_code,is_parter,sign_time,first_category,second_category,third_category,source,archive_category,customer_nature,customer_level
		from 
			csx_dw.dws_crm_w_a_customer_m_v1 
		where 
			sdt ='20201126'
		group by
			customer_no,customer_name,attribute,attribute_code,is_parter,sign_time,first_category,second_category,third_category,source,archive_category,customer_nature,customer_level
		) as b on a.customer_no=b.customer_no
where
	b.attribute='福利客户'

		