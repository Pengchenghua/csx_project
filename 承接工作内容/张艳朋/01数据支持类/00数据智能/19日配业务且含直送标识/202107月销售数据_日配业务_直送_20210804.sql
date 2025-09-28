insert overwrite directory '/tmp/zhangyanpeng/20210804_linshi_1' row format delimited fields terminated by '\t' 

	select 
		*
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20210701' and '20210731'
		--and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and logistics_mode_code='1' -- 物流模式编码(1.直送 2.配送)
