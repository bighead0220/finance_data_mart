/*
Author             :魏银辉
Function           :资金业务拨备信息表
Destination Table  :
Load method        :INSERT
Source table       :f_cap_biz_provs_info 资金业务拨备信息表
Frequency          :D
Modify history list:Created by 魏银辉 
                    modified by wyh 20160912 修改机构代码
                    modified by zmx 20161009 修改t1
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/
/*临时表创建区END*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_cap_biz_provs_info
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_cap_biz_provs_info
(
		etl_date         --数据日期  
		,agmt_id         --协议编号  
		,cur_cd          --货币代码  
		,org_num         --机构号   
		,provs_dt        --计提日期  
		,due_dt          --到期日期  
		,Prin_Subj       --本金科目  
		,deval_prep_bal  --减值准备余额
		,sys_src         --系统来源  
)                    
SELECT 
		'$TXDATE'::date                     as etl_date        --数据日期    
		,t.deal_no                                   as agmt_id         --协议编号    
		,'156'                                       as cur_cd          --货币代码    
		,coalesce(T_org_2.name,'')                                  as org_num         --机构号     
		,to_date(T.settle_dt,'YYYYMMDD')             as provs_dt        --计提日期    
		,to_date(T.cur_mat_dt,'YYYYMMDD')            as due_dt          --到期日期    
		,coalesce(T1.p_prin_acc,'')                  as Prin_Subj       --本金科目    
		,abs(coalesce(T1.p_prin_amount,0))           as deval_prep_bal  --减值准备余额  
		,'COS'                                       as sys_src         --系统来源    
from dw_sdata.cos_000_deals T --交易信息主表
inner join 
(select       deal_no
                --1520
                ,max(case when map_code like '1520%'
                          then map_code end)    as p_prin_acc  --本金科目
                ,sum(case when map_code like '1520%'
                          then amount else 0 end)   as p_prin_amount --名义本金              
     from      dw_sdata.cos_000_qta_gl_accounting --账务信息
     where     ret_code='000000'  --会计处理平台处理成功
       and     map_code like '1520%'  --减值准备
       and     gl_date<='$TXDATE'::date
    group by   deal_no
) t1
on T1.DEAL_NO=T.DEAL_NO
left join dw_sdata.cos_000_bustruct T_org_1
on t.entity = T_org_1.thekey
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date
left join dw_sdata.cos_000_anacode T_org_2
on T_org_1.analyse04 = T_org_2.thekey 
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date                  --modified 20160912
where t.start_dt <= '$TXDATE'::date
and t.end_dt >'$TXDATE'::date
and t.entity<>'NONE'
; 


/*数据处理区END*/
COMMIT;

