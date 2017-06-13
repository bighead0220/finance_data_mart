/*
Author             :XMH
Function           :票据信息表
Load method        :INSERT
source table       :dw_sdata.bbs_001_draft_info,dw_sdata.bbs_001_customer_info,dw_sdata.bbs_001_union_bank,dw_sdata.bbs_001_union_bank
Destination Table  :f_agt_bill_info
Frequency          :D
Modify history list:Created by徐铭浩2016年5月4日10:05:55
                   :Modify  by lxz 20160614 根据变更记录添加where T.DRAFT_NUMBER is not null
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
*-------------------------------------------逻辑说明END------------------------------------------
*/

/*数据回退区*/
Delete /* +direct */ from  f_fdm.f_agt_bill_info
where etl_date='$TXDATE'::date
/*数据回退区end*/
;
/*数据处理区*/
insert into f_fdm.f_agt_bill_info
(
    Grp_Typ                      --组别          
    ,ETL_Date                    --数据日期      
    ,FLOW_ID                     --流水ID        
    ,Bill_Num                    --票号          
    ,Par_Amt                     --票面金额      
    ,Draw_Day                    --出票日        
    ,Due_Dt                      --到期日        
    ,Drawr_Cust_Id               --出票人客户编号
    ,Draw_Cust_Nm                --出票客户名称  
    ,Draw_Line_Id                --出票行编号    
    ,Acptr_Cust_Id               --承兑人客户编号
    ,Acpt_Line_Cust_Id           --承兑行客户编号
    ,Bill_Attr_Cd                --票据属性代码  
    ,Bill_Typ_Cd                 --票据类型代码  
    ,Cur_Cd                      --货币代码      
    ,Bill_Src_Cd                 --票据来源代码  
    ,Recvr_Cust_Id               --收款人客户编号
    ,Bill_Stat_Cd                --票据状态代码  
    ,Sys_Src                     --系统来源             
)
select 
      '1'                                         as GRP_TYp
      ,'$TXDATE'::date                   as etl_date
      ,coalesce(T.ID::numeric(80),0)              as FLOW_ID           
      ,coalesce(T.DRAFT_NUMBER,'')                as Bill_Num                                                                  
      ,coalesce(T.FACE_AMOUNT,0)                  as Par_Amt         
      ,case when T.REMIT_DATE is null then T.REMIT_DATE::date else T.REMIT_DATE::date end            as  Draw_Day                                                                  
      ,case when T.MATURITY_DATE is null then T.MATURITY_DATE::date else T.MATURITY_DATE::date end   as  Due_Dt                                                                   
      ,coalesce(T1.CUST_NO,'')                    as Drawr_Cust_Id                                                                   
       ,coalesce(T.REMITTER_NAME,'')              as Draw_Cust_Nm                                                            
       ,coalesce(T2.UBANK_NO,'')                  as Draw_Line_Id                                                                 
       ,coalesce(T.ACCEPTOR,'')                   as Acptr_Cust_Id                                                                  
       ,coalesce(T3.UBANK_NO,'')                  as Acpt_Line_Cust_Id                                                          
       ,coalesce(T.DRAFT_ATTR,'')                 as Bill_Attr_Cd                                                                   
       ,coalesce(T.DRAFT_TYPE,'')                 as Bill_Typ_Cd                                                      
       ,coalesce('156','')                        as Cur_Cd                                                                     
       ,coalesce(T.SRC_TYPE,'')                   as Bill_Src_Cd                                                                   
       ,coalesce(T.PAYEE_ID,0)                    as Recvr_Cust_Id                                                                      
       ,coalesce(T.STATUS,'')                     as Bill_Stat_Cd                                                                      
       ,'BBS'                                     as Sys_Src 
from dw_sdata.bbs_001_draft_info  T
left join   dw_sdata.bbs_001_customer_info T1
on T.REMITTER_ID=T1.ID and T.DRAFT_NUMBER is not null
AND T1.start_dt<='$TXDATE'::date and'$TXDATE'::date<T1.end_dt
Left Join dw_sdata.bbs_001_union_bank  T2
on T.REMITTER_BANK_ID=T2.ID
and T2.start_dt<='$TXDATE'::date and'$TXDATE'::date<T2.end_dt
left join  dw_sdata.bbs_001_union_bank  T3
on T.ACCEPTOR_BANK_ID=T3.ID
and T3.start_dt<='$TXDATE'::date and'$TXDATE'::date<T3.end_dt
where T.DRAFT_NUMBER is not null
and T.start_dt<='$TXDATE'::date and'$TXDATE'::date<T.end_dt
;
/*数据回退区end*/
commit;
                 
