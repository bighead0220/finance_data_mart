/*
Author             :刘东燕
Function           :同业客户基本信息表
Load method        :INSERT
Source table       :dw_sdata.ecf_002_t01_cust_info_T  同业客户基本信息               
Destination Table  :f_cust_ibank  同业客户基本信息表
Frequency          :D
Modify history list:Created by刘东燕2016年4月19日10:05:55
                   :Modified  by wyh at 20160825 修改主表逻辑 
				   modified by wyh at 20161014 last_updated_ts 替换为 updated_ts
				            modified by wyh at 20161019   修改ROW_NUMBER逻辑:order by UPDATED_TS desc,last_updated_ts desc
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_cust_ibank
where etl_date='$TXDATE':: date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_cust_ibank
(
                 Grp_Typ                                 --组别
                ,ETL_Date                                --数据日期
                ,Cust_Num                                --客户号
                ,Ibank_Cust_Legl_Nm                      --同业客户法定名称
                ,Is_Crdt_Cust_Ind                        --是否授信客户标志
                ,Is_Lpr_Ind                              --是否法人标志
                ,Cust_Typ_Cd                             --客户类型代码
                ,Fin_Lics_Id                             --金融许可证编号
                ,Org_Org_Cd                              --组织机构代码
                ,Inds_Typ_Cd                             --行业类型代码
                ,Cert_Typ_Cd	                         --证件类型代码
                ,Cert_Num                                 --证件号码
                ,Sys_Src                                 --系统来源
) 
SELECT
         '1'                                             as Grp_Typ   
         ,'$TXDATE':: date                      as ETL_Date 
         ,coalesce(T.ECIF_CUST_NO,'')                    as Cust_Num 
         ,coalesce(T.PARTY_NAME,'')                      as Ibank_Cust_Legl_Nm 
         ,coalesce(T.CREDIT_FLAG,'')                                  as  CREDIT_FLAG 
         ,coalesce(T.LEGAL_FLAG,'')                                   as LEGAL_FLAG               
         ,coalesce(substr(T.CUSTOMER_TYPE_CD,5),'')                   as Cust_Typ_Cd
         ,coalesce(T.FIN_LIC_NO,'')                      as Fin_Lics_Id
         ,coalesce(T.COMPANY_NO,'')                      as Org_Org_Cd
         ,coalesce(substr(T.INDUSTRY_TYPE,5),'')                      as Inds_Typ_Cd
         ,NVL(T4.TGT_CD,'@'||substr(T.CERT_TYPE,5))      as Cert_Typ_Cd    
         ,coalesce(CERT_NO,'')                           as Cert_Num
         ,'ECF'                                          as Sys_Src
        
FROM     (select *,row_number() over(partition by ECIF_CUST_NO  order by updated_ts desc ,last_updated_ts desc) as num  --modified by wyh at 20160825   --modified by wyh at 20161019   
from  dw_sdata.ecf_002_t01_cust_info_T
where --UPDATED_TS = '99991231 00:00:00:000000'
--and     
START_DT <= '$TXDATE'::date
and     end_dt  > '$TXDATE'::date
)  t
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T4 --需转换代码表
ON         substr(T.CERT_TYPE,5)=T4.SRC_CD                     --源代码值相同                        
AND        T4.DATA_PLTF_SRC_TAB_NM = 'ECF_002_T01_CUST_INFO_T' --数据平台源表主干名
AND        T4.Data_Pltf_Src_Fld_Nm ='CERT_TYPE'                --数据平台源字段名
WHERE t.num = 1 --modified by wyh at 20160825
--     updated_ts='99991231 00:00:00:000000'
--AND       T.start_dt<='$TXDATE':: date
--AND       T.end_dt>'$TXDATE':: date
;
 
/*数据处理区END*/
COMMIT;

