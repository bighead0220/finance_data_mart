/*
Author             :zmx
Function           :保险公司基本信息表 
Load method        :
Source table       :ICS_001_SMCTL_INSU_CODE
Destination Table  :f_cust_insu   
Frequency          :
Modify history list:Created by Liuxz at 2016-08-11 15:23:56
                   :modify by zmx at 2016-8-25 (添加拉链日期)
*/


-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明
保险代销账户信息表
*/
-------------------------------------------逻辑说明END------------------------------------------
/*数据回退区*/
DELETE FROM f_fdm.f_cust_insu
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
insert into f_fdm.f_cust_insu
        (grp_typ                                                     --组别
        ,etl_date                                                    --数据日期
        ,insu_corp_cd                                                --保险公司代码
        ,insu_corp_typ                                               --保险公司类型
        ,online_ind                                                  --联机标志
        ,bat_tx_stl_mode                                             --批量交易结算方式
        ,cn_comnt                                                    --中文说明
        ,cn_shtn                                                     --中文简写
        ,en_nm                                                       --英文名称
        ,en_abbr                                                     --英文缩写
        ,corp_addr                                                   --公司地址
        ,zip_cd                                                      --邮政编码
        ,corp_tel                                                    --公司电话
        ,corp_fax                                                    --公司传真
        ,eml_addr                                                    --电子邮件地址
        ,rgst_cap                                                    --注册资本金
        ,corp_rat                                                    --公司评级
        ,divid_corp_qty                                              --分公司数量
        ,co_form                                                     --合作形式
        ,contcr                                                      --联系人
        ,contcr_tel                                                  --联系人电话
        ,sys_src                                                     --系统来源
        )
select   1                                                           as   grp_typ              --组别          
        ,'$TXDATE'::date                                    as   etl_date              --数据日期        
        ,INSU_CODE                                                   as   insu_corp_cd         --保险公司代码      
        ,INSU_TYPE                                                   as   insu_corp_typ        --保险公司类型      
        ,ONLINE_FLAG                                                 as   online_ind           --联机标志        
        ,POL_BAL_MODE                                                as   bat_tx_stl_mode      --批量交易结算方式    
        ,INSU_CH_NAME                                                as   cn_comnt             --中文说明        
        ,INSU_CH_AB                                                  as   cn_shtn              --中文简写        
        ,INSU_EN_NAME                                                as   en_nm                --英文名称        
        ,INSU_EN_AB                                                  as   en_abbr              --英文缩写        
        ,INSU_COMP_ADDR                                              as   corp_addr            --公司地址        
        ,INSU_PSCD                                                   as   zip_cd               --邮政编码        
        ,INSU_TEL                                                    as   corp_tel             --公司电话        
        ,INSU_FAX                                                    as   corp_fax             --公司传真        
        ,INSU_EMAIL                                                  as   eml_addr             --电子邮件地址      
        ,INSU_REG_BAL                                                as   rgst_cap             --注册资本金       
        ,INSU_LEVEL                                                  as   corp_rat             --公司评级        
        ,INSU_SUB_NUM                                                as   divid_corp_qty       --分公司数量       
        ,INSU_COOP_FORM                                              as   co_form              --合作形式        
        ,INSU_LINKMAN                                                as   contcr					     --联系人         
        ,INSU_LINKMAN_TEL                                            as   contcr_tel           --联系人电话       
        ,'ICS'                                                       as   sys_src              --系统来源        
                                                                                                                                                
from    dw_sdata.ICS_001_SMCTL_INSU_CODE T
where t.start_dt <= '$TXDATE'::date  
  and t.end_dt > '$TXDATE'::date  ;
/*数据处理区END*/
commit;
