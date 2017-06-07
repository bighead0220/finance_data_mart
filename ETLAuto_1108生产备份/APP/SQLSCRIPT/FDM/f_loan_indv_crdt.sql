/*
Author             :Liuxz
Function           : 个人授信额度信息
Load method        :
Source table       :dw_sdata.pcs_006_tb_lin_line_agreement,dw_sdata.pcs_006_tb_lin_line,dw_sdata.pcs_006_tb_csm_customer,dw_sdata.pcs_006_tb_lin_org_manage

Destination Table  :f_fdm.f_loan_indv_crdt
Frequency          :D
Modify history list:Created by Liuxz at 2016-04-22 11:22:14.136000
                   :Modify  by Liuxz at 2016-5-25 修改贴源层表名
                    Modify Lxz 20160616 删除 申请业务种类ID 字段 （变更记录69）
                    modified by Liuxz 20160630 额度状态代码 主要担保方式代码  转换开发
                    modified by liuxz 20160718 修改整体格式
                     modified by liuxz 20160819 增加coalesce
*/
 

-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明

*/
-------------------------------------------逻辑说明END------------------------------------------
/*临时表创建区*/
/*临时表创建区END*/

/*数据回退区*/

DELETE /* +direct */ from f_fdm.f_loan_indv_crdt
where etl_date = '$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/
/*组别1*/ 
insert /* +direct */ into f_fdm.f_loan_indv_crdt
        (Grp_Typ                                                --组别
        ,etl_date                                               --数据日期
        ,crdt_lmt_id                                            --授信额度编号
        ,cust_num                                               --客户号
        ,belg_org_num                                           --所属机构号
        ,cur_cd                                                 --货币代码
        ,prod_cd                                                --产品代码
        ,is_can_cir_ind                                         --是否可循环标志
        ,efft_dt                                                --生效日期
        ,invldtn_dt                                             --失效日期
        ,crdt_amt                                               --授信金额
        ,aval_lmt                                               --可用额度
        ,frz_lmt                                                --冻结额度
        ,lmt_stat_cd                                            --额度状态代码
        ,guar_mode_cd                                           --担保方式代码
        ,majr_guar_mode_cd                                      --主要担保方式代码
        ,draw_clos_dt_prd                                       --提款截止日期
        ,sys_src                                                --系统来源
        )
select  '1'                                                                as Grp_Typ                  --组别
        ,'$TXDATE'::date                                          as etl_date                 --数据日期
        ,coalesce(T.LINE_ID,'')||'-'||coalesce(T.CONTRACT_NUM,'')          as crdt_lmt_id              --授信额度编号
        ,coalesce(T2.CUS_NO,'')                                            as cust_num                 --客户号
        ,coalesce(T3.CUR_ACC_ORG_ID,'')                                    as belg_org_num             --所属机构号
        ,T1.CURRENCY                                                       as cur_cd                   --货币代码
        ,T1.APP_OP_ID                                                      as prod_cd                  --产品代码
        ,T1.CYCLE_FLAG                                                     as is_can_cir_ind           --是否可循环标志
        ,T1.LINE_BEGIN_DATE                                                 as efft_dt                  --生效日期
        ,T1.LINE_MATURE_DATE                                                as invldtn_dt               --失效日期
        ,T1.LINE_AMOUNT                                                 as crdt_amt                 --授信金额
        ,(CASE
                WHEN T1.LINE_STATUS='1' THEN T1.LINE_BALANCE
                ELSE 0.00
        END
        )                                                                  as aval_lmt                 --可用额度
        ,(CASE
                WHEN T1.LINE_STATUS='4' THEN T1.LINE_BALANCE
                ELSE 0.00
        END
        )                                                                  as frz_lmt                  --冻结额度
        ,coalesce(T4.TGT_CD,'@'||T1.LINE_STATUS)                           as lmt_stat_cd              --额度状态代码
        ,T1.COM_SECURITY_KIND                                              as guar_mode_cd             --担保方式代码
        ,coalesce(T5.TGT_CD,'@'||T1.SECURITY_KIND)                         as majr_guar_mode_cd        --主要担保方式代码
        ,T1.LINE_AJUSTEND_DATE                                              as draw_clos_dt_prd         --提款截止日期
        ,'PCS'                                                             as sys_src                  --系统来源
from    dw_sdata.pcs_006_tb_lin_line_agreement  T
inner join dw_sdata.pcs_006_tb_lin_line    T1
on        T.line_id=T1.line_id
AND       T1.START_DT<='$TXDATE'::date 
and       T1.END_DT>'$TXDATE'::date 
left join dw_sdata.pcs_006_tb_csm_customer      T2
on        T1.CUS_ID=T2.CUS_ID
AND       T2.START_DT<='$TXDATE'::date 
and       T2.END_DT>'$TXDATE'::date
left join dw_sdata.pcs_006_tb_lin_org_manage    T3
on        T.LINE_ID=T3.LINE_ID
AND       T3.START_DT<='$TXDATE'::date 
and       T3.END_DT>'$TXDATE'::date 
left join f_fdm.CD_RF_STD_CD_TRAN_REF T4
on       T1.LINE_STATUS=T4.SRC_CD
and      T4.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_LIN_LINE' 
and      T4.Data_Pltf_Src_Fld_Nm ='LINE_STATUS'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T5
on       T1.SECURITY_KIND=T5.SRC_CD
and      T5.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_LIN_LINE' 
and      T5.Data_Pltf_Src_Fld_Nm ='SECURITY_KIND'
where     T.START_DT<='$TXDATE'::date 
and       T.END_DT>'$TXDATE'::date 
;
/*组别2*/
insert /* +direct */ into f_fdm.f_loan_indv_crdt
        (Grp_Typ                                                         --组别
        ,etl_date                                                        --数据日期
        ,crdt_lmt_id                                                     --授信额度编号
        ,cust_num                                                        --客户号
        ,belg_org_num                                                    --所属机构号
        ,cur_cd                                                          --货币代码
        ,prod_cd                                                         --产品代码
        ,is_can_cir_ind                                                  --是否可循环标志
        ,efft_dt                                                         --生效日期
        ,invldtn_dt                                                      --失效日期
        ,crdt_amt                                                        --授信金额
        ,aval_lmt                                                        --可用额度
        ,frz_lmt                                                         --冻结额度
        ,lmt_stat_cd                                                     --额度状态代码
        ,guar_mode_cd                                                    --担保方式代码
        ,majr_guar_mode_cd                                               --主要担保方式代码
        ,draw_clos_dt_prd                                                --提款截止日期
        ,sys_src                                                         --系统来源
        )
select  '2'                                                                         as Grp_Typ                  --组别
        ,'$TXDATE'::date                                                   as etl_date                 --数据日期
        ,coalesce(T1.LINE_ID,'')||'-'||coalesce(T.CONTRACT_NUM,'')                   as crdt_lmt_id              --授信额度编号
        ,coalesce(T2.CUS_NO,'')                                                     as cust_num                 --客户号
        ,coalesce(T3.CUR_ACC_ORG_ID,'')                                                          as belg_org_num             --所属机构号
        ,T1.CURRENCY                                                                as cur_cd                   --货币代码
        ,T1.APP_OP_ID                                                               as prod_cd                  --产品代码
        ,T1.CYCLE_FLAG                                                              as is_can_cir_ind           --是否可循环标志
        ,T1.LINE_BEGIN_DATE                                                          as efft_dt                  --生效日期
        ,T1.LINE_MATURE_DATE                                                         as invldtn_dt               --失效日期
        ,T1.LINE_AMOUNT                                                          as crdt_amt                 --授信金额
        ,(CASE
                WHEN T1.LINE_STATUS='1' THEN T1.LINE_BALANCE
                ELSE 0.00
        END
        )                                                                           as aval_lmt                 --可用额度
        ,(CASE
                WHEN T1.LINE_STATUS='4' THEN T1.LINE_BALANCE
                ELSE 0.00
        END
        )                                                                           as frz_lmt                  --冻结额度
        ,coalesce(T4.TGT_CD,'@'||T1.LINE_STATUS)                                    as lmt_stat_cd              --额度状态代码
        ,T1.COM_SECURITY_KIND                                                       as guar_mode_cd             --担保方式代码
        ,coalesce(T5.TGT_CD,'@'||T1.SECURITY_KIND)                                  as majr_guar_mode_cd        --主要担保方式代码
        ,T1.LINE_AJUSTEND_DATE                                                       as draw_clos_dt_prd         --提款截止日期
        ,'PCS'                                                                      as sys_src                  --系统来源
from    dw_sdata.pcs_006_tb_lin_line_agreement   T
inner join dw_sdata.pcs_006_tb_lin_line    T1
on        T.line_id=T1.GROUP_LINE_ID
AND       T1.APP_OP_ID = '300202'
AND       T1.VIRTUAL_FLAG<> '0'  
AND       T1.START_DT<='$TXDATE'::date 
and       T1.END_DT>'$TXDATE'::date 
left join dw_sdata.pcs_006_tb_csm_customer      T2
on        T1.CUS_ID=T2.CUS_ID
AND       T2.START_DT<='$TXDATE'::date 
AND       T2.END_DT>'$TXDATE'::date 
left join dw_sdata.pcs_006_tb_lin_org_manage    T3
on       T1.LINE_ID=T3.LINE_ID
AND      T3.START_DT<='$TXDATE'::date 
AND      T3.END_DT>'$TXDATE'::date 
left join f_fdm.CD_RF_STD_CD_TRAN_REF T4
on       T1.LINE_STATUS=T4.SRC_CD
and      T4.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_LIN_LINE' 
and      T4.Data_Pltf_Src_Fld_Nm ='LINE_STATUS'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T5
on       T1.SECURITY_KIND=T5.SRC_CD
and      T5.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_LIN_LINE' 
and      T5.Data_Pltf_Src_Fld_Nm ='SECURITY_KIND'
where    T.START_DT<='$TXDATE'::date 
and      T.END_DT>'$TXDATE'::date 
;
/*数据处理区END*/
commit
;
