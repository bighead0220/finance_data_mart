/*
Author             :Liuxz
Function           :
Load method        :
Source table       : dw_sdata.pcs_006_tb_pub_security,dw_sdata.pcs_006_tb_lon_loan,dw_sdata.pcs_006_tb_csm_customer,dw_sdata.pcs_006_tb_pub_guarantee,dw_sdata.pcs_006_tb_lon_loan,dw_sdata.ccs_004_tb_con_subcontract
Destination Table  :f_fdm.f_loan_guar_contr
Frequency          :D
Modify history list:Created by Liuxz at 2016-04-21 12:48:15.753000
                   :Modify  by Liuxz 20160606 组别1添加T表过滤调价T.SECURITY_STATUS <>'5' （变更记录47）
                    modify by liuxz 20160621 修改组2担保类型代码的取数逻辑 (变更记录83)
                    modified by liuxz 20160715 担保类型代码 协议状态代码 代码转换
                     modified by liuxz 20160718 修改整体格式。修改临时表名称
                    modified by zhangliang 20160929   修改组1客户表拉链时间限制
*/


-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明
贷款担保合同信息
*/
-------------------------------------------逻辑说明END------------------------------------------
/*创建临时表区*/
create local temporary table if not exists tt_f_loan_guar_contr_tmp_1  --组别3的t1表
on commit preserve rows as
  SELECT   B.BIZ_LIMIT_CONT_NUM
          ,SUM(B.GUARANTY_AMT) AS GUARANTY_AMT --本次担保债权金额
          ,(SUM(B.GUARANTY_AMT)/SUM(CASE
                                            WHEN B.GUARANTY_TYPE_CD='1' AND  A.COLLATERAL_TYPE_CD='35' THEN B.GUARANTY_USE_AMT  --本次抵质押品占用价值
                                            WHEN B.GUARANTY_TYPE_CD='2' THEN B.GUARANTY_USE_AMT  --本次抵质押品占用价值
                                            ELSE B.GUARANTY_AMT   --本次担保债权金额
                                            END
                                            )
          ) *100 AS GUARANTY_RATE--担保比率
            FROM    dw_sdata.ccs_004_tb_grt_collateral A     --抵质押物
            INNER JOIN  dw_sdata.ccs_004_tb_grt_business_relation   B   --额度业务合同与担保品关联关系
                     ON A.GUARANTY_ID=B.GUARANTY_ID
                     AND B.VALID_IND='1' --是否有效=1有效　　
                     AND B.GUARANTY_RELATION_TYPE_CD='4' --担保关联类型代码=4从合同
                     AND B.START_DT<='$TXDATE'::date
                     AND B.END_DT>'$TXDATE'::date   
                     WHERE A.COLLATERAL_STATUS_CD='1'
                     AND A.START_DT<='$TXDATE'::date
                     AND A.END_DT>'$TXDATE'::date
                     GROUP BY B.BIZ_LIMIT_CONT_NUM
;
/*创建临时表区END*/

/*数据回退区*/

DELETE /* +direct */ from f_fdm.f_loan_guar_contr
where etl_date = '$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/
/*组别1*/
insert /* +direct */ into f_fdm.f_loan_guar_contr
(etl_date                                                                                  --数据日期
 ,grp_typ                                                                                  --组别
,guar_agmt_id                                                                              --担保协议编号
,cust_num                                                                                  --客户号
,cur_cd                                                                                    --货币代码
,loan_contr_agmt_id                                                                        --贷款合同协议编号
,guar_contr_st_dt                                                                          --担保合同起期
,guar_contr_stp_dt                                                                         --担保合同止期
,guar_amt                                                                                  --担保金额
,guar_typ_cd                                                                               --担保类型代码
,guar_ratio                                                                                --担保比率
,guartr_id                                                                                 --担保人编号
,guartr_nm                                                                                 --担保人名称
,guar_claim_amt                                                                            --担保债权金额
,agmt_stat_cd                                                                              --协议状态代码
,sys_src                                                                                   --系统来源
)
select  '$TXDATE'::date                                                         as etl_date             --数据日期
         ,'1'                                                                            as grp_typ              --组别
         ,coalesce(t.SECURITY_ID,'')||'-'||coalesce(t.SECURITY_CONTRACT_NO,'')           as guar_agmt_id         --担保协议编号
         ,coalesce(t2.CUS_NO,'')                                                         as cust_num             --客户号
         ,t.CURRENCY                                                                     as cur_cd               --货币代码
         ,coalesce(t1.LOAN_CONTRACT_NO,'')                                               as loan_contr_agmt_id   --贷款合同协议编号
         ,t.SECURITY_BEGIN_DATE                                                          as guar_contr_st_dt     --担保合同起期
         ,t.SECURITY_END_DATE                                                            as guar_contr_stp_dt    --担保合同止期
         ,t.SECURITY_AMOUNT                                                              as guar_amt             --担保金额
         ,coalesce(T3.TGT_CD,'@'||T.SECURITY_KIND)                                       as guar_typ_cd          --担保类型代码
         ,t.SECURITY_RATIO                                                               as guar_ratio           --担保比率
         ,''                                                                             as guartr_id            --担保人编号
         ,''                                                                             as guartr_nm            --担保人名称
         ,0.00                                                                           as guar_claim_amt       --担保债权金额
         ,t.SECURITY_STATUS                                                              as agmt_stat_cd         --协议状态代码
         ,'PCS'                                                                          as sys_src              --系统来源
from    dw_sdata.pcs_006_tb_pub_security t
left join dw_sdata.pcs_006_tb_lon_loan t1
on      t.app_id=t1.loan_id
AND     T1.START_DT<='$TXDATE'::date
AND     T1.END_DT>'$TXDATE'::date
left join dw_sdata.pcs_006_tb_csm_customer t2
on      t1.cus_id=t2.cus_id
AND     T2.START_DT<='$TXDATE'::date
AND     T2.END_DT>'$TXDATE'::date
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T3 --需转换代码表
ON      T.SECURITY_KIND=T3.SRC_CD                       --源代码值相同
AND     T3.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_PUB_SECURITY' --数据平台源表主干名
AND     T3.Data_Pltf_Src_Fld_Nm ='SECURITY_KIND'     
WHERE   T.SECURITY_STATUS <>'5'
AND     T.START_DT<='$TXDATE'::date
AND     T.END_DT>'$TXDATE'::date

;
/*组别2*/
insert /* +direct */ into f_fdm.f_loan_guar_contr
(etl_date                                                                                    --数据日期
,grp_typ                                                                                     --组别
,guar_agmt_id                                                                                --担保协议编号
,cust_num                                                                                    --客户号
,cur_cd                                                                                      --货币代码
,loan_contr_agmt_id                                                                          --贷款合同协议编号
,guar_contr_st_dt                                                                            --担保合同起期
,guar_contr_stp_dt                                                                           --担保合同止期
,guar_amt                                                                                    --担保金额
,guar_typ_cd                                                                                 --担保类型代码
,guar_ratio                                                                                  --担保比率
,guartr_id                                                                                   --担保人编号
,guartr_nm                                                                                   --担保人名称
,guar_claim_amt                                                                              --担保债权金额
,agmt_stat_cd                                                                                --协议状态代码
,sys_src                                                                                     --系统来源
)
select  '$TXDATE'::date                                                              as etl_date            --数据日期
        ,'2'                                                                                  as grp_typ             --组别
        ,coalesce(t.ENSURE_ID,'')||'-'||coalesce(t.GUARANTEE_CONTRACT_NUM,'')                 as guar_agmt_id        --担保协议编号
        ,coalesce(t2.CUS_NO,'')                                                               as cust_num            --客户号
        ,t.CURRENCY                                                                           as cur_cd              --货币代码
        ,coalesce(t1.LOAN_CONTRACT_NO,'')                                                     as loan_contr_agmt_id  --贷款合同协议编号
        ,t.GUARANTEE_BEGIN_DATE                                                               as guar_contr_st_dt    --担保合同起期
        ,t.GUARANTEE_MATURITY_DATE                                                            as guar_contr_stp_dt   --担保合同止期
        ,t.GUARANTEE_AMOUNT                                                                   as guar_amt            --担保金额
        ,'GUAR'                                                                               as guar_typ_cd         --担保类型代码
        ,0.00                                                                                 as guar_ratio          --担保比率
        ,coalesce(t2.CUS_NO,'')                                                               as guartr_id           --担保人编号
        ,coalesce(t2.CUS_NAME,'')                                                             as guartr_nm           --担保人名称
        ,0.00                                                                                 as guar_claim_amt      --担保债权金额
        ,coalesce(T3.TGT_CD,'@'||T.GUARANTEE_STATE)                                           as agmt_stat_cd        --协议状态代码
        ,'PCS'                                                                                as sys_src             --系统来源
from    dw_sdata.pcs_006_tb_pub_guarantee t
left join   dw_sdata.pcs_006_tb_lon_loan t1
on      t.app_id=t1.loan_id
AND     T1.START_DT<='$TXDATE'::date
AND     T1.END_DT>'$TXDATE'::date
left join dw_sdata.pcs_006_tb_csm_customer t2
on      t.GUARANTEE_ID=t2.CUS_ID
AND     T2.START_DT<='$TXDATE'::date
AND     T2.END_DT>'$TXDATE'::date
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T3 --需转换代码表
ON      T.GUARANTEE_STATE=T3.SRC_CD                       --源代码值相同
AND     T3.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_PUB_GUARANTEE' --数据平台源表主干名
AND     T3.Data_Pltf_Src_Fld_Nm ='GUARANTEE_STATE'  
WHERE   T.START_DT<='$TXDATE'::date
AND     T.END_DT>'$TXDATE'::date
;
/*组别3*/
insert /* +direct */ into f_fdm.f_loan_guar_contr
(etl_date                                                               --数据日期
,grp_typ                                                                --组别
,guar_agmt_id                                                           --担保协议编号
,cust_num                                                               --客户号
,cur_cd                                                                 --货币代码
,loan_contr_agmt_id                                                     --贷款合同协议编号
,guar_contr_st_dt                                                       --担保合同起期
,guar_contr_stp_dt                                                      --担保合同止期
,guar_amt                                                               --担保金额
,guar_typ_cd                                                            --担保类型代码
,guar_ratio                                                             --担保比率
,guartr_id                                                              --担保人编号
,guartr_nm                                                              --担保人名称
,guar_claim_amt                                                         --担保债权金额
,agmt_stat_cd                                                           --协议状态代码
,sys_src                                                                --系统来源
)
select  '$TXDATE'::date                                    as etl_date                --数据日期
        ,'3'                                                        as grp_typ                 --组别
        ,t.SUBCONTRACT_NUM                                          as guar_agmt_id            --担保协议编号
        ,t.CUSTOMER_NUM                                             as cust_num                --客户号
        ,t.CURRENCY_CD                                              as cur_cd                  --货币代码
        ,t.CONTRACT_NUM                                             as loan_contr_agmt_id      --贷款合同协议编号
        ,t.START_DATE                                               as guar_contr_st_dt        --担保合同起期
        ,t.EXPIRATION_DATE                                          as guar_contr_stp_dt       --担保合同止期
        ,t.SUB_CONTRACT_AMT                                         as guar_amt                --担保金额
        ,coalesce(t2.TGT_CD,'@'||t.SUBCONTRACT_TYPE_CD)             as guar_typ_cd             --担保类型代码
        ,t1.GUARANTY_RATE                                           as guar_ratio              --担保比率
        ,t.GUARANT_CUSTOMER_NUM                                     as guartr_id               --担保人编号
        ,t.warrantor_name                                           as guartr_nm               --担保人名称
        ,t1.GUARANTY_AMT                                            as guar_claim_amt          --担保债权金额
        ,coalesce(t3.TGT_CD,'@'||t.SUBCONTRACT_STATUS_CD)           as agmt_stat_cd            --协议状态代码
        ,'CCS'                                                      as sys_src                 --系统来源
from    dw_sdata.ccs_004_tb_con_subcontract t
inner join  tt_f_loan_guar_contr_tmp_1 T1
ON T.SUBCONTRACT_NUM=T1.BIZ_LIMIT_CONT_NUM
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T2 --需转换代码表
ON      T.SUBCONTRACT_TYPE_CD=T2.SRC_CD                       --源代码值相同
AND     T2.DATA_PLTF_SRC_TAB_NM = 'CCS_004_TB_CON_SUBCONTRACT' --数据平台源表主干名
AND     T2.Data_Pltf_Src_Fld_Nm ='SUBCONTRACT_TYPE_CD'  
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T3 --需转换代码表
ON      T.SUBCONTRACT_STATUS_CD=T3.SRC_CD                       --源代码值相同
AND     T3.DATA_PLTF_SRC_TAB_NM = 'CCS_004_TB_CON_SUBCONTRACT' --数据平台源表主干名
AND     T3.Data_Pltf_Src_Fld_Nm ='SUBCONTRACT_STATUS_CD' 
WHERE T.START_DT<='$TXDATE'::date
AND   T.END_DT>'$TXDATE'::date
;
/*数据处理区END*/
commit;
