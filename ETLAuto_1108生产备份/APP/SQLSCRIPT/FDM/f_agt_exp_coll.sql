/*
Author             :刘东燕
Function           :出口托收业务信息表
Load method        :INSERT
                   :dw_sdata.iss_001_ex_doccollinfo,dw_sdata.iss_001_bu_transactioninfo,dw_sdata.iss_001_PA_ORG,dw_sdata.iss_001_ex_doccolladdinfo              
Destination Table  :f_agt_exp_coll  出口托收业务信息表
Frequency          :D
Modify history list:Created by刘东燕2016年4月19日10:05:55
                   :Modify  by liudongyan 20160901 新增T2.use_flag='02'
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/
--月积数等临时表--
create local temporary table tt_f_agt_exp_coll_yjs  --月积数，年积数，月日均余额，年日均余额临时表
on commit preserve rows as 
select * 
from f_fdm.f_agt_exp_coll
where 1=2;
/*临时表创建区END*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_agt_exp_coll
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_agt_exp_coll
(
                Grp_Typ                                      --组别
               ,etl_date                                     --数据日期
               ,agmt_id                                      --协议编号
               ,cust_id                                      --客户编号
               ,org_num                                      --机构号
               ,cur_cd                                       --货币代码
               ,prod_cd                                      --产品代码
               ,coll_dt                                      --托收日期
               ,due_dt                                       --到期日期
               ,subj_cd                                      --科目代码
               ,coll_amt                                     --托收金额
               ,coll_bal                                     --托收余额
               ,coll_typ_cd                                  --托收类型代码
               ,agmt_stat_cd                                 --协议状态代码
               ,mth_accm                                     --月积数
               ,yr_accm                                      --年积数
               ,mth_day_avg_bal                              --月日均余额
               ,yr_day_avg_bal                               --年日均余额
               ,sys_src                                      --系统来源

)
SELECT
          '1'                                               as Grp_Typ
          ,'$TXDATE'::date                         as etl_date
          ,T.BIZNO                             as agmt_id
          ,T.DRAWERNO                                       as cust_id
         ,coalesce(T2.BRH_CODE,T1.TRANSACTORGNO)            as org_num
          
          ,NVL(T4.TGT_CD,'@'||T.DRAFTCUR)                   as cur_cd
          ,''                                             as prod_cd
          ,T.COLLDATE                                       as coll_dt
          ,T.PAYMENTDATE                                    as due_dt
          ,''                                             as subj_cd
          ,coalesce(T.DRAFTAMT,0)                           as coll_amt
          ,coalesce(T3.DRAFTBALANCEAMT,0)                   as coll_bal
          ,T.COLLTYPE                                       as coll_typ_cd
          ,(CASE 
                WHEN T.MATURITY IS NULL THEN '1' 
            ELSE '2' 
            END
            )                                               as agmt_stat_cd
          ,0.00                                             as mth_accm
          ,0.00                                             as yr_accm
          ,0.00                                             as mth_day_avg_bal
          ,0.00                                             as yr_day_avg_bal
          ,'ISS'                                            as sys_src

        
FROM  dw_sdata.iss_001_ex_doccollinfo                      AS T
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T4 --需转换代码表
ON  T.DRAFTCUR=T4.SRC_CD                       --源代码值相同
AND  T4.DATA_PLTF_SRC_TAB_NM = 'ISS_001_EX_DOCCOLLINFO' --数据平台源表主干名
AND  T4.Data_Pltf_Src_Fld_Nm ='DRAFTCUR'                --数据平台源字段名
LEFT JOIN dw_sdata.iss_001_bu_transactioninfo             AS T1
ON        T.TXNSERIALNO = T1.TXNSERIALNO
AND T1.start_dt<='$TXDATE'::date
AND  T1.end_dt>'$TXDATE'::date 
--LEFT JOIN dw_sdata.iss_001_PA_ORG                       AS T2
--ON         T1.TRANSACTORGNO = T2.ORGNO 
left join dw_sdata.ogs_000_tbl_new_old_rel as t2
on T1.TRANSACTORGNO = T2.BRH_SV_NEW_CODE 
and T2.sys_type='99700010000' 
and T2.use_flag='02'
AND T2.start_dt<='$TXDATE'::date
AND  T2.end_dt>'$TXDATE'::date
LEFT JOIN dw_sdata.iss_001_ex_doccolladdinfo           AS T3
ON         T.TXNSERIALNO = T3.TXNSERIALNO
AND T3.start_dt<='$TXDATE'::date
AND  T3.end_dt>'$TXDATE'::date
WHERE  T.start_dt<='$TXDATE'::date
AND  T.end_dt>'$TXDATE'::date
;
 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
 
insert into tt_f_agt_exp_coll_yjs
(
       agmt_id
      ,Mth_Accm 
      ,Yr_Accm 
      ,Mth_Day_Avg_Bal 
      ,Yr_Day_Avg_Bal 
)
select 
      t.agmt_id
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.coll_bal
            else t.coll_bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.coll_bal
            else t.coll_bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.coll_bal
            else t.coll_bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.coll_bal
           else t.coll_bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_agt_exp_coll     t
left join f_fdm.f_agt_exp_coll t1
on         t.agmt_id= t1.agmt_id
and    t1.etl_date='$TXDATE'::date-1  --modified by wyh 
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_agt_exp_coll   t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  tt_f_agt_exp_coll_yjs    t1
where t.agmt_id= t1.agmt_id
and   t.etl_date='$TXDATE'::date
;
/*数据处理区END*/
COMMIT;
