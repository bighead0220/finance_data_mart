/*
Author             :zhangwj
Function           :进口代收业务信息表
Load method        :
Source table       :dw_sdata.iss_001_im_icinfo,dw_sdata.iss_001_im_icpayment,dw_sdata.iss_001_bu_transactioninfo,dw_sdata.iss_001_PA_ORG
Destination Table  :f_fdm.f_agt_imp_clltn
Frequency          :D
Modify history list:Created by zhangwj at 2016-4-28 10:53 v1.0
                    Changed by zhangwj at 2016-5-24 10:06 v1.1   大数据贴源层表名修改，表为拉链表或流水表，与之保持一致
                    Changed by zhangwj at 2016-5-30 14:06 v1.2   解决数据总量验证存在的问题
                   changed by liudongyan at 20160910 修改T3映射规则及机构号的取数规则。
                              :Modify  by
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/
--月积数等临时表--
create local temporary table tt_f_agt_imp_clltn_temp_yjs  --月积数，年积数，月日均余额，年日均余额临时表
on commit preserve rows as 
select * 
from f_fdm.f_agt_imp_clltn 
where 1=2;
/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_agt_imp_clltn
where etl_date = '$TXDATE'
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_agt_imp_clltn
      (grp_typ                                       --组别
      ,etl_date                                      --数据日期
      ,agmt_id                                       --协议编号
      ,cust_id                                       --客户编号
      ,org_num                                       --机构号
      ,cur_cd                                        --货币代码
      ,prod_cd                                       --产品代码
      ,clltn_dt                                      --代收日期
      ,due_dt                                        --到期日期
      ,subj_cd                                       --科目代码
      ,draft_amt                                     --汇票金额
      ,curr_bal                                      --当前余额
      ,is_mtg_exchg                                  --是否押汇
      ,is_adv_money                                  --是否垫付
      ,is_multi_remtt                                --是否多次付汇
      ,is_ovrs_era_pay                               --是否海外代付
      ,agmt_stat_cd                                  --协议状态代码
      ,mth_accm                                      --月积数
      ,yr_accm                                       --年积数
      ,mth_day_avg_bal                               --月日均余额
      ,yr_day_avg_bal                                --年日均余额
      ,sys_src                                       --系统来源
      )
 select
       1                                                    as  grp_typ         --组别
       ,'$TXDATE'::date                            as  etl_date        --数据日期
       ,coalesce(T.ICNO,'')                                 as  agmt_id         --协议编号
       ,coalesce(T.DRAWEENO,'')                             as  cust_id         --客户编号
       ,COALESCE(T3.BRH_CODE,T2.TRANSACTORGNO)               as  org_num         --机构号
       ,NVL(T4.TGT_CD,'@'||T.DRAFTCURSIGN)                  as  cur_cd          --货币代码
       ,''                                   as  prod_cd         --映射暂空    --产品代码
       ,coalesce(T.DOCARRIVALDATE ,'$MAXDATE'::date)    as  clltn_dt        --代收日期
       ,coalesce(T.MATURITYDATE , '$MAXDATE'::date)     as  due_dt          --到期日期
       ,''                                   as  subj_cd         --映射暂空    --科目代码
       ,coalesce(T.DRAFTAMT ,0)                             as  draft_amt       --汇票金额
       ,coalesce(T.draftamt-T.paymentamt,0)                 as  curr_bal        --当前余额
       ,(CASE  
             WHEN T1.ISLOAN='Y' THEN 1 
         ELSE '0'
         END
          )                                                 as  is_mtg_exchg    --是否押汇
       ,(CASE  
            WHEN T1.ISPADDING='Y' THEN '1' 
         ELSE '0'
         END
         )                                                  as  is_adv_money    --是否垫付
       ,(case 
             when T1.ISMULTIPAYMENT='Y' then '1' 
         else '0' 
         end
         )                      as  is_multi_remtt  --是否多次付汇
       ,(CASE 
             WHEN T.INSTEADOFPAYMENT='Y' THEN '1'
         ELSE '0' 
         END 
         )                                                  as  is_ovrs_era_pay --是否海外代付
       ,(CASE 
              WHEN T.ISCLOSE='Y' THEN '2' 
         ELSE '1' 
         END
           )                                                as  agmt_stat_cd     --协议状态代码
       ,0                                    as  mth_accm         --映射未确定   --月积数
       ,0                                    as  yr_accm          --映射未确定   --年积数
       ,0                                    as  mth_day_avg_bal  --映射未确定   --月日均余额
       ,0                                    as  yr_day_avg_bal   --映射未确定   --年日均余额
       ,'ISS'                                               as  sys_src          --系统来源
 from  dw_sdata.iss_001_im_icinfo      T    --代收信息
left join  f_fdm.CD_RF_STD_CD_TRAN_REF T4 --需转换代码表
on  t.DRAFTCURSIGN=T4.SRC_CD                       --源代码值相同
and  T4.DATA_PLTF_SRC_TAB_NM = 'ISS_001_IM_ICINFO' --数据平台源表主干名
and  T4.Data_Pltf_Src_Fld_Nm ='DRAFTCURSIGN'                --数据平台源字段名
 left join
          (select T.ISLOAN,T.ICNO,T.ISPADDING,T.ISMULTIPAYMENT, ROW_NUMBER() OVER(PARTITION BY T.icno ) NUM ,T.START_DT,T.END_DT
            from dw_sdata.iss_001_im_icpayment T) T1
ON  T.ICNO =T1.ICNO 
AND T1.NUM=1
 and       T1.start_dt<='$TXDATE'::date
 and       T1.end_dt>'$TXDATE'::date
 left join dw_sdata.iss_001_bu_transactioninfo   T2       
 on        T2.TXNSERIALNO = T.TXNSERIALNO
 and       T2.start_dt<='$TXDATE'::date
 and       T2.end_dt>'$TXDATE'::date
 left join dw_sdata.ogs_000_tbl_new_old_rel   T3     
 on        T2.transactorgno=T3.brh_sv_new_code
and      T3.sys_type='99700010000'
and      T3.use_flag='02'
 and       T3.start_dt<='$TXDATE'::date
 and       T3.end_dt>'$TXDATE'::date
 where     T.start_dt<='$TXDATE'::date
 and       T.end_dt>'$TXDATE'::date
 ;
  /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
insert into tt_f_agt_imp_clltn_temp_yjs
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
            then t.curr_bal
            else t.curr_bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.curr_bal
            else t.curr_bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.curr_bal
            else t.curr_bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.curr_bal
           else t.curr_bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_agt_imp_clltn     t
left join  f_fdm.f_agt_imp_clltn t1
on        t.agmt_id=t1.agmt_id
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_agt_imp_clltn    t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  tt_f_agt_imp_clltn_temp_yjs   t1
where t.agmt_id=t1.agmt_id
and   t.etl_date='$TXDATE'::date
;

/*数据处理区END*/

 COMMIT;
