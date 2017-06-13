/*
Author             :Liuxz
Function           :
Load method        :
Source table       :dw_sdata.acc_003_t_acc_assets_ledger,dw_sdata.BBS_TBL_ACC_INFO,dw_sdata.bbs_001_draft_info,dw_sdata.bbs_001_buy_details,dw_sdata.bbs_001_buy_contract,dw_sdata.bbs_001_customer_info,dw_sdata.bbs_001_branch_info,dw_sdata.BBS_DRAFT_CLASSIFICATION,dw_sdata.acc_003_t_accdata_last_item_no,dw_sdata.acc_003_t_accdata_last_item_no 
Destination Table  :f_fdm.f_agt_bill_discount
Frequency          :D
Modify history list:Created by Liuxz at 2016-04-27 10:07:30.260000
                   :Modify  by Liuxz 2016-05-25 更改贴源层表名
                   :Modify  by Liuxz 2016-05-27 更新T2.DRAFT_NUMBER is not null
                   :Modify  by xsh 20160715 在表f_agt_bill_discount_yjs_tmp前面增加schema前缀f_fdm
                    modified by liuxz 20160718 更改月积数临时表格式、修改整体格式
                     modified by Liuxz 20160816 变更记录143.但是T7表不需要修改，已经与变更后的关联一致。
                    modified by liudongyan 20160908新增字段当前余额，月积数，年积数，月日均余额，年日均余额。见变更记录38
*/


-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明
票据贴现信息表
*/
-------------------------------------------逻辑说明END------------------------------------------
/*临时表创建区*/ 

create local temporary table tt_f_agt_bill_discount_yjs     --月积数，年积数，月日均余额，年日均余额临时表
on commit preserve rows as 
select * 
from f_fdm.f_agt_bill_discount 
where 1=2; 

/*临时表创建区END*/ 

/*数据回退区*/

delete /* +direct */ from f_fdm.f_agt_bill_discount
where etl_date='$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/

insert /* +direct */ into f_fdm.f_agt_bill_discount
        (Grp_Typ                                              --组别
        ,etl_date                                             --数据日期
        ,agmt_id                                              --协议编号
        ,cust_num                                             --客户号
        ,org_num                                              --机构号
        ,prod_cd                                              --产品代码
        ,acct_stat_cd                                         --账户状态代码
        ,fiv_cls_cd                                           --五级分类
        ,bill_id                                              --票据编号
        ,discnt_dt                                            --贴现日期
        ,due_dt                                               --到期日期
        ,int_rate                                             --利率
        ,cur_cd                                               --货币代码
        ,prin_subj                                            --本金科目
        ,discnt_amt                                           --贴现金额
        ,discnt_int                                           --贴现利息
        ,int_subj                                             --利息科目
        ,curmth_amtbl_int                                     --当月摊销利息
        ,int_adj_subj                                         --利息调整科目
        ,int_adj_amt                                          --利息调整金额
        ,curr_bal                                             --当前余额
        ,mth_accm                                             --月积数
        ,yr_accm                                              --年积数
        ,mth_day_avg_bal                                      --月日均余额
        ,Yr_Day_Avg_Bal                                       --年日均余额
        ,discnt_cate_cd                                       --贴现类别代码
        ,loan_deval_prep_bal                                  --贷款减值准备余额
        ,loan_deval_prep_amt                                  --贷款减值准备发生额
        ,sys_src                                              --系统来源
        )
select  '1'                                                       as Grp_Typ                   --组别
        ,'$TXDATE'::date                                 as etl_date                  --数据日期
        ,T.ACC                                                    as agmt_id                   --协议编号
        ,coalesce(T5.CUST_NO,'')                                  as cust_num                  --客户号
        ,coalesce(T6.BRH_NO,'')                                                as org_num                   --机构号
        ,T4.BUY_TYPE                                              as prod_cd                   --产品代码
        ,T4.DISAFFIRM_STATUS                                      as acct_stat_cd              --账户状态代码
        ,coalesce(T7.DRAFT_GRADE,'')                              as fiv_cls                   --五级分类
        ,T2.DRAFT_NUMBER                                          as bill_id                   --票据编号
        ,to_date(T4.BUY_DATE,'YYYYMMDD')                          as discnt_dt                 --贴现日期
        ,to_date(T3.PAYMENT_DATE,'YYYYMMDD')                      as due_dt                    --到期日期
        ,T4.RATE                                                  as int_rate                  --利率
        ,'156'                                                   as cur_cd                    --货币代码
        ,T.ITM_NO                                                 as prin_subj                 --本金科目
        --,T3.PAY_AMOUNT                                            as discnt_amt                --贴现金额
        ,T2.FACE_AMOUNT                                           as discnt_amt                --贴现金额 modify by duhy 20160802
        ,T3.PAYMENT_MATURITY                                      as discnt_int                --贴现利息
        ,coalesce(T8.ITM_NO,'')                                                as int_subj                  --利息科目
        ,(case
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD') >'$TXDATE'::date and to_date(T4.BUY_DATE,'YYYYMMDD')<'$MONTHBGNDAY'::date then T2.FACE_AMOUNT*T4.RATE*('$TXDATE'::date-'$MONTHBGNDAY'::date+1)/360
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD') >'$TXDATE'::date and to_date(T4.BUY_DATE,'YYYYMMDD')>='$MONTHBGNDAY'::date    then T2.FACE_AMOUNT*T4.RATE*('$TXDATE'::date-to_date(T4.BUY_DATE,'YYYYMMDD')+1)/360
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD')>='$MONTHBGNDAY'::date and to_date(T3.PAYMENT_DATE,'YYYYMMDD')<'$TXDATE'::date then T2.FACE_AMOUNT*T4.RATE*(to_date(T3.PAYMENT_DATE,'YYYYMMDD')-'$MONTHBGNDAY'::date+1)/360
                else 0
        END
        )                                                         as curmth_amtbl_int          --当月摊销利息
        ,coalesce(T9.ITM_NO,'')                                                as int_adj_subj              --利息调整科目
        ,T3.PAYMENT_MATURITY-
        (case
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD') >'$TXDATE'::date and to_date(T4.BUY_DATE,'YYYYMMDD')<'$MONTHBGNDAY'::date then T2.FACE_AMOUNT*T4.RATE*('$TXDATE'::date-'$MONTHBGNDAY'::date+1)/360
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD') >'$TXDATE'::date and to_date(T4.BUY_DATE,'YYYYMMDD')>='$MONTHBGNDAY'::date    then T2.FACE_AMOUNT*T4.RATE*('$TXDATE'::date-to_date(T4.BUY_DATE,'YYYYMMDD')+1)/360
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD')>='$MONTHBGNDAY'::date and to_date(T3.PAYMENT_DATE,'YYYYMMDD')<'$TXDATE'::date then T2.FACE_AMOUNT*T4.RATE*(to_date(T3.PAYMENT_DATE,'YYYYMMDD')-'$MONTHBGNDAY'::date+1)/360
                else 0
        END
        )                                                         as int_adj_amt              --利息调整金额
        ,T2.FACE_AMOUNT-(T3.PAYMENT_MATURITY-                                                                                                                                                                                                               
        (case                                                                                                                                                                                                                      
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD') >'$TXDATE'::date and to_date(T4.BUY_DATE,'YYYYMMDD')<'$MONTHBGNDAY'::date then T2.FACE_AMOUNT*T4.RATE*('$TXDATE'::date-'$MONTHBGNDAY'::date+1)/360                        
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD') >'$TXDATE'::date and to_date(T4.BUY_DATE,'YYYYMMDD')>='$MONTHBGNDAY'::date    then T2.FACE_AMOUNT*T4.RATE*('$TXDATE'::date-to_date(T4.BUY_DATE,'YYYYMMDD')+1)/360         
                when to_date(T3.PAYMENT_DATE,'YYYYMMDD')>='$MONTHBGNDAY'::date and to_date(T3.PAYMENT_DATE,'YYYYMMDD')<'$TXDATE'::date then T2.FACE_AMOUNT*T4.RATE*(to_date(T3.PAYMENT_DATE,'YYYYMMDD')-'$MONTHBGNDAY'::date+1)/360
                else 0                                                                                                                                                                                                             
        END 
        )                                                           
        )                                                         as curr_bal --当前余额
       ,0.00                                                      as mth_accm  --月积数
       ,0.00                                                      as yr_accm --年积数
       ,0.00                                                      as mth_day_avg_bal--月日均余额
       ,0.00                                                      as Yr_Day_Avg_Bal --年日均余额
        ,T4.CENTRAL_BANKFLG                                       as discnt_cate_cd           --贴现类别代码
        ,0                                                        as loan_deval_prep_bal      --贷款减值准备余额
        ,0                                                        as loan_deval_prep_amt      --贷款减值准备发生额
        ,'BBS'                                                    as sys_src                  --系统来源
from    dw_sdata.acc_003_t_acc_assets_ledger T
inner join  dw_sdata.bbs_001_tbl_acc_info T1
on       T.ACC=T1.ACC_NO
AND      T1.START_DT<='$TXDATE'::date
AND      T1.END_DT>'$TXDATE'::date
inner join  dw_sdata.bbs_001_draft_info T2
on       T1.draft_id = T2.id
AND      T2.DRAFT_NUMBER is not null
AND      T2.DRAFT_NUMBER !=''
AND      T2.START_DT<='$TXDATE'::date
AND      T2.END_DT>'$TXDATE'::date
inner join  dw_sdata.bbs_001_buy_details T3
on       T2.id = T3.draft_id
and      T3.BUY_TYPE='01'
AND      T3.START_DT<='$TXDATE'::date
AND      T3.END_DT>'$TXDATE'::date
inner join  dw_sdata.bbs_001_buy_contract T4
on       T3.BUY_CONTRACT_ID = T4.ID
AND      T4.START_DT<='$TXDATE'::date
AND      T4.END_DT>'$TXDATE'::date
left join   dw_sdata.bbs_001_customer_info T5
on       T4.CUSTOMER_ID=T5.ID
AND      T5.START_DT<='$TXDATE'::date
AND      T5.END_DT>'$TXDATE'::date
left join   dw_sdata.bbs_001_branch_info T6
on       T4.BRANCH_ID=T6.ID
AND      T6.START_DT<='$TXDATE'::date
AND      T6.END_DT>'$TXDATE'::date
--left join   dw_sdata.bbs_000_draft_classification T7 modify by duhy 20160802
left join (SELECT DRAFT_ID,
                  DRAFT_GRADE,
                  START_DT,
                  END_DT,
                  ROW_NUMBER() OVER(PARTITION BY DRAFT_ID ORDER BY CLASSIFY_DATE DESC) AS NUM
            FROM dw_sdata.bbs_000_draft_classification
           WHERE start_dt<='$TXDATE'::date
             AND end_dt>'$TXDATE'::date
          ) T7
on       T3.DRAFT_ID=T7.DRAFT_ID
and      t7.num = 1
left join   dw_sdata.acc_003_t_accdata_last_item_no T8
on       T.itm_no = T8.amt_itm
and      T8.first_itm = '18'
AND      T8.START_DT<='$TXDATE'::date
AND      T8.END_DT>'$TXDATE'::date
left join   dw_sdata.acc_003_t_accdata_last_item_no T9
on       T.itm_no = T9.amt_itm
and      T9.first_itm = '16'
AND      T9.START_DT<='$TXDATE'::date
AND      T9.END_DT>'$TXDATE'::date
where    T.SYS_CODE = '99470000000' 
AND      T.START_DT<='$TXDATE'::date
AND      T.END_DT>'$TXDATE'::date
;


/*计算月积数、年积数、月日均余额、年日均余额*/
insert into tt_f_agt_bill_discount_yjs
(
Agmt_Id 
,Mth_Accm 
,Yr_Accm 
,Mth_Day_Avg_Bal 
,Yr_Day_Avg_Bal 
)
select t.Agmt_Id
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.Mth_Accm,0)
        end
       )                                                                              as Mth_Accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.Yr_Accm,0)
        end
       )                                                                              as Yr_Accm  --年积数
       ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.Mth_Accm,0)
        end
        )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)                      as Mth_Day_Avg_Bal  --月日均余额
       ,(case 
            when '$TXDATE' = '$YEARBGNDAY' then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.Yr_Accm,0)
        end
        )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                           as Yr_Day_Avg_Bal   --年日均余额
from f_fdm.f_agt_bill_discount t
left join f_fdm.f_agt_bill_discount t1
on t.Agmt_Id=t1.Agmt_Id
and t1.etl_date='$TXDATE'::date-1
where t.etl_date='$TXDATE'::date
;
/*计算END*/

/*更新目标表月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_agt_bill_discount t
set Mth_Accm=t1.Mth_Accm
   ,Yr_Accm=t1.Yr_Accm
   ,Mth_Day_Avg_Bal=t1.Mth_Day_Avg_Bal
   ,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from tt_f_agt_bill_discount_yjs t1
where t.Agmt_Id=t1.Agmt_Id
and   t.etl_date='$TXDATE'::date
;
 
/*更新END*/ 

/*数据处理区END*/ 

commit;
