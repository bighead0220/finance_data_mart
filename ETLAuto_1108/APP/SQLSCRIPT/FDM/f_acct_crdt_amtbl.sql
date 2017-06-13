/*
Author             :刘东燕
Function           :信用卡账户分期信息表
Load method        :INSERT
Source table       :dw_sdata.ccb_000_mpur,dw_sdata.ecf_001_t01_cust_info,dw_sdata.ecf_004_t01_cust_info,dw_sdata.ccb_000_acct,dw_sdata.ccb_000_prprd
Destination Table  :f_acct_crdt_amtbl  信用卡账户分期信息表
Frequency          :D
Modify history list:Created by刘东燕2016年4月19日10:05:55
                   :Modify  by zhangliang 20160901   修改关联表t2
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/
/*临时表创建区END*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_acct_crdt_amtbl
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_acct_crdt_amtbl
(
        Grp_Typ                                                                     --组别
       ,etl_date                                                                    --数据日期
       ,agmt_id                                                                     --协议编号
       ,amtbl_pay_ordr_num                                                          --分期付款序号
       ,card_num                                                                    --卡号
       ,cust_num                                                                    --客户号
       ,cur_cd                                                                      --货币代码
       ,crdt_card_acct_typ_cd                                                       --信用卡账户种类代码
       ,prin_subj                                                                   --本金科目
       ,int_subj                                                                    --利息科目
       ,comm_fee_subj                                                               --手续费科目
       ,Amtbl_Pay_Stat_Cd                                                            --分期付款状态代码
       ,amtbl_pay_typ_cd                                                            --分期付款种类代码
       ,amtbl_amt                                                                   --分期金额
       ,totl_amtbl_mths                                                             --总分期月数
       ,amtbl_int                                                                   --分期利息
       ,amtbl_pay_comm_fee                                                          --分期付款手续费
       ,st_int_dt                                                                   --起息日期
       ,amtbl_pay_int_rate                                                          --分期付款利率
       ,amtbl_pay_fee_rate                                                          --分期付款费率
       ,remn_un_ret_prin                                                            --剩余未还本金
       ,remn_un_ret_fee                                                             --剩余未还费用
       ,remn_un_ret_int                                                             --剩余未还利息
       ,amtbl_pay_prd_cnt                                                           --分期付款期数
       ,amtbl_dt                                                                    --摊销日期
       ,sys_src                                                                     --系统来源
)
SELECT 
       '1'                                                                                as Grp_Typ
       ,'$TXDATE'::date                                                          as etl_date
       ,T.XACCOUNT                                                                        as agmt_id
       ,T.MP_NUMBER                                                                       as amtbl_pay_ordr_num
       ,T.CARD_NBR                                                                        as card_num
       ,coalesce(T2.ECIF_CUST_NO,'')                                                      as cust_num
       ,T.CURR_NUM                                                                        as cur_cd
       ,coalesce(to_char(T1.CATEGORY),'')                                                 as crdt_card_acct_typ_cd
       ,'11354000101R'                                                                    as prin_subj
       ,'51053100601R'                                                                    as int_subj
       ,'55052001101R'                                                                    as comm_fee_subj
       ,T.STATUS                                                                          as Amtbl_Pay_Stat_Cd 
       ,T.MP_TYPE                                                                         as amtbl_pay_typ_cd
       ,coalesce(T.ORIG_PURCH/100,0)                                                      as amtbl_amt
       ,coalesce(T.NBR_MTHS,0)                                                            as totl_amtbl_mths
       ,coalesce(T.ORIG_INT/100,0)                                                        as amtbl_int
       ,coalesce(T.ORIG_FEE/100,0)                                                        as amtbl_pay_comm_fee
       ,to_date(to_char(T.PURCH_DAY),'yyyymmdd')                                          as st_int_dt
       ,coalesce(T.INT_RATE,0)                                                            as amtbl_pay_int_rate
       ,coalesce(T3.FEE_PCNT,0)                                                           as amtbl_pay_fee_rate
       ,coalesce(T.REM_PPL/100,0)                                                         as remn_un_ret_prin
       ,coalesce(T.REM_FEE/100,0)                                                         as remn_un_ret_fee
       ,coalesce(T.REM_INT/100,0)                                                         as remn_un_ret_int
       ,coalesce(T.INSTL_CNT,0)                                                           as amtbl_pay_prd_cnt
       ,to_date(to_char(T.LST_INSTDY),'yyyymmdd')                                         as amtbl_dt
       ,'CCB'                                                                             as sys_src

         
FROM      dw_sdata.ccb_000_mpur     AS T                               
LEFT JOIN dw_sdata.ccb_000_acct     AS T1           
ON       T.XACCOUNT=T1.XACCOUNT
AND      T1.start_dt<='$TXDATE'::date
AND      T1.end_dt>'$TXDATE'::date
left join (select * from 
(SELECT ECIF_CUST_NO,CERT_NO,IS_VIP_FLAG,start_dt,end_dt,
row_number() over(partition by CERT_NO order by IS_VIP_FLAG desc) as num2 
FROM 
(select * from 
(select ECIF_CUST_NO,CERT_NO,IS_VIP_FLAG,start_dt,end_dt,
row_number() over(partition by CERT_NO order by cert_due_date desc) as num 
from dw_sdata.ecf_001_t01_cust_info
where UPDATED_TS = '99991231 00:00:00:000000'
and start_dt<='$TXDATE'::date
and '$TXDATE'::date<end_dt ) a
where a.num=1
union all
select * from (select ECIF_CUST_NO,CERT_NO,IS_VIP_FLAG,start_dt,end_dt,
row_number() over(partition by CERT_NO  order by cert_due_date desc) as num 
from dw_sdata.ecf_004_t01_cust_info 
where UPDATED_TS = '99991231 00:00:00:000000'
and start_dt<='$TXDATE'::date
and '$TXDATE'::date<end_dt) b
where b.num=1
 ) c)d
where d.num2=1) T2
ON       T1.CUSTR_NBR=T2.CERT_NO
LEFT JOIN   dw_sdata.ccb_000_prprd        AS T3 
ON       T.PROD_ID=T3.PROD_ID
AND      T3.start_dt<='$TXDATE'::date
AND      T3.end_dt>'$TXDATE'::date
WHERE    T.start_dt<='$TXDATE'::date
AND      T.end_dt>'$TXDATE'::date
; 


/*数据处理区END*/
COMMIT;

