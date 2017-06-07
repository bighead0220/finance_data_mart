/*
Author             :魏银辉
Function           :信用卡
Load method        :INSERT
Source table       :
Destination Table  :GL_CRDT_CARD 信用卡
Frequency          :M
Modify history list:Created by 魏银辉 at 20160829
                    modified by wyh at 20161011 增加月末跑批控制语句
                    modified by wyh at 20161013 增加T1表的关联条件
*/

-------------------------------------------逻辑说明---------------------------------------------
/*

*/
-------------------------------------------逻辑说明END------------------------------------------
/*月末跑批控制语句*/
SELECT COUNT(1) 
FROM dual
where '$MONTHENDDAY'='$TXDATE';
.IF ActivityCount <= 0 THEN .GOTO SCRIPT_END;
/*月末跑批控制语句END*/
/*临时表创建区*/
/*临时表创建区END*/

/*数据回退区*/
DELETE FROM f_rdm.GL_CRDT_CARD
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_CRDT_CARD
(
		etl_date                 --数据日期         
		,Prod_Nm                 --产品名称   
		,Pri_Card_Card_Num       --主卡卡号   
		,fst_lvl_brch            --一级分行   
		,Cust_Id                 --客户编号   
		,crdt_lmt                --授信额度   
		,od_prin                 --透支本金   
		,Fiv_Cls                 --五级分类   
		,is_nt_ovrd              --是否逾期   
		,ovrd_prin_amt           --逾期本金金额 
		,ovrd_int_amt            --逾期利息金额 
		,resv_provs              --准备金计提  
		,Ovrd_Days               --逾期天数   
		,Wrtoff_Dt               --核销日期   
		,Wrtoff_Prin             --核销本金   
		,wrtoff_late_chrg        --核销滞纳金  
		,wrtoff_comm_fee         --核销手续费  
		,wrtoff_in_bal_int       --核销表内利息 
		,wrtoff_off_bal_int      --核销表外利息 
		,wrtoff_post_repay_dt    --核销后还款日期
		,wrtoff_post_repay_amt   --核销后还款金额
		,sys_src                 --来源系统   
		,cur_cd                  --币种
)
SELECT
    '$TXDATE'::DATE                 as etl_date                --数据日期             
    ,max(t.Prod_Cd)                               as Prod_Nm                 --产品名称          
    ,t.card_num                              as Pri_Card_Card_Num       --主卡卡号         modified at 20160912 
    ,max(t.Org_Num)                               as fst_lvl_brch            --一级分行         
    ,min(t.Cust_Id)                               as Cust_Id                 --客户编号         
    ,max(t.Crdt_Lmt)                              as crdt_lmt                --授信额度         
    ,max(t.curr_bal)                              as od_prin                 --透支本金         
    ,''                                      as Fiv_Cls                 --五级分类         
    ,min(t.Ovrd_Stat)                             as is_nt_ovrd              --是否逾期         
    ,max(t.remn_ovrd_prin)                        as ovrd_prin_amt           --逾期本金金额       
    ,max(t.remn_ovrd_int)                         as ovrd_int_amt            --逾期利息金额       
    ,0                                       as resv_provs              --准备金计提        
    ,'$TXDATE'::DATE - min(t.ovrd_dt)    as Ovrd_Days               --逾期天数         
    ,min(t.wrtoff_dt)                             as Wrtoff_Dt               --核销日期         
    ,max(t.wrtoff_prin)                           as Wrtoff_Prin             --核销本金         
    ,max(t.wrtoff_comm_fee)                       as wrtoff_late_chrg        --核销滞纳金        
    ,max(t.wrtoff_late_chrg)                      as wrtoff_comm_fee         --核销手续费        
    ,max(t.wrtoff_off_bal_int)                    as wrtoff_in_bal_int       --核销表内利息       
    ,max(t.wrtoff_in_bal_int)                     as wrtoff_off_bal_int      --核销表外利息       
    ,max(t1.wrtoff_post_retr_dt)                          as wrtoff_post_repay_dt    --核销后还款日期      
    ,sum(t1.wrtoff_post_retr_prin + t1.wrtoff_post_retr_int)                                       as wrtoff_post_repay_amt   --核销后还款金额      
    ,max(t.Sys_Src)                               as sys_src                 --来源系统         
    ,t.Cur_Cd                                     as cur_cd                  --币种           
FROM   f_fdm.f_acct_crdt_info t

left join  (select
                    orgnl_agmt_id
                    ,MAX(wrtoff_post_retr_dt)           AS wrtoff_post_retr_dt
                    ,sum(wrtoff_post_retr_prin)         AS wrtoff_post_retr_prin
                    ,sum(wrtoff_post_retr_int)          AS wrtoff_post_retr_int
            from f_fdm.f_agt_asst_consv
            where sys_src = 'CCB'
            AND SUBSTR(to_char(wrtoff_post_retr_dt,'YYYYMMDD'),1,6) = SUBSTR('$TXDATE',1,6)
            and etl_date = '$TXDATE'::date
            GROUP BY orgnl_agmt_id
           ) T1
on T.Agmt_Id = T1.orgnl_agmt_id

where etl_date = '$TXDATE'::DATE 
group by t.card_num,t.Cur_Cd
;
/*数据处理区END*/

COMMIT;
