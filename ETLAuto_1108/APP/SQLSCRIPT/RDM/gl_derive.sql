/*
Author             :liudongyan
Function           :衍生
Load method        :INSERT
Source table       :f_Cap_Raw_TX
Destination Table  :gl_derive   衍生
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by 魏银辉 at 2016-8-9 14:54:40
                    modified by wyh at 20160831 增加字段 本金科目
                    modified by wyh at 20161011 增加月末跑批控制语句
*/

-------------------------------------------逻辑说明---------------------------------------------

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
DELETE FROM f_rdm.gl_derive
WHERE  etl_date='$TXDATE'::date;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.gl_derive
(
        etl_date	                            --数据日期
        ,tx_num	                                --交易号
        ,Prod_Nm	                            --产品名称
        ,net_prc_mkt_val	                    --净价市值
        ,notnl_prin_amt	                        --名义本金额
        ,Cur_Cd	                                --币种
        ,sys_src	                            --来源系统
        ,prin_subj                                  --本金科目    modified 20160831
)
SELECT
       '$TXDATE'::date                 as etl_date
       ,Agmt_Id                                 as tx_num
       ,Prod_Cd                                 as Prod_Nm
       --,Curr_Val                                as net_prc_mkt_val
       ,0.00                                    as net_prc_mkt_val
       ,Notnl_Prin                              as notnl_prin_amt
       ,Cur_Cd                                  as Cur_Cd
       ,Sys_Src                                 as sys_src
       ,prin_subj                               as prin_subj
FROM   f_fdm.f_Cap_Raw_TX	T
WHERE T.etl_date ='$TXDATE'::date
;


/*数据处理区END*/

COMMIT;
