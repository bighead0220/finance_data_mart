/*
Author             :魏银辉
Function           :投资
Load method        :INSERT
Source table       :f_fdm.f_Cap_Bond_Inves
Destination Table  :GL_INVES 投资
Frequency          :M
Modify history list:Created by 魏银辉 at 2016-8-8 17:07:02
                    modified by wyh at 20160830  	添加币种
	            modified by wyh at 20160831         添加本金科目  增加组别2
                    modified by wyh at 20161011 增加月末跑批控制语句
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
DELETE FROM f_rdm.GL_INVES
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_INVES
(
        etl_date            --数据日期
        ,tx_num             --交易号
        ,acct_nm            --证券代码
        ,tx_tool_cls        --交易工具分类
        ,issu_day           --发行日
        ,Due_Dt             --到期日
        ,remn_par           --剩余面值
        ,recvbl_int_bal     --应收利息余额
        ,int_adj_bal        --利息调整余额
        ,val_chg_bal        --公允价值变动余额
        ,deval_prep_bal     --减值准备余额
        ,sys_src            --来源系统
        ,cur_cd             --币种               modified 20160830
        ,prin_subj          --本金科目           modified 20160831
)
SELECT
        '$TXDATE'::date          AS etl_date
        ,T.Agmt_Id                        AS tx_num
        ,T.Bond_Cd                        AS acct_nm
        ,T.TX_Tool_Cls                    AS tx_tool_cls
        ,T.Bond_Issu_Dt                   AS issu_day
        ,T.Due_Dt                         AS Due_Dt
        ,T.Book_Bal                       AS remn_par
        ,T.Accm_Provs_Int                 AS recvbl_int_bal
        ,T.Accm_Int_Adj_Amt               AS int_adj_bal
        ,T.Accm_Valtn_Prft_Loss_Amt       AS val_chg_bal
        ,0                                 AS deval_prep_bal
        ,T.Sys_Src                        AS sys_src
        ,T.cur_cd                         as cur_cd 
        ,prin_subj                        as prin_subj
FROM  f_fdm.f_Cap_Bond_Inves T
WHERE T.etl_date = '$TXDATE'::date
/*增加新的组别2:modified by wyh 20160831*/
union all 
select 
        '$TXDATE'::date          AS etl_date 
        ,t.Agmt_Id                        as tx_num             --交易号                
        ,''                               as acct_nm            --证券代码         
        ,''                               as tx_tool_cls        --交易工具分类       
        ,t.provs_dt                       as issu_day           --发行日          
        ,t.due_dt                         as Due_Dt             --到期日          
        ,0                                as remn_par           --剩余面值         
        ,0                                as recvbl_int_bal     --应收利息余额       
        ,0                                as int_adj_bal        --利息调整余额       
        ,0                                as val_chg_bal        --公允价值变动余额     
        ,t.deval_prep_bal                 as deval_prep_bal     --减值准备余额       
        ,t.sys_src                        as sys_src            --来源系统         
        ,t.cur_cd                         as cur_cd             --币种           
        ,t.prin_subj                      as prin_subj          --本金科目        
from  f_fdm.f_cap_biz_provs_info t
WHERE T.etl_date = '$TXDATE'::date
;

/*数据处理区END*/

COMMIT;
