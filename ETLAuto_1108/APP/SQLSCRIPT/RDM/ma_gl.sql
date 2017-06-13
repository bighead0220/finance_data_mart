/*
Author             :liudongyan
Function           :总账
Load method        :INSERT
Source table       :f_fnc_gl_info 总账信息表
Destination Table  :ma_gl 总账
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by
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
DELETE FROM f_rdm.ma_gl
WHERE  etl_date='$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_gl
(
       etl_date	                --数据日期
      ,org_cd	                --机构代码
      ,cur_cd	                --币种
      ,duty_ctr_cd	        --责任中心代码
      ,ACCOUNT_cd               --自然科目代码
      ,dtl_subj_cd	        --明细科目代码
      ,prod_cd	                --产品代码
      ,biz_line_cd	        --业务条线代码
      ,inn_reco_cd	        --内部往来代码
      ,BUSINESS_UNIT_cd	        --事业部代码
      ,SPARE_cd	                --备用段代码
      ,today_debit_amt	        --本月累计借方发生额
      ,today_crdt_amt	        --本月累计贷方发生额
      ,debit_bal	        --借方余额
      ,crdt_bal	                --贷方余额
      ,curmth_accm_debit_amt    --本期借方月日均余额
      ,curmth_accm_crdt_amt     --本期贷方月日均余额
      ,term_ind	                --期限标志
      ,sys_src	                --系统来源                      

)
SELECT
       '$TXDATE'::date  as etl_date	                --数据日期
      ,org_cd                 as org_cd	                  --机构代码
      ,cur_cd                 as cur_cd	                  --币种
      ,duty_ctr_cd            as duty_ctr_cd	            --责任中心代码
      ,ACCOUNT_cd             as ACCOUNT_cd	              --自然科目代码
      ,dtl_subj_cd            as dtl_subj_cd	            --明细科目代码
      ,prod_cd                as prod_cd	                --产品代码
      ,biz_line_cd	      as biz_line_cd	            --业务条线代码
      ,inn_reco_cd            as inn_reco_cd	            --内部往来代码
      ,BUSINESS_UNIT_cd       as BUSINESS_UNIT_cd	        --事业部代码
      ,SPARE_cd	              as SPARE_cd	                --备用段代码
      ,today_debit_amt        as today_debit_amt	        --本月累计借方发生额
      ,today_crdt_amt         as today_crdt_amt	          --本月累计贷方发生额
      ,debit_bal              as debit_bal	              --借方余额
      ,crdt_bal               as crdt_bal	                --贷方余额
      ,curmth_accm_debit_amt  as curmth_accm_debit_amt	  --本期借方月日均余额
      ,curmth_accm_crdt_amt   as curmth_accm_crdt_amt	    --本期贷方月日均余额
      ,term_ind	              as term_ind	                --期限标志                                   
      ,sys_src	               as sys_src	                --系统来源                                                      
       
FROM  f_fdm.f_fnc_gl_info
WHERE etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
