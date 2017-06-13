/*
Author             :liudongyan
Function           :科目对照关系
Load method        :INSERT
Source table       :f_fnc_Accti2gl 
Destination Table  :ma_accti2gl
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
DELETE FROM f_rdm.ma_accti2gl
WHERE  etl_date='$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.ma_accti2gl
(
        etl_date	            --数据日期
       ,src_sys_subj_cd	      --源系统科目代码
       ,majr_gl_subj_cd	    --大总账科目代码
       ,majr_gl_dtl_subj_cd	  --大总账明细科目代码
       ,majr_gl_prod_cd	      --大总账产品代码
       ,majr_gl_line_cd	      --大总账条线代码
       ,st_use_dt	            --启用日期
       ,invldtn_dt	          --失效日期
       ,sys_src   	          --系统来源

)
SELECT
       '$TXDATE'::date                 as etl_date	              --数据日期        
       ,T.src_sys_subj_cd                         as src_sys_subj_cd	      --源系统科目代码     
       ,T.majr_gl_subj_cd                         as majr_gl_subj_cd	      --大总账科目代码       
       ,T.majr_gl_dtl_subj_cd                     as majr_gl_dtl_subj_cd	  --大总账明细科目代码   
       ,T.majr_gl_prod_cd                         as majr_gl_prod_cd	      --大总账产品代码     
       ,T.majr_gl_line_cd                         as majr_gl_line_cd	      --大总账条线代码     
       ,T.st_use_dt                               as st_use_dt	            --启用日期        
       ,T.invldtn_dt                              as invldtn_dt	            --失效日期        
       ,T.sys_src                                 as sys_src	              --系统来源        

FROM  f_adm.a_fnc_Accti2gl  as T     -- 科目对照关系表
WHERE T.etl_date ='$TXDATE'::date
;

/*数据处理区END*/

COMMIT;
