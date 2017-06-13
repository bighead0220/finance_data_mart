/*
Author             :刘东燕
Function           :科目对照关系表
Load method        :INSERT
Source table       :dap_subjview 科目对照表                  
Destination Table  :f_fnc_Accti2gl 科目对照关系表
Frequency          :D
Modify history list:Created by刘东燕2016年4月19日10:05:55
                   :Modify  by liuxz 20160714 全部注释
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*数据回退区*/

DELETE /* +direct */ from f_fdm.f_fnc_Accti2gl
where etl_date='$TXDATE'::date;
--;

/*数据回退区END*/
/*数据处理区*/
/*
INSERT INTO f_fdm.f_fnc_Accti2gl
(
                Grp_Typ                                                                      --组别
               ,ETL_Date                                                                     --数据日期
               ,Accti_Deal_Pltf_Subj_Cd                                                      --会计处理平台科目代码
               ,Gl_Subj_Cd                                                                   --总账科目代码

)
SELECT
         '1'                                                                                 as Grp_Typ   
         ,'$TXDATE'::date                                                                 as ETL_Date 
         ,T.PK_SUBJVIEW                                                                      as Accti_Deal_Pltf_Subj_Cd  
         ,T.SUBJCODE                                                                         as Gl_Subj_Cd 
        
FROM       dw_sdata.DAP_SUBJVIEW                                                      AS T


;
*/
/*数据处理区END*/

COMMIT;

