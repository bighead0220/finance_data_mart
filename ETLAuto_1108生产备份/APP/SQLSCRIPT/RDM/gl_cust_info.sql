/*
Author             :liudongyan
Function           :客户信息
Load method        :INSERT
Source table       :f_cust_corp 公司客户基本信息
                    f_cust_indv 个人客户基本信息表
                    --f_loan_indv_dubil  个人贷款借据信息表
Destination Table  :GL_CUST_INFO   客户信息
Frequency          :M
Modify history list:Created by liudongyan 2016/6/15 10:19:34
                    Modify by 魏银辉 at 2016-8-8 10:40:06
                        修改内容：NO2.修改第2组“客户行业类型代码（国标１级）”的映射规则 20160804
                    modified by wyh at 20160830
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
DELETE FROM f_rdm.GL_CUST_INFO
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_CUST_INFO
(
      etl_date	             --数据日期
     ,Cust_Id	             --客户编号
     ,cust_nm	             --客户名称
     ,Cust_Inds_Typ_Cd	     --客户行业类型代码
     ,sys_src	             --来源系统
)
SELECT
       '$TXDATE'::date     as etl_date
      ,Cust_Num                     as Cust_Id
      ,Cust_Nm                      as cust_nm
      ,Cust_Inds_Cd                 as Cust_Inds_Typ_Cd
      ,Sys_Src                      as sys_src
FROM  f_fdm.f_cust_corp --公司客户基本信息
WHERE etl_date ='$TXDATE'::date
UNION ALL
/*                       modified 20160830  个人换成同业
SELECT
      '$TXDATE'::date      as etl_date
      ,T.Cust_Num                   as Cust_Id
      ,T.Cust_Nm                    as cust_nm
      ,''                           as Cust_Inds_Typ_Cd
      --,T1.Loan_Drct_Inds_Cd              as Cust_Inds_Typ_Cd
      ,T.Sys_Src                    as sys_src
FROM  f_fdm.f_cust_indv   AS T --个人客户基本信息表
-- NO2.修改,暂时注释保留历史
-- LEFT JOIN f_fdm.f_loan_indv_contr AS T1 --个人贷款合同信息表
-- ON    T.cust_num=T1.Cust_Num
-- AND   T1.etl_date ='$TXDATE'::date
WHERE T.etl_date ='$TXDATE'::date
*/ 
select 
      '$TXDATE'::date      as etl_date
       ,cust_num                     as Cust_Id          
       ,ibank_cust_legl_nm           as cust_nm          
       ,inds_typ_cd                 as Cust_Inds_Typ_Cd 
       ,sys_src                      as sys_src          
from f_fdm.f_cust_ibank
WHERE etl_date ='$TXDATE'::date
;

/*数据处理区END*/

COMMIT;
