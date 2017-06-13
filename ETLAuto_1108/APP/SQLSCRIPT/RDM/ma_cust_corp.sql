/*
Author             :liudongyan
Function           :公司客户
Load method        :INSERT
Source table       :f_cust_corp 公司客户基本信息表
Destination Table  :ma_cust_corp 公司客户
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by xsh 1.将Cust_Crdt_Lvl_Cd as Cust_Crdt_Lvl_Cd 修改为 Cust_Crdt_Lvl_Cd as Cust_rat
                                   2.cust_nm的长度由原来在80改为200;Corp_Grp_Prnt_Corp_Nm的长度由原来在80改为了400,与基础层一致  
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
DELETE FROM f_rdm.ma_cust_corp
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_cust_corp 
(
       etl_date                               --数据日期
      ,Cust_Num                               --客户号
      ,Cust_Nm                                --客户名称
      ,Cust_Org_Org_Cd                        --客户组织机构代码
      ,Belg_Org_Num                           --所属机构号
      ,Ovrs_Cust_Rgst_Num                     --境外客户注册号
      ,Open_Acct_Dt                           --开户日期
      ,Cust_Typ_Cd                            --客户类型
      ,Cust_Inds_Cd                           --客户行业
      ,Cust_Size_Cd                           --客户规模
      ,Rgst_Cap                               --注册资本
      ,Cust_rat                               --客户评级
      ,Is_Grp_Cust_Ind                        --是否集团客户标志
      ,Corp_Grp_Prnt_Corp_Nm                  --企业集团母公司名称
      ,Corp_Grp_Prnt_Corp_Cust_Num            --企业集团母公司客户号
      ,Is_VIP                                 --是否VIP
      ,Strgy_Cust_Ind                         --战略客户标识
      ,Pltf_Ind                               --平台标识
      ,Prtnr_Ind                              --合作方标识

)
SELECT
       '$TXDATE'::date               as etl_date
       ,Cust_Num                              as Cust_Num
       ,Cust_Nm                               as Cust_Nm
       ,Cust_Org_Org_Cd                       as Cust_Org_Org_Cd
       ,Belg_Org_Num                          as Belg_Org_Num
       ,Ovrs_Cust_Rgst_Num                    as Ovrs_Cust_Rgst_Num
       ,Open_Acct_Dt                          as Open_Acct_Dt
       ,Cust_Typ_Cd                           as Cust_Typ_Cd
       ,Cust_Inds_Cd                          as Cust_Inds_Cd
       ,Cust_Size_Cd                          as Cust_Size_Cd
       ,Rgst_Cap                              as Rgst_Cap
       ,Cust_Crdt_Lvl_Cd                      as Cust_rat
       ,Is_Grp_Cust_Ind                       as Is_Grp_Cust_Ind
       ,Corp_Grp_Prnt_Corp_Nm                 as Corp_Grp_Prnt_Corp_Nm
       ,Corp_Grp_Prnt_Corp_Cust_Num           as Corp_Grp_Prnt_Corp_Cust_Num
       ,Is_VIP                                as Is_VIP
       ,Strgy_Cust_Ind                        as Strgy_Cust_Ind
       ,Pltf_Ind                              as Pltf_Ind
       ,Prtnr_Ind                             as Prtnr_Ind
  
FROM  f_fdm.f_cust_corp
WHERE etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;

