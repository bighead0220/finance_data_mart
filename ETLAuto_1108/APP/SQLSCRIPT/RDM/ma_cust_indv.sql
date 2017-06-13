/*
Author             :liudongyan
Function           :个人客户
Load method        :INSERT
Source table       :f_cust_indv 个人客户基本信息表
Destination Table  :ma_cust_indv   个人客户
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
DELETE FROM f_rdm.ma_cust_indv
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_cust_indv
(
       etl_date                              --数据日期
       ,cust_num                             -- 客户号
       ,cust_nm                              --客户名称
       ,nation_cd                            --国籍
       ,gender                               --性别
       ,birth_dt                             --出生日期
       ,carr_cd                              --职业
       ,marrg_stat_cd                        --婚姻状态
       ,belg_org_num                         --所属机构号
       ,open_acct_dt                         --开户日期
       ,cust_typ_cd                          --客户类型
       ,cust_lvl_cd                          --客户等级
       ,crdt_card_cust_lvl_cd                --信用卡客户等级
       ,is_vip                               --是否VIP
       ,AUM                                  --AUM值
       ,cust_div_grp                         --客户分群  
)
SELECT
        '$TXDATE'::date               as etl_date
        ,cust_num                              as cust_num
        ,cust_nm                               as cust_nm
        ,nation_cd                             as nation_cd
        ,gender_cd                             as gender
        ,birth_dt                              as birth_dt
        ,carr_cd                               as carr_cd
        ,marrg_stat_cd                         as marrg_stat_cd
        ,belg_org_num                          as belg_org_num
        ,open_acct_dt                          as open_acct_dt
        ,cust_typ_cd                           as cust_typ_cd
        ,cust_lvl_cd                           as cust_lvl_cd
        ,crdt_card_cust_lvl_cd                 as crdt_card_cust_lvl_cd
        ,is_vip                                as is_vip
        ,fin_asst_totl_amt                     as AUM
        ,cust_div_grp                          as cust_div_grp

FROM  f_fdm.f_cust_indv
WHERE etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
