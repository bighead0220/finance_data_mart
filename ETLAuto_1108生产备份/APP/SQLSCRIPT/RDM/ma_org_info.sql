/*
Author             :liudongyan
Function           :机构
Load method        :INSERT
Source table       :f_org_info    
Destination Table  :ma_org_info   机构
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by liudongyan at 20160926 修改面积的取数规则
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
DELETE FROM f_rdm.ma_org_info
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_org_info  
(
            etl_date                              --数据日期
           ,org_cd                                --机构代码
           ,org_nm                                --机构名称
           ,org_hrcy                              --机构层级
           ,org_sht_nm                            --机构简称
           ,admin_lvl                             --行政级别
           ,org_func                              --机构职能
           ,org_attr                              --机构属性
           ,org_stat_cd                           --机构状态代码
           ,admin_div_cd                          --行政区划代码
           ,econ_regn_attr_cd                     --经济区域属性代码
           ,regn_attr                             --地域属性
           ,org_cnt                               --机构数
           ,pers_cnt                              --人数
           ,up_org_cd                             --上级机构代码
           ,up_org_nm                             --上级机构名称
           ,is_dtl_org                            --是否明细机构
           ,st_use_dt                             --启用日期
           ,invldtn_dt                            --失效日期
           ,area                                  --面积
           ,seat_cnt                              --台席数
 
)
SELECT
       '$TXDATE'::date                 as etl_date
       ,org_cd                                  as org_cd
       ,org_nm                                  as org_nm
       ,org_hrcy                                as org_hrcy
       ,org_sht_nm                              as org_sht_nm
       ,admin_lvl                               as admin_lvl
       ,org_func                                as org_func
       ,org_attr                                as org_attr
       ,org_stat_cd                             as org_stat_cd
       ,admin_div_cd                            as admin_div_cd
       ,econ_regn_attr_cd                       as econ_regn_attr_cd
       ,regn_attr                               as regn_attr
       ,org_cnt                                 as org_cnt
       ,pers_cnt                                as pers_cnt
       ,up_org_cd                               as up_org_cd
       ,up_org_nm                               as up_org_nm
       ,is_dtl_org                              as is_dtl_org
       ,st_use_dt                               as st_use_dt
       ,invldtn_dt                              as invldtn_dt
       ,brch_biz_area+post_biz_area+offi_area                                   as area
       ,seat_cnt                                as seat_cnt 
FROM  f_fdm.f_org_info --本行机构信息表
WHERE etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
