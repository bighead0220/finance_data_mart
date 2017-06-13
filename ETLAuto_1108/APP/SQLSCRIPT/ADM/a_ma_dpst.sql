/*数据回退区*/
delete from  f_adm.A_MA_DPST where etl_date = '$TXDATE'::DATE;
/*数据回退区end*/

COPY f_adm.A_MA_DPST
(
 etl_date            ,
 acct_num            ,
 sub_acct_num        ,
 accti_pltf_subj_num ,
 fst_lvl_brch        ,
 cust_cd             ,
 Cur_Cd              ,
 book_orgnl_amt      ,
 stbl_int_rate       ,
 sys_src             ,
 prin_subj           
)
WITH SOURCE HDFS (url='http://$hadoop_namenode_id:50070/webhdfs/v1/$path/$file_name.dat*',USERNAME='$hadoop_user')
FILTER SearchAndReplace(pattern='\\',replace_with='')
DELIMITER '$delimiter' 
ENFORCELENGTH ABORT ON ERROR DIRECT

;  


