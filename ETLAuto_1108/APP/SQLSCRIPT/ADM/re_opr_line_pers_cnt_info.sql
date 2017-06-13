/*数据回退区*/
delete from  F_ADM.a_ma_re_opr_line_pers_cnt_info where AS_OF_DATE = '$TXDATE'::DATE;
/*数据回退区end*/

COPY F_ADM.a_ma_re_opr_line_pers_cnt_info
(
 tmp_AS_OF_DATE filler date,
AS_OF_DATE as tmp_AS_OF_DATE,
 tmp_ORG_CODE filler varchar(50),
ORG_CODE as rtrim(tmp_ORG_CODE),
 tmp_LOB_ID filler varchar(50),
LOB_ID as rtrim(tmp_LOB_ID),
 tmp_EMPLOYEES_COUNT filler  numeric(24,0),
EMPLOYEES_COUNT as tmp_EMPLOYEES_COUNT,
 tmp_MAN_ORG_PEOPLE_COUNT filler varchar(50),
MAN_ORG_PEOPLE_COUNT as rtrim(tmp_MAN_ORG_PEOPLE_COUNT),
 tmp_SYS_SRC filler  varchar(50),
SYS_SRC as rtrim(tmp_SYS_SRC)
)
WITH SOURCE HDFS (url='http://$hadoop_namenode_id:50070/webhdfs/v1/$path2/$file_name.dat',USERNAME='$hadoop_user')
FILTER SearchAndReplace(pattern='\\',replace_with='')
DELIMITER '$delimiter' 
ENFORCELENGTH ABORT ON ERROR DIRECT

;  


