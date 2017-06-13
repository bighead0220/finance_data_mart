#!/usr/bin/perl

use strict;     # Declare using Perl strict syntax
use DBI;        # If you are using other package, declare here

# ------------ Variable Section ------------
my ${AUTO_HOME} = $ENV{"AUTO_HOME"};

my ${WML_DB} = $ENV{"AUTO_WML_DB"};
if ( !defined(${WML_DB}) ) {
    ${WML_DB} = "WML";
}
my ${WTL_DB} = $ENV{"AUTO_WTL_DB"};
if ( !defined(${WTL_DB}) ) {
    ${WTL_DB} = "WTL";
}
my ${WMLVIEW_DB} = $ENV{"AUTO_WMLVIEW_DB"};
if ( !defined(${WMLVIEW_DB}) ) {
    ${WMLVIEW_DB} = "WMLVIEW";
}
my ${WTLVIEW_DB} = $ENV{"AUTO_WTLVIEW_DB"};
if ( !defined(${WTLVIEW_DB}) ) {
    ${WTLVIEW_DB} = "WTLVIEW";
}

my ${NULL_DATE} = "1900-01-02";
my ${MIN_DATE} = "1900-01-01";
my ${MAX_DATE} = "2100-12-31";

my ${LOGON_FILE} = "${AUTO_HOME}/etc/VERTICA_LOGON";
my ${LOGON_STR};
my ${CONTROL_FILE};
my ${TX_DATE};
my ${TX_DATE_YYYYMMDD};
my ${TX_MON_DAY_MMDD};

# ------------ VSQL function ------------
sub run_vsql_command
{
  #my $rc = open(VSQL, "${LOGON_STR}");
  my $rc = open(VSQL, "|vsql -h 22.224.65.171 -p 5433 -d CPCIMDB_TEST -U dwtrans -w dwtrans2016");

  unless ($rc) {
      print "Could not invoke VSQL command
";
      return -1;
  }

# ------ Below are VSQL scripts ----------
  print VSQL <<ENDOFINPUT;

\\set ON_ERROR_STOP on

--Step0:
DELETE FROM dw_sdata.CCS_004_TB_GRT_BUSINESS_RELATION WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_004_TB_GRT_BUSINESS_RELATION SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_120 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_004_TB_GRT_BUSINESS_RELATION WHERE 1=0;

--Step2:
INSERT  INTO T_120 (
  CREDIT_BIZ_GUA_RELATION_ID,
  GUARANTY_TYPE_CD,
  GUARANTY_ID,
  BIZ_LIMIT_CONT_NUM,
  BIZ_DETAIL_ID,
  GUARANTY_AMT,
  GUARANTY_RELATION_TYPE_CD,
  CUSTOMER_NUM,
  CURRENCY_CD,
  GUARANTEE_TYPE,
  EXCHANGE_RATE,
  GUARANTY_RATE,
  EXCEED_GUARANTY_RATE_LIMIT_IND,
  CHANGE_ACTION_CD,
  VALID_IND,
  SYS_UPDATE_TIME,
  CHANGE_COMMENTS,
  GUARANTY_NAME,
  GUARANTY_USE_AMT,
  SETTLED_ADD_IND,
  INDEPOT_IND,
  GRT_NUM,
  GUARANTY_STATUS_CD,
  REG_STATUS,
  RSPCODE,
  RSPTIME,
  IS_GUARANTY_NOTARIAL,
  IS_GUARANTY_INSURE,
  IS_IMPAWN_WARDSHIP,
  IS_EXCEED_REMARK,
  IMPAWN_UP_LIMIT,
  GUARANTY_START_DATE,
  GUARANTY_END_DATE,
  GUARANTEE_TYPE_CD,
  GUARANTEE_FREE_LIMIT,
  IF_OLD_RELATION,
  FXBS_TIME_MARK,
  CONFIRMATION_SHEET_NUM,
  CHANGE_BIZ_NUM,
  GUA_CHANGE_TYPE,
  GUARANTY_RATE_ONE,
  GUARANTY_RATE_TWO,
  GUARANTY_RATE_THREE,
  GUARANTY_RATE_FOUR,
  GUARANTY_RATE_FIVE,
  IF_CREDIT_REPLACE,
  GUARANTEE_PREOCCUPY_LIMIT,
  start_dt,
  end_dt)
SELECT
  N.CREDIT_BIZ_GUA_RELATION_ID,
  N.GUARANTY_TYPE_CD,
  N.GUARANTY_ID,
  N.BIZ_LIMIT_CONT_NUM,
  N.BIZ_DETAIL_ID,
  N.GUARANTY_AMT,
  N.GUARANTY_RELATION_TYPE_CD,
  N.CUSTOMER_NUM,
  N.CURRENCY_CD,
  N.GUARANTEE_TYPE,
  N.EXCHANGE_RATE,
  N.GUARANTY_RATE,
  N.EXCEED_GUARANTY_RATE_LIMIT_IND,
  N.CHANGE_ACTION_CD,
  N.VALID_IND,
  N.SYS_UPDATE_TIME,
  N.CHANGE_COMMENTS,
  N.GUARANTY_NAME,
  N.GUARANTY_USE_AMT,
  N.SETTLED_ADD_IND,
  N.INDEPOT_IND,
  N.GRT_NUM,
  N.GUARANTY_STATUS_CD,
  N.REG_STATUS,
  N.RSPCODE,
  N.RSPTIME,
  N.IS_GUARANTY_NOTARIAL,
  N.IS_GUARANTY_INSURE,
  N.IS_IMPAWN_WARDSHIP,
  N.IS_EXCEED_REMARK,
  N.IMPAWN_UP_LIMIT,
  N.GUARANTY_START_DATE,
  N.GUARANTY_END_DATE,
  N.GUARANTEE_TYPE_CD,
  N.GUARANTEE_FREE_LIMIT,
  N.IF_OLD_RELATION,
  N.FXBS_TIME_MARK,
  N.CONFIRMATION_SHEET_NUM,
  N.CHANGE_BIZ_NUM,
  N.GUA_CHANGE_TYPE,
  N.GUARANTY_RATE_ONE,
  N.GUARANTY_RATE_TWO,
  N.GUARANTY_RATE_THREE,
  N.GUARANTY_RATE_FOUR,
  N.GUARANTY_RATE_FIVE,
  N.IF_CREDIT_REPLACE,
  N.GUARANTEE_PREOCCUPY_LIMIT,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CREDIT_BIZ_GUA_RELATION_ID, '' ) AS CREDIT_BIZ_GUA_RELATION_ID ,
  COALESCE(GUARANTY_TYPE_CD, '' ) AS GUARANTY_TYPE_CD ,
  COALESCE(GUARANTY_ID, '' ) AS GUARANTY_ID ,
  COALESCE(BIZ_LIMIT_CONT_NUM, '' ) AS BIZ_LIMIT_CONT_NUM ,
  COALESCE(BIZ_DETAIL_ID, '' ) AS BIZ_DETAIL_ID ,
  COALESCE(GUARANTY_AMT, 0 ) AS GUARANTY_AMT ,
  COALESCE(GUARANTY_RELATION_TYPE_CD, '' ) AS GUARANTY_RELATION_TYPE_CD ,
  COALESCE(CUSTOMER_NUM, '' ) AS CUSTOMER_NUM ,
  COALESCE(CURRENCY_CD, '' ) AS CURRENCY_CD ,
  COALESCE(GUARANTEE_TYPE, '' ) AS GUARANTEE_TYPE ,
  COALESCE(EXCHANGE_RATE, 0 ) AS EXCHANGE_RATE ,
  COALESCE(GUARANTY_RATE, 0 ) AS GUARANTY_RATE ,
  COALESCE(EXCEED_GUARANTY_RATE_LIMIT_IND, '' ) AS EXCEED_GUARANTY_RATE_LIMIT_IND ,
  COALESCE(CHANGE_ACTION_CD, '' ) AS CHANGE_ACTION_CD ,
  COALESCE(VALID_IND, '' ) AS VALID_IND ,
  COALESCE(SYS_UPDATE_TIME,'4999-12-31 00:00:00' ) AS SYS_UPDATE_TIME ,
  COALESCE(CHANGE_COMMENTS, '' ) AS CHANGE_COMMENTS ,
  COALESCE(GUARANTY_NAME, '' ) AS GUARANTY_NAME ,
  COALESCE(GUARANTY_USE_AMT, 0 ) AS GUARANTY_USE_AMT ,
  COALESCE(SETTLED_ADD_IND, '' ) AS SETTLED_ADD_IND ,
  COALESCE(INDEPOT_IND, '' ) AS INDEPOT_IND ,
  COALESCE(GRT_NUM, '' ) AS GRT_NUM ,
  COALESCE(GUARANTY_STATUS_CD, '' ) AS GUARANTY_STATUS_CD ,
  COALESCE(REG_STATUS, '' ) AS REG_STATUS ,
  COALESCE(RSPCODE, '' ) AS RSPCODE ,
  COALESCE(RSPTIME,'4999-12-31 00:00:00' ) AS RSPTIME ,
  COALESCE(IS_GUARANTY_NOTARIAL, '' ) AS IS_GUARANTY_NOTARIAL ,
  COALESCE(IS_GUARANTY_INSURE, '' ) AS IS_GUARANTY_INSURE ,
  COALESCE(IS_IMPAWN_WARDSHIP, '' ) AS IS_IMPAWN_WARDSHIP ,
  COALESCE(IS_EXCEED_REMARK, '' ) AS IS_EXCEED_REMARK ,
  COALESCE(IMPAWN_UP_LIMIT, 0 ) AS IMPAWN_UP_LIMIT ,
  COALESCE(GUARANTY_START_DATE,DATE('4999-12-31') ) AS GUARANTY_START_DATE ,
  COALESCE(GUARANTY_END_DATE,DATE('4999-12-31') ) AS GUARANTY_END_DATE ,
  COALESCE(GUARANTEE_TYPE_CD, '' ) AS GUARANTEE_TYPE_CD ,
  COALESCE(GUARANTEE_FREE_LIMIT, 0 ) AS GUARANTEE_FREE_LIMIT ,
  COALESCE(IF_OLD_RELATION, '' ) AS IF_OLD_RELATION ,
  COALESCE(FXBS_TIME_MARK,'4999-12-31 00:00:00' ) AS FXBS_TIME_MARK ,
  COALESCE(CONFIRMATION_SHEET_NUM, '' ) AS CONFIRMATION_SHEET_NUM ,
  COALESCE(CHANGE_BIZ_NUM, '' ) AS CHANGE_BIZ_NUM ,
  COALESCE(GUA_CHANGE_TYPE, '' ) AS GUA_CHANGE_TYPE ,
  COALESCE(GUARANTY_RATE_ONE, 0 ) AS GUARANTY_RATE_ONE ,
  COALESCE(GUARANTY_RATE_TWO, 0 ) AS GUARANTY_RATE_TWO ,
  COALESCE(GUARANTY_RATE_THREE, 0 ) AS GUARANTY_RATE_THREE ,
  COALESCE(GUARANTY_RATE_FOUR, 0 ) AS GUARANTY_RATE_FOUR ,
  COALESCE(GUARANTY_RATE_FIVE, 0 ) AS GUARANTY_RATE_FIVE ,
  COALESCE(IF_CREDIT_REPLACE, '' ) AS IF_CREDIT_REPLACE ,
  COALESCE(GUARANTEE_PREOCCUPY_LIMIT, 0 ) AS GUARANTEE_PREOCCUPY_LIMIT 
 FROM  dw_tdata.CCS_004_TB_GRT_BUSINESS_RELATION_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CREDIT_BIZ_GUA_RELATION_ID ,
  GUARANTY_TYPE_CD ,
  GUARANTY_ID ,
  BIZ_LIMIT_CONT_NUM ,
  BIZ_DETAIL_ID ,
  GUARANTY_AMT ,
  GUARANTY_RELATION_TYPE_CD ,
  CUSTOMER_NUM ,
  CURRENCY_CD ,
  GUARANTEE_TYPE ,
  EXCHANGE_RATE ,
  GUARANTY_RATE ,
  EXCEED_GUARANTY_RATE_LIMIT_IND ,
  CHANGE_ACTION_CD ,
  VALID_IND ,
  SYS_UPDATE_TIME ,
  CHANGE_COMMENTS ,
  GUARANTY_NAME ,
  GUARANTY_USE_AMT ,
  SETTLED_ADD_IND ,
  INDEPOT_IND ,
  GRT_NUM ,
  GUARANTY_STATUS_CD ,
  REG_STATUS ,
  RSPCODE ,
  RSPTIME ,
  IS_GUARANTY_NOTARIAL ,
  IS_GUARANTY_INSURE ,
  IS_IMPAWN_WARDSHIP ,
  IS_EXCEED_REMARK ,
  IMPAWN_UP_LIMIT ,
  GUARANTY_START_DATE ,
  GUARANTY_END_DATE ,
  GUARANTEE_TYPE_CD ,
  GUARANTEE_FREE_LIMIT ,
  IF_OLD_RELATION ,
  FXBS_TIME_MARK ,
  CONFIRMATION_SHEET_NUM ,
  CHANGE_BIZ_NUM ,
  GUA_CHANGE_TYPE ,
  GUARANTY_RATE_ONE ,
  GUARANTY_RATE_TWO ,
  GUARANTY_RATE_THREE ,
  GUARANTY_RATE_FOUR ,
  GUARANTY_RATE_FIVE ,
  IF_CREDIT_REPLACE ,
  GUARANTEE_PREOCCUPY_LIMIT 
 FROM dw_sdata.CCS_004_TB_GRT_BUSINESS_RELATION 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CREDIT_BIZ_GUA_RELATION_ID = T.CREDIT_BIZ_GUA_RELATION_ID
WHERE
(T.CREDIT_BIZ_GUA_RELATION_ID IS NULL)
 OR N.GUARANTY_TYPE_CD<>T.GUARANTY_TYPE_CD
 OR N.GUARANTY_ID<>T.GUARANTY_ID
 OR N.BIZ_LIMIT_CONT_NUM<>T.BIZ_LIMIT_CONT_NUM
 OR N.BIZ_DETAIL_ID<>T.BIZ_DETAIL_ID
 OR N.GUARANTY_AMT<>T.GUARANTY_AMT
 OR N.GUARANTY_RELATION_TYPE_CD<>T.GUARANTY_RELATION_TYPE_CD
 OR N.CUSTOMER_NUM<>T.CUSTOMER_NUM
 OR N.CURRENCY_CD<>T.CURRENCY_CD
 OR N.GUARANTEE_TYPE<>T.GUARANTEE_TYPE
 OR N.EXCHANGE_RATE<>T.EXCHANGE_RATE
 OR N.GUARANTY_RATE<>T.GUARANTY_RATE
 OR N.EXCEED_GUARANTY_RATE_LIMIT_IND<>T.EXCEED_GUARANTY_RATE_LIMIT_IND
 OR N.CHANGE_ACTION_CD<>T.CHANGE_ACTION_CD
 OR N.VALID_IND<>T.VALID_IND
 OR N.SYS_UPDATE_TIME<>T.SYS_UPDATE_TIME
 OR N.CHANGE_COMMENTS<>T.CHANGE_COMMENTS
 OR N.GUARANTY_NAME<>T.GUARANTY_NAME
 OR N.GUARANTY_USE_AMT<>T.GUARANTY_USE_AMT
 OR N.SETTLED_ADD_IND<>T.SETTLED_ADD_IND
 OR N.INDEPOT_IND<>T.INDEPOT_IND
 OR N.GRT_NUM<>T.GRT_NUM
 OR N.GUARANTY_STATUS_CD<>T.GUARANTY_STATUS_CD
 OR N.REG_STATUS<>T.REG_STATUS
 OR N.RSPCODE<>T.RSPCODE
 OR N.RSPTIME<>T.RSPTIME
 OR N.IS_GUARANTY_NOTARIAL<>T.IS_GUARANTY_NOTARIAL
 OR N.IS_GUARANTY_INSURE<>T.IS_GUARANTY_INSURE
 OR N.IS_IMPAWN_WARDSHIP<>T.IS_IMPAWN_WARDSHIP
 OR N.IS_EXCEED_REMARK<>T.IS_EXCEED_REMARK
 OR N.IMPAWN_UP_LIMIT<>T.IMPAWN_UP_LIMIT
 OR N.GUARANTY_START_DATE<>T.GUARANTY_START_DATE
 OR N.GUARANTY_END_DATE<>T.GUARANTY_END_DATE
 OR N.GUARANTEE_TYPE_CD<>T.GUARANTEE_TYPE_CD
 OR N.GUARANTEE_FREE_LIMIT<>T.GUARANTEE_FREE_LIMIT
 OR N.IF_OLD_RELATION<>T.IF_OLD_RELATION
 OR N.FXBS_TIME_MARK<>T.FXBS_TIME_MARK
 OR N.CONFIRMATION_SHEET_NUM<>T.CONFIRMATION_SHEET_NUM
 OR N.CHANGE_BIZ_NUM<>T.CHANGE_BIZ_NUM
 OR N.GUA_CHANGE_TYPE<>T.GUA_CHANGE_TYPE
 OR N.GUARANTY_RATE_ONE<>T.GUARANTY_RATE_ONE
 OR N.GUARANTY_RATE_TWO<>T.GUARANTY_RATE_TWO
 OR N.GUARANTY_RATE_THREE<>T.GUARANTY_RATE_THREE
 OR N.GUARANTY_RATE_FOUR<>T.GUARANTY_RATE_FOUR
 OR N.GUARANTY_RATE_FIVE<>T.GUARANTY_RATE_FIVE
 OR N.IF_CREDIT_REPLACE<>T.IF_CREDIT_REPLACE
 OR N.GUARANTEE_PREOCCUPY_LIMIT<>T.GUARANTEE_PREOCCUPY_LIMIT
;

--Step3:
UPDATE dw_sdata.CCS_004_TB_GRT_BUSINESS_RELATION P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_120
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CREDIT_BIZ_GUA_RELATION_ID=T_120.CREDIT_BIZ_GUA_RELATION_ID
;

--Step4:
INSERT  INTO dw_sdata.CCS_004_TB_GRT_BUSINESS_RELATION SELECT * FROM T_120;

COMMIT;

ENDOFINPUT

  close(VSQL);

  my $RET_CODE = $? >> 8;

  if ( $RET_CODE == 0 ) {
      return 0;
  }
  else {
      return 1;
  }
}

# ------------ main function ------------
sub main
{
   my $ret;
   open(LOGONFILE_H, "${LOGON_FILE}");
   ${LOGON_STR} = <LOGONFILE_H>;
   close(LOGONFILE_H);
   
   # Get the decoded logon string
   my($user,$passwd) = split(',',${LOGON_STR}); 
   #my $decodepasswd = `${AUTO_HOME}/bin/IceCode.exe -d "$passwd" "$user"`;                     
   #${LOGON_STR} = "|vsql -h 192.168.2.44 -p 5433 -d CPCIMDB_TEST -U ".$user." -w ".$decodepasswd;

   # Call vsql command to load data
   $ret = run_vsql_command();

   print "run_vsql_command() = $ret";
   return $ret;
}

# ------------ program section ------------
if ( $#ARGV < 0 ) {
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
";
   print "
";
   exit(1);
}

# Get the first argument
${CONTROL_FILE} = $ARGV[0];

if (${CONTROL_FILE} =~/[0-9]{8}($|\.)/) {
   ${TX_DATE_YYYYMMDD} = substr($&,0,8);
}
else{
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
";
   print "
";
   exit(1);
}

${TX_MON_DAY_MMDD} = substr(${TX_DATE_YYYYMMDD}, length(${TX_DATE_YYYYMMDD})-4,4);
${TX_DATE} = substr(${TX_DATE_YYYYMMDD}, 0, 4)."-".substr(${TX_DATE_YYYYMMDD}, 4, 2)."-".substr(${TX_DATE_YYYYMMDD}, 6, 2);
open(STDERR, ">&STDOUT");

my $ret = main();

exit($ret);