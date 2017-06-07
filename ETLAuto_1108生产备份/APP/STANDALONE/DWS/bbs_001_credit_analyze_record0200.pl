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
DELETE FROM dw_sdata.BBS_001_CREDIT_ANALYZE_RECORD WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_CREDIT_ANALYZE_RECORD SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_43 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_CREDIT_ANALYZE_RECORD WHERE 1=0;

--Step2:
INSERT  INTO T_43 (
  ID,
  BIZ_TYPE,
  STATE,
  CREDIT_ID,
  CREDIT_REAL_ID,
  CONTRACT_ID,
  DETAIL_ID,
  DRAFT_ID,
  DRAFT_NO,
  CREDIT_CUST_ID,
  CREDIT_CUST_NM,
  PRODUCT_ID,
  PRODUCT_NM,
  TXN_AMT,
  BEFORE_AMT,
  LEFT_AMT,
  MISC,
  LAST_UPD_OPER_ID,
  LAST_UPD_TIME,
  CREDIT_FLAG,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.BIZ_TYPE,
  N.STATE,
  N.CREDIT_ID,
  N.CREDIT_REAL_ID,
  N.CONTRACT_ID,
  N.DETAIL_ID,
  N.DRAFT_ID,
  N.DRAFT_NO,
  N.CREDIT_CUST_ID,
  N.CREDIT_CUST_NM,
  N.PRODUCT_ID,
  N.PRODUCT_NM,
  N.TXN_AMT,
  N.BEFORE_AMT,
  N.LEFT_AMT,
  N.MISC,
  N.LAST_UPD_OPER_ID,
  N.LAST_UPD_TIME,
  N.CREDIT_FLAG,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(BIZ_TYPE, '' ) AS BIZ_TYPE ,
  COALESCE(STATE, '' ) AS STATE ,
  COALESCE(CREDIT_ID, 0 ) AS CREDIT_ID ,
  COALESCE(CREDIT_REAL_ID, 0 ) AS CREDIT_REAL_ID ,
  COALESCE(CONTRACT_ID, 0 ) AS CONTRACT_ID ,
  COALESCE(DETAIL_ID, 0 ) AS DETAIL_ID ,
  COALESCE(DRAFT_ID, 0 ) AS DRAFT_ID ,
  COALESCE(DRAFT_NO, '' ) AS DRAFT_NO ,
  COALESCE(CREDIT_CUST_ID, 0 ) AS CREDIT_CUST_ID ,
  COALESCE(CREDIT_CUST_NM, '' ) AS CREDIT_CUST_NM ,
  COALESCE(PRODUCT_ID, 0 ) AS PRODUCT_ID ,
  COALESCE(PRODUCT_NM, '' ) AS PRODUCT_NM ,
  COALESCE(TXN_AMT, 0 ) AS TXN_AMT ,
  COALESCE(BEFORE_AMT, 0 ) AS BEFORE_AMT ,
  COALESCE(LEFT_AMT, 0 ) AS LEFT_AMT ,
  COALESCE(MISC, '' ) AS MISC ,
  COALESCE(LAST_UPD_OPER_ID, 0 ) AS LAST_UPD_OPER_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(CREDIT_FLAG, '' ) AS CREDIT_FLAG 
 FROM  dw_tdata.BBS_001_CREDIT_ANALYZE_RECORD_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  BIZ_TYPE ,
  STATE ,
  CREDIT_ID ,
  CREDIT_REAL_ID ,
  CONTRACT_ID ,
  DETAIL_ID ,
  DRAFT_ID ,
  DRAFT_NO ,
  CREDIT_CUST_ID ,
  CREDIT_CUST_NM ,
  PRODUCT_ID ,
  PRODUCT_NM ,
  TXN_AMT ,
  BEFORE_AMT ,
  LEFT_AMT ,
  MISC ,
  LAST_UPD_OPER_ID ,
  LAST_UPD_TIME ,
  CREDIT_FLAG 
 FROM dw_sdata.BBS_001_CREDIT_ANALYZE_RECORD 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.BIZ_TYPE<>T.BIZ_TYPE
 OR N.STATE<>T.STATE
 OR N.CREDIT_ID<>T.CREDIT_ID
 OR N.CREDIT_REAL_ID<>T.CREDIT_REAL_ID
 OR N.CONTRACT_ID<>T.CONTRACT_ID
 OR N.DETAIL_ID<>T.DETAIL_ID
 OR N.DRAFT_ID<>T.DRAFT_ID
 OR N.DRAFT_NO<>T.DRAFT_NO
 OR N.CREDIT_CUST_ID<>T.CREDIT_CUST_ID
 OR N.CREDIT_CUST_NM<>T.CREDIT_CUST_NM
 OR N.PRODUCT_ID<>T.PRODUCT_ID
 OR N.PRODUCT_NM<>T.PRODUCT_NM
 OR N.TXN_AMT<>T.TXN_AMT
 OR N.BEFORE_AMT<>T.BEFORE_AMT
 OR N.LEFT_AMT<>T.LEFT_AMT
 OR N.MISC<>T.MISC
 OR N.LAST_UPD_OPER_ID<>T.LAST_UPD_OPER_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.CREDIT_FLAG<>T.CREDIT_FLAG
;

--Step3:
UPDATE dw_sdata.BBS_001_CREDIT_ANALYZE_RECORD P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_43
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_43.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_CREDIT_ANALYZE_RECORD SELECT * FROM T_43;

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
   print "Usage: [perl 程序名 Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
   print "Usage: [perl 程序名 Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
