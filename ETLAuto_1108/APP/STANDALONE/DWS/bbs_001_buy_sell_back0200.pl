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
DELETE FROM dw_sdata.BBS_001_BUY_SELL_BACK WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_BUY_SELL_BACK SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_42 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_BUY_SELL_BACK WHERE 1=0;

--Step2:
INSERT  INTO T_42 (
  ID,
  BRANCH_ID,
  CONTRACT_ID,
  AUDIT_STATUS,
  ACCOUNT_STATUS,
  CALC_STATUS,
  BUY_SELL_RATE_TYPE,
  BUY_SELL_RATE,
  EXP_STATUS,
  RETURN_DATE,
  STTLM_MK,
  OPERATOR_ID,
  TXN_DATE,
  APPNO,
  MISC,
  LAST_UPD_OPER_ID,
  LAST_UPD_TIME,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.BRANCH_ID,
  N.CONTRACT_ID,
  N.AUDIT_STATUS,
  N.ACCOUNT_STATUS,
  N.CALC_STATUS,
  N.BUY_SELL_RATE_TYPE,
  N.BUY_SELL_RATE,
  N.EXP_STATUS,
  N.RETURN_DATE,
  N.STTLM_MK,
  N.OPERATOR_ID,
  N.TXN_DATE,
  N.APPNO,
  N.MISC,
  N.LAST_UPD_OPER_ID,
  N.LAST_UPD_TIME,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(BRANCH_ID, 0 ) AS BRANCH_ID ,
  COALESCE(CONTRACT_ID, 0 ) AS CONTRACT_ID ,
  COALESCE(AUDIT_STATUS, '' ) AS AUDIT_STATUS ,
  COALESCE(ACCOUNT_STATUS, '' ) AS ACCOUNT_STATUS ,
  COALESCE(CALC_STATUS, '' ) AS CALC_STATUS ,
  COALESCE(BUY_SELL_RATE_TYPE, '' ) AS BUY_SELL_RATE_TYPE ,
  COALESCE(BUY_SELL_RATE, 0 ) AS BUY_SELL_RATE ,
  COALESCE(EXP_STATUS, '' ) AS EXP_STATUS ,
  COALESCE(RETURN_DATE, '' ) AS RETURN_DATE ,
  COALESCE(STTLM_MK, '' ) AS STTLM_MK ,
  COALESCE(OPERATOR_ID, 0 ) AS OPERATOR_ID ,
  COALESCE(TXN_DATE, '' ) AS TXN_DATE ,
  COALESCE(APPNO, '' ) AS APPNO ,
  COALESCE(MISC, '' ) AS MISC ,
  COALESCE(LAST_UPD_OPER_ID, 0 ) AS LAST_UPD_OPER_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME 
 FROM  dw_tdata.BBS_001_BUY_SELL_BACK_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  BRANCH_ID ,
  CONTRACT_ID ,
  AUDIT_STATUS ,
  ACCOUNT_STATUS ,
  CALC_STATUS ,
  BUY_SELL_RATE_TYPE ,
  BUY_SELL_RATE ,
  EXP_STATUS ,
  RETURN_DATE ,
  STTLM_MK ,
  OPERATOR_ID ,
  TXN_DATE ,
  APPNO ,
  MISC ,
  LAST_UPD_OPER_ID ,
  LAST_UPD_TIME 
 FROM dw_sdata.BBS_001_BUY_SELL_BACK 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.BRANCH_ID<>T.BRANCH_ID
 OR N.CONTRACT_ID<>T.CONTRACT_ID
 OR N.AUDIT_STATUS<>T.AUDIT_STATUS
 OR N.ACCOUNT_STATUS<>T.ACCOUNT_STATUS
 OR N.CALC_STATUS<>T.CALC_STATUS
 OR N.BUY_SELL_RATE_TYPE<>T.BUY_SELL_RATE_TYPE
 OR N.BUY_SELL_RATE<>T.BUY_SELL_RATE
 OR N.EXP_STATUS<>T.EXP_STATUS
 OR N.RETURN_DATE<>T.RETURN_DATE
 OR N.STTLM_MK<>T.STTLM_MK
 OR N.OPERATOR_ID<>T.OPERATOR_ID
 OR N.TXN_DATE<>T.TXN_DATE
 OR N.APPNO<>T.APPNO
 OR N.MISC<>T.MISC
 OR N.LAST_UPD_OPER_ID<>T.LAST_UPD_OPER_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
;

--Step3:
UPDATE dw_sdata.BBS_001_BUY_SELL_BACK P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_42
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_42.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_BUY_SELL_BACK SELECT * FROM T_42;

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
