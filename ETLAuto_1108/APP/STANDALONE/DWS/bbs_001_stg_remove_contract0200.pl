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
DELETE FROM dw_sdata.BBS_001_STG_REMOVE_CONTRACT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_STG_REMOVE_CONTRACT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_58 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_STG_REMOVE_CONTRACT WHERE 1=0;

--Step2:
INSERT  INTO T_58 (
  ID,
  BUSINESS_NO,
  APPNO,
  BRH_ID,
  CUST_ACCNO,
  CUST_ID,
  STATUS,
  ACCOUNT_STATUS,
  REMOVE_DATE,
  CREATE_OPR_ID,
  CREATE_TIME,
  LAST_UPD_OPR_ID,
  LAST_UPD_TIME,
  RISK_CHECK_STATUS,
  LOGIC_CHECK_STATUS,
  GROUP_FLAG,
  PRETREATMENT_DATE,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.BUSINESS_NO,
  N.APPNO,
  N.BRH_ID,
  N.CUST_ACCNO,
  N.CUST_ID,
  N.STATUS,
  N.ACCOUNT_STATUS,
  N.REMOVE_DATE,
  N.CREATE_OPR_ID,
  N.CREATE_TIME,
  N.LAST_UPD_OPR_ID,
  N.LAST_UPD_TIME,
  N.RISK_CHECK_STATUS,
  N.LOGIC_CHECK_STATUS,
  N.GROUP_FLAG,
  N.PRETREATMENT_DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(BUSINESS_NO, '' ) AS BUSINESS_NO ,
  COALESCE(APPNO, '' ) AS APPNO ,
  COALESCE(BRH_ID, 0 ) AS BRH_ID ,
  COALESCE(CUST_ACCNO, '' ) AS CUST_ACCNO ,
  COALESCE(CUST_ID, 0 ) AS CUST_ID ,
  COALESCE(STATUS, '' ) AS STATUS ,
  COALESCE(ACCOUNT_STATUS, '' ) AS ACCOUNT_STATUS ,
  COALESCE(REMOVE_DATE, '' ) AS REMOVE_DATE ,
  COALESCE(CREATE_OPR_ID, 0 ) AS CREATE_OPR_ID ,
  COALESCE(CREATE_TIME, '' ) AS CREATE_TIME ,
  COALESCE(LAST_UPD_OPR_ID, 0 ) AS LAST_UPD_OPR_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(RISK_CHECK_STATUS, '' ) AS RISK_CHECK_STATUS ,
  COALESCE(LOGIC_CHECK_STATUS, '' ) AS LOGIC_CHECK_STATUS ,
  COALESCE(GROUP_FLAG, '' ) AS GROUP_FLAG ,
  COALESCE(PRETREATMENT_DATE, '' ) AS PRETREATMENT_DATE 
 FROM  dw_tdata.BBS_001_STG_REMOVE_CONTRACT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  BUSINESS_NO ,
  APPNO ,
  BRH_ID ,
  CUST_ACCNO ,
  CUST_ID ,
  STATUS ,
  ACCOUNT_STATUS ,
  REMOVE_DATE ,
  CREATE_OPR_ID ,
  CREATE_TIME ,
  LAST_UPD_OPR_ID ,
  LAST_UPD_TIME ,
  RISK_CHECK_STATUS ,
  LOGIC_CHECK_STATUS ,
  GROUP_FLAG ,
  PRETREATMENT_DATE 
 FROM dw_sdata.BBS_001_STG_REMOVE_CONTRACT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.BUSINESS_NO<>T.BUSINESS_NO
 OR N.APPNO<>T.APPNO
 OR N.BRH_ID<>T.BRH_ID
 OR N.CUST_ACCNO<>T.CUST_ACCNO
 OR N.CUST_ID<>T.CUST_ID
 OR N.STATUS<>T.STATUS
 OR N.ACCOUNT_STATUS<>T.ACCOUNT_STATUS
 OR N.REMOVE_DATE<>T.REMOVE_DATE
 OR N.CREATE_OPR_ID<>T.CREATE_OPR_ID
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.LAST_UPD_OPR_ID<>T.LAST_UPD_OPR_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.RISK_CHECK_STATUS<>T.RISK_CHECK_STATUS
 OR N.LOGIC_CHECK_STATUS<>T.LOGIC_CHECK_STATUS
 OR N.GROUP_FLAG<>T.GROUP_FLAG
 OR N.PRETREATMENT_DATE<>T.PRETREATMENT_DATE
;

--Step3:
UPDATE dw_sdata.BBS_001_STG_REMOVE_CONTRACT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_58
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_58.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_STG_REMOVE_CONTRACT SELECT * FROM T_58;

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
