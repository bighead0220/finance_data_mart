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
DELETE FROM dw_sdata.ECF_001_T09_ASSET_ASSESS WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ECF_001_T09_ASSET_ASSESS SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_165 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ECF_001_T09_ASSET_ASSESS WHERE 1=0;

--Step2:
INSERT  INTO T_165 (
  ASSET_ID,
  PARTY_ID,
  STAT_ORG_ID,
  SUMM_DATE,
  FIN_ASSET_AMT,
  LOCAL_CUR_BAL_AMT,
  LOCAL_FIX_BAL_AMT,
  TO_RMB_CUR_BAL_AMT,
  TO_RMB_FIX_BAL_AMT,
  LOCAL_CUR_MON_AVG_BAL_AMT_L1,
  LOCAL_CUR_MON_AVG_BAL_AMT_L3,
  LOCAL_CUR_MON_AVG_BAL_AMT_L6,
  LOCAL_CUR_QUA_AVG_BAL_AMT,
  LOCAL_CUR_YR_AVG_BAL_AMT,
  LOCAL_FIX_MON_AVG_BAL_AMT_L1,
  LOCAL_FIX_MON_AVG_BAL_AMT_L3,
  LOCAL_FIX_MON_AVG_BAL_AMT_L6,
  LOCAL_FIX_QUA_AVG_BAL_AMT,
  LOCAL_FIX_YR_AVG_BAL_AMT,
  TO_RMB_CUR_MON_AVG_BAL_AMT_L1,
  TO_RMB_CUR_MON_AVG_BAL_AMT_L3,
  TO_RMB_CUR_MON_AVG_BAL_AMT_L6,
  TO_RMB_CUR_QUA_AVG_BAL_AMT,
  TO_RMB_CUR_YR_AVG_BAL_AMT,
  TO_RMB_FIX_MON_AVG_BAL_AMT_L1,
  TO_RMB_FIX_MON_AVG_BAL_AMT_L3,
  TO_RMB_FIX_MON_AVG_BAL_AMT_L6,
  TO_RMB_FIX_QUA_AVG_BAL_AMT,
  TO_RMB_FIX_YR_AVG_BAL_AMT,
  TXDATE,
  FIRST_ACCT_OPEN_DATE,
  LAST_UPDATED_TE,
  LAST_UPDATED_ORG,
  CREATED_TS,
  UPDATED_TS,
  INIT_SYSTEM_ID,
  INIT_CREATED_TS,
  LAST_SYSTEM_ID,
  LAST_UPDATED_TS,
  start_dt,
  end_dt)
SELECT
  N.ASSET_ID,
  N.PARTY_ID,
  N.STAT_ORG_ID,
  N.SUMM_DATE,
  N.FIN_ASSET_AMT,
  N.LOCAL_CUR_BAL_AMT,
  N.LOCAL_FIX_BAL_AMT,
  N.TO_RMB_CUR_BAL_AMT,
  N.TO_RMB_FIX_BAL_AMT,
  N.LOCAL_CUR_MON_AVG_BAL_AMT_L1,
  N.LOCAL_CUR_MON_AVG_BAL_AMT_L3,
  N.LOCAL_CUR_MON_AVG_BAL_AMT_L6,
  N.LOCAL_CUR_QUA_AVG_BAL_AMT,
  N.LOCAL_CUR_YR_AVG_BAL_AMT,
  N.LOCAL_FIX_MON_AVG_BAL_AMT_L1,
  N.LOCAL_FIX_MON_AVG_BAL_AMT_L3,
  N.LOCAL_FIX_MON_AVG_BAL_AMT_L6,
  N.LOCAL_FIX_QUA_AVG_BAL_AMT,
  N.LOCAL_FIX_YR_AVG_BAL_AMT,
  N.TO_RMB_CUR_MON_AVG_BAL_AMT_L1,
  N.TO_RMB_CUR_MON_AVG_BAL_AMT_L3,
  N.TO_RMB_CUR_MON_AVG_BAL_AMT_L6,
  N.TO_RMB_CUR_QUA_AVG_BAL_AMT,
  N.TO_RMB_CUR_YR_AVG_BAL_AMT,
  N.TO_RMB_FIX_MON_AVG_BAL_AMT_L1,
  N.TO_RMB_FIX_MON_AVG_BAL_AMT_L3,
  N.TO_RMB_FIX_MON_AVG_BAL_AMT_L6,
  N.TO_RMB_FIX_QUA_AVG_BAL_AMT,
  N.TO_RMB_FIX_YR_AVG_BAL_AMT,
  N.TXDATE,
  N.FIRST_ACCT_OPEN_DATE,
  N.LAST_UPDATED_TE,
  N.LAST_UPDATED_ORG,
  N.CREATED_TS,
  N.UPDATED_TS,
  N.INIT_SYSTEM_ID,
  N.INIT_CREATED_TS,
  N.LAST_SYSTEM_ID,
  N.LAST_UPDATED_TS,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ASSET_ID, '' ) AS ASSET_ID ,
  COALESCE(PARTY_ID, '' ) AS PARTY_ID ,
  COALESCE(STAT_ORG_ID, '' ) AS STAT_ORG_ID ,
  COALESCE(SUMM_DATE, '' ) AS SUMM_DATE ,
  COALESCE(FIN_ASSET_AMT, 0 ) AS FIN_ASSET_AMT ,
  COALESCE(LOCAL_CUR_BAL_AMT, 0 ) AS LOCAL_CUR_BAL_AMT ,
  COALESCE(LOCAL_FIX_BAL_AMT, 0 ) AS LOCAL_FIX_BAL_AMT ,
  COALESCE(TO_RMB_CUR_BAL_AMT, 0 ) AS TO_RMB_CUR_BAL_AMT ,
  COALESCE(TO_RMB_FIX_BAL_AMT, 0 ) AS TO_RMB_FIX_BAL_AMT ,
  COALESCE(LOCAL_CUR_MON_AVG_BAL_AMT_L1, 0 ) AS LOCAL_CUR_MON_AVG_BAL_AMT_L1 ,
  COALESCE(LOCAL_CUR_MON_AVG_BAL_AMT_L3, 0 ) AS LOCAL_CUR_MON_AVG_BAL_AMT_L3 ,
  COALESCE(LOCAL_CUR_MON_AVG_BAL_AMT_L6, 0 ) AS LOCAL_CUR_MON_AVG_BAL_AMT_L6 ,
  COALESCE(LOCAL_CUR_QUA_AVG_BAL_AMT, 0 ) AS LOCAL_CUR_QUA_AVG_BAL_AMT ,
  COALESCE(LOCAL_CUR_YR_AVG_BAL_AMT, 0 ) AS LOCAL_CUR_YR_AVG_BAL_AMT ,
  COALESCE(LOCAL_FIX_MON_AVG_BAL_AMT_L1, 0 ) AS LOCAL_FIX_MON_AVG_BAL_AMT_L1 ,
  COALESCE(LOCAL_FIX_MON_AVG_BAL_AMT_L3, 0 ) AS LOCAL_FIX_MON_AVG_BAL_AMT_L3 ,
  COALESCE(LOCAL_FIX_MON_AVG_BAL_AMT_L6, 0 ) AS LOCAL_FIX_MON_AVG_BAL_AMT_L6 ,
  COALESCE(LOCAL_FIX_QUA_AVG_BAL_AMT, 0 ) AS LOCAL_FIX_QUA_AVG_BAL_AMT ,
  COALESCE(LOCAL_FIX_YR_AVG_BAL_AMT, 0 ) AS LOCAL_FIX_YR_AVG_BAL_AMT ,
  COALESCE(TO_RMB_CUR_MON_AVG_BAL_AMT_L1, 0 ) AS TO_RMB_CUR_MON_AVG_BAL_AMT_L1 ,
  COALESCE(TO_RMB_CUR_MON_AVG_BAL_AMT_L3, 0 ) AS TO_RMB_CUR_MON_AVG_BAL_AMT_L3 ,
  COALESCE(TO_RMB_CUR_MON_AVG_BAL_AMT_L6, 0 ) AS TO_RMB_CUR_MON_AVG_BAL_AMT_L6 ,
  COALESCE(TO_RMB_CUR_QUA_AVG_BAL_AMT, 0 ) AS TO_RMB_CUR_QUA_AVG_BAL_AMT ,
  COALESCE(TO_RMB_CUR_YR_AVG_BAL_AMT, 0 ) AS TO_RMB_CUR_YR_AVG_BAL_AMT ,
  COALESCE(TO_RMB_FIX_MON_AVG_BAL_AMT_L1, 0 ) AS TO_RMB_FIX_MON_AVG_BAL_AMT_L1 ,
  COALESCE(TO_RMB_FIX_MON_AVG_BAL_AMT_L3, 0 ) AS TO_RMB_FIX_MON_AVG_BAL_AMT_L3 ,
  COALESCE(TO_RMB_FIX_MON_AVG_BAL_AMT_L6, 0 ) AS TO_RMB_FIX_MON_AVG_BAL_AMT_L6 ,
  COALESCE(TO_RMB_FIX_QUA_AVG_BAL_AMT, 0 ) AS TO_RMB_FIX_QUA_AVG_BAL_AMT ,
  COALESCE(TO_RMB_FIX_YR_AVG_BAL_AMT, 0 ) AS TO_RMB_FIX_YR_AVG_BAL_AMT ,
  COALESCE(TXDATE, '' ) AS TXDATE ,
  COALESCE(FIRST_ACCT_OPEN_DATE, '' ) AS FIRST_ACCT_OPEN_DATE ,
  COALESCE(LAST_UPDATED_TE, '' ) AS LAST_UPDATED_TE ,
  COALESCE(LAST_UPDATED_ORG, '' ) AS LAST_UPDATED_ORG ,
  COALESCE(CREATED_TS, '' ) AS CREATED_TS ,
  COALESCE(UPDATED_TS, '' ) AS UPDATED_TS ,
  COALESCE(INIT_SYSTEM_ID, '' ) AS INIT_SYSTEM_ID ,
  COALESCE(INIT_CREATED_TS, '' ) AS INIT_CREATED_TS ,
  COALESCE(LAST_SYSTEM_ID, '' ) AS LAST_SYSTEM_ID ,
  COALESCE(LAST_UPDATED_TS, '' ) AS LAST_UPDATED_TS 
 FROM  dw_tdata.ECF_001_T09_ASSET_ASSESS_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ASSET_ID ,
  PARTY_ID ,
  STAT_ORG_ID ,
  SUMM_DATE ,
  FIN_ASSET_AMT ,
  LOCAL_CUR_BAL_AMT ,
  LOCAL_FIX_BAL_AMT ,
  TO_RMB_CUR_BAL_AMT ,
  TO_RMB_FIX_BAL_AMT ,
  LOCAL_CUR_MON_AVG_BAL_AMT_L1 ,
  LOCAL_CUR_MON_AVG_BAL_AMT_L3 ,
  LOCAL_CUR_MON_AVG_BAL_AMT_L6 ,
  LOCAL_CUR_QUA_AVG_BAL_AMT ,
  LOCAL_CUR_YR_AVG_BAL_AMT ,
  LOCAL_FIX_MON_AVG_BAL_AMT_L1 ,
  LOCAL_FIX_MON_AVG_BAL_AMT_L3 ,
  LOCAL_FIX_MON_AVG_BAL_AMT_L6 ,
  LOCAL_FIX_QUA_AVG_BAL_AMT ,
  LOCAL_FIX_YR_AVG_BAL_AMT ,
  TO_RMB_CUR_MON_AVG_BAL_AMT_L1 ,
  TO_RMB_CUR_MON_AVG_BAL_AMT_L3 ,
  TO_RMB_CUR_MON_AVG_BAL_AMT_L6 ,
  TO_RMB_CUR_QUA_AVG_BAL_AMT ,
  TO_RMB_CUR_YR_AVG_BAL_AMT ,
  TO_RMB_FIX_MON_AVG_BAL_AMT_L1 ,
  TO_RMB_FIX_MON_AVG_BAL_AMT_L3 ,
  TO_RMB_FIX_MON_AVG_BAL_AMT_L6 ,
  TO_RMB_FIX_QUA_AVG_BAL_AMT ,
  TO_RMB_FIX_YR_AVG_BAL_AMT ,
  TXDATE ,
  FIRST_ACCT_OPEN_DATE ,
  LAST_UPDATED_TE ,
  LAST_UPDATED_ORG ,
  CREATED_TS ,
  UPDATED_TS ,
  INIT_SYSTEM_ID ,
  INIT_CREATED_TS ,
  LAST_SYSTEM_ID ,
  LAST_UPDATED_TS 
 FROM dw_sdata.ECF_001_T09_ASSET_ASSESS 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ASSET_ID = T.ASSET_ID
WHERE
(T.ASSET_ID IS NULL)
 OR N.PARTY_ID<>T.PARTY_ID
 OR N.STAT_ORG_ID<>T.STAT_ORG_ID
 OR N.SUMM_DATE<>T.SUMM_DATE
 OR N.FIN_ASSET_AMT<>T.FIN_ASSET_AMT
 OR N.LOCAL_CUR_BAL_AMT<>T.LOCAL_CUR_BAL_AMT
 OR N.LOCAL_FIX_BAL_AMT<>T.LOCAL_FIX_BAL_AMT
 OR N.TO_RMB_CUR_BAL_AMT<>T.TO_RMB_CUR_BAL_AMT
 OR N.TO_RMB_FIX_BAL_AMT<>T.TO_RMB_FIX_BAL_AMT
 OR N.LOCAL_CUR_MON_AVG_BAL_AMT_L1<>T.LOCAL_CUR_MON_AVG_BAL_AMT_L1
 OR N.LOCAL_CUR_MON_AVG_BAL_AMT_L3<>T.LOCAL_CUR_MON_AVG_BAL_AMT_L3
 OR N.LOCAL_CUR_MON_AVG_BAL_AMT_L6<>T.LOCAL_CUR_MON_AVG_BAL_AMT_L6
 OR N.LOCAL_CUR_QUA_AVG_BAL_AMT<>T.LOCAL_CUR_QUA_AVG_BAL_AMT
 OR N.LOCAL_CUR_YR_AVG_BAL_AMT<>T.LOCAL_CUR_YR_AVG_BAL_AMT
 OR N.LOCAL_FIX_MON_AVG_BAL_AMT_L1<>T.LOCAL_FIX_MON_AVG_BAL_AMT_L1
 OR N.LOCAL_FIX_MON_AVG_BAL_AMT_L3<>T.LOCAL_FIX_MON_AVG_BAL_AMT_L3
 OR N.LOCAL_FIX_MON_AVG_BAL_AMT_L6<>T.LOCAL_FIX_MON_AVG_BAL_AMT_L6
 OR N.LOCAL_FIX_QUA_AVG_BAL_AMT<>T.LOCAL_FIX_QUA_AVG_BAL_AMT
 OR N.LOCAL_FIX_YR_AVG_BAL_AMT<>T.LOCAL_FIX_YR_AVG_BAL_AMT
 OR N.TO_RMB_CUR_MON_AVG_BAL_AMT_L1<>T.TO_RMB_CUR_MON_AVG_BAL_AMT_L1
 OR N.TO_RMB_CUR_MON_AVG_BAL_AMT_L3<>T.TO_RMB_CUR_MON_AVG_BAL_AMT_L3
 OR N.TO_RMB_CUR_MON_AVG_BAL_AMT_L6<>T.TO_RMB_CUR_MON_AVG_BAL_AMT_L6
 OR N.TO_RMB_CUR_QUA_AVG_BAL_AMT<>T.TO_RMB_CUR_QUA_AVG_BAL_AMT
 OR N.TO_RMB_CUR_YR_AVG_BAL_AMT<>T.TO_RMB_CUR_YR_AVG_BAL_AMT
 OR N.TO_RMB_FIX_MON_AVG_BAL_AMT_L1<>T.TO_RMB_FIX_MON_AVG_BAL_AMT_L1
 OR N.TO_RMB_FIX_MON_AVG_BAL_AMT_L3<>T.TO_RMB_FIX_MON_AVG_BAL_AMT_L3
 OR N.TO_RMB_FIX_MON_AVG_BAL_AMT_L6<>T.TO_RMB_FIX_MON_AVG_BAL_AMT_L6
 OR N.TO_RMB_FIX_QUA_AVG_BAL_AMT<>T.TO_RMB_FIX_QUA_AVG_BAL_AMT
 OR N.TO_RMB_FIX_YR_AVG_BAL_AMT<>T.TO_RMB_FIX_YR_AVG_BAL_AMT
 OR N.TXDATE<>T.TXDATE
 OR N.FIRST_ACCT_OPEN_DATE<>T.FIRST_ACCT_OPEN_DATE
 OR N.LAST_UPDATED_TE<>T.LAST_UPDATED_TE
 OR N.LAST_UPDATED_ORG<>T.LAST_UPDATED_ORG
 OR N.CREATED_TS<>T.CREATED_TS
 OR N.UPDATED_TS<>T.UPDATED_TS
 OR N.INIT_SYSTEM_ID<>T.INIT_SYSTEM_ID
 OR N.INIT_CREATED_TS<>T.INIT_CREATED_TS
 OR N.LAST_SYSTEM_ID<>T.LAST_SYSTEM_ID
 OR N.LAST_UPDATED_TS<>T.LAST_UPDATED_TS
;

--Step3:
UPDATE dw_sdata.ECF_001_T09_ASSET_ASSESS P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_165
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ASSET_ID=T_165.ASSET_ID
;

--Step4:
INSERT  INTO dw_sdata.ECF_001_T09_ASSET_ASSESS SELECT * FROM T_165;

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