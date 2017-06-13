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
DELETE FROM dw_sdata.ICS_001_SMCTL_TX_MAIN WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ICS_001_SMCTL_TX_MAIN SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_229 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ICS_001_SMCTL_TX_MAIN WHERE 1=0;

--Step2:
INSERT  INTO T_229 (
  TX_CODE,
  JNL_FLAG,
  COND_TX,
  TX_TYPE,
  QX_FLAG,
  CHECK_FLAG,
  REPEAT_FLAG,
  MNT_FLAG,
  PRE_SVC_NAME,
  SVC_NAME,
  VALID_FLAG,
  OUTSYS_CODE,
  CRSP_TX_CODE,
  TIMEOUT,
  PERMIT_FLAG,
  DEBUG_FLAG,
  FML_FLAG,
  start_dt,
  end_dt)
SELECT
  N.TX_CODE,
  N.JNL_FLAG,
  N.COND_TX,
  N.TX_TYPE,
  N.QX_FLAG,
  N.CHECK_FLAG,
  N.REPEAT_FLAG,
  N.MNT_FLAG,
  N.PRE_SVC_NAME,
  N.SVC_NAME,
  N.VALID_FLAG,
  N.OUTSYS_CODE,
  N.CRSP_TX_CODE,
  N.TIMEOUT,
  N.PERMIT_FLAG,
  N.DEBUG_FLAG,
  N.FML_FLAG,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(TX_CODE, '' ) AS TX_CODE ,
  COALESCE(JNL_FLAG, '' ) AS JNL_FLAG ,
  COALESCE(COND_TX, '' ) AS COND_TX ,
  COALESCE(TX_TYPE, '' ) AS TX_TYPE ,
  COALESCE(QX_FLAG, '' ) AS QX_FLAG ,
  COALESCE(CHECK_FLAG, '' ) AS CHECK_FLAG ,
  COALESCE(REPEAT_FLAG, '' ) AS REPEAT_FLAG ,
  COALESCE(MNT_FLAG, '' ) AS MNT_FLAG ,
  COALESCE(PRE_SVC_NAME, '' ) AS PRE_SVC_NAME ,
  COALESCE(SVC_NAME, '' ) AS SVC_NAME ,
  COALESCE(VALID_FLAG, '' ) AS VALID_FLAG ,
  COALESCE(OUTSYS_CODE, '' ) AS OUTSYS_CODE ,
  COALESCE(CRSP_TX_CODE, '' ) AS CRSP_TX_CODE ,
  COALESCE(TIMEOUT, 0 ) AS TIMEOUT ,
  COALESCE(PERMIT_FLAG, '' ) AS PERMIT_FLAG ,
  COALESCE(DEBUG_FLAG, '' ) AS DEBUG_FLAG ,
  COALESCE(FML_FLAG, '' ) AS FML_FLAG 
 FROM  dw_tdata.ICS_001_SMCTL_TX_MAIN_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  TX_CODE ,
  JNL_FLAG ,
  COND_TX ,
  TX_TYPE ,
  QX_FLAG ,
  CHECK_FLAG ,
  REPEAT_FLAG ,
  MNT_FLAG ,
  PRE_SVC_NAME ,
  SVC_NAME ,
  VALID_FLAG ,
  OUTSYS_CODE ,
  CRSP_TX_CODE ,
  TIMEOUT ,
  PERMIT_FLAG ,
  DEBUG_FLAG ,
  FML_FLAG 
 FROM dw_sdata.ICS_001_SMCTL_TX_MAIN 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.TX_CODE = T.TX_CODE
WHERE
(T.TX_CODE IS NULL)
 OR N.JNL_FLAG<>T.JNL_FLAG
 OR N.COND_TX<>T.COND_TX
 OR N.TX_TYPE<>T.TX_TYPE
 OR N.QX_FLAG<>T.QX_FLAG
 OR N.CHECK_FLAG<>T.CHECK_FLAG
 OR N.REPEAT_FLAG<>T.REPEAT_FLAG
 OR N.MNT_FLAG<>T.MNT_FLAG
 OR N.PRE_SVC_NAME<>T.PRE_SVC_NAME
 OR N.SVC_NAME<>T.SVC_NAME
 OR N.VALID_FLAG<>T.VALID_FLAG
 OR N.OUTSYS_CODE<>T.OUTSYS_CODE
 OR N.CRSP_TX_CODE<>T.CRSP_TX_CODE
 OR N.TIMEOUT<>T.TIMEOUT
 OR N.PERMIT_FLAG<>T.PERMIT_FLAG
 OR N.DEBUG_FLAG<>T.DEBUG_FLAG
 OR N.FML_FLAG<>T.FML_FLAG
;

--Step3:
UPDATE dw_sdata.ICS_001_SMCTL_TX_MAIN P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_229
WHERE P.End_Dt=DATE('2100-12-31')
AND P.TX_CODE=T_229.TX_CODE
;

--Step4:
INSERT  INTO dw_sdata.ICS_001_SMCTL_TX_MAIN SELECT * FROM T_229;

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
