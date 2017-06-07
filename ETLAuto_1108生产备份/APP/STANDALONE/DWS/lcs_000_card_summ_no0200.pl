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
DELETE FROM dw_sdata.LCS_000_CARD_SUMM_NO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.LCS_000_CARD_SUMM_NO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_264 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.LCS_000_CARD_SUMM_NO WHERE 1=0;

--Step2:
INSERT  INTO T_264 (
  INNTRAN_CODE,
  TRAN_TYPE,
  TRAN_ATTR,
  AMT_TYPE,
  CHNL_NO,
  AMT_MODE,
  RPT_SUMM_NO,
  LOG_SUMM_NO,
  start_dt,
  end_dt)
SELECT
  N.INNTRAN_CODE,
  N.TRAN_TYPE,
  N.TRAN_ATTR,
  N.AMT_TYPE,
  N.CHNL_NO,
  N.AMT_MODE,
  N.RPT_SUMM_NO,
  N.LOG_SUMM_NO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INNTRAN_CODE, '' ) AS INNTRAN_CODE ,
  COALESCE(TRAN_TYPE, 0 ) AS TRAN_TYPE ,
  COALESCE(TRAN_ATTR, 0 ) AS TRAN_ATTR ,
  COALESCE(AMT_TYPE, 0 ) AS AMT_TYPE ,
  COALESCE(CHNL_NO, '' ) AS CHNL_NO ,
  COALESCE(AMT_MODE, 0 ) AS AMT_MODE ,
  COALESCE(RPT_SUMM_NO, 0 ) AS RPT_SUMM_NO ,
  COALESCE(LOG_SUMM_NO, 0 ) AS LOG_SUMM_NO 
 FROM  dw_tdata.LCS_000_CARD_SUMM_NO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INNTRAN_CODE ,
  TRAN_TYPE ,
  TRAN_ATTR ,
  AMT_TYPE ,
  CHNL_NO ,
  AMT_MODE ,
  RPT_SUMM_NO ,
  LOG_SUMM_NO 
 FROM dw_sdata.LCS_000_CARD_SUMM_NO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INNTRAN_CODE = T.INNTRAN_CODE AND N.TRAN_TYPE = T.TRAN_TYPE AND N.TRAN_ATTR = T.TRAN_ATTR AND N.AMT_TYPE = T.AMT_TYPE
WHERE
(T.INNTRAN_CODE IS NULL AND T.TRAN_TYPE IS NULL AND T.TRAN_ATTR IS NULL AND T.AMT_TYPE IS NULL)
 OR N.CHNL_NO<>T.CHNL_NO
 OR N.AMT_MODE<>T.AMT_MODE
 OR N.RPT_SUMM_NO<>T.RPT_SUMM_NO
 OR N.LOG_SUMM_NO<>T.LOG_SUMM_NO
;

--Step3:
UPDATE dw_sdata.LCS_000_CARD_SUMM_NO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_264
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INNTRAN_CODE=T_264.INNTRAN_CODE
AND P.TRAN_TYPE=T_264.TRAN_TYPE
AND P.TRAN_ATTR=T_264.TRAN_ATTR
AND P.AMT_TYPE=T_264.AMT_TYPE
;

--Step4:
INSERT  INTO dw_sdata.LCS_000_CARD_SUMM_NO SELECT * FROM T_264;

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
