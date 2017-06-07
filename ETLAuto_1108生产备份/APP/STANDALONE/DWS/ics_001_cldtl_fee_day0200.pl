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
DELETE FROM dw_sdata.ICS_001_CLDTL_FEE_DAY WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ICS_001_CLDTL_FEE_DAY SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_224 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ICS_001_CLDTL_FEE_DAY WHERE 1=0;

--Step2:
INSERT  INTO T_224 (
  CLR_DATE,
  INSU_CODE,
  FEE_SETTLE_UNIT,
  TX_SETTLE_UNIT,
  FEE_TYPE,
  FEE_IN_FLAG,
  POST_FLAG,
  FEE_AMT,
  start_dt,
  end_dt)
SELECT
  N.CLR_DATE,
  N.INSU_CODE,
  N.FEE_SETTLE_UNIT,
  N.TX_SETTLE_UNIT,
  N.FEE_TYPE,
  N.FEE_IN_FLAG,
  N.POST_FLAG,
  N.FEE_AMT,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(INSU_CODE, '' ) AS INSU_CODE ,
  COALESCE(FEE_SETTLE_UNIT, '' ) AS FEE_SETTLE_UNIT ,
  COALESCE(TX_SETTLE_UNIT, '' ) AS TX_SETTLE_UNIT ,
  COALESCE(FEE_TYPE, '' ) AS FEE_TYPE ,
  COALESCE(FEE_IN_FLAG, '' ) AS FEE_IN_FLAG ,
  COALESCE(POST_FLAG, '' ) AS POST_FLAG ,
  COALESCE(FEE_AMT, 0 ) AS FEE_AMT 
 FROM  dw_tdata.ICS_001_CLDTL_FEE_DAY_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CLR_DATE ,
  INSU_CODE ,
  FEE_SETTLE_UNIT ,
  TX_SETTLE_UNIT ,
  FEE_TYPE ,
  FEE_IN_FLAG ,
  POST_FLAG ,
  FEE_AMT 
 FROM dw_sdata.ICS_001_CLDTL_FEE_DAY 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CLR_DATE = T.CLR_DATE AND N.INSU_CODE = T.INSU_CODE AND N.FEE_SETTLE_UNIT = T.FEE_SETTLE_UNIT AND N.TX_SETTLE_UNIT = T.TX_SETTLE_UNIT AND N.FEE_TYPE = T.FEE_TYPE AND N.FEE_IN_FLAG = T.FEE_IN_FLAG AND N.POST_FLAG = T.POST_FLAG
WHERE
(T.CLR_DATE IS NULL AND T.INSU_CODE IS NULL AND T.FEE_SETTLE_UNIT IS NULL AND T.TX_SETTLE_UNIT IS NULL AND T.FEE_TYPE IS NULL AND T.FEE_IN_FLAG IS NULL AND T.POST_FLAG IS NULL)
 OR N.FEE_AMT<>T.FEE_AMT
;

--Step3:
UPDATE dw_sdata.ICS_001_CLDTL_FEE_DAY P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_224
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CLR_DATE=T_224.CLR_DATE
AND P.INSU_CODE=T_224.INSU_CODE
AND P.FEE_SETTLE_UNIT=T_224.FEE_SETTLE_UNIT
AND P.TX_SETTLE_UNIT=T_224.TX_SETTLE_UNIT
AND P.FEE_TYPE=T_224.FEE_TYPE
AND P.FEE_IN_FLAG=T_224.FEE_IN_FLAG
AND P.POST_FLAG=T_224.POST_FLAG
;

--Step4:
INSERT  INTO dw_sdata.ICS_001_CLDTL_FEE_DAY SELECT * FROM T_224;

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
