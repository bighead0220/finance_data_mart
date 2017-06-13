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
DELETE FROM dw_sdata.LCS_000_PUB_ITMRATE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.LCS_000_PUB_ITMRATE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_308 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.LCS_000_PUB_ITMRATE WHERE 1=0;

--Step2:
INSERT  INTO T_308 (
  ITM_NO,
  ITM_NAME,
  CURR_TYPE,
  INT_CAL_WAY,
  RATE_KIND,
  RATE_LEV,
  RATE_NO,
  start_dt,
  end_dt)
SELECT
  N.ITM_NO,
  N.ITM_NAME,
  N.CURR_TYPE,
  N.INT_CAL_WAY,
  N.RATE_KIND,
  N.RATE_LEV,
  N.RATE_NO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ITM_NO, '' ) AS ITM_NO ,
  COALESCE(ITM_NAME, '' ) AS ITM_NAME ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(INT_CAL_WAY, '' ) AS INT_CAL_WAY ,
  COALESCE(RATE_KIND, '' ) AS RATE_KIND ,
  COALESCE(RATE_LEV, '' ) AS RATE_LEV ,
  COALESCE(RATE_NO, 0 ) AS RATE_NO 
 FROM  dw_tdata.LCS_000_PUB_ITMRATE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ITM_NO ,
  ITM_NAME ,
  CURR_TYPE ,
  INT_CAL_WAY ,
  RATE_KIND ,
  RATE_LEV ,
  RATE_NO 
 FROM dw_sdata.LCS_000_PUB_ITMRATE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ITM_NO = T.ITM_NO AND N.CURR_TYPE = T.CURR_TYPE AND N.RATE_KIND = T.RATE_KIND AND N.RATE_LEV = T.RATE_LEV
WHERE
(T.ITM_NO IS NULL AND T.CURR_TYPE IS NULL AND T.RATE_KIND IS NULL AND T.RATE_LEV IS NULL)
 OR N.ITM_NAME<>T.ITM_NAME
 OR N.INT_CAL_WAY<>T.INT_CAL_WAY
 OR N.RATE_NO<>T.RATE_NO
;

--Step3:
UPDATE dw_sdata.LCS_000_PUB_ITMRATE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_308
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ITM_NO=T_308.ITM_NO
AND P.CURR_TYPE=T_308.CURR_TYPE
AND P.RATE_KIND=T_308.RATE_KIND
AND P.RATE_LEV=T_308.RATE_LEV
;

--Step4:
INSERT  INTO dw_sdata.LCS_000_PUB_ITMRATE SELECT * FROM T_308;

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
