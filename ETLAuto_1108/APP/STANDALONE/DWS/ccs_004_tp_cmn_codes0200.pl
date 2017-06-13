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
DELETE FROM dw_sdata.CCS_004_TP_CMN_CODES WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_004_TP_CMN_CODES SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_125 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_004_TP_CMN_CODES WHERE 1=0;

--Step2:
INSERT  INTO T_125 (
  CODE_ID,
  PARENT_CODE_ID,
  CODE_LEVEL_CD,
  CODE_CD,
  CODE_KEY,
  CODE_NAME,
  CODE_TYPE_CD,
  ORDER_NO,
  DEFAULT_IND,
  EC_CODE,
  IS_FLAG,
  FXBS_TIME_MARK,
  start_dt,
  end_dt)
SELECT
  N.CODE_ID,
  N.PARENT_CODE_ID,
  N.CODE_LEVEL_CD,
  N.CODE_CD,
  N.CODE_KEY,
  N.CODE_NAME,
  N.CODE_TYPE_CD,
  N.ORDER_NO,
  N.DEFAULT_IND,
  N.EC_CODE,
  N.IS_FLAG,
  N.FXBS_TIME_MARK,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CODE_ID, '' ) AS CODE_ID ,
  COALESCE(PARENT_CODE_ID, '' ) AS PARENT_CODE_ID ,
  COALESCE(CODE_LEVEL_CD, '' ) AS CODE_LEVEL_CD ,
  COALESCE(CODE_CD, '' ) AS CODE_CD ,
  COALESCE(CODE_KEY, '' ) AS CODE_KEY ,
  COALESCE(CODE_NAME, '' ) AS CODE_NAME ,
  COALESCE(CODE_TYPE_CD, '' ) AS CODE_TYPE_CD ,
  COALESCE(ORDER_NO, '' ) AS ORDER_NO ,
  COALESCE(DEFAULT_IND, '' ) AS DEFAULT_IND ,
  COALESCE(EC_CODE, '' ) AS EC_CODE ,
  COALESCE(IS_FLAG, '' ) AS IS_FLAG ,
  COALESCE(FXBS_TIME_MARK,'4999-12-31 00:00:00' ) AS FXBS_TIME_MARK 
 FROM  dw_tdata.CCS_004_TP_CMN_CODES_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CODE_ID ,
  PARENT_CODE_ID ,
  CODE_LEVEL_CD ,
  CODE_CD ,
  CODE_KEY ,
  CODE_NAME ,
  CODE_TYPE_CD ,
  ORDER_NO ,
  DEFAULT_IND ,
  EC_CODE ,
  IS_FLAG ,
  FXBS_TIME_MARK 
 FROM dw_sdata.CCS_004_TP_CMN_CODES 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CODE_ID = T.CODE_ID
WHERE
(T.CODE_ID IS NULL)
 OR N.PARENT_CODE_ID<>T.PARENT_CODE_ID
 OR N.CODE_LEVEL_CD<>T.CODE_LEVEL_CD
 OR N.CODE_CD<>T.CODE_CD
 OR N.CODE_KEY<>T.CODE_KEY
 OR N.CODE_NAME<>T.CODE_NAME
 OR N.CODE_TYPE_CD<>T.CODE_TYPE_CD
 OR N.ORDER_NO<>T.ORDER_NO
 OR N.DEFAULT_IND<>T.DEFAULT_IND
 OR N.EC_CODE<>T.EC_CODE
 OR N.IS_FLAG<>T.IS_FLAG
 OR N.FXBS_TIME_MARK<>T.FXBS_TIME_MARK
;

--Step3:
UPDATE dw_sdata.CCS_004_TP_CMN_CODES P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_125
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CODE_ID=T_125.CODE_ID
;

--Step4:
INSERT  INTO dw_sdata.CCS_004_TP_CMN_CODES SELECT * FROM T_125;

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
