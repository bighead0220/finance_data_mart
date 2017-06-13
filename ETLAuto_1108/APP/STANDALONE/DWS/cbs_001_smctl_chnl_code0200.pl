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
DELETE FROM dw_sdata.CBS_001_SMCTL_CHNL_CODE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CBS_001_SMCTL_CHNL_CODE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_82 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CBS_001_SMCTL_CHNL_CODE WHERE 1=0;

--Step2:
INSERT  INTO T_82 (
  CHNL_CODE,
  CHNL_NAME,
  CHNL_ON_OFF,
  SET_UNIT_FLAG,
  SET_TERM_FLAG,
  DUMMY_UNIT_CODE,
  DUMMY_OPERATOR,
  START_TIME,
  END_TIME,
  UNIT_INT_FLAG,
  CHNL_TYPE,
  CRSP_CHNL_CODE,
  start_dt,
  end_dt)
SELECT
  N.CHNL_CODE,
  N.CHNL_NAME,
  N.CHNL_ON_OFF,
  N.SET_UNIT_FLAG,
  N.SET_TERM_FLAG,
  N.DUMMY_UNIT_CODE,
  N.DUMMY_OPERATOR,
  N.START_TIME,
  N.END_TIME,
  N.UNIT_INT_FLAG,
  N.CHNL_TYPE,
  N.CRSP_CHNL_CODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CHNL_CODE, '' ) AS CHNL_CODE ,
  COALESCE(CHNL_NAME, '' ) AS CHNL_NAME ,
  COALESCE(CHNL_ON_OFF, '' ) AS CHNL_ON_OFF ,
  COALESCE(SET_UNIT_FLAG, '' ) AS SET_UNIT_FLAG ,
  COALESCE(SET_TERM_FLAG, '' ) AS SET_TERM_FLAG ,
  COALESCE(DUMMY_UNIT_CODE, '' ) AS DUMMY_UNIT_CODE ,
  COALESCE(DUMMY_OPERATOR, '' ) AS DUMMY_OPERATOR ,
  COALESCE(START_TIME, '' ) AS START_TIME ,
  COALESCE(END_TIME, '' ) AS END_TIME ,
  COALESCE(UNIT_INT_FLAG, '' ) AS UNIT_INT_FLAG ,
  COALESCE(CHNL_TYPE, '' ) AS CHNL_TYPE ,
  COALESCE(CRSP_CHNL_CODE, '' ) AS CRSP_CHNL_CODE 
 FROM  dw_tdata.CBS_001_SMCTL_CHNL_CODE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CHNL_CODE ,
  CHNL_NAME ,
  CHNL_ON_OFF ,
  SET_UNIT_FLAG ,
  SET_TERM_FLAG ,
  DUMMY_UNIT_CODE ,
  DUMMY_OPERATOR ,
  START_TIME ,
  END_TIME ,
  UNIT_INT_FLAG ,
  CHNL_TYPE ,
  CRSP_CHNL_CODE 
 FROM dw_sdata.CBS_001_SMCTL_CHNL_CODE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CHNL_CODE = T.CHNL_CODE
WHERE
(T.CHNL_CODE IS NULL)
 OR N.CHNL_NAME<>T.CHNL_NAME
 OR N.CHNL_ON_OFF<>T.CHNL_ON_OFF
 OR N.SET_UNIT_FLAG<>T.SET_UNIT_FLAG
 OR N.SET_TERM_FLAG<>T.SET_TERM_FLAG
 OR N.DUMMY_UNIT_CODE<>T.DUMMY_UNIT_CODE
 OR N.DUMMY_OPERATOR<>T.DUMMY_OPERATOR
 OR N.START_TIME<>T.START_TIME
 OR N.END_TIME<>T.END_TIME
 OR N.UNIT_INT_FLAG<>T.UNIT_INT_FLAG
 OR N.CHNL_TYPE<>T.CHNL_TYPE
 OR N.CRSP_CHNL_CODE<>T.CRSP_CHNL_CODE
;

--Step3:
UPDATE dw_sdata.CBS_001_SMCTL_CHNL_CODE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_82
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CHNL_CODE=T_82.CHNL_CODE
;

--Step4:
INSERT  INTO dw_sdata.CBS_001_SMCTL_CHNL_CODE SELECT * FROM T_82;

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
