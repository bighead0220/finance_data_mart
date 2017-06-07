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
DELETE FROM dw_sdata.ACC_000_T_MNG_PA_CURRTYPE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_000_T_MNG_PA_CURRTYPE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_6 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_000_T_MNG_PA_CURRTYPE WHERE 1=0;

--Step2:
INSERT  INTO T_6 (
  CURR_TYPE,
  CURR_TYPE_NAME,
  CURR_SYM,
  CURR_SIGN,
  CURR_ISO,
  DEC_DIGIT,
  CURR_UNIT,
  FRA_CURR1,
  FRA_CURR2,
  FRA_CURR3,
  FRA_CURR4,
  CURR_MIN,
  COUNORREG_NO,
  COUNORREG_NAME,
  COUNORREG_EN_NAME,
  CURR_STAT,
  LAST_TLR,
  LAST_DATE,
  start_dt,
  end_dt)
SELECT
  N.CURR_TYPE,
  N.CURR_TYPE_NAME,
  N.CURR_SYM,
  N.CURR_SIGN,
  N.CURR_ISO,
  N.DEC_DIGIT,
  N.CURR_UNIT,
  N.FRA_CURR1,
  N.FRA_CURR2,
  N.FRA_CURR3,
  N.FRA_CURR4,
  N.CURR_MIN,
  N.COUNORREG_NO,
  N.COUNORREG_NAME,
  N.COUNORREG_EN_NAME,
  N.CURR_STAT,
  N.LAST_TLR,
  N.LAST_DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(CURR_TYPE_NAME, '' ) AS CURR_TYPE_NAME ,
  COALESCE(CURR_SYM, '' ) AS CURR_SYM ,
  COALESCE(CURR_SIGN, '' ) AS CURR_SIGN ,
  COALESCE(CURR_ISO, '' ) AS CURR_ISO ,
  COALESCE(DEC_DIGIT, 0 ) AS DEC_DIGIT ,
  COALESCE(CURR_UNIT, '' ) AS CURR_UNIT ,
  COALESCE(FRA_CURR1, '' ) AS FRA_CURR1 ,
  COALESCE(FRA_CURR2, '' ) AS FRA_CURR2 ,
  COALESCE(FRA_CURR3, '' ) AS FRA_CURR3 ,
  COALESCE(FRA_CURR4, '' ) AS FRA_CURR4 ,
  COALESCE(CURR_MIN, 0 ) AS CURR_MIN ,
  COALESCE(COUNORREG_NO, '' ) AS COUNORREG_NO ,
  COALESCE(COUNORREG_NAME, '' ) AS COUNORREG_NAME ,
  COALESCE(COUNORREG_EN_NAME, '' ) AS COUNORREG_EN_NAME ,
  COALESCE(CURR_STAT, '' ) AS CURR_STAT ,
  COALESCE(LAST_TLR, '' ) AS LAST_TLR ,
  COALESCE(LAST_DATE, '' ) AS LAST_DATE 
 FROM  dw_tdata.ACC_000_T_MNG_PA_CURRTYPE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CURR_TYPE ,
  CURR_TYPE_NAME ,
  CURR_SYM ,
  CURR_SIGN ,
  CURR_ISO ,
  DEC_DIGIT ,
  CURR_UNIT ,
  FRA_CURR1 ,
  FRA_CURR2 ,
  FRA_CURR3 ,
  FRA_CURR4 ,
  CURR_MIN ,
  COUNORREG_NO ,
  COUNORREG_NAME ,
  COUNORREG_EN_NAME ,
  CURR_STAT ,
  LAST_TLR ,
  LAST_DATE 
 FROM dw_sdata.ACC_000_T_MNG_PA_CURRTYPE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CURR_TYPE = T.CURR_TYPE
WHERE
(T.CURR_TYPE IS NULL)
 OR N.CURR_TYPE_NAME<>T.CURR_TYPE_NAME
 OR N.CURR_SYM<>T.CURR_SYM
 OR N.CURR_SIGN<>T.CURR_SIGN
 OR N.CURR_ISO<>T.CURR_ISO
 OR N.DEC_DIGIT<>T.DEC_DIGIT
 OR N.CURR_UNIT<>T.CURR_UNIT
 OR N.FRA_CURR1<>T.FRA_CURR1
 OR N.FRA_CURR2<>T.FRA_CURR2
 OR N.FRA_CURR3<>T.FRA_CURR3
 OR N.FRA_CURR4<>T.FRA_CURR4
 OR N.CURR_MIN<>T.CURR_MIN
 OR N.COUNORREG_NO<>T.COUNORREG_NO
 OR N.COUNORREG_NAME<>T.COUNORREG_NAME
 OR N.COUNORREG_EN_NAME<>T.COUNORREG_EN_NAME
 OR N.CURR_STAT<>T.CURR_STAT
 OR N.LAST_TLR<>T.LAST_TLR
 OR N.LAST_DATE<>T.LAST_DATE
;

--Step3:
UPDATE dw_sdata.ACC_000_T_MNG_PA_CURRTYPE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_6
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CURR_TYPE=T_6.CURR_TYPE
;

--Step4:
INSERT  INTO dw_sdata.ACC_000_T_MNG_PA_CURRTYPE SELECT * FROM T_6;

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
