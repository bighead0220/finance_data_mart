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
DELETE FROM dw_sdata.TGS_000_ADM_AGENCY WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.TGS_000_ADM_AGENCY SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_342 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.TGS_000_ADM_AGENCY WHERE 1=0;

--Step2:
INSERT  INTO T_342 (
  ID_,
  PARENT_,
  LEVEL_CODE_,
  ORDER_CODE_,
  LEVEL_,
  LEAF_,
  FULL_NAME_,
  SHORT_NAME_,
  CODE_,
  STATUS_,
  PROPERTY_,
  FUNCTION_,
  TYPE_,
  AREA_CODE_,
  HR_AGENCY_CODE_,
  ORG_SERIES_,
  POST_NAME_,
  OLD_CODE_,
  PARENT_CODE_,
  start_dt,
  end_dt)
SELECT
  N.ID_,
  N.PARENT_,
  N.LEVEL_CODE_,
  N.ORDER_CODE_,
  N.LEVEL_,
  N.LEAF_,
  N.FULL_NAME_,
  N.SHORT_NAME_,
  N.CODE_,
  N.STATUS_,
  N.PROPERTY_,
  N.FUNCTION_,
  N.TYPE_,
  N.AREA_CODE_,
  N.HR_AGENCY_CODE_,
  N.ORG_SERIES_,
  N.POST_NAME_,
  N.OLD_CODE_,
  N.PARENT_CODE_,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID_, 0 ) AS ID_ ,
  COALESCE(PARENT_, 0 ) AS PARENT_ ,
  COALESCE(LEVEL_CODE_, '' ) AS LEVEL_CODE_ ,
  COALESCE(ORDER_CODE_, '' ) AS ORDER_CODE_ ,
  COALESCE(LEVEL_, '' ) AS LEVEL_ ,
  COALESCE(LEAF_, 0 ) AS LEAF_ ,
  COALESCE(FULL_NAME_, '' ) AS FULL_NAME_ ,
  COALESCE(SHORT_NAME_, '' ) AS SHORT_NAME_ ,
  COALESCE(CODE_, '' ) AS CODE_ ,
  COALESCE(STATUS_, '' ) AS STATUS_ ,
  COALESCE(PROPERTY_, '' ) AS PROPERTY_ ,
  COALESCE(FUNCTION_, '' ) AS FUNCTION_ ,
  COALESCE(TYPE_, '' ) AS TYPE_ ,
  COALESCE(AREA_CODE_, '' ) AS AREA_CODE_ ,
  COALESCE(HR_AGENCY_CODE_, '' ) AS HR_AGENCY_CODE_ ,
  COALESCE(ORG_SERIES_, '' ) AS ORG_SERIES_ ,
  COALESCE(POST_NAME_, '' ) AS POST_NAME_ ,
  COALESCE(OLD_CODE_, '' ) AS OLD_CODE_ ,
  COALESCE(PARENT_CODE_, '' ) AS PARENT_CODE_ 
 FROM  dw_tdata.TGS_000_ADM_AGENCY_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID_ ,
  PARENT_ ,
  LEVEL_CODE_ ,
  ORDER_CODE_ ,
  LEVEL_ ,
  LEAF_ ,
  FULL_NAME_ ,
  SHORT_NAME_ ,
  CODE_ ,
  STATUS_ ,
  PROPERTY_ ,
  FUNCTION_ ,
  TYPE_ ,
  AREA_CODE_ ,
  HR_AGENCY_CODE_ ,
  ORG_SERIES_ ,
  POST_NAME_ ,
  OLD_CODE_ ,
  PARENT_CODE_ 
 FROM dw_sdata.TGS_000_ADM_AGENCY 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID_ = T.ID_
WHERE
(T.ID_ IS NULL)
 OR N.PARENT_<>T.PARENT_
 OR N.LEVEL_CODE_<>T.LEVEL_CODE_
 OR N.ORDER_CODE_<>T.ORDER_CODE_
 OR N.LEVEL_<>T.LEVEL_
 OR N.LEAF_<>T.LEAF_
 OR N.FULL_NAME_<>T.FULL_NAME_
 OR N.SHORT_NAME_<>T.SHORT_NAME_
 OR N.CODE_<>T.CODE_
 OR N.STATUS_<>T.STATUS_
 OR N.PROPERTY_<>T.PROPERTY_
 OR N.FUNCTION_<>T.FUNCTION_
 OR N.TYPE_<>T.TYPE_
 OR N.AREA_CODE_<>T.AREA_CODE_
 OR N.HR_AGENCY_CODE_<>T.HR_AGENCY_CODE_
 OR N.ORG_SERIES_<>T.ORG_SERIES_
 OR N.POST_NAME_<>T.POST_NAME_
 OR N.OLD_CODE_<>T.OLD_CODE_
 OR N.PARENT_CODE_<>T.PARENT_CODE_
;

--Step3:
UPDATE dw_sdata.TGS_000_ADM_AGENCY P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_342
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID_=T_342.ID_
;

--Step4:
INSERT  INTO dw_sdata.TGS_000_ADM_AGENCY SELECT * FROM T_342;

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
