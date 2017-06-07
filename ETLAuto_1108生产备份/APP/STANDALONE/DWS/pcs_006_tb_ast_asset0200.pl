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
DELETE FROM dw_sdata.PCS_006_TB_AST_ASSET WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_006_TB_AST_ASSET SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_328 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_006_TB_AST_ASSET WHERE 1=0;

--Step2:
INSERT  INTO T_328 (
  ASSET_ID,
  ASSET_NO,
  ASSETTYPE_ID,
  ASSET_DESCRIBTION,
  ASSET_STATUS,
  ASSET_NAME,
  ASSESSMENT_VALUE_FORMAL,
  ASSESSMENT_VALUE_BEFOREHAND,
  FIRST_EVALUATION_DATE,
  NEW_EVALUATION_DATE,
  NEW_EVALUATION_END_DATE,
  DELFLAG,
  SYSTEM_FLAG,
  CREATE_TIME,
  UPDATE_TIME,
  USER_ID,
  TRUNC_NO,
  start_dt,
  end_dt)
SELECT
  N.ASSET_ID,
  N.ASSET_NO,
  N.ASSETTYPE_ID,
  N.ASSET_DESCRIBTION,
  N.ASSET_STATUS,
  N.ASSET_NAME,
  N.ASSESSMENT_VALUE_FORMAL,
  N.ASSESSMENT_VALUE_BEFOREHAND,
  N.FIRST_EVALUATION_DATE,
  N.NEW_EVALUATION_DATE,
  N.NEW_EVALUATION_END_DATE,
  N.DELFLAG,
  N.SYSTEM_FLAG,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.USER_ID,
  N.TRUNC_NO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ASSET_ID, '' ) AS ASSET_ID ,
  COALESCE(ASSET_NO, '' ) AS ASSET_NO ,
  COALESCE(ASSETTYPE_ID, '' ) AS ASSETTYPE_ID ,
  COALESCE(ASSET_DESCRIBTION, '' ) AS ASSET_DESCRIBTION ,
  COALESCE(ASSET_STATUS, '' ) AS ASSET_STATUS ,
  COALESCE(ASSET_NAME, '' ) AS ASSET_NAME ,
  COALESCE(ASSESSMENT_VALUE_FORMAL, 0 ) AS ASSESSMENT_VALUE_FORMAL ,
  COALESCE(ASSESSMENT_VALUE_BEFOREHAND, 0 ) AS ASSESSMENT_VALUE_BEFOREHAND ,
  COALESCE(FIRST_EVALUATION_DATE,DATE('4999-12-31') ) AS FIRST_EVALUATION_DATE ,
  COALESCE(NEW_EVALUATION_DATE,DATE('4999-12-31') ) AS NEW_EVALUATION_DATE ,
  COALESCE(NEW_EVALUATION_END_DATE,DATE('4999-12-31') ) AS NEW_EVALUATION_END_DATE ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(SYSTEM_FLAG, '' ) AS SYSTEM_FLAG ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(USER_ID, '' ) AS USER_ID ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO 
 FROM  dw_tdata.PCS_006_TB_AST_ASSET_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ASSET_ID ,
  ASSET_NO ,
  ASSETTYPE_ID ,
  ASSET_DESCRIBTION ,
  ASSET_STATUS ,
  ASSET_NAME ,
  ASSESSMENT_VALUE_FORMAL ,
  ASSESSMENT_VALUE_BEFOREHAND ,
  FIRST_EVALUATION_DATE ,
  NEW_EVALUATION_DATE ,
  NEW_EVALUATION_END_DATE ,
  DELFLAG ,
  SYSTEM_FLAG ,
  CREATE_TIME ,
  UPDATE_TIME ,
  USER_ID ,
  TRUNC_NO 
 FROM dw_sdata.PCS_006_TB_AST_ASSET 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ASSET_ID = T.ASSET_ID
WHERE
(T.ASSET_ID IS NULL)
 OR N.ASSET_NO<>T.ASSET_NO
 OR N.ASSETTYPE_ID<>T.ASSETTYPE_ID
 OR N.ASSET_DESCRIBTION<>T.ASSET_DESCRIBTION
 OR N.ASSET_STATUS<>T.ASSET_STATUS
 OR N.ASSET_NAME<>T.ASSET_NAME
 OR N.ASSESSMENT_VALUE_FORMAL<>T.ASSESSMENT_VALUE_FORMAL
 OR N.ASSESSMENT_VALUE_BEFOREHAND<>T.ASSESSMENT_VALUE_BEFOREHAND
 OR N.FIRST_EVALUATION_DATE<>T.FIRST_EVALUATION_DATE
 OR N.NEW_EVALUATION_DATE<>T.NEW_EVALUATION_DATE
 OR N.NEW_EVALUATION_END_DATE<>T.NEW_EVALUATION_END_DATE
 OR N.DELFLAG<>T.DELFLAG
 OR N.SYSTEM_FLAG<>T.SYSTEM_FLAG
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.USER_ID<>T.USER_ID
 OR N.TRUNC_NO<>T.TRUNC_NO
;

--Step3:
UPDATE dw_sdata.PCS_006_TB_AST_ASSET P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_328
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ASSET_ID=T_328.ASSET_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_006_TB_AST_ASSET SELECT * FROM T_328;

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
