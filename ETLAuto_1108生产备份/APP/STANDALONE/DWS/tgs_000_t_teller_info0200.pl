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
DELETE FROM dw_sdata.TGS_000_T_TELLER_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.TGS_000_T_TELLER_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_343 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.TGS_000_T_TELLER_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_343 (
  ID_,
  NAME_,
  CODE_,
  ORG_CODE_,
  FLAG_,
  ID_CARD_,
  SEX_,
  DIPLOMA_,
  JOB_ID_,
  STATUS_,
  TYPE_,
  PHONE_,
  ADDRESS_,
  SCHOOL_,
  MAJOR_,
  WORK_DATE_,
  JOIN_DATE_,
  CONTRACT_START_DATE_,
  CONTRACT_END_DATE_,
  CONTRACT_TYPE_,
  REGISTER_DATE_,
  CREATE_TIME_,
  UPDATE_TIME_,
  CREATE_USER_,
  UPDATE_USER_,
  DEPT_,
  BIRTHDAY_,
  JOB_DATE_,
  PERSON_NO_,
  OPERATE_STATUS_,
  PERSON_FLAG_,
  EMAIL_,
  CERT_,
  TRAIL_,
  SYSTEMS_,
  AGENCY_LEVEL_CODE_,
  AGENCY_LEVEL_,
  start_dt,
  end_dt)
SELECT
  N.ID_,
  N.NAME_,
  N.CODE_,
  N.ORG_CODE_,
  N.FLAG_,
  N.ID_CARD_,
  N.SEX_,
  N.DIPLOMA_,
  N.JOB_ID_,
  N.STATUS_,
  N.TYPE_,
  N.PHONE_,
  N.ADDRESS_,
  N.SCHOOL_,
  N.MAJOR_,
  N.WORK_DATE_,
  N.JOIN_DATE_,
  N.CONTRACT_START_DATE_,
  N.CONTRACT_END_DATE_,
  N.CONTRACT_TYPE_,
  N.REGISTER_DATE_,
  N.CREATE_TIME_,
  N.UPDATE_TIME_,
  N.CREATE_USER_,
  N.UPDATE_USER_,
  N.DEPT_,
  N.BIRTHDAY_,
  N.JOB_DATE_,
  N.PERSON_NO_,
  N.OPERATE_STATUS_,
  N.PERSON_FLAG_,
  N.EMAIL_,
  N.CERT_,
  N.TRAIL_,
  N.SYSTEMS_,
  N.AGENCY_LEVEL_CODE_,
  N.AGENCY_LEVEL_,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID_, '' ) AS ID_ ,
  COALESCE(NAME_, '' ) AS NAME_ ,
  COALESCE(CODE_, '' ) AS CODE_ ,
  COALESCE(ORG_CODE_, '' ) AS ORG_CODE_ ,
  COALESCE(FLAG_, '' ) AS FLAG_ ,
  COALESCE(ID_CARD_, '' ) AS ID_CARD_ ,
  COALESCE(SEX_, '' ) AS SEX_ ,
  COALESCE(DIPLOMA_, '' ) AS DIPLOMA_ ,
  COALESCE(JOB_ID_, '' ) AS JOB_ID_ ,
  COALESCE(STATUS_, '' ) AS STATUS_ ,
  COALESCE(TYPE_, '' ) AS TYPE_ ,
  COALESCE(PHONE_, '' ) AS PHONE_ ,
  COALESCE(ADDRESS_, '' ) AS ADDRESS_ ,
  COALESCE(SCHOOL_, '' ) AS SCHOOL_ ,
  COALESCE(MAJOR_, '' ) AS MAJOR_ ,
  COALESCE(WORK_DATE_, '' ) AS WORK_DATE_ ,
  COALESCE(JOIN_DATE_, '' ) AS JOIN_DATE_ ,
  COALESCE(CONTRACT_START_DATE_, '' ) AS CONTRACT_START_DATE_ ,
  COALESCE(CONTRACT_END_DATE_, '' ) AS CONTRACT_END_DATE_ ,
  COALESCE(CONTRACT_TYPE_, '' ) AS CONTRACT_TYPE_ ,
  COALESCE(REGISTER_DATE_, '' ) AS REGISTER_DATE_ ,
  COALESCE(CREATE_TIME_, '' ) AS CREATE_TIME_ ,
  COALESCE(UPDATE_TIME_, '' ) AS UPDATE_TIME_ ,
  COALESCE(CREATE_USER_, '' ) AS CREATE_USER_ ,
  COALESCE(UPDATE_USER_, '' ) AS UPDATE_USER_ ,
  COALESCE(DEPT_, '' ) AS DEPT_ ,
  COALESCE(BIRTHDAY_, '' ) AS BIRTHDAY_ ,
  COALESCE(JOB_DATE_, '' ) AS JOB_DATE_ ,
  COALESCE(PERSON_NO_, '' ) AS PERSON_NO_ ,
  COALESCE(OPERATE_STATUS_, '' ) AS OPERATE_STATUS_ ,
  COALESCE(PERSON_FLAG_, '' ) AS PERSON_FLAG_ ,
  COALESCE(EMAIL_, '' ) AS EMAIL_ ,
  COALESCE(CERT_, '' ) AS CERT_ ,
  COALESCE(TRAIL_, 0 ) AS TRAIL_ ,
  COALESCE(SYSTEMS_, '' ) AS SYSTEMS_ ,
  COALESCE(AGENCY_LEVEL_CODE_, '' ) AS AGENCY_LEVEL_CODE_ ,
  COALESCE(AGENCY_LEVEL_, '' ) AS AGENCY_LEVEL_ 
 FROM  dw_tdata.TGS_000_T_TELLER_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID_ ,
  NAME_ ,
  CODE_ ,
  ORG_CODE_ ,
  FLAG_ ,
  ID_CARD_ ,
  SEX_ ,
  DIPLOMA_ ,
  JOB_ID_ ,
  STATUS_ ,
  TYPE_ ,
  PHONE_ ,
  ADDRESS_ ,
  SCHOOL_ ,
  MAJOR_ ,
  WORK_DATE_ ,
  JOIN_DATE_ ,
  CONTRACT_START_DATE_ ,
  CONTRACT_END_DATE_ ,
  CONTRACT_TYPE_ ,
  REGISTER_DATE_ ,
  CREATE_TIME_ ,
  UPDATE_TIME_ ,
  CREATE_USER_ ,
  UPDATE_USER_ ,
  DEPT_ ,
  BIRTHDAY_ ,
  JOB_DATE_ ,
  PERSON_NO_ ,
  OPERATE_STATUS_ ,
  PERSON_FLAG_ ,
  EMAIL_ ,
  CERT_ ,
  TRAIL_ ,
  SYSTEMS_ ,
  AGENCY_LEVEL_CODE_ ,
  AGENCY_LEVEL_ 
 FROM dw_sdata.TGS_000_T_TELLER_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID_ = T.ID_
WHERE
(T.ID_ IS NULL)
 OR N.NAME_<>T.NAME_
 OR N.CODE_<>T.CODE_
 OR N.ORG_CODE_<>T.ORG_CODE_
 OR N.FLAG_<>T.FLAG_
 OR N.ID_CARD_<>T.ID_CARD_
 OR N.SEX_<>T.SEX_
 OR N.DIPLOMA_<>T.DIPLOMA_
 OR N.JOB_ID_<>T.JOB_ID_
 OR N.STATUS_<>T.STATUS_
 OR N.TYPE_<>T.TYPE_
 OR N.PHONE_<>T.PHONE_
 OR N.ADDRESS_<>T.ADDRESS_
 OR N.SCHOOL_<>T.SCHOOL_
 OR N.MAJOR_<>T.MAJOR_
 OR N.WORK_DATE_<>T.WORK_DATE_
 OR N.JOIN_DATE_<>T.JOIN_DATE_
 OR N.CONTRACT_START_DATE_<>T.CONTRACT_START_DATE_
 OR N.CONTRACT_END_DATE_<>T.CONTRACT_END_DATE_
 OR N.CONTRACT_TYPE_<>T.CONTRACT_TYPE_
 OR N.REGISTER_DATE_<>T.REGISTER_DATE_
 OR N.CREATE_TIME_<>T.CREATE_TIME_
 OR N.UPDATE_TIME_<>T.UPDATE_TIME_
 OR N.CREATE_USER_<>T.CREATE_USER_
 OR N.UPDATE_USER_<>T.UPDATE_USER_
 OR N.DEPT_<>T.DEPT_
 OR N.BIRTHDAY_<>T.BIRTHDAY_
 OR N.JOB_DATE_<>T.JOB_DATE_
 OR N.PERSON_NO_<>T.PERSON_NO_
 OR N.OPERATE_STATUS_<>T.OPERATE_STATUS_
 OR N.PERSON_FLAG_<>T.PERSON_FLAG_
 OR N.EMAIL_<>T.EMAIL_
 OR N.CERT_<>T.CERT_
 OR N.TRAIL_<>T.TRAIL_
 OR N.SYSTEMS_<>T.SYSTEMS_
 OR N.AGENCY_LEVEL_CODE_<>T.AGENCY_LEVEL_CODE_
 OR N.AGENCY_LEVEL_<>T.AGENCY_LEVEL_
;

--Step3:
UPDATE dw_sdata.TGS_000_T_TELLER_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_343
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID_=T_343.ID_
;

--Step4:
INSERT  INTO dw_sdata.TGS_000_T_TELLER_INFO SELECT * FROM T_343;

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
