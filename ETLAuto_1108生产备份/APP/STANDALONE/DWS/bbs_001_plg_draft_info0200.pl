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
DELETE FROM dw_sdata.BBS_001_PLG_DRAFT_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_PLG_DRAFT_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_50 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_PLG_DRAFT_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_50 (
  ID,
  BRH_ID,
  DRAFT_ID,
  START_DATE,
  END_DATE,
  STATUS,
  TMP_STATUS,
  SAVE_BUSINESS_NO,
  CUST_ACCNO,
  CUST_ID,
  SAME_CITY_FLAG,
  DRAFT_CATEGORY,
  DRAFT_QUALITY,
  PLG_RATE,
  PLG_CREDIT,
  QUERY_FLAG,
  BACK_FLAG,
  BACK_ACCOUNT,
  STG_SAVE_COMMENT,
  CREATE_OPR_ID,
  CREATE_TIME,
  LAST_UPD_OPR_ID,
  LAST_UPD_TIME,
  PAY_NAME,
  PAY_ACCOUNT,
  PAY_OPEN_BANK_ID,
  SEND_CLT_FLAG,
  PLG_FLAG,
  CONTRACT_ID,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.BRH_ID,
  N.DRAFT_ID,
  N.START_DATE,
  N.END_DATE,
  N.STATUS,
  N.TMP_STATUS,
  N.SAVE_BUSINESS_NO,
  N.CUST_ACCNO,
  N.CUST_ID,
  N.SAME_CITY_FLAG,
  N.DRAFT_CATEGORY,
  N.DRAFT_QUALITY,
  N.PLG_RATE,
  N.PLG_CREDIT,
  N.QUERY_FLAG,
  N.BACK_FLAG,
  N.BACK_ACCOUNT,
  N.STG_SAVE_COMMENT,
  N.CREATE_OPR_ID,
  N.CREATE_TIME,
  N.LAST_UPD_OPR_ID,
  N.LAST_UPD_TIME,
  N.PAY_NAME,
  N.PAY_ACCOUNT,
  N.PAY_OPEN_BANK_ID,
  N.SEND_CLT_FLAG,
  N.PLG_FLAG,
  N.CONTRACT_ID,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(BRH_ID, 0 ) AS BRH_ID ,
  COALESCE(DRAFT_ID, 0 ) AS DRAFT_ID ,
  COALESCE(START_DATE, '' ) AS START_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE ,
  COALESCE(STATUS, '' ) AS STATUS ,
  COALESCE(TMP_STATUS, '' ) AS TMP_STATUS ,
  COALESCE(SAVE_BUSINESS_NO, '' ) AS SAVE_BUSINESS_NO ,
  COALESCE(CUST_ACCNO, '' ) AS CUST_ACCNO ,
  COALESCE(CUST_ID, 0 ) AS CUST_ID ,
  COALESCE(SAME_CITY_FLAG, '' ) AS SAME_CITY_FLAG ,
  COALESCE(DRAFT_CATEGORY, '' ) AS DRAFT_CATEGORY ,
  COALESCE(DRAFT_QUALITY, '' ) AS DRAFT_QUALITY ,
  COALESCE(PLG_RATE, 0 ) AS PLG_RATE ,
  COALESCE(PLG_CREDIT, 0 ) AS PLG_CREDIT ,
  COALESCE(QUERY_FLAG, '' ) AS QUERY_FLAG ,
  COALESCE(BACK_FLAG, '' ) AS BACK_FLAG ,
  COALESCE(BACK_ACCOUNT, '' ) AS BACK_ACCOUNT ,
  COALESCE(STG_SAVE_COMMENT, '' ) AS STG_SAVE_COMMENT ,
  COALESCE(CREATE_OPR_ID, 0 ) AS CREATE_OPR_ID ,
  COALESCE(CREATE_TIME, '' ) AS CREATE_TIME ,
  COALESCE(LAST_UPD_OPR_ID, 0 ) AS LAST_UPD_OPR_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(PAY_NAME, '' ) AS PAY_NAME ,
  COALESCE(PAY_ACCOUNT, '' ) AS PAY_ACCOUNT ,
  COALESCE(PAY_OPEN_BANK_ID, 0 ) AS PAY_OPEN_BANK_ID ,
  COALESCE(SEND_CLT_FLAG, '' ) AS SEND_CLT_FLAG ,
  COALESCE(PLG_FLAG, '' ) AS PLG_FLAG ,
  COALESCE(CONTRACT_ID, 0 ) AS CONTRACT_ID 
 FROM  dw_tdata.BBS_001_PLG_DRAFT_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  BRH_ID ,
  DRAFT_ID ,
  START_DATE ,
  END_DATE ,
  STATUS ,
  TMP_STATUS ,
  SAVE_BUSINESS_NO ,
  CUST_ACCNO ,
  CUST_ID ,
  SAME_CITY_FLAG ,
  DRAFT_CATEGORY ,
  DRAFT_QUALITY ,
  PLG_RATE ,
  PLG_CREDIT ,
  QUERY_FLAG ,
  BACK_FLAG ,
  BACK_ACCOUNT ,
  STG_SAVE_COMMENT ,
  CREATE_OPR_ID ,
  CREATE_TIME ,
  LAST_UPD_OPR_ID ,
  LAST_UPD_TIME ,
  PAY_NAME ,
  PAY_ACCOUNT ,
  PAY_OPEN_BANK_ID ,
  SEND_CLT_FLAG ,
  PLG_FLAG ,
  CONTRACT_ID 
 FROM dw_sdata.BBS_001_PLG_DRAFT_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.BRH_ID<>T.BRH_ID
 OR N.DRAFT_ID<>T.DRAFT_ID
 OR N.START_DATE<>T.START_DATE
 OR N.END_DATE<>T.END_DATE
 OR N.STATUS<>T.STATUS
 OR N.TMP_STATUS<>T.TMP_STATUS
 OR N.SAVE_BUSINESS_NO<>T.SAVE_BUSINESS_NO
 OR N.CUST_ACCNO<>T.CUST_ACCNO
 OR N.CUST_ID<>T.CUST_ID
 OR N.SAME_CITY_FLAG<>T.SAME_CITY_FLAG
 OR N.DRAFT_CATEGORY<>T.DRAFT_CATEGORY
 OR N.DRAFT_QUALITY<>T.DRAFT_QUALITY
 OR N.PLG_RATE<>T.PLG_RATE
 OR N.PLG_CREDIT<>T.PLG_CREDIT
 OR N.QUERY_FLAG<>T.QUERY_FLAG
 OR N.BACK_FLAG<>T.BACK_FLAG
 OR N.BACK_ACCOUNT<>T.BACK_ACCOUNT
 OR N.STG_SAVE_COMMENT<>T.STG_SAVE_COMMENT
 OR N.CREATE_OPR_ID<>T.CREATE_OPR_ID
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.LAST_UPD_OPR_ID<>T.LAST_UPD_OPR_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.PAY_NAME<>T.PAY_NAME
 OR N.PAY_ACCOUNT<>T.PAY_ACCOUNT
 OR N.PAY_OPEN_BANK_ID<>T.PAY_OPEN_BANK_ID
 OR N.SEND_CLT_FLAG<>T.SEND_CLT_FLAG
 OR N.PLG_FLAG<>T.PLG_FLAG
 OR N.CONTRACT_ID<>T.CONTRACT_ID
;

--Step3:
UPDATE dw_sdata.BBS_001_PLG_DRAFT_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_50
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_50.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_PLG_DRAFT_INFO SELECT * FROM T_50;

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
