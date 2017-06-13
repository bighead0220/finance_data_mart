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
DELETE FROM dw_sdata.ICS_001_BORGT_CUST WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ICS_001_BORGT_CUST SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_222 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ICS_001_BORGT_CUST WHERE 1=0;

--Step2:
INSERT  INTO T_222 (
  CUST_ID,
  CERT_TYPE,
  CERT_ID,
  PROV_CODE,
  NAME,
  SEX,
  UNIT_CODE,
  BIRTHDAY,
  COMU_ADDR,
  ZIP,
  COMU_TEL,
  MOBILE,
  E_MAIL,
  HOME_TEL,
  HOME_ADDR,
  HOME_ZIP,
  SAV_ACCT,
  OCUP_JTYPE,
  INCOME,
  MARR_STAT,
  POL_NO,
  SUM_AMT,
  VALID_AMT,
  INVALID_DATE,
  INPUT_DATE,
  SALE_CODE,
  start_dt,
  end_dt)
SELECT
  N.CUST_ID,
  N.CERT_TYPE,
  N.CERT_ID,
  N.PROV_CODE,
  N.NAME,
  N.SEX,
  N.UNIT_CODE,
  N.BIRTHDAY,
  N.COMU_ADDR,
  N.ZIP,
  N.COMU_TEL,
  N.MOBILE,
  N.E_MAIL,
  N.HOME_TEL,
  N.HOME_ADDR,
  N.HOME_ZIP,
  N.SAV_ACCT,
  N.OCUP_JTYPE,
  N.INCOME,
  N.MARR_STAT,
  N.POL_NO,
  N.SUM_AMT,
  N.VALID_AMT,
  N.INVALID_DATE,
  N.INPUT_DATE,
  N.SALE_CODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CUST_ID, '' ) AS CUST_ID ,
  COALESCE(CERT_TYPE, '' ) AS CERT_TYPE ,
  COALESCE(CERT_ID, '' ) AS CERT_ID ,
  COALESCE(PROV_CODE, '' ) AS PROV_CODE ,
  COALESCE(NAME, '' ) AS NAME ,
  COALESCE(SEX, '' ) AS SEX ,
  COALESCE(UNIT_CODE, '' ) AS UNIT_CODE ,
  COALESCE(BIRTHDAY, '' ) AS BIRTHDAY ,
  COALESCE(COMU_ADDR, '' ) AS COMU_ADDR ,
  COALESCE(ZIP, '' ) AS ZIP ,
  COALESCE(COMU_TEL, '' ) AS COMU_TEL ,
  COALESCE(MOBILE, '' ) AS MOBILE ,
  COALESCE(E_MAIL, '' ) AS E_MAIL ,
  COALESCE(HOME_TEL, '' ) AS HOME_TEL ,
  COALESCE(HOME_ADDR, '' ) AS HOME_ADDR ,
  COALESCE(HOME_ZIP, '' ) AS HOME_ZIP ,
  COALESCE(SAV_ACCT, '' ) AS SAV_ACCT ,
  COALESCE(OCUP_JTYPE, '' ) AS OCUP_JTYPE ,
  COALESCE(INCOME, 0 ) AS INCOME ,
  COALESCE(MARR_STAT, '' ) AS MARR_STAT ,
  COALESCE(POL_NO, 0 ) AS POL_NO ,
  COALESCE(SUM_AMT, 0 ) AS SUM_AMT ,
  COALESCE(VALID_AMT, 0 ) AS VALID_AMT ,
  COALESCE(INVALID_DATE, '' ) AS INVALID_DATE ,
  COALESCE(INPUT_DATE, '' ) AS INPUT_DATE ,
  COALESCE(SALE_CODE, '' ) AS SALE_CODE 
 FROM  dw_tdata.ICS_001_BORGT_CUST_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CUST_ID ,
  CERT_TYPE ,
  CERT_ID ,
  PROV_CODE ,
  NAME ,
  SEX ,
  UNIT_CODE ,
  BIRTHDAY ,
  COMU_ADDR ,
  ZIP ,
  COMU_TEL ,
  MOBILE ,
  E_MAIL ,
  HOME_TEL ,
  HOME_ADDR ,
  HOME_ZIP ,
  SAV_ACCT ,
  OCUP_JTYPE ,
  INCOME ,
  MARR_STAT ,
  POL_NO ,
  SUM_AMT ,
  VALID_AMT ,
  INVALID_DATE ,
  INPUT_DATE ,
  SALE_CODE 
 FROM dw_sdata.ICS_001_BORGT_CUST 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CUST_ID = T.CUST_ID
WHERE
(T.CUST_ID IS NULL)
 OR N.CERT_TYPE<>T.CERT_TYPE
 OR N.CERT_ID<>T.CERT_ID
 OR N.PROV_CODE<>T.PROV_CODE
 OR N.NAME<>T.NAME
 OR N.SEX<>T.SEX
 OR N.UNIT_CODE<>T.UNIT_CODE
 OR N.BIRTHDAY<>T.BIRTHDAY
 OR N.COMU_ADDR<>T.COMU_ADDR
 OR N.ZIP<>T.ZIP
 OR N.COMU_TEL<>T.COMU_TEL
 OR N.MOBILE<>T.MOBILE
 OR N.E_MAIL<>T.E_MAIL
 OR N.HOME_TEL<>T.HOME_TEL
 OR N.HOME_ADDR<>T.HOME_ADDR
 OR N.HOME_ZIP<>T.HOME_ZIP
 OR N.SAV_ACCT<>T.SAV_ACCT
 OR N.OCUP_JTYPE<>T.OCUP_JTYPE
 OR N.INCOME<>T.INCOME
 OR N.MARR_STAT<>T.MARR_STAT
 OR N.POL_NO<>T.POL_NO
 OR N.SUM_AMT<>T.SUM_AMT
 OR N.VALID_AMT<>T.VALID_AMT
 OR N.INVALID_DATE<>T.INVALID_DATE
 OR N.INPUT_DATE<>T.INPUT_DATE
 OR N.SALE_CODE<>T.SALE_CODE
;

--Step3:
UPDATE dw_sdata.ICS_001_BORGT_CUST P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_222
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CUST_ID=T_222.CUST_ID
;

--Step4:
INSERT  INTO dw_sdata.ICS_001_BORGT_CUST SELECT * FROM T_222;

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
