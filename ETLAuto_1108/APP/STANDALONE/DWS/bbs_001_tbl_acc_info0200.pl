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
DELETE FROM dw_sdata.BBS_001_TBL_ACC_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_TBL_ACC_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_62 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_TBL_ACC_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_62 (
  ID,
  ACC_NO,
  TYPE,
  ACC_NAME,
  OPEN_DATE,
  CLZ_DATE,
  DRAFT_ID,
  OPEN_BIZ_TYPE,
  CLZ_BIZ_TYPE,
  STATUS,
  OPE_BRH,
  ACC_ACCEPT_TYPE,
  ACC_PRODUCT_TYPE,
  TXN_BUSS_TYPE,
  BANK_TYPE,
  TXN_NAME_CODE,
  OPEN_INTEREST,
  CLOZ_INTEREST,
  ACC_SRC_TYPE,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.ACC_NO,
  N.TYPE,
  N.ACC_NAME,
  N.OPEN_DATE,
  N.CLZ_DATE,
  N.DRAFT_ID,
  N.OPEN_BIZ_TYPE,
  N.CLZ_BIZ_TYPE,
  N.STATUS,
  N.OPE_BRH,
  N.ACC_ACCEPT_TYPE,
  N.ACC_PRODUCT_TYPE,
  N.TXN_BUSS_TYPE,
  N.BANK_TYPE,
  N.TXN_NAME_CODE,
  N.OPEN_INTEREST,
  N.CLOZ_INTEREST,
  N.ACC_SRC_TYPE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(ACC_NO, '' ) AS ACC_NO ,
  COALESCE(TYPE, '' ) AS TYPE ,
  COALESCE(ACC_NAME, '' ) AS ACC_NAME ,
  COALESCE(OPEN_DATE, '' ) AS OPEN_DATE ,
  COALESCE(CLZ_DATE, '' ) AS CLZ_DATE ,
  COALESCE(DRAFT_ID, 0 ) AS DRAFT_ID ,
  COALESCE(OPEN_BIZ_TYPE, '' ) AS OPEN_BIZ_TYPE ,
  COALESCE(CLZ_BIZ_TYPE, '' ) AS CLZ_BIZ_TYPE ,
  COALESCE(STATUS, '' ) AS STATUS ,
  COALESCE(OPE_BRH, '' ) AS OPE_BRH ,
  COALESCE(ACC_ACCEPT_TYPE, '' ) AS ACC_ACCEPT_TYPE ,
  COALESCE(ACC_PRODUCT_TYPE, '' ) AS ACC_PRODUCT_TYPE ,
  COALESCE(TXN_BUSS_TYPE, '' ) AS TXN_BUSS_TYPE ,
  COALESCE(BANK_TYPE, '' ) AS BANK_TYPE ,
  COALESCE(TXN_NAME_CODE, '' ) AS TXN_NAME_CODE ,
  COALESCE(OPEN_INTEREST, 0 ) AS OPEN_INTEREST ,
  COALESCE(CLOZ_INTEREST, 0 ) AS CLOZ_INTEREST ,
  COALESCE(ACC_SRC_TYPE, '' ) AS ACC_SRC_TYPE 
 FROM  dw_tdata.BBS_001_TBL_ACC_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  ACC_NO ,
  TYPE ,
  ACC_NAME ,
  OPEN_DATE ,
  CLZ_DATE ,
  DRAFT_ID ,
  OPEN_BIZ_TYPE ,
  CLZ_BIZ_TYPE ,
  STATUS ,
  OPE_BRH ,
  ACC_ACCEPT_TYPE ,
  ACC_PRODUCT_TYPE ,
  TXN_BUSS_TYPE ,
  BANK_TYPE ,
  TXN_NAME_CODE ,
  OPEN_INTEREST ,
  CLOZ_INTEREST ,
  ACC_SRC_TYPE 
 FROM dw_sdata.BBS_001_TBL_ACC_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.ACC_NO<>T.ACC_NO
 OR N.TYPE<>T.TYPE
 OR N.ACC_NAME<>T.ACC_NAME
 OR N.OPEN_DATE<>T.OPEN_DATE
 OR N.CLZ_DATE<>T.CLZ_DATE
 OR N.DRAFT_ID<>T.DRAFT_ID
 OR N.OPEN_BIZ_TYPE<>T.OPEN_BIZ_TYPE
 OR N.CLZ_BIZ_TYPE<>T.CLZ_BIZ_TYPE
 OR N.STATUS<>T.STATUS
 OR N.OPE_BRH<>T.OPE_BRH
 OR N.ACC_ACCEPT_TYPE<>T.ACC_ACCEPT_TYPE
 OR N.ACC_PRODUCT_TYPE<>T.ACC_PRODUCT_TYPE
 OR N.TXN_BUSS_TYPE<>T.TXN_BUSS_TYPE
 OR N.BANK_TYPE<>T.BANK_TYPE
 OR N.TXN_NAME_CODE<>T.TXN_NAME_CODE
 OR N.OPEN_INTEREST<>T.OPEN_INTEREST
 OR N.CLOZ_INTEREST<>T.CLOZ_INTEREST
 OR N.ACC_SRC_TYPE<>T.ACC_SRC_TYPE
;

--Step3:
UPDATE dw_sdata.BBS_001_TBL_ACC_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_62
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_62.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_TBL_ACC_INFO SELECT * FROM T_62;

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
