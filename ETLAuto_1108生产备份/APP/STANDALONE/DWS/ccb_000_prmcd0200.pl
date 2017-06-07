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
DELETE FROM dw_sdata.CCB_000_PRMCD WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCB_000_PRMCD SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_99 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCB_000_PRMCD WHERE 1=0;

--Step2:
INSERT  INTO T_99 (
  PRODUCT,
  PROD_DESC,
  PROD_LEVEL,
  CATEGORY,
  SERV_CODE,
  CARD_PLAN,
  CURR_NUM2,
  PRODUCT_TY,
  PROD_GRP,
  BRANCH,
  FEE_GROUP,
  APPROD_GRP,
  HCARDPRE,
  LCARDPRE,
  YR_1ST_ISS,
  start_dt,
  end_dt)
SELECT
  N.PRODUCT,
  N.PROD_DESC,
  N.PROD_LEVEL,
  N.CATEGORY,
  N.SERV_CODE,
  N.CARD_PLAN,
  N.CURR_NUM2,
  N.PRODUCT_TY,
  N.PROD_GRP,
  N.BRANCH,
  N.FEE_GROUP,
  N.APPROD_GRP,
  N.HCARDPRE,
  N.LCARDPRE,
  N.YR_1ST_ISS,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PRODUCT, 0 ) AS PRODUCT ,
  COALESCE(PROD_DESC, '' ) AS PROD_DESC ,
  COALESCE(PROD_LEVEL, 0 ) AS PROD_LEVEL ,
  COALESCE(CATEGORY, 0 ) AS CATEGORY ,
  COALESCE(SERV_CODE, '' ) AS SERV_CODE ,
  COALESCE(CARD_PLAN, '' ) AS CARD_PLAN ,
  COALESCE(CURR_NUM2, 0 ) AS CURR_NUM2 ,
  COALESCE(PRODUCT_TY, '' ) AS PRODUCT_TY ,
  COALESCE(PROD_GRP, '' ) AS PROD_GRP ,
  COALESCE(BRANCH, 0 ) AS BRANCH ,
  COALESCE(FEE_GROUP, 0 ) AS FEE_GROUP ,
  COALESCE(APPROD_GRP, '' ) AS APPROD_GRP ,
  COALESCE(HCARDPRE, 0 ) AS HCARDPRE ,
  COALESCE(LCARDPRE, 0 ) AS LCARDPRE ,
  COALESCE(YR_1ST_ISS, 0 ) AS YR_1ST_ISS 
 FROM  dw_tdata.CCB_000_PRMCD_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PRODUCT ,
  PROD_DESC ,
  PROD_LEVEL ,
  CATEGORY ,
  SERV_CODE ,
  CARD_PLAN ,
  CURR_NUM2 ,
  PRODUCT_TY ,
  PROD_GRP ,
  BRANCH ,
  FEE_GROUP ,
  APPROD_GRP ,
  HCARDPRE ,
  LCARDPRE ,
  YR_1ST_ISS 
 FROM dw_sdata.CCB_000_PRMCD 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PRODUCT = T.PRODUCT
WHERE
(T.PRODUCT IS NULL)
 OR N.PROD_DESC<>T.PROD_DESC
 OR N.PROD_LEVEL<>T.PROD_LEVEL
 OR N.CATEGORY<>T.CATEGORY
 OR N.SERV_CODE<>T.SERV_CODE
 OR N.CARD_PLAN<>T.CARD_PLAN
 OR N.CURR_NUM2<>T.CURR_NUM2
 OR N.PRODUCT_TY<>T.PRODUCT_TY
 OR N.PROD_GRP<>T.PROD_GRP
 OR N.BRANCH<>T.BRANCH
 OR N.FEE_GROUP<>T.FEE_GROUP
 OR N.APPROD_GRP<>T.APPROD_GRP
 OR N.HCARDPRE<>T.HCARDPRE
 OR N.LCARDPRE<>T.LCARDPRE
 OR N.YR_1ST_ISS<>T.YR_1ST_ISS
;

--Step3:
UPDATE dw_sdata.CCB_000_PRMCD P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_99
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PRODUCT=T_99.PRODUCT
;

--Step4:
INSERT  INTO dw_sdata.CCB_000_PRMCD SELECT * FROM T_99;

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
