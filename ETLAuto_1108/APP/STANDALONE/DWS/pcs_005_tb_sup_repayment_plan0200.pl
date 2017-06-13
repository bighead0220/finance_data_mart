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
DELETE FROM dw_sdata.PCS_005_TB_SUP_REPAYMENT_PLAN WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_005_TB_SUP_REPAYMENT_PLAN SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_322 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_005_TB_SUP_REPAYMENT_PLAN WHERE 1=0;

--Step2:
INSERT  INTO T_322 (
  PRV_COD,
  OPN_DEP,
  TAL_DEP,
  DUE_NUM,
  PRM_CLS,
  AST_CLS,
  TRAN_FROM,
  CURR_PERI,
  EFFE_CURR_PERI,
  BEG_DATE,
  END_DATE,
  PROV_DATE,
  CURR_PRJ_PRN,
  CURR_PRJ_ITR,
  SPEC_AMT,
  ITR_RATE_CHG_FLG,
  ITR_RATE_CHG_NUM,
  FORW_PAY_PRN_FLG,
  RMK1,
  RMK2,
  ITR_FRE_DEAL_FLG,
  start_dt,
  end_dt)
SELECT
  N.PRV_COD,
  N.OPN_DEP,
  N.TAL_DEP,
  N.DUE_NUM,
  N.PRM_CLS,
  N.AST_CLS,
  N.TRAN_FROM,
  N.CURR_PERI,
  N.EFFE_CURR_PERI,
  N.BEG_DATE,
  N.END_DATE,
  N.PROV_DATE,
  N.CURR_PRJ_PRN,
  N.CURR_PRJ_ITR,
  N.SPEC_AMT,
  N.ITR_RATE_CHG_FLG,
  N.ITR_RATE_CHG_NUM,
  N.FORW_PAY_PRN_FLG,
  N.RMK1,
  N.RMK2,
  N.ITR_FRE_DEAL_FLG,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PRV_COD, '' ) AS PRV_COD ,
  COALESCE(OPN_DEP, '' ) AS OPN_DEP ,
  COALESCE(TAL_DEP, '' ) AS TAL_DEP ,
  COALESCE(DUE_NUM, '' ) AS DUE_NUM ,
  COALESCE(PRM_CLS, '' ) AS PRM_CLS ,
  COALESCE(AST_CLS, '' ) AS AST_CLS ,
  COALESCE(TRAN_FROM, '' ) AS TRAN_FROM ,
  COALESCE(CURR_PERI, 0 ) AS CURR_PERI ,
  COALESCE(EFFE_CURR_PERI, 0 ) AS EFFE_CURR_PERI ,
  COALESCE(BEG_DATE, '' ) AS BEG_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE ,
  COALESCE(PROV_DATE, '' ) AS PROV_DATE ,
  COALESCE(CURR_PRJ_PRN, 0 ) AS CURR_PRJ_PRN ,
  COALESCE(CURR_PRJ_ITR, 0 ) AS CURR_PRJ_ITR ,
  COALESCE(SPEC_AMT, 0 ) AS SPEC_AMT ,
  COALESCE(ITR_RATE_CHG_FLG, '' ) AS ITR_RATE_CHG_FLG ,
  COALESCE(ITR_RATE_CHG_NUM, 0 ) AS ITR_RATE_CHG_NUM ,
  COALESCE(FORW_PAY_PRN_FLG, '' ) AS FORW_PAY_PRN_FLG ,
  COALESCE(RMK1, 0 ) AS RMK1 ,
  COALESCE(RMK2, '' ) AS RMK2 ,
  COALESCE(ITR_FRE_DEAL_FLG, '' ) AS ITR_FRE_DEAL_FLG 
 FROM  dw_tdata.PCS_005_TB_SUP_REPAYMENT_PLAN_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PRV_COD ,
  OPN_DEP ,
  TAL_DEP ,
  DUE_NUM ,
  PRM_CLS ,
  AST_CLS ,
  TRAN_FROM ,
  CURR_PERI ,
  EFFE_CURR_PERI ,
  BEG_DATE ,
  END_DATE ,
  PROV_DATE ,
  CURR_PRJ_PRN ,
  CURR_PRJ_ITR ,
  SPEC_AMT ,
  ITR_RATE_CHG_FLG ,
  ITR_RATE_CHG_NUM ,
  FORW_PAY_PRN_FLG ,
  RMK1 ,
  RMK2 ,
  ITR_FRE_DEAL_FLG 
 FROM dw_sdata.PCS_005_TB_SUP_REPAYMENT_PLAN 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.DUE_NUM = T.DUE_NUM
WHERE
(T.DUE_NUM IS NULL)
 OR N.PRV_COD<>T.PRV_COD
 OR N.OPN_DEP<>T.OPN_DEP
 OR N.TAL_DEP<>T.TAL_DEP
 OR N.PRM_CLS<>T.PRM_CLS
 OR N.AST_CLS<>T.AST_CLS
 OR N.TRAN_FROM<>T.TRAN_FROM
 OR N.CURR_PERI<>T.CURR_PERI
 OR N.EFFE_CURR_PERI<>T.EFFE_CURR_PERI
 OR N.BEG_DATE<>T.BEG_DATE
 OR N.END_DATE<>T.END_DATE
 OR N.PROV_DATE<>T.PROV_DATE
 OR N.CURR_PRJ_PRN<>T.CURR_PRJ_PRN
 OR N.CURR_PRJ_ITR<>T.CURR_PRJ_ITR
 OR N.SPEC_AMT<>T.SPEC_AMT
 OR N.ITR_RATE_CHG_FLG<>T.ITR_RATE_CHG_FLG
 OR N.ITR_RATE_CHG_NUM<>T.ITR_RATE_CHG_NUM
 OR N.FORW_PAY_PRN_FLG<>T.FORW_PAY_PRN_FLG
 OR N.RMK1<>T.RMK1
 OR N.RMK2<>T.RMK2
 OR N.ITR_FRE_DEAL_FLG<>T.ITR_FRE_DEAL_FLG
;

--Step3:
UPDATE dw_sdata.PCS_005_TB_SUP_REPAYMENT_PLAN P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_322
WHERE P.End_Dt=DATE('2100-12-31')
AND P.DUE_NUM=T_322.DUE_NUM
;

--Step4:
INSERT  INTO dw_sdata.PCS_005_TB_SUP_REPAYMENT_PLAN SELECT * FROM T_322;

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
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
