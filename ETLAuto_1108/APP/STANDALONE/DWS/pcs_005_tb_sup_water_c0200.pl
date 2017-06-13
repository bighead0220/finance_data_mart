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

DELETE FROM dw_sdata.PCS_005_TB_SUP_WATER_C WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.PCS_005_TB_SUP_WATER_C(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  PRV_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUP_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACS_METH_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRN_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPN_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TAL_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_FROM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACS_METH_STAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUP_STAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  REC_SEQ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RCN_STAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CUS_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DUE_NUM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRW_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRN_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RECALL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_CLAS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  STD_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRW_LGO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT_INCUR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  REL_TIM_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BAL_DIRT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUMMARY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRN_TM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  HOST_TM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUP_AID_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SEQ_NUM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CUST_ACC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PACK_SIGN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUSN_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRN_NM_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MATH_FLG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SND_ACC_PLT_FLG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHBK_FIL_NM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHBK_FIL_SEQ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACCT_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRIM_ACCT_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRIM_OPEN_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRIM_ACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK4 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(PRV_COD, '' ) AS PRV_COD ,
  COALESCE(SUP_DATE, '' ) AS SUP_DATE ,
  COALESCE(ACS_METH_DATE, '' ) AS ACS_METH_DATE ,
  COALESCE(TRN_DEP, '' ) AS TRN_DEP ,
  COALESCE(OPN_DEP, '' ) AS OPN_DEP ,
  COALESCE(TAL_DEP, '' ) AS TAL_DEP ,
  COALESCE(TRAN_FROM, '' ) AS TRAN_FROM ,
  COALESCE(ACS_METH_STAN, 0 ) AS ACS_METH_STAN ,
  COALESCE(SUP_STAN, 0 ) AS SUP_STAN ,
  COALESCE(REC_SEQ, 0 ) AS REC_SEQ ,
  COALESCE(RCN_STAN, '' ) AS RCN_STAN ,
  COALESCE(CUS_NO, '' ) AS CUS_NO ,
  COALESCE(DUE_NUM, '' ) AS DUE_NUM ,
  COALESCE(BRW_NAME, '' ) AS BRW_NAME ,
  COALESCE(ACC_TYP, '' ) AS ACC_TYP ,
  COALESCE(TRN_COD, '' ) AS TRN_COD ,
  COALESCE(CURR_COD, '' ) AS CURR_COD ,
  COALESCE(RECALL, '' ) AS RECALL ,
  COALESCE(ACC_CLAS, '' ) AS ACC_CLAS ,
  COALESCE(STD_COD, '' ) AS STD_COD ,
  COALESCE(BRW_LGO, '' ) AS BRW_LGO ,
  COALESCE(AMT_INCUR, 0 ) AS AMT_INCUR ,
  COALESCE(REL_TIM_BAL, 0 ) AS REL_TIM_BAL ,
  COALESCE(BAL_DIRT, '' ) AS BAL_DIRT ,
  COALESCE(SUMMARY, '' ) AS SUMMARY ,
  COALESCE(OPR, '' ) AS OPR ,
  COALESCE(AUT, '' ) AS AUT ,
  COALESCE(TRN_TM, '' ) AS TRN_TM ,
  COALESCE(HOST_TM, '' ) AS HOST_TM ,
  COALESCE(SUP_AID_DATE, '' ) AS SUP_AID_DATE ,
  COALESCE(SEQ_NUM, 0 ) AS SEQ_NUM ,
  COALESCE(CUST_ACC, '' ) AS CUST_ACC ,
  COALESCE(PACK_SIGN, '' ) AS PACK_SIGN ,
  COALESCE(BUSN_COD, '' ) AS BUSN_COD ,
  COALESCE(TRN_NM_COD, '' ) AS TRN_NM_COD ,
  COALESCE(MATH_FLG, '' ) AS MATH_FLG ,
  COALESCE(SND_ACC_PLT_FLG, '' ) AS SND_ACC_PLT_FLG ,
  COALESCE(CHBK_FIL_NM, '' ) AS CHBK_FIL_NM ,
  COALESCE(CHBK_FIL_SEQ, '' ) AS CHBK_FIL_SEQ ,
  COALESCE(ACCT_DATE, '' ) AS ACCT_DATE ,
  COALESCE(PRIM_ACCT_TYP, '' ) AS PRIM_ACCT_TYP ,
  COALESCE(PRIM_OPEN_DEP, '' ) AS PRIM_OPEN_DEP ,
  COALESCE(PRIM_ACCT, '' ) AS PRIM_ACCT ,
  COALESCE(RMK1, '' ) AS RMK1 ,
  COALESCE(RMK2, '' ) AS RMK2 ,
  COALESCE(RMK3, '' ) AS RMK3 ,
  COALESCE(RMK4, '' ) AS RMK4 ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.PCS_005_TB_SUP_WATER_C_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
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
