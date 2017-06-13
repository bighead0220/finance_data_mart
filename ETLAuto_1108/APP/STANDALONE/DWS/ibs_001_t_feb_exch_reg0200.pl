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

DELETE FROM dw_sdata.IBS_001_T_FEB_EXCH_REG WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.IBS_001_T_FEB_EXCH_REG(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  TRAN_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLT_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  STAT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_INST ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TLR_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUTH_TLR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_TIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IO_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRC_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CSH_TSF_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUY_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUY_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SELL_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SELL_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DRAW_CSH_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE4 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUY_PRC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SELL_PRC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUT_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUT_ACC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUT_SUB_ACC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUT_OP_INST ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IN_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IN_ACC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IN_SUB_ACC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IN_OP_INST ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RECORD_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  APPROVE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  APPROVE_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAPER_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAPER_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGT_PAPER_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGT_PAPER_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OLD_TRAN_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OLD_CLT_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(TRAN_DATE, 0 ) AS TRAN_DATE ,
  COALESCE(CLT_SEQNO, '' ) AS CLT_SEQNO ,
  COALESCE(STAT, '' ) AS STAT ,
  COALESCE(TRAN_INST, '' ) AS TRAN_INST ,
  COALESCE(TLR_NO, '' ) AS TLR_NO ,
  COALESCE(AUTH_TLR, '' ) AS AUTH_TLR ,
  COALESCE(TRAN_TIME, '' ) AS TRAN_TIME ,
  COALESCE(IO_FLAG, '' ) AS IO_FLAG ,
  COALESCE(TYPE, '' ) AS TYPE ,
  COALESCE(PRC_FLAG, '' ) AS PRC_FLAG ,
  COALESCE(CSH_TSF_FLAG, '' ) AS CSH_TSF_FLAG ,
  COALESCE(FLAG, '' ) AS FLAG ,
  COALESCE(TRAN_NO, '' ) AS TRAN_NO ,
  COALESCE(SUB_CODE, '' ) AS SUB_CODE ,
  COALESCE(BUY_CURR_TYPE, '' ) AS BUY_CURR_TYPE ,
  COALESCE(BUY_AMT, 0 ) AS BUY_AMT ,
  COALESCE(SELL_CURR_TYPE, '' ) AS SELL_CURR_TYPE ,
  COALESCE(SELL_AMT, 0 ) AS SELL_AMT ,
  COALESCE(DRAW_CSH_AMT, 0 ) AS DRAW_CSH_AMT ,
  COALESCE(FEE1, 0 ) AS FEE1 ,
  COALESCE(FEE2, 0 ) AS FEE2 ,
  COALESCE(FEE3, 0 ) AS FEE3 ,
  COALESCE(FEE4, 0 ) AS FEE4 ,
  COALESCE(BUY_PRC, 0 ) AS BUY_PRC ,
  COALESCE(SELL_PRC, 0 ) AS SELL_PRC ,
  COALESCE(OUT_FLAG, '' ) AS OUT_FLAG ,
  COALESCE(OUT_ACC, '' ) AS OUT_ACC ,
  COALESCE(OUT_SUB_ACC, 0 ) AS OUT_SUB_ACC ,
  COALESCE(OUT_OP_INST, '' ) AS OUT_OP_INST ,
  COALESCE(IN_FLAG, '' ) AS IN_FLAG ,
  COALESCE(IN_ACC, '' ) AS IN_ACC ,
  COALESCE(IN_SUB_ACC, 0 ) AS IN_SUB_ACC ,
  COALESCE(IN_OP_INST, '' ) AS IN_OP_INST ,
  COALESCE(RECORD_FLAG, '' ) AS RECORD_FLAG ,
  COALESCE(ACC_NAME, '' ) AS ACC_NAME ,
  COALESCE(APPROVE_TYPE, '' ) AS APPROVE_TYPE ,
  COALESCE(APPROVE_NO, '' ) AS APPROVE_NO ,
  COALESCE(PAPER_TYPE, '' ) AS PAPER_TYPE ,
  COALESCE(PAPER_NO, '' ) AS PAPER_NO ,
  COALESCE(AGT_PAPER_TYPE, '' ) AS AGT_PAPER_TYPE ,
  COALESCE(AGT_PAPER_NO, '' ) AS AGT_PAPER_NO ,
  COALESCE(OLD_TRAN_DATE, 0 ) AS OLD_TRAN_DATE ,
  COALESCE(OLD_CLT_SEQNO, '' ) AS OLD_CLT_SEQNO ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.IBS_001_T_FEB_EXCH_REG_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
