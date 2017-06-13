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

DELETE FROM dw_sdata.COS_000_GL_ENTRY WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.COS_000_GL_ENTRY(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  ACTION_DT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACTION_ORIG_DT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUTO_REVERSAL_STATUS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BASE_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BASE_CCY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BASE_AMT_RATE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BASE_FXRATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CCY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CFLOW_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHART_ACC_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  COMMENT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  COMMENTS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEAL_LEG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEAL_MAP_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEAL_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ENTITY_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXCH_FLUC_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXCH_GROUP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXCH_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_ENTRY_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_ENTRY_LINK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_ENTRY_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_OWNER_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_REVAL_SET_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_TAG_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_BASE_SET_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_HOLDS_ROUND_ADJ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IMAGE_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INPUT_DT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INT_FACE_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IS_BASE_SET ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IS_BASE_TX ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IS_CONTINGENT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IS_INTERCOMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IS_REVAL_MARGIN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  LEDGER_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MATCHED_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RATE_DT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  REVERSAL_STATUS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RULE_SET_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RULE_SET_SEQ_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRANS_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_POST_SOURCE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  POST_ITEM_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(ACTION_DT, '' ) AS ACTION_DT ,
  COALESCE(ACTION_ORIG_DT, '' ) AS ACTION_ORIG_DT ,
  COALESCE(AMOUNT, 0 ) AS AMOUNT ,
  COALESCE(AUTO_REVERSAL_STATUS, 0 ) AS AUTO_REVERSAL_STATUS ,
  COALESCE(BASE_AMT, 0 ) AS BASE_AMT ,
  COALESCE(BASE_CCY, '' ) AS BASE_CCY ,
  COALESCE(BASE_AMT_RATE_TYPE, 0 ) AS BASE_AMT_RATE_TYPE ,
  COALESCE(BASE_FXRATE, 0 ) AS BASE_FXRATE ,
  COALESCE(CCY, '' ) AS CCY ,
  COALESCE(CFLOW_ID, 0 ) AS CFLOW_ID ,
  COALESCE(CHART_ACC_ID, 0 ) AS CHART_ACC_ID ,
  COALESCE(COMMENT_TYPE, 0 ) AS COMMENT_TYPE ,
  COALESCE(COMMENTS, '' ) AS COMMENTS ,
  COALESCE(DEAL_LEG, 0 ) AS DEAL_LEG ,
  COALESCE(DEAL_MAP_ID, 0 ) AS DEAL_MAP_ID ,
  COALESCE(DEAL_NO, 0 ) AS DEAL_NO ,
  COALESCE(DEAL_TYPE, 0 ) AS DEAL_TYPE ,
  COALESCE(ENTITY_ID, 0 ) AS ENTITY_ID ,
  COALESCE(EXCH_FLUC_AMT, 0 ) AS EXCH_FLUC_AMT ,
  COALESCE(EXCH_GROUP, '' ) AS EXCH_GROUP ,
  COALESCE(EXCH_TYPE, '' ) AS EXCH_TYPE ,
  COALESCE(GL_ENTRY_ID, 0 ) AS GL_ENTRY_ID ,
  COALESCE(GL_ENTRY_LINK, 0 ) AS GL_ENTRY_LINK ,
  COALESCE(GL_ENTRY_TYPE, '' ) AS GL_ENTRY_TYPE ,
  COALESCE(GL_OWNER_ID, 0 ) AS GL_OWNER_ID ,
  COALESCE(GL_REVAL_SET_NO, 0 ) AS GL_REVAL_SET_NO ,
  COALESCE(GL_TAG_ID, 0 ) AS GL_TAG_ID ,
  COALESCE(GL_BASE_SET_NO, 0 ) AS GL_BASE_SET_NO ,
  COALESCE(GL_HOLDS_ROUND_ADJ, 0 ) AS GL_HOLDS_ROUND_ADJ ,
  COALESCE(IMAGE_NO, 0 ) AS IMAGE_NO ,
  COALESCE(INPUT_DT, '' ) AS INPUT_DT ,
  COALESCE(INT_FACE_NO, 0 ) AS INT_FACE_NO ,
  COALESCE(IS_BASE_SET, 0 ) AS IS_BASE_SET ,
  COALESCE(IS_BASE_TX, 0 ) AS IS_BASE_TX ,
  COALESCE(IS_CONTINGENT, 0 ) AS IS_CONTINGENT ,
  COALESCE(IS_INTERCOMP, 0 ) AS IS_INTERCOMP ,
  COALESCE(IS_REVAL_MARGIN, 0 ) AS IS_REVAL_MARGIN ,
  COALESCE(LEDGER_ID, 0 ) AS LEDGER_ID ,
  COALESCE(MATCHED_AMT, 0 ) AS MATCHED_AMT ,
  COALESCE(RATE_DT, '' ) AS RATE_DT ,
  COALESCE(REVERSAL_STATUS, 0 ) AS REVERSAL_STATUS ,
  COALESCE(RULE_SET_ID, 0 ) AS RULE_SET_ID ,
  COALESCE(RULE_SET_SEQ_NO, 0 ) AS RULE_SET_SEQ_NO ,
  COALESCE(TRANS_TYPE, '' ) AS TRANS_TYPE ,
  COALESCE(GL_POST_SOURCE, 0 ) AS GL_POST_SOURCE ,
  COALESCE(POST_ITEM_ID, 0 ) AS POST_ITEM_ID ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.COS_000_GL_ENTRY_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
