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
DELETE FROM dw_sdata.OGS_000_TBL_FINC_BRH_MEM_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.OGS_000_TBL_FINC_BRH_MEM_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_310 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.OGS_000_TBL_FINC_BRH_MEM_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_310 (
  BRH_CODE,
  BRH_BANK_NUM,
  BRH_POST_NUM,
  UP_JUNIOR_COLL_NUM,
  AVG_AGE,
  BANK_BUS_NUM,
  GUA_NUM,
  FINC_MEM_NUM,
  DT_MGR_NUM,
  JDT_MGR_NUM,
  CASH_MGR_NUM,
  JCASH_MGR_NUM,
  LAST_UPD_TM,
  LAST_UPD_OPR,
  LAST_UPD_TXN,
  RESV1,
  start_dt,
  end_dt)
SELECT
  N.BRH_CODE,
  N.BRH_BANK_NUM,
  N.BRH_POST_NUM,
  N.UP_JUNIOR_COLL_NUM,
  N.AVG_AGE,
  N.BANK_BUS_NUM,
  N.GUA_NUM,
  N.FINC_MEM_NUM,
  N.DT_MGR_NUM,
  N.JDT_MGR_NUM,
  N.CASH_MGR_NUM,
  N.JCASH_MGR_NUM,
  N.LAST_UPD_TM,
  N.LAST_UPD_OPR,
  N.LAST_UPD_TXN,
  N.RESV1,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(BRH_CODE, '' ) AS BRH_CODE ,
  COALESCE(BRH_BANK_NUM, 0 ) AS BRH_BANK_NUM ,
  COALESCE(BRH_POST_NUM, 0 ) AS BRH_POST_NUM ,
  COALESCE(UP_JUNIOR_COLL_NUM, 0 ) AS UP_JUNIOR_COLL_NUM ,
  COALESCE(AVG_AGE, 0 ) AS AVG_AGE ,
  COALESCE(BANK_BUS_NUM, 0 ) AS BANK_BUS_NUM ,
  COALESCE(GUA_NUM, 0 ) AS GUA_NUM ,
  COALESCE(FINC_MEM_NUM, 0 ) AS FINC_MEM_NUM ,
  COALESCE(DT_MGR_NUM, 0 ) AS DT_MGR_NUM ,
  COALESCE(JDT_MGR_NUM, 0 ) AS JDT_MGR_NUM ,
  COALESCE(CASH_MGR_NUM, 0 ) AS CASH_MGR_NUM ,
  COALESCE(JCASH_MGR_NUM, 0 ) AS JCASH_MGR_NUM ,
  COALESCE(LAST_UPD_TM, '' ) AS LAST_UPD_TM ,
  COALESCE(LAST_UPD_OPR, '' ) AS LAST_UPD_OPR ,
  COALESCE(LAST_UPD_TXN, '' ) AS LAST_UPD_TXN ,
  COALESCE(RESV1, '' ) AS RESV1 
 FROM  dw_tdata.OGS_000_TBL_FINC_BRH_MEM_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  BRH_CODE ,
  BRH_BANK_NUM ,
  BRH_POST_NUM ,
  UP_JUNIOR_COLL_NUM ,
  AVG_AGE ,
  BANK_BUS_NUM ,
  GUA_NUM ,
  FINC_MEM_NUM ,
  DT_MGR_NUM ,
  JDT_MGR_NUM ,
  CASH_MGR_NUM ,
  JCASH_MGR_NUM ,
  LAST_UPD_TM ,
  LAST_UPD_OPR ,
  LAST_UPD_TXN ,
  RESV1 
 FROM dw_sdata.OGS_000_TBL_FINC_BRH_MEM_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BRH_CODE = T.BRH_CODE
WHERE
(T.BRH_CODE IS NULL)
 OR N.BRH_BANK_NUM<>T.BRH_BANK_NUM
 OR N.BRH_POST_NUM<>T.BRH_POST_NUM
 OR N.UP_JUNIOR_COLL_NUM<>T.UP_JUNIOR_COLL_NUM
 OR N.AVG_AGE<>T.AVG_AGE
 OR N.BANK_BUS_NUM<>T.BANK_BUS_NUM
 OR N.GUA_NUM<>T.GUA_NUM
 OR N.FINC_MEM_NUM<>T.FINC_MEM_NUM
 OR N.DT_MGR_NUM<>T.DT_MGR_NUM
 OR N.JDT_MGR_NUM<>T.JDT_MGR_NUM
 OR N.CASH_MGR_NUM<>T.CASH_MGR_NUM
 OR N.JCASH_MGR_NUM<>T.JCASH_MGR_NUM
 OR N.LAST_UPD_TM<>T.LAST_UPD_TM
 OR N.LAST_UPD_OPR<>T.LAST_UPD_OPR
 OR N.LAST_UPD_TXN<>T.LAST_UPD_TXN
 OR N.RESV1<>T.RESV1
;

--Step3:
UPDATE dw_sdata.OGS_000_TBL_FINC_BRH_MEM_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_310
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BRH_CODE=T_310.BRH_CODE
;

--Step4:
INSERT  INTO dw_sdata.OGS_000_TBL_FINC_BRH_MEM_INFO SELECT * FROM T_310;

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
