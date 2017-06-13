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
DELETE FROM dw_sdata.BBS_001_ECDS_ACCEPT_APPLY WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_ECDS_ACCEPT_APPLY SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_49 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_ECDS_ACCEPT_APPLY WHERE 1=0;

--Step2:
INSERT  INTO T_49 (
  SWT_BIZ_ID,
  MSGID,
  CREDTTM,
  DRAFT_NUMBER,
  ISSE_CURCD,
  ISSE_AMT,
  MESG_TYPE,
  UCONDL_CONSGNTMRK,
  CONTRACTNO,
  INVC_NB,
  BTCH_NB,
  DRWR_ROLE,
  DRWR_CMON_ID,
  DRWR_ACTNO,
  DRWR_UBANK,
  DRWR_AGCY_UBANK,
  DF_TP,
  DF_DRAFT_NUMBER,
  DF_ISSE_CURCD,
  DF_ISSEAMT,
  DF_ISSE_DT,
  DF_DUE_DT,
  DF_BAN_ENDRSMT_MK,
  DF_DRWR_ROLE,
  DF_DRWR_NAME,
  DF_DRWR_CMONID,
  DF_DRWR_ACTNO,
  DF_DRWR_UBANK,
  DF_DRWR_CDTRATGS,
  DF_DRWR_CDTRATGSAGCY,
  DF_DRWR_CDTRATGDUEDT,
  DF_ACCEPTR_NAME,
  DF_ACCEPTR_ACTNO,
  DF_ACCEPTR_UBANK,
  DF_PYEE_NAME,
  DF_PYEE_ACTNO,
  DF_PYEE_UBANK,
  APPLY_STATUS,
  APPLOCK,
  DETAIL_ID,
  LAST_UPD_OPER_ID,
  LAST_UPD_TIME,
  RMRK_BY_PROPSR,
  DF_RMRK,
  start_dt,
  end_dt)
SELECT
  N.SWT_BIZ_ID,
  N.MSGID,
  N.CREDTTM,
  N.DRAFT_NUMBER,
  N.ISSE_CURCD,
  N.ISSE_AMT,
  N.MESG_TYPE,
  N.UCONDL_CONSGNTMRK,
  N.CONTRACTNO,
  N.INVC_NB,
  N.BTCH_NB,
  N.DRWR_ROLE,
  N.DRWR_CMON_ID,
  N.DRWR_ACTNO,
  N.DRWR_UBANK,
  N.DRWR_AGCY_UBANK,
  N.DF_TP,
  N.DF_DRAFT_NUMBER,
  N.DF_ISSE_CURCD,
  N.DF_ISSEAMT,
  N.DF_ISSE_DT,
  N.DF_DUE_DT,
  N.DF_BAN_ENDRSMT_MK,
  N.DF_DRWR_ROLE,
  N.DF_DRWR_NAME,
  N.DF_DRWR_CMONID,
  N.DF_DRWR_ACTNO,
  N.DF_DRWR_UBANK,
  N.DF_DRWR_CDTRATGS,
  N.DF_DRWR_CDTRATGSAGCY,
  N.DF_DRWR_CDTRATGDUEDT,
  N.DF_ACCEPTR_NAME,
  N.DF_ACCEPTR_ACTNO,
  N.DF_ACCEPTR_UBANK,
  N.DF_PYEE_NAME,
  N.DF_PYEE_ACTNO,
  N.DF_PYEE_UBANK,
  N.APPLY_STATUS,
  N.APPLOCK,
  N.DETAIL_ID,
  N.LAST_UPD_OPER_ID,
  N.LAST_UPD_TIME,
  N.RMRK_BY_PROPSR,
  N.DF_RMRK,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(SWT_BIZ_ID, 0 ) AS SWT_BIZ_ID ,
  COALESCE(MSGID, '' ) AS MSGID ,
  COALESCE(CREDTTM, '' ) AS CREDTTM ,
  COALESCE(DRAFT_NUMBER, '' ) AS DRAFT_NUMBER ,
  COALESCE(ISSE_CURCD, '' ) AS ISSE_CURCD ,
  COALESCE(ISSE_AMT, 0 ) AS ISSE_AMT ,
  COALESCE(MESG_TYPE, '' ) AS MESG_TYPE ,
  COALESCE(UCONDL_CONSGNTMRK, '' ) AS UCONDL_CONSGNTMRK ,
  COALESCE(CONTRACTNO, '' ) AS CONTRACTNO ,
  COALESCE(INVC_NB, '' ) AS INVC_NB ,
  COALESCE(BTCH_NB, '' ) AS BTCH_NB ,
  COALESCE(DRWR_ROLE, '' ) AS DRWR_ROLE ,
  COALESCE(DRWR_CMON_ID, '' ) AS DRWR_CMON_ID ,
  COALESCE(DRWR_ACTNO, '' ) AS DRWR_ACTNO ,
  COALESCE(DRWR_UBANK, '' ) AS DRWR_UBANK ,
  COALESCE(DRWR_AGCY_UBANK, '' ) AS DRWR_AGCY_UBANK ,
  COALESCE(DF_TP, '' ) AS DF_TP ,
  COALESCE(DF_DRAFT_NUMBER, '' ) AS DF_DRAFT_NUMBER ,
  COALESCE(DF_ISSE_CURCD, '' ) AS DF_ISSE_CURCD ,
  COALESCE(DF_ISSEAMT, 0 ) AS DF_ISSEAMT ,
  COALESCE(DF_ISSE_DT, '' ) AS DF_ISSE_DT ,
  COALESCE(DF_DUE_DT, '' ) AS DF_DUE_DT ,
  COALESCE(DF_BAN_ENDRSMT_MK, '' ) AS DF_BAN_ENDRSMT_MK ,
  COALESCE(DF_DRWR_ROLE, '' ) AS DF_DRWR_ROLE ,
  COALESCE(DF_DRWR_NAME, '' ) AS DF_DRWR_NAME ,
  COALESCE(DF_DRWR_CMONID, '' ) AS DF_DRWR_CMONID ,
  COALESCE(DF_DRWR_ACTNO, '' ) AS DF_DRWR_ACTNO ,
  COALESCE(DF_DRWR_UBANK, '' ) AS DF_DRWR_UBANK ,
  COALESCE(DF_DRWR_CDTRATGS, '' ) AS DF_DRWR_CDTRATGS ,
  COALESCE(DF_DRWR_CDTRATGSAGCY, '' ) AS DF_DRWR_CDTRATGSAGCY ,
  COALESCE(DF_DRWR_CDTRATGDUEDT, '' ) AS DF_DRWR_CDTRATGDUEDT ,
  COALESCE(DF_ACCEPTR_NAME, '' ) AS DF_ACCEPTR_NAME ,
  COALESCE(DF_ACCEPTR_ACTNO, '' ) AS DF_ACCEPTR_ACTNO ,
  COALESCE(DF_ACCEPTR_UBANK, '' ) AS DF_ACCEPTR_UBANK ,
  COALESCE(DF_PYEE_NAME, '' ) AS DF_PYEE_NAME ,
  COALESCE(DF_PYEE_ACTNO, '' ) AS DF_PYEE_ACTNO ,
  COALESCE(DF_PYEE_UBANK, '' ) AS DF_PYEE_UBANK ,
  COALESCE(APPLY_STATUS, '' ) AS APPLY_STATUS ,
  COALESCE(APPLOCK, '' ) AS APPLOCK ,
  COALESCE(DETAIL_ID, 0 ) AS DETAIL_ID ,
  COALESCE(LAST_UPD_OPER_ID, 0 ) AS LAST_UPD_OPER_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(RMRK_BY_PROPSR, '' ) AS RMRK_BY_PROPSR ,
  COALESCE(DF_RMRK, '' ) AS DF_RMRK 
 FROM  dw_tdata.BBS_001_ECDS_ACCEPT_APPLY_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  SWT_BIZ_ID ,
  MSGID ,
  CREDTTM ,
  DRAFT_NUMBER ,
  ISSE_CURCD ,
  ISSE_AMT ,
  MESG_TYPE ,
  UCONDL_CONSGNTMRK ,
  CONTRACTNO ,
  INVC_NB ,
  BTCH_NB ,
  DRWR_ROLE ,
  DRWR_CMON_ID ,
  DRWR_ACTNO ,
  DRWR_UBANK ,
  DRWR_AGCY_UBANK ,
  DF_TP ,
  DF_DRAFT_NUMBER ,
  DF_ISSE_CURCD ,
  DF_ISSEAMT ,
  DF_ISSE_DT ,
  DF_DUE_DT ,
  DF_BAN_ENDRSMT_MK ,
  DF_DRWR_ROLE ,
  DF_DRWR_NAME ,
  DF_DRWR_CMONID ,
  DF_DRWR_ACTNO ,
  DF_DRWR_UBANK ,
  DF_DRWR_CDTRATGS ,
  DF_DRWR_CDTRATGSAGCY ,
  DF_DRWR_CDTRATGDUEDT ,
  DF_ACCEPTR_NAME ,
  DF_ACCEPTR_ACTNO ,
  DF_ACCEPTR_UBANK ,
  DF_PYEE_NAME ,
  DF_PYEE_ACTNO ,
  DF_PYEE_UBANK ,
  APPLY_STATUS ,
  APPLOCK ,
  DETAIL_ID ,
  LAST_UPD_OPER_ID ,
  LAST_UPD_TIME ,
  RMRK_BY_PROPSR ,
  DF_RMRK 
 FROM dw_sdata.BBS_001_ECDS_ACCEPT_APPLY 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.SWT_BIZ_ID = T.SWT_BIZ_ID
WHERE
(T.SWT_BIZ_ID IS NULL)
 OR N.MSGID<>T.MSGID
 OR N.CREDTTM<>T.CREDTTM
 OR N.DRAFT_NUMBER<>T.DRAFT_NUMBER
 OR N.ISSE_CURCD<>T.ISSE_CURCD
 OR N.ISSE_AMT<>T.ISSE_AMT
 OR N.MESG_TYPE<>T.MESG_TYPE
 OR N.UCONDL_CONSGNTMRK<>T.UCONDL_CONSGNTMRK
 OR N.CONTRACTNO<>T.CONTRACTNO
 OR N.INVC_NB<>T.INVC_NB
 OR N.BTCH_NB<>T.BTCH_NB
 OR N.DRWR_ROLE<>T.DRWR_ROLE
 OR N.DRWR_CMON_ID<>T.DRWR_CMON_ID
 OR N.DRWR_ACTNO<>T.DRWR_ACTNO
 OR N.DRWR_UBANK<>T.DRWR_UBANK
 OR N.DRWR_AGCY_UBANK<>T.DRWR_AGCY_UBANK
 OR N.DF_TP<>T.DF_TP
 OR N.DF_DRAFT_NUMBER<>T.DF_DRAFT_NUMBER
 OR N.DF_ISSE_CURCD<>T.DF_ISSE_CURCD
 OR N.DF_ISSEAMT<>T.DF_ISSEAMT
 OR N.DF_ISSE_DT<>T.DF_ISSE_DT
 OR N.DF_DUE_DT<>T.DF_DUE_DT
 OR N.DF_BAN_ENDRSMT_MK<>T.DF_BAN_ENDRSMT_MK
 OR N.DF_DRWR_ROLE<>T.DF_DRWR_ROLE
 OR N.DF_DRWR_NAME<>T.DF_DRWR_NAME
 OR N.DF_DRWR_CMONID<>T.DF_DRWR_CMONID
 OR N.DF_DRWR_ACTNO<>T.DF_DRWR_ACTNO
 OR N.DF_DRWR_UBANK<>T.DF_DRWR_UBANK
 OR N.DF_DRWR_CDTRATGS<>T.DF_DRWR_CDTRATGS
 OR N.DF_DRWR_CDTRATGSAGCY<>T.DF_DRWR_CDTRATGSAGCY
 OR N.DF_DRWR_CDTRATGDUEDT<>T.DF_DRWR_CDTRATGDUEDT
 OR N.DF_ACCEPTR_NAME<>T.DF_ACCEPTR_NAME
 OR N.DF_ACCEPTR_ACTNO<>T.DF_ACCEPTR_ACTNO
 OR N.DF_ACCEPTR_UBANK<>T.DF_ACCEPTR_UBANK
 OR N.DF_PYEE_NAME<>T.DF_PYEE_NAME
 OR N.DF_PYEE_ACTNO<>T.DF_PYEE_ACTNO
 OR N.DF_PYEE_UBANK<>T.DF_PYEE_UBANK
 OR N.APPLY_STATUS<>T.APPLY_STATUS
 OR N.APPLOCK<>T.APPLOCK
 OR N.DETAIL_ID<>T.DETAIL_ID
 OR N.LAST_UPD_OPER_ID<>T.LAST_UPD_OPER_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.RMRK_BY_PROPSR<>T.RMRK_BY_PROPSR
 OR N.DF_RMRK<>T.DF_RMRK
;

--Step3:
UPDATE dw_sdata.BBS_001_ECDS_ACCEPT_APPLY P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_49
WHERE P.End_Dt=DATE('2100-12-31')
AND P.SWT_BIZ_ID=T_49.SWT_BIZ_ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_ECDS_ACCEPT_APPLY SELECT * FROM T_49;

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
