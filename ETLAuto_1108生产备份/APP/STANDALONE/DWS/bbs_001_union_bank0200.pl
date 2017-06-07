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
DELETE FROM dw_sdata.BBS_001_UNION_BANK WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_UNION_BANK SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_63 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_UNION_BANK WHERE 1=0;

--Step2:
INSERT  INTO T_63 (
  ID,
  UBANK_NO,
  CTGY,
  CLSS,
  UBANK_CAT_ID,
  DRCT,
  NDCD,
  SPRR_LST,
  PBCBK,
  UBANK_CITY,
  UBANK_NAME,
  SHRT_NM,
  UBANK_ADDRESS,
  UBANK_ZIP,
  UBANK_TEL,
  EMAIL,
  FCTV_DT,
  UPD_TIME,
  TERM_NB,
  PROC_STATUS,
  RMRK,
  XPRY_DT,
  STATUS,
  UBANK_LINKMAN,
  CERT_INFO_CN,
  CERT_INFO_SN,
  CERT_BIND_STATUS,
  CERT_VALIDE_DATE,
  CERT_INVALIDE_DATE,
  LAST_UPD_OPRID,
  LAST_UPD_TXN_ID,
  LAST_UPD_TS,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.UBANK_NO,
  N.CTGY,
  N.CLSS,
  N.UBANK_CAT_ID,
  N.DRCT,
  N.NDCD,
  N.SPRR_LST,
  N.PBCBK,
  N.UBANK_CITY,
  N.UBANK_NAME,
  N.SHRT_NM,
  N.UBANK_ADDRESS,
  N.UBANK_ZIP,
  N.UBANK_TEL,
  N.EMAIL,
  N.FCTV_DT,
  N.UPD_TIME,
  N.TERM_NB,
  N.PROC_STATUS,
  N.RMRK,
  N.XPRY_DT,
  N.STATUS,
  N.UBANK_LINKMAN,
  N.CERT_INFO_CN,
  N.CERT_INFO_SN,
  N.CERT_BIND_STATUS,
  N.CERT_VALIDE_DATE,
  N.CERT_INVALIDE_DATE,
  N.LAST_UPD_OPRID,
  N.LAST_UPD_TXN_ID,
  N.LAST_UPD_TS,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(UBANK_NO, '' ) AS UBANK_NO ,
  COALESCE(CTGY, '' ) AS CTGY ,
  COALESCE(CLSS, '' ) AS CLSS ,
  COALESCE(UBANK_CAT_ID, 0 ) AS UBANK_CAT_ID ,
  COALESCE(DRCT, '' ) AS DRCT ,
  COALESCE(NDCD, '' ) AS NDCD ,
  COALESCE(SPRR_LST, '' ) AS SPRR_LST ,
  COALESCE(PBCBK, '' ) AS PBCBK ,
  COALESCE(UBANK_CITY, '' ) AS UBANK_CITY ,
  COALESCE(UBANK_NAME, '' ) AS UBANK_NAME ,
  COALESCE(SHRT_NM, '' ) AS SHRT_NM ,
  COALESCE(UBANK_ADDRESS, '' ) AS UBANK_ADDRESS ,
  COALESCE(UBANK_ZIP, '' ) AS UBANK_ZIP ,
  COALESCE(UBANK_TEL, '' ) AS UBANK_TEL ,
  COALESCE(EMAIL, '' ) AS EMAIL ,
  COALESCE(FCTV_DT, '' ) AS FCTV_DT ,
  COALESCE(UPD_TIME, '' ) AS UPD_TIME ,
  COALESCE(TERM_NB, '' ) AS TERM_NB ,
  COALESCE(PROC_STATUS, '' ) AS PROC_STATUS ,
  COALESCE(RMRK, '' ) AS RMRK ,
  COALESCE(XPRY_DT, '' ) AS XPRY_DT ,
  COALESCE(STATUS, '' ) AS STATUS ,
  COALESCE(UBANK_LINKMAN, '' ) AS UBANK_LINKMAN ,
  COALESCE(CERT_INFO_CN, '' ) AS CERT_INFO_CN ,
  COALESCE(CERT_INFO_SN, '' ) AS CERT_INFO_SN ,
  COALESCE(CERT_BIND_STATUS, '' ) AS CERT_BIND_STATUS ,
  COALESCE(CERT_VALIDE_DATE, '' ) AS CERT_VALIDE_DATE ,
  COALESCE(CERT_INVALIDE_DATE, '' ) AS CERT_INVALIDE_DATE ,
  COALESCE(LAST_UPD_OPRID, 0 ) AS LAST_UPD_OPRID ,
  COALESCE(LAST_UPD_TXN_ID, '' ) AS LAST_UPD_TXN_ID ,
  COALESCE(LAST_UPD_TS, '' ) AS LAST_UPD_TS 
 FROM  dw_tdata.BBS_001_UNION_BANK_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  UBANK_NO ,
  CTGY ,
  CLSS ,
  UBANK_CAT_ID ,
  DRCT ,
  NDCD ,
  SPRR_LST ,
  PBCBK ,
  UBANK_CITY ,
  UBANK_NAME ,
  SHRT_NM ,
  UBANK_ADDRESS ,
  UBANK_ZIP ,
  UBANK_TEL ,
  EMAIL ,
  FCTV_DT ,
  UPD_TIME ,
  TERM_NB ,
  PROC_STATUS ,
  RMRK ,
  XPRY_DT ,
  STATUS ,
  UBANK_LINKMAN ,
  CERT_INFO_CN ,
  CERT_INFO_SN ,
  CERT_BIND_STATUS ,
  CERT_VALIDE_DATE ,
  CERT_INVALIDE_DATE ,
  LAST_UPD_OPRID ,
  LAST_UPD_TXN_ID ,
  LAST_UPD_TS 
 FROM dw_sdata.BBS_001_UNION_BANK 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.UBANK_NO<>T.UBANK_NO
 OR N.CTGY<>T.CTGY
 OR N.CLSS<>T.CLSS
 OR N.UBANK_CAT_ID<>T.UBANK_CAT_ID
 OR N.DRCT<>T.DRCT
 OR N.NDCD<>T.NDCD
 OR N.SPRR_LST<>T.SPRR_LST
 OR N.PBCBK<>T.PBCBK
 OR N.UBANK_CITY<>T.UBANK_CITY
 OR N.UBANK_NAME<>T.UBANK_NAME
 OR N.SHRT_NM<>T.SHRT_NM
 OR N.UBANK_ADDRESS<>T.UBANK_ADDRESS
 OR N.UBANK_ZIP<>T.UBANK_ZIP
 OR N.UBANK_TEL<>T.UBANK_TEL
 OR N.EMAIL<>T.EMAIL
 OR N.FCTV_DT<>T.FCTV_DT
 OR N.UPD_TIME<>T.UPD_TIME
 OR N.TERM_NB<>T.TERM_NB
 OR N.PROC_STATUS<>T.PROC_STATUS
 OR N.RMRK<>T.RMRK
 OR N.XPRY_DT<>T.XPRY_DT
 OR N.STATUS<>T.STATUS
 OR N.UBANK_LINKMAN<>T.UBANK_LINKMAN
 OR N.CERT_INFO_CN<>T.CERT_INFO_CN
 OR N.CERT_INFO_SN<>T.CERT_INFO_SN
 OR N.CERT_BIND_STATUS<>T.CERT_BIND_STATUS
 OR N.CERT_VALIDE_DATE<>T.CERT_VALIDE_DATE
 OR N.CERT_INVALIDE_DATE<>T.CERT_INVALIDE_DATE
 OR N.LAST_UPD_OPRID<>T.LAST_UPD_OPRID
 OR N.LAST_UPD_TXN_ID<>T.LAST_UPD_TXN_ID
 OR N.LAST_UPD_TS<>T.LAST_UPD_TS
;

--Step3:
UPDATE dw_sdata.BBS_001_UNION_BANK P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_63
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_63.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_UNION_BANK SELECT * FROM T_63;

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
