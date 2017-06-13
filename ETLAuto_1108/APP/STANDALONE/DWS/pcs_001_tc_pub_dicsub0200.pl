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
DELETE FROM dw_sdata.PCS_001_TC_PUB_DICSUB WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_001_TC_PUB_DICSUB SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_315 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_001_TC_PUB_DICSUB WHERE 1=0;

--Step2:
INSERT  INTO T_315 (
  SUBDIC_ID,
  BIGCLASS_CD,
  SUBDIC_CD,
  SUBDIC_NAME,
  REL_DIC_CD,
  SUBDIC_ORDER,
  SUBDIC_VERSION,
  SUBDIC_DESC,
  DELFLAG,
  CREATE_TIME,
  UPDATE_TIME,
  TRUNC_NO,
  start_dt,
  end_dt)
SELECT
  N.SUBDIC_ID,
  N.BIGCLASS_CD,
  N.SUBDIC_CD,
  N.SUBDIC_NAME,
  N.REL_DIC_CD,
  N.SUBDIC_ORDER,
  N.SUBDIC_VERSION,
  N.SUBDIC_DESC,
  N.DELFLAG,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.TRUNC_NO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(SUBDIC_ID, '' ) AS SUBDIC_ID ,
  COALESCE(BIGCLASS_CD, '' ) AS BIGCLASS_CD ,
  COALESCE(SUBDIC_CD, '' ) AS SUBDIC_CD ,
  COALESCE(SUBDIC_NAME, '' ) AS SUBDIC_NAME ,
  COALESCE(REL_DIC_CD, '' ) AS REL_DIC_CD ,
  COALESCE(SUBDIC_ORDER, '' ) AS SUBDIC_ORDER ,
  COALESCE(SUBDIC_VERSION, '' ) AS SUBDIC_VERSION ,
  COALESCE(SUBDIC_DESC, '' ) AS SUBDIC_DESC ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO 
 FROM  dw_tdata.PCS_001_TC_PUB_DICSUB_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  SUBDIC_ID ,
  BIGCLASS_CD ,
  SUBDIC_CD ,
  SUBDIC_NAME ,
  REL_DIC_CD ,
  SUBDIC_ORDER ,
  SUBDIC_VERSION ,
  SUBDIC_DESC ,
  DELFLAG ,
  CREATE_TIME ,
  UPDATE_TIME ,
  TRUNC_NO 
 FROM dw_sdata.PCS_001_TC_PUB_DICSUB 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.SUBDIC_ID = T.SUBDIC_ID
WHERE
(T.SUBDIC_ID IS NULL)
 OR N.BIGCLASS_CD<>T.BIGCLASS_CD
 OR N.SUBDIC_CD<>T.SUBDIC_CD
 OR N.SUBDIC_NAME<>T.SUBDIC_NAME
 OR N.REL_DIC_CD<>T.REL_DIC_CD
 OR N.SUBDIC_ORDER<>T.SUBDIC_ORDER
 OR N.SUBDIC_VERSION<>T.SUBDIC_VERSION
 OR N.SUBDIC_DESC<>T.SUBDIC_DESC
 OR N.DELFLAG<>T.DELFLAG
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.TRUNC_NO<>T.TRUNC_NO
;

--Step3:
UPDATE dw_sdata.PCS_001_TC_PUB_DICSUB P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_315
WHERE P.End_Dt=DATE('2100-12-31')
AND P.SUBDIC_ID=T_315.SUBDIC_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_001_TC_PUB_DICSUB SELECT * FROM T_315;

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
