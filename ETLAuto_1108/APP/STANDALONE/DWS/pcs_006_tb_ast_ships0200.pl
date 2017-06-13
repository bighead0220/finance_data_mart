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
DELETE FROM dw_sdata.PCS_006_TB_AST_SHIPS WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_006_TB_AST_SHIPS SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_380 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_006_TB_AST_SHIPS WHERE 1=0;

--Step2:
INSERT  INTO T_380 (
  ASSET_ID,
  ASSET_NAME,
  CERTIFICATE_NUM,
  CER_ISSUING_AUTHORITY,
  SHIP_REGCER_NUM,
  SHIP_REGCER_ISSAUT,
  CONTRACT_NO,
  INVOICE_NO,
  MANUFACTURERS,
  FACTORY_MODEL,
  SHIP_BRAND,
  PLACE_OF_ORIGIN,
  SHIP_GRADES,
  FLAG_CERTIFICATE,
  SECURITY_CARD_NUM,
  CREATE_TIME,
  UPDATE_TIME,
  DELFLAG,
  TRUNC_NO,
  start_dt,
  end_dt)
SELECT
  N.ASSET_ID,
  N.ASSET_NAME,
  N.CERTIFICATE_NUM,
  N.CER_ISSUING_AUTHORITY,
  N.SHIP_REGCER_NUM,
  N.SHIP_REGCER_ISSAUT,
  N.CONTRACT_NO,
  N.INVOICE_NO,
  N.MANUFACTURERS,
  N.FACTORY_MODEL,
  N.SHIP_BRAND,
  N.PLACE_OF_ORIGIN,
  N.SHIP_GRADES,
  N.FLAG_CERTIFICATE,
  N.SECURITY_CARD_NUM,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.DELFLAG,
  N.TRUNC_NO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ASSET_ID, '' ) AS ASSET_ID ,
  COALESCE(ASSET_NAME, '' ) AS ASSET_NAME ,
  COALESCE(CERTIFICATE_NUM, '' ) AS CERTIFICATE_NUM ,
  COALESCE(CER_ISSUING_AUTHORITY, '' ) AS CER_ISSUING_AUTHORITY ,
  COALESCE(SHIP_REGCER_NUM, '' ) AS SHIP_REGCER_NUM ,
  COALESCE(SHIP_REGCER_ISSAUT, '' ) AS SHIP_REGCER_ISSAUT ,
  COALESCE(CONTRACT_NO, '' ) AS CONTRACT_NO ,
  COALESCE(INVOICE_NO, '' ) AS INVOICE_NO ,
  COALESCE(MANUFACTURERS, '' ) AS MANUFACTURERS ,
  COALESCE(FACTORY_MODEL, '' ) AS FACTORY_MODEL ,
  COALESCE(SHIP_BRAND, '' ) AS SHIP_BRAND ,
  COALESCE(PLACE_OF_ORIGIN, '' ) AS PLACE_OF_ORIGIN ,
  COALESCE(SHIP_GRADES, '' ) AS SHIP_GRADES ,
  COALESCE(FLAG_CERTIFICATE, '' ) AS FLAG_CERTIFICATE ,
  COALESCE(SECURITY_CARD_NUM, '' ) AS SECURITY_CARD_NUM ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO 
 FROM  dw_tdata.PCS_006_TB_AST_SHIPS_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ASSET_ID ,
  ASSET_NAME ,
  CERTIFICATE_NUM ,
  CER_ISSUING_AUTHORITY ,
  SHIP_REGCER_NUM ,
  SHIP_REGCER_ISSAUT ,
  CONTRACT_NO ,
  INVOICE_NO ,
  MANUFACTURERS ,
  FACTORY_MODEL ,
  SHIP_BRAND ,
  PLACE_OF_ORIGIN ,
  SHIP_GRADES ,
  FLAG_CERTIFICATE ,
  SECURITY_CARD_NUM ,
  CREATE_TIME ,
  UPDATE_TIME ,
  DELFLAG ,
  TRUNC_NO 
 FROM dw_sdata.PCS_006_TB_AST_SHIPS 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ASSET_ID = T.ASSET_ID
WHERE
(T.ASSET_ID IS NULL)
 OR N.ASSET_NAME<>T.ASSET_NAME
 OR N.CERTIFICATE_NUM<>T.CERTIFICATE_NUM
 OR N.CER_ISSUING_AUTHORITY<>T.CER_ISSUING_AUTHORITY
 OR N.SHIP_REGCER_NUM<>T.SHIP_REGCER_NUM
 OR N.SHIP_REGCER_ISSAUT<>T.SHIP_REGCER_ISSAUT
 OR N.CONTRACT_NO<>T.CONTRACT_NO
 OR N.INVOICE_NO<>T.INVOICE_NO
 OR N.MANUFACTURERS<>T.MANUFACTURERS
 OR N.FACTORY_MODEL<>T.FACTORY_MODEL
 OR N.SHIP_BRAND<>T.SHIP_BRAND
 OR N.PLACE_OF_ORIGIN<>T.PLACE_OF_ORIGIN
 OR N.SHIP_GRADES<>T.SHIP_GRADES
 OR N.FLAG_CERTIFICATE<>T.FLAG_CERTIFICATE
 OR N.SECURITY_CARD_NUM<>T.SECURITY_CARD_NUM
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.DELFLAG<>T.DELFLAG
 OR N.TRUNC_NO<>T.TRUNC_NO
;

--Step3:
UPDATE dw_sdata.PCS_006_TB_AST_SHIPS P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_380
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ASSET_ID=T_380.ASSET_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_006_TB_AST_SHIPS SELECT * FROM T_380;

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
