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
DELETE FROM dw_sdata.PCS_006_TB_AST_CAR WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_006_TB_AST_CAR SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_373 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_006_TB_AST_CAR WHERE 1=0;

--Step2:
INSERT  INTO T_373 (
  ASSET_ID,
  ASSET_NAME,
  CAR_NAME,
  BUYCAR_CONTNO,
  PLACE_OF_ORIGIN,
  MANUFACTURERS,
  BRAND,
  MANUF_MODEL,
  MOTOR_VEHICLE_CERNUM,
  ISSUING_AUTHORITY,
  DRIVING_LICENSE_NUM,
  DRILIC_ISSUING_AGENCY,
  CERTIFICATE,
  NUM_OF_SEATS,
  DATE_OF_MANUFACTURE,
  LNVOICE_NUM,
  TRAFFIC_GRADE,
  WHETHER_MODIFY,
  MODIFY_COMPANY,
  ROAD_TRANSPORT_PERMITS,
  RTP_ISSUING_AUTHORITY,
  CAR_COLOR,
  CREATE_TIME,
  UPDATE_TIME,
  DELFLAG,
  TRUNC_NO,
  CAR_CATEGORY_ONE,
  CAR_CATEGORY_TWO,
  CAR_CATEGORY_THREE,
  start_dt,
  end_dt)
SELECT
  N.ASSET_ID,
  N.ASSET_NAME,
  N.CAR_NAME,
  N.BUYCAR_CONTNO,
  N.PLACE_OF_ORIGIN,
  N.MANUFACTURERS,
  N.BRAND,
  N.MANUF_MODEL,
  N.MOTOR_VEHICLE_CERNUM,
  N.ISSUING_AUTHORITY,
  N.DRIVING_LICENSE_NUM,
  N.DRILIC_ISSUING_AGENCY,
  N.CERTIFICATE,
  N.NUM_OF_SEATS,
  N.DATE_OF_MANUFACTURE,
  N.LNVOICE_NUM,
  N.TRAFFIC_GRADE,
  N.WHETHER_MODIFY,
  N.MODIFY_COMPANY,
  N.ROAD_TRANSPORT_PERMITS,
  N.RTP_ISSUING_AUTHORITY,
  N.CAR_COLOR,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.DELFLAG,
  N.TRUNC_NO,
  N.CAR_CATEGORY_ONE,
  N.CAR_CATEGORY_TWO,
  N.CAR_CATEGORY_THREE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ASSET_ID, '' ) AS ASSET_ID ,
  COALESCE(ASSET_NAME, '' ) AS ASSET_NAME ,
  COALESCE(CAR_NAME, '' ) AS CAR_NAME ,
  COALESCE(BUYCAR_CONTNO, '' ) AS BUYCAR_CONTNO ,
  COALESCE(PLACE_OF_ORIGIN, '' ) AS PLACE_OF_ORIGIN ,
  COALESCE(MANUFACTURERS, '' ) AS MANUFACTURERS ,
  COALESCE(BRAND, '' ) AS BRAND ,
  COALESCE(MANUF_MODEL, '' ) AS MANUF_MODEL ,
  COALESCE(MOTOR_VEHICLE_CERNUM, '' ) AS MOTOR_VEHICLE_CERNUM ,
  COALESCE(ISSUING_AUTHORITY, '' ) AS ISSUING_AUTHORITY ,
  COALESCE(DRIVING_LICENSE_NUM, '' ) AS DRIVING_LICENSE_NUM ,
  COALESCE(DRILIC_ISSUING_AGENCY, '' ) AS DRILIC_ISSUING_AGENCY ,
  COALESCE(CERTIFICATE, '' ) AS CERTIFICATE ,
  COALESCE(NUM_OF_SEATS, 0 ) AS NUM_OF_SEATS ,
  COALESCE(DATE_OF_MANUFACTURE,DATE('4999-12-31') ) AS DATE_OF_MANUFACTURE ,
  COALESCE(LNVOICE_NUM, '' ) AS LNVOICE_NUM ,
  COALESCE(TRAFFIC_GRADE, '' ) AS TRAFFIC_GRADE ,
  COALESCE(WHETHER_MODIFY, '' ) AS WHETHER_MODIFY ,
  COALESCE(MODIFY_COMPANY, '' ) AS MODIFY_COMPANY ,
  COALESCE(ROAD_TRANSPORT_PERMITS, '' ) AS ROAD_TRANSPORT_PERMITS ,
  COALESCE(RTP_ISSUING_AUTHORITY, '' ) AS RTP_ISSUING_AUTHORITY ,
  COALESCE(CAR_COLOR, '' ) AS CAR_COLOR ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO ,
  COALESCE(CAR_CATEGORY_ONE, '' ) AS CAR_CATEGORY_ONE ,
  COALESCE(CAR_CATEGORY_TWO, '' ) AS CAR_CATEGORY_TWO ,
  COALESCE(CAR_CATEGORY_THREE, '' ) AS CAR_CATEGORY_THREE 
 FROM  dw_tdata.PCS_006_TB_AST_CAR_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ASSET_ID ,
  ASSET_NAME ,
  CAR_NAME ,
  BUYCAR_CONTNO ,
  PLACE_OF_ORIGIN ,
  MANUFACTURERS ,
  BRAND ,
  MANUF_MODEL ,
  MOTOR_VEHICLE_CERNUM ,
  ISSUING_AUTHORITY ,
  DRIVING_LICENSE_NUM ,
  DRILIC_ISSUING_AGENCY ,
  CERTIFICATE ,
  NUM_OF_SEATS ,
  DATE_OF_MANUFACTURE ,
  LNVOICE_NUM ,
  TRAFFIC_GRADE ,
  WHETHER_MODIFY ,
  MODIFY_COMPANY ,
  ROAD_TRANSPORT_PERMITS ,
  RTP_ISSUING_AUTHORITY ,
  CAR_COLOR ,
  CREATE_TIME ,
  UPDATE_TIME ,
  DELFLAG ,
  TRUNC_NO ,
  CAR_CATEGORY_ONE ,
  CAR_CATEGORY_TWO ,
  CAR_CATEGORY_THREE 
 FROM dw_sdata.PCS_006_TB_AST_CAR 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ASSET_ID = T.ASSET_ID
WHERE
(T.ASSET_ID IS NULL)
 OR N.ASSET_NAME<>T.ASSET_NAME
 OR N.CAR_NAME<>T.CAR_NAME
 OR N.BUYCAR_CONTNO<>T.BUYCAR_CONTNO
 OR N.PLACE_OF_ORIGIN<>T.PLACE_OF_ORIGIN
 OR N.MANUFACTURERS<>T.MANUFACTURERS
 OR N.BRAND<>T.BRAND
 OR N.MANUF_MODEL<>T.MANUF_MODEL
 OR N.MOTOR_VEHICLE_CERNUM<>T.MOTOR_VEHICLE_CERNUM
 OR N.ISSUING_AUTHORITY<>T.ISSUING_AUTHORITY
 OR N.DRIVING_LICENSE_NUM<>T.DRIVING_LICENSE_NUM
 OR N.DRILIC_ISSUING_AGENCY<>T.DRILIC_ISSUING_AGENCY
 OR N.CERTIFICATE<>T.CERTIFICATE
 OR N.NUM_OF_SEATS<>T.NUM_OF_SEATS
 OR N.DATE_OF_MANUFACTURE<>T.DATE_OF_MANUFACTURE
 OR N.LNVOICE_NUM<>T.LNVOICE_NUM
 OR N.TRAFFIC_GRADE<>T.TRAFFIC_GRADE
 OR N.WHETHER_MODIFY<>T.WHETHER_MODIFY
 OR N.MODIFY_COMPANY<>T.MODIFY_COMPANY
 OR N.ROAD_TRANSPORT_PERMITS<>T.ROAD_TRANSPORT_PERMITS
 OR N.RTP_ISSUING_AUTHORITY<>T.RTP_ISSUING_AUTHORITY
 OR N.CAR_COLOR<>T.CAR_COLOR
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.DELFLAG<>T.DELFLAG
 OR N.TRUNC_NO<>T.TRUNC_NO
 OR N.CAR_CATEGORY_ONE<>T.CAR_CATEGORY_ONE
 OR N.CAR_CATEGORY_TWO<>T.CAR_CATEGORY_TWO
 OR N.CAR_CATEGORY_THREE<>T.CAR_CATEGORY_THREE
;

--Step3:
UPDATE dw_sdata.PCS_006_TB_AST_CAR P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_373
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ASSET_ID=T_373.ASSET_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_006_TB_AST_CAR SELECT * FROM T_373;

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
