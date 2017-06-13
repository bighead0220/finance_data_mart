#!/bin/bash

echo -n "Password:" && stty -echo && read plaintext && stty echo && echo

password=`perl -e "use etl_unix; print ETL::Encrypt(${plaintext});"`

echo ${password}

exit 0
