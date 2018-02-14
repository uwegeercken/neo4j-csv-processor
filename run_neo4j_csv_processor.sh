#!/bin/bash
#
# script to envoke the csv processor program
#
# last update uwe geercken, 2018-02-14
#

## required libraries to run the process
neo4j_jdbc_driver_jar=neo4j-jdbc-driver-3.0.jar
csv_processor_jar=neo4j_csv_processor-0.2.0.jar

## arguments to the program.
## all arguments except "delimiter" are mandatory
hostname=localhost
username=neo4j
password=yourneo4jpassword
output_folder=/home/uwe/development/data
csv_filename=test_1.csv
delimiter=;


# run the program
java -cp ${neo4j_jdbc_driver_jar}:${csv_processor_jar} com.datamelt.neo4j.csv.processor.NodeToCsvProcessor -h="${hostname}" -u="${username}" -p="${password}" -o="${output_folder}" -c="${csv_filename}" -d="${delimiter}"
