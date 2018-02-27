#!/bin/bash
#
# script to envoke the csv processor program
#
# you need a running instance of neo4j. the active database must be that
# database which contains the metadata defining which labels and properties
# of the the nodes and relations should be mapped to which CSV columns.
#
#
# last update uwe geercken, 2018-02-27
#

## required libraries to run the process
neo4j_jdbc_driver_jar=neo4j-jdbc-driver-3.0.jar
csv_processor_jar=neo4j_csv_processor-0.2.0.jar

## arguments to the program.
## all arguments except "delimiter" are mandatory
hostname="localhost"
username="neo4j"
password="fasthans"
output_folder="/opt/neo4j-community-3.3.2/import/epx"
csv_filename="/opt/neo4j-community-3.3.2/import/epx/epx_201801_neo4j.csv"
delimiter=";"
metalabel="Meta"


# run the program
java -cp ${neo4j_jdbc_driver_jar}:${csv_processor_jar} com.datamelt.neo4j.csv.processor.NodeToCsvProcessor -h="${hostname}" -u="${username}" -p="${password}" -o="${output_folder}" -c="${csv_filename}" -d="${delimiter}" -l="${metalabel}"

