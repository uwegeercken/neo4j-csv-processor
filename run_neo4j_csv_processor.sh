#!/bin/bash

neo4j_jdbc_driver_jar=neo4j-jdbc-driver-3.0.jar
csv_processor_jar=neo4j_csv_processor-0.2.0.jar

hostname=localhost
username=neo4j
password=fasthans
output_folder=/home/uwe/development/data
csv_filename=test_1.csv
delimiter=;


java -cp ${neo4j_jdbc_driver_jar}:${csv_processor_jar} com.datamelt.neo4j.csv.processor.NodeToCsvProcessor -h="${hostname}" -u="${username}" -p="${password}" -o="${output_folder}" -c="${csv_filename}" -d="${delimiter}"
