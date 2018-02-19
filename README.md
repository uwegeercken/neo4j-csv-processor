# neo4j-csv-processor

This Java application is  used for preparing data from a CSV file for the neo4j-import tool. As the import tool uses special format, it is a lot of manual work to convert an existing CSV file to this format. So this tool automates this process and reduces the manual effort.

What is special about this tool is, that the mapping of the nodes and relations to the CSV file is done in neo4j itself. I believe it is a typical use-case to first model the database in neo4j. Then, at a point where the schema is clear and defined, one wants to import data. So the starting point of the process is neo4j and that is where the tool starts.

Following this use-case, you can go ahead and model your graph as you would usually do. Before running this tool, the nodes have to be tagged with a specific label; these are in turn the nodes that the tool will use. The node properties contain the name of the column in the CSV file to use. There are also some other properties that have to be defined serving as metadata to the tool; e.g. which column in the CSV file is the key for a node and the namespace to use for the neo4j-import tool.

The tool will extract the nodes, their properties and the relations between the nodes and finally create the CSV files. These are then ready to use with the neo4j-import tool.

Run the tool:

First download the neo4j_csv_processor-0.2.0.jar file.

java -cp neo4j_csv_processor-0.2.0.jar com.datamelt.neo4j.csv.processor.NodeToCsvProcessor <arguments>

Pass following arguments:

-h = neo4j hostname
-u = neo4j user
-p = neo4j user password
-o = output folder
-c = CSV file to use (path and filename)
-d = delimiter used in the CSV file

Example:

java -cp neo4j_csv_processor-0.2.0.jar com.datamelt.neo4j.csv.processor.NodeToCsvProcessor -h=localhost -u=neo4j -p=xxxx -o=/opt/neo4j-community-3.3.2/import -c=/data/file.csv -d=;


This is the first version of the tool with only basic functionallity. I will continue to integrate more features in the future.

Some ideas for future versions:
- make the tool feature complete according to the neo4j-import tool
- integrate a ruleengine to allow to process, filter and modify (adjust) the data before import
- handle large data sets

last update: Uwe Geercken, 2018-02-19

