# neo4j-csv-processor

This Java application is  used for preparing data from a CSV file for the neo4j-import tool. As the import tool uses special format, it is a lot of manual work to convert an existing CSV file into this format. So this tool automates this process and reduces the manual effort.

What is special about this tool is, that the mapping of the nodes and relations to the CSV file is done in neo4j itself. I believe it is a typical use-case to first model the database in neo4j. Then, at a point where the schema is clear and defined, one wants to import data. So the starting point of the process is neo4j and that is where the tool starts.

Following this use-case, you can go ahead and model your graph as you would usually do - create nodes, relationships and define properties. Before running this tool, the nodes have to be tagged with a specific label (default: "Meta"); these will be the nodes that the tool will use during processing. The values of the node properties contain the name of the column in the CSV file to use. You may also add a property for the namespace of the node (named: "namespace").

The CSV file needs to have a header row which also uses the same delimiter as the data rows. For the tool to work, you need to add a label to those nodes that shall be considered for processing by the tool. The default name of the label is "Meta" but you can use a different name. Specify this same name in the "-l=" argument when running the tool to tell the tool which nodes are to be used. You must mark/tag a property as the unique ID field by adding ":ID" at the end of the property value.

You may also specify when a field is of type "int" (integer) or "float". Add ":int" or ":float" after the value of the property and before the ":ID" tag (if present).

Here are examples:

We assume we have a CSV file with following columns: key, last_name, birth_date, year_of_birth. Then the node could look like this:

create (node:Employee:Meta {lname:"last_name", employee_key:"key:ID", birth_date:"birth_date", year_of_birth: "born:int"});

Note that we have a label "Meta". When we run the neo4j-csv-processor we will tell it to use nodes which have this label. We also have the mandatory "ID" field and the year_of_birth is marked as type "integer".

or

create (node:Employee:MySchema {lname:"last_name", employee_key:"key:float:ID", birth_date:"birth_date", year_of_birth: "born:int"});

Here we have changed the name of the label (to "MySchema") and we have flagged the column "key" from the CSV file as the ID field and declared it as a "float" value.

or

create (node:Employee:Schema {namespace: "EMP-NS", lname:"last_name", employee_key:"key:ID", birth_date:"birth_date", year_of_birth: "born:int"});

Here we have added a definition for the namespace that shall be used to identify the node.

The values of the properties are the names of the columns of the CSV file. Plus the values have tags for data type ("int" and "float". "string" is the default) and one field must be tagged as the unique key field (ID) for that node.

The tool will extract the nodes from the active neo4j database, extract their properties and the relations between the nodes and finally create the CSV files. These are then ready to use with the neo4j-import tool.

Run the tool:

First download the neo4j_csv_processor-0.2.0.jar file and the neo4j jdbc file.

java -cp neo4j_csv_processor-0.2.0.jar:neo4j-jdbc-driver-3.0.jar com.datamelt.neo4j.csv.processor.NodeToCsvProcessor arguments...

Pass following arguments:

-h = neo4j hostname
-u = neo4j user
-p = neo4j user password
-o = output folder
-c = CSV file to use (path and filename)
-d = optional. default ";". delimiter used in the CSV file and Header
-l = optional. default "Meta". the name of the label used as metadata. only nodes with this label are processed.

Example:

java -cp neo4j_csv_processor-0.2.0.jar:neo4j-jdbc-driver-3.0.jar com.datamelt.neo4j.csv.processor.NodeToCsvProcessor -h=localhost -u=neo4j -p=xxxx -o=/opt/neo4j-community-3.3.2/import -c=/data/file.csv -d=; -l=MySchema

There is also a shell script available which you can use to run the tool: run_neo4j_csv_processor.sh

This is the first version of the tool with only basic functionallity. I will continue to integrate more features in the future.

Some ideas for future versions:
- make the tool feature complete according to the neo4j-import tool
- integrate a ruleengine to allow to process, filter and modify (adjust) the data before import
- handle large data sets

last update: Uwe Geercken, 2018-02-27

