-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "f5181b9f-5e33-4e46-ab81-b316b571336e",
-- META       "default_lakehouse_name": "adventureworks",
-- META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "f5181b9f-5e33-4e46-ab81-b316b571336e"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "479d62fd-d01f-40c4-9f10-8384a8bc8623",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "479d62fd-d01f-40c4-9f10-8384a8bc8623",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

--get a list of tables

select * from INFORMATION_SCHEMA.TABLES



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--what tables do i have access to?

SELECT suser_sname() as 'whoami' , TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE';

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--get a list of tables and relationships
SELECT
  ChildSchema.name AS ChildSchema
 ,Child.name AS ChildTable  
 ,FK.name
 ,ParentSchema.name AS ParentSchema
 ,Parent.name AS ParentTable
FROM
 sys.foreign_keys FK
INNER JOIN
  sys.tables Child
    ON Child.object_id = FK.parent_object_id --Parent here means the "parent" of the FK, not the relation
INNER JOIN
  sys.schemas ChildSchema
    ON ChildSchema.schema_id = Child.schema_id
INNER JOIN
  sys.tables Parent
    ON Parent.object_id = FK.referenced_object_id
INNER JOIN
  sys.schemas ParentSchema
    ON ParentSchema.schema_id = Parent.schema_id
ORDER BY 
  ChildSchema.name
 ,Child.name

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--determine metadata of server
EXECUTE sp_helpsort;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--determine stats on table
EXEC sp_help 'dimaccount'

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT name, collation_name FROM sys.databases;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
