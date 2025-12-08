-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "d21695b1-aec9-4ecc-9471-c056ff071f9d",
-- META       "default_lakehouse_name": "davelake",
-- META       "default_lakehouse_workspace_id": "99328097-99f9-47f7-bb76-63191a1432c5"
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # How can we walk the LLC chain?  
-- 
-- * addresses are stored in various tables, as are _owners_, but in most cases the format is _weird_
-- * in the case of owners...their isn't a primary key in most cases.
-- * in the case of properties...their isn't always a primary key either.  
-- 
-- Here's how I'm thinking of doing it.  Create new tables:
-- 
-- |Table|Used For ... | Populated From...|
-- |---|---|---|
-- |masterEntity|has all of the different entities (owner, businesses, etc) that exist throughout the tables |opa_properties_publicowner_1 and owner_2|
-- |masterAddress|every possible address that shows up in the tables|mailing_street, address_std, location|
-- |MasterEntityAddress|opa_propoerties_public|\|
-- 
-- How is this used?
-- * multiple LLCs have the same mailing address (we can make the connection via the address)
-- * at this point, should be able to do fuzzy matching
-- * answer questions like :  What other properties does `A KENSINGTON JOINT LLC` own?
-- 
-- 
-- 
-- 
-- ## Other sources we can use for address resolution
-- * transunion
-- * other jurisdiction records (FL)


-- MARKDOWN ********************

-- |Table|Populated From | columns |
-- |---|---|---|
-- |MasterEntity|opa_properties_public |owner_1 and owner_2|
-- |master address|opa_properties_public|mailing_street, address_std, location|
-- |MasterEntityAddress|opa_propoerties_public|the 6 combinations of opa owners and addresses|

-- MARKDOWN ********************

-- ```
-- sql database connstring  
-- 
-- davew-phillystats.database.windows.net  
-- Initial Catalog=phillystats  
-- --sysadmin/Password01!!
-- CREATE USER [rross_microsoft.com#EXT#@fdpo.onmicrosoft.com] FROM EXTERNAL PROVIDER;
-- ALTER ROLE db_owner ADD MEMBER [rross_microsoft.com#EXT#@fdpo.onmicrosoft.com] ;
-- CREATE USER [pogorman_microsoft.com#EXT#@fdpo.onmicrosoft.com] FROM EXTERNAL PROVIDER;
-- ALTER ROLE db_owner ADD MEMBER [pogorman_microsoft.com#EXT#@fdpo.onmicrosoft.com] ;
-- CREATE USER [jastento_microsoft.com#EXT#@fdpo.onmicrosoft.com] FROM EXTERNAL PROVIDER;
-- ALTER ROLE db_owner ADD MEMBER [jastento_microsoft.com#EXT#@fdpo.onmicrosoft.com] ;
-- ```
-- 
-- ```
-- az account set --subscription 46dbfa9c-1d3a-4595-b77c-a400e2bd4fbe
-- az sql server ad-only-auth disable --resource-group rgPhillyStats --name davew-phillystats
-- 
-- ```


-- MARKDOWN ********************

-- 
-- ## Test Cases
-- 
-- This is a list of all of the problem properties that were given by the Philly team.  We can use these to build out the code and test it
-- 


-- CELL ********************

-- MAGIC %%sql
-- MAGIC SELECT *
-- MAGIC FROM davelake.philly_problempropertylist

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC --master "parcels" are already available in opa_properties_public (PK:  parcel_number)
-- MAGIC --master list of LLC text
-- MAGIC CREATE OR REPLACE TABLE davelake.masterEntity
-- MAGIC (
-- MAGIC     MasterEntityId STRING ,
-- MAGIC     NameText varchar(200)
-- MAGIC );
-- MAGIC --master list of address text
-- MAGIC CREATE OR REPLACE TABLE davelake.masterAddress
-- MAGIC (
-- MAGIC     MasterAddressID STRING ,
-- MAGIC     AddressText varchar(200)
-- MAGIC );
-- MAGIC --junction table for LLCs, parcels, and addresses
-- MAGIC --the PK is LLC, address, and parcel...kindof.  
-- MAGIC --in some cases the tables link the LLC (entity) to the address but the parcel_number or opa_account_num is null.  That may be a valuable relationship to maintain.  
-- MAGIC CREATE OR REPLACE TABLE davelake.masterEntityAddress
-- MAGIC (
-- MAGIC     masterEntityAddressID STRING ,
-- MAGIC     MasterEntityID STRING,
-- MAGIC     MasterAddressID STRING,
-- MAGIC     parcel_number varchar(200),  --associated with which parcel_number
-- MAGIC     Notes varchar(2000) --why this row was created
-- MAGIC );


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from opa_properties_public_pde

-- CELL ********************

-- MAGIC %%sql
-- MAGIC --get the distinct "LLC owners" (entities) from philly_opa_properties_public_pde
-- MAGIC INSERT INTO davelake.masterEntity 
-- MAGIC SELECT 
-- MAGIC     uuid() AS MasterEntityID,
-- MAGIC     DistinctOPAOwners.NameText AS NameText
-- MAGIC FROM 
-- MAGIC     (
-- MAGIC         --owner_1
-- MAGIC         SELECT 
-- MAGIC             DISTINCT opa.owner_1 AS NameText
-- MAGIC         FROM davelake.philly_opa_properties_public_pde opa
-- MAGIC         WHERE opa.owner_1 IS NOT NULL
-- MAGIC         UNION
-- MAGIC         --owner_2
-- MAGIC         SELECT 
-- MAGIC             DISTINCT opa.owner_2 AS NameText
-- MAGIC         FROM davelake.philly_opa_properties_public_pde opa
-- MAGIC         WHERE opa.owner_2 IS NOT NULL
-- MAGIC     ) DistinctOPAOwners 
-- MAGIC LEFT JOIN davelake.MasterEntity master
-- MAGIC     ON DistinctOPAOwners.NameText = master.NameText
-- MAGIC --Entity doesn't yet exist in master table
-- MAGIC WHERE master.NameText IS NULL
-- MAGIC ;
-- MAGIC 
-- MAGIC --now do distinct addresses from philly_opa_properties_public_pde
-- MAGIC --address_std, location, mailing_street are available, do all three, just in case
-- MAGIC INSERT INTO davelake.MasterAddress 
-- MAGIC SELECT 
-- MAGIC     uuid() AS MasterAddressID,
-- MAGIC     DistinctAddressesFromOPATable.AddressText 
-- MAGIC FROM 
-- MAGIC     (
-- MAGIC         --location col
-- MAGIC         SELECT 
-- MAGIC             DISTINCT opa.location AS AddressText
-- MAGIC         FROM davelake.philly_opa_properties_public_pde opa
-- MAGIC         WHERE opa.location IS NOT NULL
-- MAGIC         UNION 
-- MAGIC         --address_std
-- MAGIC         SELECT 
-- MAGIC             DISTINCT opa.address_std AS AddressText
-- MAGIC         FROM davelake.philly_opa_properties_public_pde opa
-- MAGIC         WHERE opa.address_std IS NOT NULL
-- MAGIC         UNION
-- MAGIC         --mailing_street
-- MAGIC         SELECT 
-- MAGIC             DISTINCT opa.mailing_street AS AddressText
-- MAGIC         FROM davelake.philly_opa_properties_public_pde opa
-- MAGIC         WHERE opa.mailing_street IS NOT NULL
-- MAGIC     ) DistinctAddressesFromOPATable
-- MAGIC --where the row does not yet exist
-- MAGIC LEFT JOIN davelake.MasterAddress master
-- MAGIC     ON DistinctAddressesFromOPATable.AddressText = master.AddressText
-- MAGIC WHERE master.AddressText IS NULL
-- MAGIC 
-- MAGIC 
-- MAGIC 


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_opa_properties_public_pde

--1. opa.owner_1 associated with opa.location
CREATE OR REPLACE TEMPORARY VIEW Case1
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    opa.parcel_number,
    lap.MasterEntityAddressID,
    CASE WHEN lap.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'opa.owner_1:opa.location'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN lap.Notes like '%opa.owner_1:opa.location%' THEN lap.Notes
            --new entry
            ELSE CONCAT(lap.Notes ,';opa.owner_1:opa.location') END 
    END AS Notes,
    CASE WHEN lap.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM davelake.philly_opa_properties_public_pde opa
JOIN davelake.MasterEntity llc
    ON opa.owner_1 = llc.NameText
JOIN davelake.masteraddress addr
    ON opa.location = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress lap
    ON llc.MasterEntityID = lap.MasterEntityID
    AND addr.MasterAddressID = lap.MasterAddressID
    AND opa.parcel_number = lap.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING Case1
        ON targ.MasterEntityID = Case1.MasterEntityID
        AND targ.MasterAddressID = Case1.MasterAddressID
        AND targ.parcel_number = Case1.parcel_number
WHEN MATCHED AND Case1.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = Case1.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    Case1.MasterEntityID,
    Case1.MasterAddressID,
    Case1.parcel_number ,
    Case1.Notes
FROM  Case1
WHERE Case1.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--2.owner_1 associated with address_std
CREATE OR REPLACE TEMPORARY VIEW Case2
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    opa.parcel_number,
    lap.MasterEntityAddressID,
    CASE WHEN lap.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'opa.owner_1:opa.address_std'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN lap.Notes like '%opa.owner_1:opa.address_std%' THEN lap.Notes
            --new entry
            ELSE CONCAT(lap.Notes ,';opa.owner_1:opa.address_std') END 
    END AS Notes,
    CASE WHEN lap.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM davelake.philly_opa_properties_public_pde opa
JOIN davelake.MasterEntity llc
    ON opa.owner_1 = llc.NameText
JOIN davelake.masteraddress addr
    ON opa.address_std = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress lap
    ON llc.MasterEntityID = lap.MasterEntityID
    AND addr.MasterAddressID = lap.MasterAddressID
    AND opa.parcel_number = lap.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING Case2
        ON targ.MasterEntityID = Case2.MasterEntityID
        AND targ.MasterAddressID = Case2.MasterAddressID
        AND targ.parcel_number = Case2.parcel_number
WHEN MATCHED AND Case2.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = Case2.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    Case2.MasterEntityID,
    Case2.MasterAddressID,
    Case2.parcel_number ,
    Case2.Notes
FROM  Case2
WHERE Case2.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--3.owner_1 associated with mailing_street
CREATE OR REPLACE TEMPORARY VIEW Case3
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    opa.parcel_number,
    lap.MasterEntityAddressID,
    CASE WHEN lap.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'opa.owner_1:opa.mailing_street'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN lap.Notes like '%opa.owner_1:opa.mailing_street%' THEN lap.Notes
            --new entry
            ELSE CONCAT(lap.Notes ,';opa.owner_1:opa.mailing_street') END 
    END AS Notes,
    CASE WHEN lap.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM davelake.philly_opa_properties_public_pde opa
JOIN davelake.MasterEntity llc
    ON opa.owner_1 = llc.NameText
JOIN davelake.masteraddress addr
    ON opa.mailing_street = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress lap
    ON llc.MasterEntityID = lap.MasterEntityID
    AND addr.MasterAddressID = lap.MasterAddressID
    AND opa.parcel_number = lap.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING Case3
        ON targ.MasterEntityID = Case3.MasterEntityID
        AND targ.MasterAddressID = Case3.MasterAddressID
        AND targ.parcel_number = Case3.parcel_number
WHEN MATCHED AND Case3.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = Case3.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    Case3.MasterEntityID,
    Case3.MasterAddressID,
    Case3.parcel_number ,
    Case3.Notes
FROM  Case3
WHERE Case3.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--4.owner_2 associated with location
CREATE OR REPLACE TEMPORARY VIEW Case4
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    opa.parcel_number,
    lap.MasterEntityAddressID,
    CASE WHEN lap.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'opa.owner_1:opa.location'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN lap.Notes like '%opa.owner_1:opa.location%' THEN lap.Notes
            --new entry
            ELSE CONCAT(lap.Notes ,';opa.owner_1:opa.location') END 
    END AS Notes,
    CASE WHEN lap.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM davelake.philly_opa_properties_public_pde opa
JOIN davelake.MasterEntity llc
    ON opa.owner_2 = llc.NameText
JOIN davelake.masteraddress addr
    ON opa.location = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress lap
    ON llc.MasterEntityID = lap.MasterEntityID
    AND addr.MasterAddressID = lap.MasterAddressID
    AND opa.parcel_number = lap.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING Case4
        ON targ.MasterEntityID = Case4.MasterEntityID
        AND targ.MasterAddressID = Case4.MasterAddressID
        AND targ.parcel_number = Case4.parcel_number
WHEN MATCHED AND Case4.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = Case4.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    Case4.MasterEntityID,
    Case4.MasterAddressID,
    Case4.parcel_number ,
    Case4.Notes
FROM  Case4
WHERE Case4.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--5.owner_2 associated with address_std
CREATE OR REPLACE TEMPORARY VIEW Case5
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    opa.parcel_number,
    lap.MasterEntityAddressID,
    CASE WHEN lap.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'opa.owner_1:opa.address_std'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN lap.Notes like '%opa.owner_1:opa.address_std%' THEN lap.Notes
            --new entry
            ELSE CONCAT(lap.Notes ,';opa.owner_1:opa.address_std') END 
    END AS Notes,
    CASE WHEN lap.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM davelake.philly_opa_properties_public_pde opa
JOIN davelake.MasterEntity llc
    ON opa.owner_2 = llc.NameText
JOIN davelake.masteraddress addr
    ON opa.address_std = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress lap
    ON llc.MasterEntityID = lap.MasterEntityID
    AND addr.MasterAddressID = lap.MasterAddressID
    AND opa.parcel_number = lap.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING Case5
        ON targ.MasterEntityID = Case5.MasterEntityID
        AND targ.MasterAddressID = Case5.MasterAddressID
        AND targ.parcel_number = Case5.parcel_number
WHEN MATCHED AND Case5.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = Case5.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    Case5.MasterEntityID,
    Case5.MasterAddressID,
    Case5.parcel_number ,
    Case5.Notes
FROM  Case5
WHERE Case5.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--6.owner_2 associated wtih mailing_street
CREATE OR REPLACE TEMPORARY VIEW Case6
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    opa.parcel_number,
    lap.MasterEntityAddressID,
    CASE WHEN lap.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'opa.owner_1:opa.mailing_street'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN lap.Notes like '%opa.owner_1:opa.mailing_street%' THEN lap.Notes
            --new entry
            ELSE CONCAT(lap.Notes ,';opa.owner_1:opa.mailing_street') END 
    END AS Notes,
    CASE WHEN lap.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM davelake.philly_opa_properties_public_pde opa
JOIN davelake.MasterEntity llc
    ON opa.owner_2 = llc.NameText
JOIN davelake.masteraddress addr
    ON opa.mailing_street = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress lap
    ON llc.MasterEntityID = lap.MasterEntityID
    AND addr.MasterAddressID = lap.MasterAddressID
    AND opa.parcel_number = lap.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING Case6
        ON targ.MasterEntityID = Case6.MasterEntityID
        AND targ.MasterAddressID = Case6.MasterAddressID
        AND targ.parcel_number = Case6.parcel_number
WHEN MATCHED AND Case6.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = Case6.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    Case6.MasterEntityID,
    Case6.MasterAddressID,
    Case6.parcel_number ,
    Case6.Notes
FROM  Case6
WHERE Case6.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_appeals

-- CELL ********************

--get the distinct "LLC owners" from philly_appeals (opa_owner, primaryapellant)
INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    DistinctOwners.NameText AS NameText
FROM 
    (
        --opa_owner
        SELECT 
            DISTINCT opa.opa_owner AS NameText
        FROM davelake.philly_appeals opa
        WHERE opa.opa_owner IS NOT NULL
        UNION
        --primaryappellant
        SELECT 
            DISTINCT opa.primaryappellant AS NameText
        FROM davelake.philly_appeals opa
        WHERE opa.primaryappellant IS NOT NULL
    ) DistinctOwners 
LEFT JOIN davelake.MasterEntity master
    ON DistinctOwners.NameText = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL
;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do distinct addresses from philly_appeals
--address col
INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    DistinctAddresses.AddressText 
FROM 
    (
        --address col
        SELECT 
            DISTINCT pa.address AS AddressText
        FROM davelake.philly_appeals pa
        WHERE pa.address IS NOT NULL
    ) DistinctAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON DistinctAddresses.AddressText = master.AddressText
WHERE master.AddressText IS NULL

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_appeals
--Case 1:  opa_owner associated with address
CREATE OR REPLACE TEMPORARY VIEW Case1
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    pa.opa_account_num,
    lap.MasterEntityAddressID,
    CASE WHEN lap.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'appeal.opa_owner:appeal.address'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN lap.Notes like '%appeal.opa_owner:appeal.address%' THEN lap.Notes
            --new entry
            ELSE CONCAT(lap.Notes ,';appeal.opa_owner:appeal.address') END 
    END AS Notes,
    CASE WHEN lap.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM davelake.philly_appeals pa
JOIN davelake.MasterEntity llc
    ON pa.opa_owner = llc.NameText
JOIN davelake.masteraddress addr
    ON pa.address = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress lap
    ON llc.MasterEntityID = lap.MasterEntityID
    AND addr.MasterAddressID = lap.MasterAddressID
    AND pa.opa_account_num = lap.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING Case1
        ON targ.MasterEntityID = Case1.MasterEntityID
        AND targ.MasterAddressID = Case1.MasterAddressID
        AND targ.parcel_number = Case1.opa_account_num
WHEN MATCHED AND Case1.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = Case1.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    Case1.MasterEntityID,
    Case1.MasterAddressID,
    Case1.opa_account_num ,
    Case1.Notes
FROM  Case1
WHERE Case1.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--Case 2:  primaryappellant associated with address
CREATE OR REPLACE TEMPORARY VIEW Case2
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    pa.opa_account_num,
    lap.MasterEntityAddressID,
    CASE WHEN lap.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'appeal.primaryappellant:appeal.address'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN lap.Notes like '%appeal.primaryappellant:appeal.address%' THEN lap.Notes
            --new entry
            ELSE CONCAT(lap.Notes ,';appeal.primaryappellant:appeal.address') END 
    END AS Notes,
    CASE WHEN lap.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM davelake.philly_appeals pa
JOIN davelake.MasterEntity llc
    ON pa.primaryappellant = llc.NameText
JOIN davelake.masteraddress addr
    ON pa.address = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress lap
    ON llc.MasterEntityID = lap.MasterEntityID
    AND addr.MasterAddressID = lap.MasterAddressID
    AND pa.opa_account_num = lap.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING Case2
        ON targ.MasterEntityID = Case2.MasterEntityID
        AND targ.MasterAddressID = Case2.MasterAddressID
        AND targ.parcel_number = Case2.opa_account_num
WHEN MATCHED AND Case2.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = Case2.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    Case2.MasterEntityID,
    Case2.MasterAddressID,
    Case2.opa_account_num ,
    Case2.Notes
FROM  Case2
WHERE Case2.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_business_licenses

-- CELL ********************

--get the distinct "LLC owners/entities" from business_licenses

CREATE OR REPLACE TEMPORARY VIEW BizLicEntities
AS 
SELECT  
    opa_account_num,
    upper(trim(business_name)) as EntityName
    --,'business_name' as EntityType
from davelake.philly_business_licenses 
where business_name is not null
UNION
SELECT 
    opa_account_num,
    upper(trim(opa_owner)) as EntityName
    --,'opa_owner' as EntityType
from davelake.philly_business_licenses 
where opa_owner is not null
UNION 
SELECT 
    opa_account_num,
    upper(trim(ownercontact1name)) as EntityName
    --,'ownercontact1name' as EntityType
from davelake.philly_business_licenses 
where ownercontact1name is not null
UNION 
SELECT 
    opa_account_num,
    upper(trim(ownercontact2name)) as EntityName
    --,'ownercontact2name' as EntityType
from davelake.philly_business_licenses 
where ownercontact2name is not null
UNION 
SELECT 
    opa_account_num,
    upper(trim(legalname)) as EntityName
    --,'legalname' as EntityType
from davelake.philly_business_licenses 
where legalname is not null
;
INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    BizLicEntities.EntityName AS NameText
FROM (SELECT DISTINCT EntityName FROM BizLicEntities) BizLicEntities
LEFT JOIN davelake.MasterEntity master
    ON BizLicEntities.EntityName = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL
;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do distinct addresses from business_licenses
CREATE OR REPLACE TEMPORARY VIEW BizLicAddresses
AS 
SELECT 
    opa_account_num,
    upper(trim(address)) as NameText
    --'address' as EntityType
from davelake.philly_business_licenses 
where address is not null
UNION
SELECT 
    opa_account_num,
    upper(trim(business_mailing_address)) as NameText
    --'business_mailing_address' as EntityType
from davelake.philly_business_licenses 
where business_mailing_address is not null
UNION 
SELECT 
    opa_account_num,
    upper(trim(ownercontact1mailingaddress)) as NameText
    --'ownercontact1mailingaddress' as EntityType
from davelake.philly_business_licenses 
where ownercontact1mailingaddress is not null
UNION 
SELECT 
    opa_account_num,
    upper(trim(ownercontact2mailingaddress)) as NameText
    --'ownercontact2mailingaddress' as EntityType
from davelake.philly_business_licenses 
where ownercontact2mailingaddress is not null
;

INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    BizLicAddresses.NameText 
FROM (SELECT DISTINCT NameText FROM BizLicAddresses)  BizLicAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON BizLicAddresses.NameText = master.AddressText
WHERE master.AddressText IS NULL

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for business_licenses

--JOIN the 2 preceding views to get a flattened view of distinct OPAs, Entities, and Addresses
--these are all separate views to aid debugging
CREATE OR REPLACE TEMPORARY VIEW ExistingBizLicData
AS 
select e.opa_account_num, e.EntityName, a.NameText
from BizLicEntities e 
JOIN BizLicAddresses a
ON e.opa_account_num = a.opa_account_num
;

--this will tell me if the existing business_licenses data exists in the junction table
CREATE OR REPLACE TEMPORARY VIEW CurrentStatus
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    ExistingBizLicData.opa_account_num,
    mea.MasterEntityAddressID,
    CASE WHEN mea.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'business_licenses.various:business_licenses.various'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN mea.Notes like '%business_licenses.various:business_licenses.various%' THEN mea.Notes
            --new entry
            ELSE CONCAT(mea.Notes ,';business_licenses.various:business_licenses.various') END 
    END AS Notes,
    CASE WHEN mea.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM ExistingBizLicData 
JOIN davelake.MasterEntity llc
    ON ExistingBizLicData.EntityName = llc.NameText
JOIN davelake.masteraddress addr
    ON ExistingBizLicData.NameText = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress mea
    ON llc.MasterEntityID = mea.MasterEntityID
    AND addr.MasterAddressID = mea.MasterAddressID
    AND ExistingBizLicData.opa_account_num = mea.parcel_number
;

--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING CurrentStatus
        ON targ.MasterEntityID = CurrentStatus.MasterEntityID
        AND targ.MasterAddressID = CurrentStatus.MasterAddressID
        AND targ.parcel_number = CurrentStatus.opa_account_num
WHEN MATCHED AND CurrentStatus.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = CurrentStatus.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    CurrentStatus.MasterEntityID,
    CurrentStatus.MasterAddressID,
    CurrentStatus.opa_account_num ,
    CurrentStatus.Notes
FROM  CurrentStatus
WHERE CurrentStatus.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_com_act_licenses

-- CELL ********************

--there is no parcel_num for this table, we'll just leave it as NULLL
--or maybe it should be licensenum???
--we'll use that just to maintain the code pattern below
--licensenum is the key of the table

--get the distinct "LLC owners/entities" from philly_com_act_licenses

CREATE OR REPLACE TEMPORARY VIEW ActLicEntities
AS 
SELECT  
    licensenum,
    upper(trim(companyname)) as EntityName
from davelake.philly_com_act_licenses 
where upper(trim(companyname)) is not null
UNION
SELECT 
    licensenum,
    upper(trim(ownercontact1name)) as EntityName
from davelake.philly_com_act_licenses 
where upper(trim(ownercontact1name)) is not null
UNION 
SELECT 
    licensenum,
    upper(trim(ownercontact2name)) as EntityName
    --,'ownercontact1name' as EntityType
from davelake.philly_com_act_licenses 
where upper(trim(ownercontact2name)) is not null
;
INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    ActLicEntities.EntityName AS NameText
FROM (SELECT DISTINCT EntityName FROM ActLicEntities) ActLicEntities
LEFT JOIN davelake.MasterEntity master
    ON ActLicEntities.EntityName = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL
;

--now do distinct addresses from philly_com_act_licenses
CREATE OR REPLACE TEMPORARY VIEW ActLicAddresses
AS 
SELECT 
    licensenum,
    upper(trim(ownercontact1mailingaddress)) as NameText
from davelake.philly_com_act_licenses 
where upper(trim(ownercontact1mailingaddress)) is not null
UNION
SELECT 
    licensenum,
    upper(trim(ownercontact2mailingaddress)) as NameText
from davelake.philly_com_act_licenses 
where upper(trim(ownercontact2mailingaddress)) is not null
;
INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    ActLicAddresses.NameText 
FROM (SELECT DISTINCT NameText FROM ActLicAddresses)  ActLicAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON ActLicAddresses.NameText = master.AddressText
WHERE master.AddressText IS NULL


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_com_act_licenses

--JOIN the 2 preceding views to get a flattened view of distinct Entities and Addresses

CREATE OR REPLACE TEMPORARY VIEW ExistingActLicData
AS 
select e.licensenum, e.EntityName, a.NameText
from ActLicEntities e 
JOIN ActLicAddresses a
ON e.licensenum = a.licensenum
;

--this will tell me if the existing com_act_licenses data exists in the junction table
CREATE OR REPLACE TEMPORARY VIEW CurrentStatus
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    --ExistingBizLicData.opa_account_num, (see note below)
    mea.MasterEntityAddressID,
    CASE WHEN mea.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'com_act_licenses.various:com_act_licenses.various'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN mea.Notes like '%com_act_licenses.various:com_act_licenses.various%' THEN mea.Notes
            --new entry
            ELSE CONCAT(mea.Notes ,';com_act_licenses.various:com_act_licenses.various') END 
    END AS Notes,
    CASE WHEN mea.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM ExistingActLicData 
JOIN davelake.MasterEntity llc
    ON ExistingActLicData.EntityName = llc.NameText
JOIN davelake.masteraddress addr
    ON ExistingActLicData.NameText = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress mea
    ON llc.MasterEntityID = mea.MasterEntityID
    AND addr.MasterAddressID = mea.MasterAddressID
    --for comm act licenses we don't have an OPA, so just do the mapping regardless of OPA
    --I guess we could put licensenum here
    --AND ExistingActLicData.opa_account_num = mea.parcel_number
;

--updates
--this is the throwing the error:
--Cannot perform Merge as multiple source rows matched and attempted to modify the same target row in the Delta table in possibly conflicting ways
--this is due to the lack of parcel_num means I'm losing uniqueness
--rather than overthink this, let's just skip the update case but we can definitely keep the INSERT case which is the important case anyway
-- MERGE INTO davelake.MasterEntityAddress targ
--     USING CurrentStatus
--         ON targ.MasterEntityID = CurrentStatus.MasterEntityID
--         AND targ.MasterAddressID = CurrentStatus.MasterAddressID
--         --AND targ.parcel_number = CurrentStatus.opa_account_num
-- WHEN MATCHED AND CurrentStatus.NewEntry = 0 THEN 
--     UPDATE SET targ.Notes = CurrentStatus.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    CurrentStatus.MasterEntityID,
    CurrentStatus.MasterAddressID,
    NULL, --CurrentStatus.opa_account_num ,
    CurrentStatus.Notes
FROM  CurrentStatus
WHERE CurrentStatus.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_case_investigations

-- CELL ********************

--get the distinct "LLC owners/entities" from philly_case_investigations

CREATE OR REPLACE TEMPORARY VIEW CaseEntities
AS 
SELECT  DISTINCT
    opa_account_num,
    upper(trim(opa_owner)) as EntityName
from davelake.philly_case_investigations 
where upper(trim(opa_owner)) is not null
;
INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    CaseEntities.EntityName AS NameText
FROM (SELECT DISTINCT EntityName FROM CaseEntities) CaseEntities
LEFT JOIN davelake.MasterEntity master
    ON CaseEntities.EntityName = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL
;

--now do distinct addresses from philly_case_investigations
CREATE OR REPLACE TEMPORARY VIEW CaseAddresses
AS 
SELECT DISTINCT
    opa_account_num,
    upper(trim(address)) as NameText
from davelake.philly_case_investigations 
where upper(trim(address)) is not null
;
INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    CaseAddresses.NameText 
FROM (SELECT DISTINCT NameText FROM CaseAddresses) CaseAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON CaseAddresses.NameText = master.AddressText
WHERE master.AddressText IS NULL


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_case_investigations
--JOIN the 2 preceding views to get a flattened view of distinct Entities and Addresses

CREATE OR REPLACE TEMPORARY VIEW ExistingCaseData
AS 
select e.opa_account_num, e.EntityName, a.NameText
from CaseEntities e 
JOIN CaseAddresses a
ON e.opa_account_num = a.opa_account_num
;

--this will tell me if the existing philly_case_investigations data exists in the junction table
CREATE OR REPLACE TEMPORARY VIEW CurrentStatus
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    ExistingCaseData.opa_account_num,
    mea.MasterEntityAddressID,
    CASE WHEN mea.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'case_investigations.various:case_investigations.various'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN mea.Notes like '%case_investigations.various:case_investigations.various%' THEN mea.Notes
            --new entry
            ELSE CONCAT(mea.Notes ,';case_investigations.various:case_investigations.various') END 
    END AS Notes,
    CASE WHEN mea.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM ExistingCaseData 
JOIN davelake.MasterEntity llc
    ON ExistingCaseData.EntityName = llc.NameText
JOIN davelake.masteraddress addr
    ON ExistingCaseData.NameText = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress mea
    ON llc.MasterEntityID = mea.MasterEntityID
    AND addr.MasterAddressID = mea.MasterAddressID
    AND ExistingCaseData.opa_account_num = mea.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING CurrentStatus
        ON targ.MasterEntityID = CurrentStatus.MasterEntityID
        AND targ.MasterAddressID = CurrentStatus.MasterAddressID
        AND targ.parcel_number = CurrentStatus.opa_account_num
WHEN MATCHED AND CurrentStatus.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = CurrentStatus.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    CurrentStatus.MasterEntityID,
    CurrentStatus.MasterAddressID,
    CurrentStatus.opa_account_num ,
    CurrentStatus.Notes
FROM  CurrentStatus
WHERE CurrentStatus.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_violations

-- CELL ********************

--get the distinct "LLC owners/entities" from philly_violations

CREATE OR REPLACE TEMPORARY VIEW ViolationsEntities
AS 
SELECT  DISTINCT
    opa_account_num,
    upper(trim(opa_owner)) as EntityName
from davelake.philly_violations 
where upper(trim(opa_owner)) is not null
;
INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    ViolationsEntities.EntityName AS NameText
FROM (SELECT DISTINCT EntityName FROM ViolationsEntities) ViolationsEntities
LEFT JOIN davelake.MasterEntity master
    ON ViolationsEntities.EntityName = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL
;

--now do distinct addresses from philly_case_investigations
CREATE OR REPLACE TEMPORARY VIEW ViolationsAddresses
AS 
SELECT DISTINCT
    opa_account_num,
    upper(trim(address)) as NameText
from davelake.philly_violations 
where upper(trim(address)) is not null
;
INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    ViolationsAddresses.NameText 
FROM (SELECT DISTINCT NameText FROM ViolationsAddresses) ViolationsAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON ViolationsAddresses.NameText = master.AddressText
WHERE master.AddressText IS NULL


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_violations
--JOIN the 2 preceding views to get a flattened view of distinct Entities and Addresses

CREATE OR REPLACE TEMPORARY VIEW ExistingViolationsData
AS 
select e.opa_account_num, e.EntityName, a.NameText
from ViolationsEntities e 
JOIN ViolationsAddresses a
ON e.opa_account_num = a.opa_account_num
;

--this will tell me if the existing philly_violations data exists in the junction table
CREATE OR REPLACE TEMPORARY VIEW CurrentStatus
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    ExistingViolationsData.opa_account_num,
    mea.MasterEntityAddressID,
    CASE WHEN mea.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'violations.various:violations.various'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN mea.Notes like '%violations.various:violations.various%' THEN mea.Notes
            --new entry
            ELSE CONCAT(mea.Notes ,';violations.various:violations.various') END 
    END AS Notes,
    CASE WHEN mea.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM ExistingViolationsData 
JOIN davelake.MasterEntity llc
    ON ExistingViolationsData.EntityName = llc.NameText
JOIN davelake.masteraddress addr
    ON ExistingViolationsData.NameText = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress mea
    ON llc.MasterEntityID = mea.MasterEntityID
    AND addr.MasterAddressID = mea.MasterAddressID
    AND ExistingViolationsData.opa_account_num = mea.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING CurrentStatus
        ON targ.MasterEntityID = CurrentStatus.MasterEntityID
        AND targ.MasterAddressID = CurrentStatus.MasterAddressID
        AND targ.parcel_number = CurrentStatus.opa_account_num
WHEN MATCHED AND CurrentStatus.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = CurrentStatus.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    CurrentStatus.MasterEntityID,
    CurrentStatus.MasterAddressID,
    CurrentStatus.opa_account_num ,
    CurrentStatus.Notes
FROM  CurrentStatus
WHERE CurrentStatus.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_permits

-- CELL ********************

--get the distinct "LLC owners/entities" from philly_permits

CREATE OR REPLACE TEMPORARY VIEW PermitsEntities
AS 
SELECT  DISTINCT
    opa_account_num,
    upper(trim(opa_owner)) as EntityName
from davelake.philly_permits
where upper(trim(opa_owner)) is not null
UNION
SELECT  DISTINCT
    opa_account_num,
    upper(trim(contractorname)) as EntityName
from davelake.philly_permits
where upper(trim(contractorname)) is not null
;

INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    PermitsEntities.EntityName AS NameText
FROM (SELECT DISTINCT EntityName FROM PermitsEntities) PermitsEntities
LEFT JOIN davelake.MasterEntity master
    ON PermitsEntities.EntityName = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL
;

--now do distinct addresses from philly_permits
CREATE OR REPLACE TEMPORARY VIEW PermitsAddresses
AS 
SELECT DISTINCT
    opa_account_num,
    upper(trim(address)) as NameText
from davelake.philly_permits 
where upper(trim(address)) is not null
UNION
SELECT DISTINCT
    opa_account_num,
    upper(trim(contractoraddress1)) as NameText
from davelake.philly_permits 
where upper(trim(contractoraddress1)) is not null
;
INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    PermitsAddresses.NameText 
FROM (SELECT DISTINCT NameText FROM PermitsAddresses) PermitsAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON PermitsAddresses.NameText = master.AddressText
WHERE master.AddressText IS NULL


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_permits
--JOIN the 2 preceding views to get a flattened view of distinct Entities and Addresses

CREATE OR REPLACE TEMPORARY VIEW ExistingPermitsData
AS 
select e.opa_account_num, e.EntityName, a.NameText
from PermitsEntities e 
JOIN PermitsAddresses a
ON e.opa_account_num = a.opa_account_num
;

--this will tell me if the existing philly_permits data exists in the junction table
CREATE OR REPLACE TEMPORARY VIEW CurrentStatus
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    ExistingPermitsData.opa_account_num,
    mea.MasterEntityAddressID,
    CASE WHEN mea.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'permits.various:permits.various'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN mea.Notes like '%permits.various:permits.various%' THEN mea.Notes
            --new entry
            ELSE CONCAT(mea.Notes ,';permits.various:permits.various') END 
    END AS Notes,
    CASE WHEN mea.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM ExistingPermitsData 
JOIN davelake.MasterEntity llc
    ON ExistingPermitsData.EntityName = llc.NameText
JOIN davelake.masteraddress addr
    ON ExistingPermitsData.NameText = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress mea
    ON llc.MasterEntityID = mea.MasterEntityID
    AND addr.MasterAddressID = mea.MasterAddressID
    AND ExistingPermitsData.opa_account_num = mea.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING CurrentStatus
        ON targ.MasterEntityID = CurrentStatus.MasterEntityID
        AND targ.MasterAddressID = CurrentStatus.MasterAddressID
        AND targ.parcel_number = CurrentStatus.opa_account_num
WHEN MATCHED AND CurrentStatus.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = CurrentStatus.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    CurrentStatus.MasterEntityID,
    CurrentStatus.MasterAddressID,
    CurrentStatus.opa_account_num ,
    CurrentStatus.Notes
FROM  CurrentStatus
WHERE CurrentStatus.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_demolitions

-- CELL ********************

--get the distinct "LLC owners/entities" from philly_demolitions

CREATE OR REPLACE TEMPORARY VIEW DemosEntities
AS 
SELECT  DISTINCT
    opa_account_num,
    upper(trim(opa_owner)) as EntityName
from davelake.philly_demolitions
where upper(trim(opa_owner)) is not null
UNION
SELECT  DISTINCT
    opa_account_num,
    upper(trim(applicantname)) as EntityName
from davelake.philly_demolitions
where upper(trim(applicantname)) is not null
;
INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    DemosEntities.EntityName AS NameText
FROM (SELECT DISTINCT EntityName FROM DemosEntities) DemosEntities
LEFT JOIN davelake.MasterEntity master
    ON DemosEntities.EntityName = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL
;


--now do distinct addresses from philly_demolitions
CREATE OR REPLACE TEMPORARY VIEW DemosAddresses
AS 
SELECT DISTINCT
    opa_account_num,
    upper(trim(address)) as NameText
from davelake.philly_demolitions 
where upper(trim(address)) is not null
;
INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    DemosAddresses.NameText 
FROM (SELECT DISTINCT NameText FROM DemosAddresses) DemosAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON DemosAddresses.NameText = master.AddressText
WHERE master.AddressText IS NULL


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_demolitions
--JOIN the 2 preceding views to get a flattened view of distinct Entities and Addresses

CREATE OR REPLACE TEMPORARY VIEW ExistingDemosData
AS 
select e.opa_account_num, e.EntityName, a.NameText
from DemosEntities e 
JOIN DemosAddresses a
ON e.opa_account_num = a.opa_account_num
;

--this will tell me if the existing philly_demolitions data exists in the junction table
CREATE OR REPLACE TEMPORARY VIEW CurrentStatus
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    ExistingDemosData.opa_account_num,
    mea.MasterEntityAddressID,
    CASE WHEN mea.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'demos.various:demos.various'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN mea.Notes like '%demos.various:demos.various%' THEN mea.Notes
            --new entry
            ELSE CONCAT(mea.Notes ,';demos.various:demos.various') END 
    END AS Notes,
    CASE WHEN mea.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM ExistingDemosData 
JOIN davelake.MasterEntity llc
    ON ExistingDemosData.EntityName = llc.NameText
JOIN davelake.masteraddress addr
    ON ExistingDemosData.NameText = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress mea
    ON llc.MasterEntityID = mea.MasterEntityID
    AND addr.MasterAddressID = mea.MasterAddressID
    AND ExistingDemosData.opa_account_num = mea.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING CurrentStatus
        ON targ.MasterEntityID = CurrentStatus.MasterEntityID
        AND targ.MasterAddressID = CurrentStatus.MasterAddressID
        AND targ.parcel_number = CurrentStatus.opa_account_num
WHEN MATCHED AND CurrentStatus.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = CurrentStatus.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    CurrentStatus.MasterEntityID,
    CurrentStatus.MasterAddressID,
    CurrentStatus.opa_account_num ,
    CurrentStatus.Notes
FROM  CurrentStatus
WHERE CurrentStatus.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_real_estate_tax_balances

-- CELL ********************

--get the distinct "LLC owners/entities" from philly_real_estate_tax_balances

CREATE OR REPLACE TEMPORARY VIEW TaxEntities
AS 
SELECT  DISTINCT
    opa_number AS opa_account_num,
    upper(trim(owner)) as EntityName
from davelake.philly_real_estate_tax_balances
where upper(trim(owner)) is not null
UNION
SELECT  DISTINCT
    opa_number AS opa_account_num,
    upper(trim(co_owner)) as EntityName
from davelake.philly_real_estate_tax_balances
where upper(trim(co_owner)) is not null
;
INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    TaxEntities.EntityName AS NameText
FROM (SELECT DISTINCT EntityName FROM TaxEntities) TaxEntities
LEFT JOIN davelake.MasterEntity master
    ON TaxEntities.EntityName = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL
;

--now do distinct addresses from philly_real_estate_tax_balances
CREATE OR REPLACE TEMPORARY VIEW TaxAddresses
AS 
SELECT DISTINCT
    opa_number AS opa_account_num,
    upper(trim(street_address)) as NameText
from davelake.philly_real_estate_tax_balances 
where upper(trim(street_address)) is not null
UNION
SELECT DISTINCT
    opa_number AS opa_account_num,
    upper(trim(mailing_address)) as NameText
from davelake.philly_real_estate_tax_balances 
where upper(trim(mailing_address)) is not null
;
INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    TaxAddresses.NameText 
FROM (SELECT DISTINCT NameText FROM TaxAddresses) TaxAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON TaxAddresses.NameText = master.AddressText
WHERE master.AddressText IS NULL


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_real_estate_tax_balances
--JOIN the 2 preceding views to get a flattened view of distinct Entities and Addresses

CREATE OR REPLACE TEMPORARY VIEW ExistingTaxData
AS 
select e.opa_account_num, e.EntityName, a.NameText
from TaxEntities e 
JOIN TaxAddresses a
ON e.opa_account_num = a.opa_account_num
;

--this will tell me if the existing philly_real_estate_tax_balances data exists in the junction table
CREATE OR REPLACE TEMPORARY VIEW CurrentStatus
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    ExistingTaxData.opa_account_num,
    mea.MasterEntityAddressID,
    CASE WHEN mea.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'tax.various:tax.various'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN mea.Notes like '%tax.various:tax.various%' THEN mea.Notes
            --new entry
            ELSE CONCAT(mea.Notes ,';tax.various:tax.various') END 
    END AS Notes,
    CASE WHEN mea.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM ExistingTaxData 
JOIN davelake.MasterEntity llc
    ON ExistingTaxData.EntityName = llc.NameText
JOIN davelake.masteraddress addr
    ON ExistingTaxData.NameText = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress mea
    ON llc.MasterEntityID = mea.MasterEntityID
    AND addr.MasterAddressID = mea.MasterAddressID
    AND ExistingTaxData.opa_account_num = mea.parcel_number
;
--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING CurrentStatus
        ON targ.MasterEntityID = CurrentStatus.MasterEntityID
        AND targ.MasterAddressID = CurrentStatus.MasterAddressID
        AND targ.parcel_number = CurrentStatus.opa_account_num
WHEN MATCHED AND CurrentStatus.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = CurrentStatus.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    CurrentStatus.MasterEntityID,
    CurrentStatus.MasterAddressID,
    CurrentStatus.opa_account_num ,
    CurrentStatus.Notes
FROM  CurrentStatus
WHERE CurrentStatus.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Populate tables from philly_rtt_summary

-- CELL ********************

--for this case we need to split and explode the grantors and grantees.
--let's build a view to gather every distinct combination of entities and addresses after split/explode

--likely need to split grantees/grantors on the semicolon
-- select grantees, grantors, opa_account_num, street_address 
-- from davelake.philly_rtt_summary 
-- where opa_account_num is not null
-- and opa_account_num = 602191100
-- limit 100;
CREATE OR REPLACE TEMPORARY VIEW RTTEntitiesAddresses
AS
select DISTINCT
    explode(split(grantees,";")) as Entity, 
    opa_account_num , 
    street_address, 
    'grantees' as EntityType
from davelake.philly_rtt_summary 
where opa_account_num is not null
UNION
select DISTINCT
    explode(split(grantors,";")) as Entity, 
    opa_account_num ,  
    street_address, 
    'grantors' as EntityType
from davelake.philly_rtt_summary 
where opa_account_num is not null
;

--add the new entities
INSERT INTO davelake.MasterEntity 
SELECT 
    uuid() AS MasterEntityID,
    RTTEntitiesAddresses.Entity AS NameText
FROM (SELECT DISTINCT LEFT(Entity,200) Entity FROM RTTEntitiesAddresses) RTTEntitiesAddresses
LEFT JOIN davelake.MasterEntity master
    ON RTTEntitiesAddresses.Entity = master.NameText
--LLC doesn't yet exist in master table
WHERE master.NameText IS NULL;

--add the new addresses
INSERT INTO davelake.MasterAddress 
SELECT 
    uuid() AS MasterAddressID,
    RTTEntitiesAddresses.street_address 
FROM (SELECT DISTINCT street_address FROM RTTEntitiesAddresses) RTTEntitiesAddresses
--where the row does not yet exist
LEFT JOIN davelake.MasterAddress master
    ON RTTEntitiesAddresses.street_address = master.AddressText
WHERE master.AddressText IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--now do MasterEntityAddress (junction table) for philly_rtt_summary

--this will tell me if the existing philly_rtt_summary data exists in the junction table
CREATE OR REPLACE TEMPORARY VIEW CurrentStatus
AS 
SELECT DISTINCT 
    llc.MasterEntityID,
    addr.MasterAddressID,
    RTTEntitiesAddresses.opa_account_num,
    mea.MasterEntityAddressID,
    CASE WHEN mea.MasterEntityAddressID 
        --new entry
        IS NULL THEN 'rtt.various:rtt.various'   
        --add to existing entry (ensuring idempotency)
        ELSE 
            --ignore it/entry exists already
            CASE WHEN mea.Notes like '%rtt.various:rtt.various%' THEN mea.Notes
            --new entry
            ELSE CONCAT(mea.Notes ,';rtt.various:rtt.various') END 
    END AS Notes,
    CASE WHEN mea.MasterEntityAddressID IS NULL THEN 1 ELSE 0 END AS NewEntry
FROM (select distinct entity, opa_account_num, street_address from RTTEntitiesAddresses)  RTTEntitiesAddresses
JOIN davelake.MasterEntity llc
    ON RTTEntitiesAddresses.Entity = llc.NameText
JOIN davelake.masteraddress addr
    ON RTTEntitiesAddresses.street_address = addr.AddressText
LEFT JOIN davelake.MasterEntityAddress mea
    ON llc.MasterEntityID = mea.MasterEntityID
    AND addr.MasterAddressID = mea.MasterAddressID
    AND RTTEntitiesAddresses.opa_account_num = mea.parcel_number
;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--updates
MERGE INTO davelake.MasterEntityAddress targ
    USING CurrentStatus
        ON targ.MasterEntityID = CurrentStatus.MasterEntityID
        AND targ.MasterAddressID = CurrentStatus.MasterAddressID
        AND targ.parcel_number = CurrentStatus.opa_account_num
WHEN MATCHED AND CurrentStatus.NewEntry = 0 THEN 
    UPDATE SET targ.Notes = CurrentStatus.Notes;
--inserts
INSERT INTO davelake.MasterEntityAddress
SELECT 
    uuid() AS MasterEntityAddressID,
    CurrentStatus.MasterEntityID,
    CurrentStatus.MasterAddressID,
    CurrentStatus.opa_account_num ,
    CurrentStatus.Notes
FROM  CurrentStatus
WHERE CurrentStatus.NewEntry = 1

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Unit Tester Block

-- CELL ********************

SELECT 'Rollback Code'
--rollback code
-- DESCRIBE HISTORY masteraddress
-- restore table masteraddress to version as of 20

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--sanity checker, looking for dups
select NameText, count(*) from davelake.MasterEntity GROUP BY NameText Having count(*) > 1;
select MasterEntityID, count(*) from davelake.MasterEntity GROUP BY MasterEntityID Having count(*) > 1;
select AddressText, count(*) from davelake.Masteraddress GROUP BY AddressText Having count(*) > 1;
select MasterAddressID, count(*) from davelake.Masteraddress GROUP BY MasterAddressID Having count(*) > 1;
--there may be dups if the parcel_number is null
select MasterEntityID, MasterAddressID, parcel_number, count(*) from davelake.MasterEntityAddress GROUP BY MasterEntityID, MasterAddressID, parcel_number Having count(*) > 1;
select MasterEntityAddressID, count(*) from davelake.MasterEntityAddress GROUP BY MasterEntityAddressID Having count(*) > 1;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--tester block
select * from davelake.MasterEntity where MasterEntityID = '9d64b9f1-8776-4922-89ed-c5b1994b5286' limit 10;
select * from davelake.masteraddress where addresstext = '2837 KENSINGTON AVE' limit 10;
select * from davelake.MasterEntityAddress where MasterAddressID = '552b014f-6bd9-4afa-91e3-22fed0ac566a' ;
select * from davelake.MasterEntityAddress where MasterEntityID = '9d64b9f1-8776-4922-89ed-c5b1994b5286' ;
select * from davelake.masteraddress where masteraddressid = '8acb8b1f-3a08-47dd-a375-67c8ce025606'


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Analytics

-- CELL ********************

SELECT *
FROM davelake.philly_problempropertylist

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE VIEW LLCQuery 
AS
select 
    me.nametext as EntityName,
    ma.AddressText,
    mea.parcel_number,
    opa.owner_1 AS assoc_parcel_owner,
    opa.address_std AS assoc_parcel_address,
    mea.Notes
from masterentity me
join masterentityaddress mea on me.masterentityid = mea.masterentityid
join masteraddress ma on mea.masteraddressid = ma.masteraddressid
LEFT join philly_opa_properties_public_pde opa ON mea.parcel_number= opa.parcel_number


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--all associations to 2837 KENSINGTON AVE
select * from LLCQuery WHERE AddressText = '2837 KENSINGTON AVE'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- In this case there is a non-breaking space after the names.  This seems like a systemic issue with the data.  You can backtrace it using this data to the system of record and the carto tables.  You can see in the code above I trim the data to account for this.  

-- CELL ********************

select * from LLCQuery WHERE entityname = 'HIM YIENG' or entityname = 'YIENG HIM';
select * from LLCQuery where entityname like ''

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--1225 S 54TH ST
--HIM YIENG

--what are the general issues I found across datasets?

select * from LLCQuery WHERE entityname = 'DEUTSCHE BANK TRUST COMPANY AMERICAS AS TRUSTEE FOR THE REGISTERED HOLDERS OF BANC OF AMERICA MERRILL LYNCH COMMERCIAL MORTGAGE INC MULTIFAMILY MORTGAGE PASS-THROUGH CERTIFICATES SERIES 2015-SB10'

ucc filing:  knockwood associates is an investment vehicle


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--SELECT DISTINCT LEFT(Entity,200) Entity FROM RTTEntitiesAddresses
might be ok if its a foreclosure
SELECT  Entity FROM RTTEntitiesAddresses where len(entity)> 150;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SELECT  Entity FROM RTTEntitiesAddresses where entity like '% MERS %'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

select * from philly_real_estate_tax_balances limit 10
real estate tax
l and i abate work invoice
comerical trash fees

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
