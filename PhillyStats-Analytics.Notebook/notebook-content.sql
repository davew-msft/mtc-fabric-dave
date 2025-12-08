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

-- # Analytics Notebook  
-- Goal is to find the patterns of the problem given the examples we were provided.  

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

-- MARKDOWN ********************

-- ## philly_flattened_properties table
-- 
-- * 1 entry for every property
-- * will have all of the `features` (attributes, properties) for every property
-- * will have a score between 0-1 for each `factor`, named like `F1_Repairs`
-- * in subsequent cells we'll build out the code to calculate each of these factors
-- * we can use this table to join to other tables, as the source of ML to predict future cases, testing, dashboard
-- 
-- Start by seeding the table with every property and base details

-- CELL ********************

-- MAGIC %%sql 
-- MAGIC 
-- MAGIC --driving table
-- MAGIC CREATE OR REPLACE TABLE davelake.philly_flattened_properties
-- MAGIC AS 
-- MAGIC select 
-- MAGIC     opa.parcel_number,
-- MAGIC     opa.location,
-- MAGIC     opa.owner_1,
-- MAGIC     opa.owner_2,
-- MAGIC     problems.MaggyReason as MaggyLabel,
-- MAGIC     0 AS F1_Repairs,
-- MAGIC     0 AS F2_CodeViolations,
-- MAGIC     0 AS F3_TaxesAndDebt,
-- MAGIC     0 AS F4_CityResources,
-- MAGIC     0 AS F5_Licenses,
-- MAGIC     0 AS F6_CorpStructures,
-- MAGIC     0 AS F7_Nuisances,
-- MAGIC     0 AS F8_Insurance,
-- MAGIC     0 AS F9_Zoning,
-- MAGIC     0 AS F10_SheriffSalePurchase,
-- MAGIC     0 AS F11_AvoidSquatters,
-- MAGIC     0 AS F12_LengthOfOwnership,
-- MAGIC     0 AS F13_HomeValueAppreciation,
-- MAGIC     0 AS F14_BusinessesInArea,
-- MAGIC     0 AS F15_NeighborhoodVacantLots,
-- MAGIC     0 AS F16_LocalInvestors,
-- MAGIC     0 AS F17_MultipleHomesteadExemptions,
-- MAGIC     CASE opa.homestead_exemption WHEN 0 THEN 0 ELSE 1 END AS F18_OwnerOccupied,
-- MAGIC     0 AS F19_DollarTransfers,
-- MAGIC     opa.homestead_exemption,
-- MAGIC     opa.zoning
-- MAGIC from davelake.philly_opa_properties_public_pde opa
-- MAGIC left join davelake.philly_problempropertylist problems
-- MAGIC     on opa.location = problems.location
-- MAGIC ;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select * from davelake.philly_flattened_properties
-- MAGIC limit 10

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ## Build out the _Factors_
-- 
-- Factors that determine this, with a code.  We'll build out the code in order in subsequent cells and explain in detail the calculations there.  
-- 
-- * `F1_Repairs`:  Fail or refuse to repair or invest in properties  
-- * `F2_CodeViolations`:  Tend to incur code violations 
-- * `F3_TaxesAndDebt`: Fail to pay Real Estate taxes and/or other city debt such as water bills and code enforcement abatement bills  
-- * `F4_CityResources`: Use city resources to maintain property.  
--     * mowing the lawn
--     * sealing the property
--     * demolishing it 
-- * `F5_Licenses`: Fail to maintain licenses. 
--     * vacant property license (non-occupied property) 
--     * rental license (occupied property)
-- * `F6_CorpStructures`: Attempt to shield their true identity via complicated corporate structures 
-- * `F7_Nuisances`: Nuisances within the neighborhood.  Either by virtue of how the property is maintained and/or the scope of the properties that they have, that might have minor problems that cumulatively become problematic. 
-- * `F8_Insurance`: not insuring the property 
-- * `F9_Zoning`: not using the property for what it is zoned for 
-- * `F10_SheriffSalePurchase`: buying the property at sheriff's sale and waiting for the neighborhood to turn around.  
-- * `F11_AvoidSquatters`: actively trying to avoid squatters 
-- * `F12_LengthOfOwnership`: length of ownership 
-- * `F13_HomeValueAppreciation`: home value appreciation 
--   * TODO:  from 8/19.  Might need to review this one with Philly team.  I don't understand the factor.  
--   * investor strips all value out of it and not putting anything into it at all and have no intention of doing that.  How would we detect/know this?  
--   * or is it a case where the property is bought in anticipation of rising property values in the area and when that doesn't happen it is just abandoned? 
-- * `F14_BusinessesInArea`: varieties of businesses in the area (grocery stores vs bodegas/papis) 
--   * vaping stores on every block (drug paraphrenalia)
-- * `F15_NeighborhoodVacantLots`: number of vacant lots in neighborhood 
-- * `F16_LocalInvestors`: in-town vs out-of-town investors (this has been proven to NOT be the latter).  
-- * `F17_MultipleHomesteadExemptions`: Multiple Homestead Exemptions 
--   * if an owner has 2 homestead exemptions on different properties, that's a red flag
-- * `F18_OwnerOccupied`:  Is Property Owner Occupied?  
--   * ie, it is flagged as a `homestead_exemption`
-- * `F19_DollarTransfers`:
--   * TODO:  real estate transfers for $1?  etc.  (the rtt_summary table)
-- 
-- Other things
-- 
-- * if a certain percentage of a block becomes non-owner occupied?  
-- * land bank data (parks that are still under land bank)  
-- 
-- * TODO (didn't understand this).  RCO's/multiple RCOs.  Registered Community Organizations.  (cleanings, events).  I you want a zoning variance you have to go to the RCO first.  In stable areas there are more RCOs over time.  
-- * rental license or vacant property license.  Not all actors get the license.  A history of this on a given property may also be indicative.  helps with triaging owner occupancy.  
-- 
-- * is assessed value radically different from sale price (and it isn't $1 sale)
-- * some problem owners buy up industrial units and convert them, unsafe
--   


-- MARKDOWN ********************

-- ### F1_Repairs
-- 
-- Fail or refuse to repair or invest in properties
-- 
-- TODO:  what is the metric/calculation we should use?  

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ### F2_CodeViolations
-- 
-- Tend to incur code violations
-- 
-- TODO: what is the metric/calculation we should use?

-- MARKDOWN ********************

-- ### F3_TaxesAndDebt
-- 
-- Fail to pay Real Estate taxes and/or other city debt such as water bills and code enforcement abatement bills
-- 
-- TODO: what is the metric/calculation we should use?

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- 
-- 
-- ### F4_CityResources
-- 
-- Use city resources to maintain property.
-- mowing the lawn
-- sealing the property
-- demolishing it
-- 
-- TODO: what is the metric/calculation we should use?
-- 
-- demolitions table
-- distinct applicantname LIKE 'CITY OF %'
-- applicanttype = CITY_DEMO
-- 9:10 in recording
-- 
-- ### F5_Licenses
-- Fail to maintain licenses.
-- vacant property license (non-occupied property)
-- rental license (occupied property)
-- 
-- TODO: what is the metric/calculation we should use?
-- wip below
-- ### F6_CorpStructures
-- 
-- Attempt to shield their true identity via complicated corporate structures
-- 
-- TODO: what is the metric/calculation we should use?
-- 
-- ### F7_Nuisances
-- 
-- Nuisances within the neighborhood. Either by virtue of how the property is maintained and/or the scope of the properties that they have, that might have minor problems that cumulatively become problematic.
-- 
-- TODO: what is the metric/calculation we should use?
-- 
-- 
-- ### F8_Insurance
-- 
-- not insuring the property
-- 
-- ### F9_Zoning
-- not using the property for what it is zoned for
-- 
-- ### F10_SheriffSalePurchase
-- 
-- buying the property at sheriff's sale and waiting for the neighborhood to turn around.
-- 
-- ### F11_AvoidSquatters
-- 
-- actively trying to avoid squatters
-- ### F12_LengthOfOwnership
-- 
-- length of ownership
-- 
-- ### F13_HomeValueAppreciation
-- 
-- TODO: from 8/19. Might need to review this one with Philly team. I don't understand the factor.
-- investor strips all value out of it and not putting anything into it at all and have no intention of doing that. How would we detect/know this?
-- 
-- ### F14_BusinessesInArea
-- 
-- varieties of businesses in the area (grocery stores vs bodegas/papis)
-- vaping stores on every block (drug paraphrenalia)
-- 
-- ### F15_NeighborhoodVacantLots
-- 
-- number of vacant lots in neighborhood
-- 
-- ### F16_LocalInvestors
-- 
-- in-town vs out-of-town investors (this has been proven to NOT be the latter).
-- 
-- 
-- 
-- 


-- MARKDOWN ********************

-- ### F17_MultipleHomesteadExemptions and 
-- ### F18_OwnerOccupied
-- 
-- homestead_exemption is amount (0 means NO)
-- 
-- good indicator of whether a property is owner-occupied (which is YES). if LLC has a homestead exemption then it is interesting b/c an LLC with possibly multiple is a legal hook to go after them.
-- 
-- And if an owner has 2 homestead exemptions on different properties, that's a red flag too.
-- 
-- 
-- `CASE opa.homestead_exemption WHEN 0 THEN 0 ELSE 1 END AS F18_OwnerOccupied`

-- CELL ********************

# need the LLC mapping tables first
# 0 AS F17_MultipleHomesteadExemptions

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- ### F19_DollarTransfers
-- 
-- F17_MultipleHomesteadExemptions: Multiple Homestead Exemptions
-- if an owner has 2 homestead exemptions on different properties, that's a red flag
-- F18_OwnerOccupied: Is Property Owner Occupied?
-- ie, it is flagged as a homestead_exemption
-- F19_DollarTransfers:
-- TODO: real estate transfers for $1? etc. (the rtt_summary table)

-- MARKDOWN ********************

-- ## WIP
-- 
-- Deed transactions...one row per transaction

-- CELL ********************

-- MAGIC %%sql 
-- MAGIC 
-- MAGIC select 
-- MAGIC     rtt.adjusted_assessed_value,
-- MAGIC     rtt.adjusted_cash_consideration,
-- MAGIC     rtt.adjusted_fair_market_value,
-- MAGIC     rtt.adjusted_local_tax_amount,
-- MAGIC     rtt.assessed_value,
-- MAGIC     rtt.document_date,
-- MAGIC     rtt.document_id,
-- MAGIC     rtt.document_type,
-- MAGIC     rtt.grantees,
-- MAGIC     rtt.grantors,
-- MAGIC     rtt.legal_remarks,
-- MAGIC     rtt.recording_date
-- MAGIC from davelake.philly_opa_properties_public_pde opa
-- MAGIC left join davelake.philly_rtt_summary rtt 
-- MAGIC     on opa.parcel_number = rtt.opa_account_num
-- MAGIC where opa.location = '2837 KENSINGTON AVE'
-- MAGIC order by rtt.document_date desc
-- MAGIC ;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- Assessments.  One row per assessment

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select 
-- MAGIC     assess.market_value,
-- MAGIC     assess.taxable_building,
-- MAGIC     assess.taxable_land,
-- MAGIC     assess.year,
-- MAGIC     assess.exempt_building,
-- MAGIC     assess.exempt_land
-- MAGIC from davelake.philly_opa_properties_public_pde opa
-- MAGIC left join davelake.philly_assessments assess
-- MAGIC     on opa.parcel_number = assess.parcel_number
-- MAGIC where opa.location = '2837 KENSINGTON AVE'
-- MAGIC order by assess.year desc
-- MAGIC limit 10;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC select 
-- MAGIC     app.appealgrounds,
-- MAGIC     app.appealstatus,
-- MAGIC     app.decision,
-- MAGIC     app.decisiondate,
-- MAGIC     app.opa_owner,
-- MAGIC     app.primaryappellant
-- MAGIC from davelake.philly_opa_properties_public_pde opa
-- MAGIC left join davelake.philly_appeals app
-- MAGIC     on opa.parcel_number = app.opa_account_num
-- MAGIC where opa.location = '2837 KENSINGTON AVE'

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql 
-- MAGIC 
-- MAGIC select 
-- MAGIC     cases.casenumber,
-- MAGIC     cases.casepriority,
-- MAGIC     cases.caseresponsibility,
-- MAGIC     cases.casetype,
-- MAGIC     cases.investigationcompleted,
-- MAGIC     cases.investigationstatus,
-- MAGIC     cases.investigationtype
-- MAGIC from davelake.philly_opa_properties_public_pde opa
-- MAGIC left join davelake.philly_case_investigations cases
-- MAGIC     on opa.parcel_number = cases.opa_account_num
-- MAGIC where opa.location = '2837 KENSINGTON AVE'
-- MAGIC order by cases.investigationcompleted desc;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
