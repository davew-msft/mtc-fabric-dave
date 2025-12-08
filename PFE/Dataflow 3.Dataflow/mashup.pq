[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Sheet1_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Sheet1 = let
  Source = Excel.Workbook(Web.Contents("https://onemtc-my.sharepoint.com/personal/davewen_onemtc_net/Documents/Apps/Microsoft Power Query/Uploaded Files/informa-filtered 1.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "Sheet1", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"MANUFACTURE_SITE", type text}, {"RESULT_ID", Int64.Type}, {"PRODUCT_NAME", type text}, {"UNIT_OPERATION", type text}, {"BATCH_NUMBER", type text}, {"BATCH_NUMBER_WITH_SUBLOT", type text}, {"MATERIAL_NUMBER", type text}, {"FINAL_DISPOSITION", type text}, {"CATEGORY_FIELD_FLAG", type text}, {"CATEGORY_FIELD_FLAG_2", type text}, {"PARAMETER_FIELD_NAME", type text}, {"PARAMETER_WITH_UNITS_FIELD_NAME", type text}, {"PARAMETER_UNITS", type text}, {"TEXT_PARAMETER_VALUE", type number}, {"NUMERIC_PARAMETER_VALUE", type number}, {"SOURCE_SYSTEM", type text}, {"SOURCE_APPROVAL_STATUS", type text}, {"INFORMA_APPROVAL_STATUS", type text}, {"LAST_DATA_EXTRACTION_DATE", type text}, {"SAMPLING_POINT_MAPPING_ID", type text}, {"PARAMETER_MAPPING_ID", type text}})
in
  #"Changed column type";
shared Sheet1_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "99328097-99f9-47f7-bb76-63191a1432c5"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8972a9ea-a943-4df8-b782-074ab1d9e236"]}[Data],
  TableNavigation = Navigation_2{[Id = "informa-spc-data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
