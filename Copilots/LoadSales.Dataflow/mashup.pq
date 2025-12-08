[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Sales csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Sales csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "99328097-99f9-47f7-bb76-63191a1432c5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2db08d21-75e1-458e-b215-b39cffbbba8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Sales.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 14, QuoteStyle = QuoteStyle.None]),
  #"Use first row as headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Use first row as headers", {{"PersonID", Int64.Type}, {"EmployeeKey", Int64.Type}, {"StoreKey", Int64.Type}, {"OrderDate", type date}, {"ShipDate", type date}, {"ShipMethodID", Int64.Type}, {"StoreName", type text}, {"Region", type text}, {"SalesOrderNumber", type text}, {"Country", type text}, {"OrderQty", Int64.Type}, {"ProductID", Int64.Type}, {"UnitPrice", type number}, {"UnitPriceDiscount", type number}}),
  #"Add column" = Table.AddColumn(#"Changed column type", "Gross Revenue", each Number.Round([UnitPrice] * [OrderQty], 2), type number),
  Multiply = Table.AddColumn(#"Add column", "Discount Value", each [Gross Revenue] * [UnitPriceDiscount], type number),
  #"Add column 1" = Table.AddColumn(Multiply, "DaysToShip", each Duration.Days(Duration.From([ShipDate] - [OrderDate])), Int64.Type)
in
  #"Add column 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Person csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Person csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "99328097-99f9-47f7-bb76-63191a1432c5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2db08d21-75e1-458e-b215-b39cffbbba8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Person.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 8, QuoteStyle = QuoteStyle.None]),
  #"Use first row as headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Use first row as headers", {{"PersonID", Int64.Type}, {"FirstName", type text}, {"LastName", type text}, {"EmailAddress", type text}, {"AddressLine1", type text}, {"AddressLine2", type text}, {"PhoneNumber", type text}, {"Demographics", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Products csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Products csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "99328097-99f9-47f7-bb76-63191a1432c5"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2db08d21-75e1-458e-b215-b39cffbbba8f"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "Products.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 3, QuoteStyle = QuoteStyle.None]),
  #"Use first row as headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Use first row as headers", {{"ProductID", Int64.Type}, {"ProductCategoryName", type text}, {"Model", type text}})
in
  #"Changed column type";
shared #"Sales csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "99328097-99f9-47f7-bb76-63191a1432c5"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "2db08d21-75e1-458e-b215-b39cffbbba8f"]}[Data],
  TableNavigation = Navigation_2{[Id = "Sales", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Person csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "99328097-99f9-47f7-bb76-63191a1432c5"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "2db08d21-75e1-458e-b215-b39cffbbba8f"]}[Data],
  TableNavigation = Navigation_2{[Id = "Person", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Products csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "99328097-99f9-47f7-bb76-63191a1432c5"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "2db08d21-75e1-458e-b215-b39cffbbba8f"]}[Data],
  TableNavigation = Navigation_2{[Id = "Products", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
