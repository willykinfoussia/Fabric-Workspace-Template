[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared workspaceId = "" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared lakehouseId = "" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared data = let
  Source = Folder.Files("C:\Users\willy.kinfoussia.external@spherea.com\OneDrive - SPHEREA\Documents\PowerQuery Project\Support Technique\Source"),
  #"Filtered rows" = Table.SelectRows(Source, each ([Extension] = ".xlsx" or [Extension] = ".xls" or [Extension] = ".csv") and not Text.StartsWith([Name], "~$"))
in
  #"Filtered rows";
shared fxTransformFile = // fxTransformFile
// Usage typique (Folder) : fxTransformFile([Content], [Name])
(content as binary, optional fileNameOrUrl as nullable text, optional preferredSheetName as nullable text, optional sparseColumnThreshold as nullable number, optional csvDelimiter as nullable text) as table =>
let
    // ── Paramètres (valeurs par défaut) ─────────────────────────────────────
    PreferredSheetName    = if preferredSheetName = null then "Sheet_Name" else preferredSheetName,
    SparseColumnThreshold = if sparseColumnThreshold = null then 0.90 else sparseColumnThreshold,
    CsvDelimiter          = if csvDelimiter = null then ";" else csvDelimiter,

    FileIdLower           = if fileNameOrUrl = null then "" else Text.Lower(fileNameOrUrl),

    // Détection CSV sur extension (nom de fichier ou URL)
    IsCsv = Text.EndsWith(FileIdLower, ".csv") or Text.Contains(FileIdLower, ".csv"),

    // ── Helpers ─────────────────────────────────────────────────────────────

    GetFirstVisibleSheet = (source as table, preferredName as text) as table =>
        try
            source{[Item = preferredName, Kind = "Sheet"]}[Data]
        otherwise
            Table.SelectRows(
                source,
                each [Kind] = "Sheet" and ([Hidden] = false or [Hidden] = null)
            ){0}[Data],

    ReplaceErrorsWithNull = (t as table) as table =>
        Table.ReplaceErrorValues(
            t,
            List.Transform(Table.ColumnNames(t), each {_, null})
        ),

    RemoveEmptyRows = (t as table) as table =>
        Table.SelectRows(
            t,
            each List.MatchesAny(Record.FieldValues(_), each _ <> null and _ <> "")
        ),

    RemoveSparseColumns = (t as table, threshold as number) as table =>
        let
            ColNames = Table.ColumnNames(t),
            RowCount = Table.RowCount(t),
            ColData  = if RowCount = 0 then {} else Table.ToColumns(t),

            CountNullOrEmpty = (lst as list) as number =>
                List.Accumulate(lst, 0, (acc, v) => if v = null or v = "" then acc + 1 else acc),

            SparseIndices =
                if RowCount = 0 then
                    {}
                else
                    List.Select(
                        List.Positions(ColData),
                        (i) => CountNullOrEmpty(ColData{i}) / RowCount >= threshold
                    ),

            ToRemove = List.Transform(SparseIndices, (i) => ColNames{i})
        in
            if List.Count(ToRemove) = 0 then t
            else Table.RemoveColumns(t, ToRemove),

    // ✅ Helper lisible : Trim + Clean sur les noms de colonnes
    CleanColumnNames = (t as table) as table =>
        Table.TransformColumnNames(
            t,
            each Text.Trim(Text.Clean(_))
        ),

    // ── Pipeline ─────────────────────────────────────────────────────────────

    RawData =
        if IsCsv then
            Csv.Document(content, [Delimiter = CsvDelimiter])
        else
            GetFirstVisibleSheet(Excel.Workbook(content, null, true), PreferredSheetName),

    NoErrors    = ReplaceErrorsWithNull(RawData),
    NoEmptyRows = RemoveEmptyRows(NoErrors),
    Buffered    = Table.Buffer(NoEmptyRows),   // un seul buffer avant analyses multipasses
    NoSparse    = RemoveSparseColumns(Buffered, SparseColumnThreshold),

    Promoted    = Table.PromoteHeaders(NoSparse, [PromoteAllScalars = true]),
    Renamed     = CleanColumnNames(Promoted),

    FinalTable  =
        Table.TransformColumnTypes(
            Renamed,
            List.Transform(Table.ColumnNames(Renamed), each {_, type text})
        )
in
    FinalTable;
shared DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = true, HierarchicalNavigation = false, EnableVorder = false, OutputMetadataRefresh = false]),
  Navigation_1 = Pattern{[workspaceId = workspaceId]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = lakehouseId]}[Data],
  TableNavigation = Navigation_2{[Id = "ExcelFile", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared ExcelFile = let
    OneFile = Table.SelectRows(data, each [Name] = "ExcelFile.xlsx"),
    Output  = fxTransformFile(OneFile{0}[Content], OneFile{0}[Name])
in
    Output;
