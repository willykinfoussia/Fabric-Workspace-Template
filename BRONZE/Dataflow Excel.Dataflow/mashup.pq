[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"File URL" = "" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
[BindToDefaultDestination = true]
shared fxTransformWebFile = // fxTransformWebFile
// Usage : fxTransformWebFile(url)

(WorkbookUrl as text, 
 optional preferredSheetName as nullable text, 
 optional sparseColumnThreshold as nullable number, 
 optional csvDelimiter as nullable text) as table =>

let
    // ── Paramètres ───────────────────────────────────────────────────────────
    PreferredSheetName    = if preferredSheetName = null then "Sheet_Name" else preferredSheetName,
    SparseColumnThreshold = if sparseColumnThreshold = null then 0.90 else sparseColumnThreshold,
    CsvDelimiter          = if csvDelimiter = null then ";" else csvDelimiter,

    UrlLower = Text.Lower(WorkbookUrl),

    // Détection CSV
    IsCsv = Text.Contains(UrlLower, ".csv"),

    // ── Helpers ──────────────────────────────────────────────────────────────

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

    CleanColumnNames = (t as table) as table =>
        Table.TransformColumnNames(
            t,
            each Text.Trim(Text.Clean(_))
        ),

    // ── Pipeline ─────────────────────────────────────────────────────────────

    FileContent = Web.Contents(WorkbookUrl),

    RawData =
        if IsCsv then
            Csv.Document(FileContent, [Delimiter = CsvDelimiter])
        else
            GetFirstVisibleSheet(
                Excel.Workbook(FileContent, null, true),
                PreferredSheetName
            ),

    NoErrors    = ReplaceErrorsWithNull(RawData),
    NoEmptyRows = RemoveEmptyRows(NoErrors),
    Buffered    = Table.Buffer(NoEmptyRows),
    NoSparse    = RemoveSparseColumns(Buffered, SparseColumnThreshold),

    Promoted    = Table.PromoteHeaders(NoSparse, [PromoteAllScalars = true]),
    Cleaned     = CleanColumnNames(Promoted),

    FinalTable =
        Table.TransformColumnTypes(
            Cleaned,
            List.Transform(Table.ColumnNames(Cleaned), each {_, type text})
        )

in
    FinalTable;
shared Configuration = let
  Source = fxTransformWebFile(#"File URL", "Sheet_Name", 0.75)
in
  Source;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "."]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseName = "LH_BRONZE"]}[Data]
in
  #"Navigation 2";
