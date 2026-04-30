[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Configuration = let
    WorkbookPath          = #"File URL",
    PreferredSheetName    = "Sheet_Name",
    SparseColumnThreshold = 0.90,

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

    // List.MatchesAny court-circuite dès le 1er élément valide
    // → évite de scanner toute la ligne quand une valeur non-nulle est trouvée tôt
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

            // List.Accumulate : pas de liste intermédiaire allouée
            // (vs List.Count(List.Select(...)) qui crée une liste filtrée en mémoire)
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

    // ── Pipeline ─────────────────────────────────────────────────────────────

    IsCsv       = Text.Contains(Text.Lower(WorkbookPath), ".csv"),
    FileContent = Web.Contents(WorkbookPath),
    RawData     = if IsCsv then
        Csv.Document(FileContent, [Delimiter = ";"])
    else
        GetFirstVisibleSheet(Excel.Workbook(FileContent, null, true), PreferredSheetName),
    NoErrors    = ReplaceErrorsWithNull(RawData),
    NoEmptyRows = RemoveEmptyRows(NoErrors),
    Buffered    = Table.Buffer(NoEmptyRows),   // ← un seul buffer, avant toutes les analyses multi-passes
    NoSparse    = RemoveSparseColumns(Buffered, SparseColumnThreshold),
    Promoted    = Table.PromoteHeaders(NoSparse, [PromoteAllScalars = true]),
    FinalTable  = Table.TransformColumnTypes(Promoted, List.Transform(Table.ColumnNames(Promoted), each {_, type text}))
in
    FinalTable;
shared #"File URL" = "" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "."]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseName = "LH_BRONZE"]}[Data]
in
  #"Navigation 2";
