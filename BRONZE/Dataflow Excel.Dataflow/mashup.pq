[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Configuration = let
    Source = Excel.Workbook(Web.Contents(#"ExcelUrl"), null, true),
    ReportCASheet = try Source{[Item="Sheet_Name", Kind="Sheet"]}[Data] otherwise null,
    FirstVisibleSheet =
    if ReportCASheet <> null
    then ReportCASheet
    else
        Table.SelectRows(
            Source,
            each [Kind] = "Sheet" and ([Hidden] = false or [Hidden] = null)
        ){0}[Data],

    // --- BEFORE ANY ROW CLEANING ---
    Remove90PctErrorColumns =
    let
        RowCount = Table.RowCount(FirstVisibleSheet),
        Cols = Table.ColumnNames(FirstVisibleSheet),

        ColumnsToRemove =
            if RowCount = 0 then
                {}
            else
                List.Select(
                    Cols,
                    (col) =>
                        let
                            L = Table.Column(FirstVisibleSheet, col),
                            ErrorCount =
                                List.Count(
                                    List.Select(
                                        L,
                                        (v) => let t = try v in t[HasError]
                                    )
                                )
                        in
                            (ErrorCount / RowCount) >= 0.9
                )
    in
        Table.RemoveColumns(FirstVisibleSheet, ColumnsToRemove),

    // NEW: sanitize Excel errors (#REF!, #VALUE!, ...) early
    SanitizeErrors =
    Table.TransformColumns(
        Remove90PctErrorColumns,
        List.Transform(
            Table.ColumnNames(Remove90PctErrorColumns),
            (c) => { c, each try _ otherwise null, type any }
        )
    ),

    // Optional: if some '#REF!' are literal text
    SanitizeRefText =
    Table.ReplaceValue(
        SanitizeErrors,
        "#REF!",
        null,
        Replacer.ReplaceValue,
        Table.ColumnNames(SanitizeErrors)
    ),

    // --- NOW SAFE ---
    #"Remove All Error Rows" =
    Table.RemoveRowsWithErrors(
        SanitizeRefText,
        Table.ColumnNames(SanitizeRefText)
    ),

    // continue with your existing logic
    CleanRows =
        Table.SelectRows(
            #"Remove All Error Rows",
            each List.NonNullCount(
                List.Select(Record.FieldValues(_), each _ <> "" and _ <> null)
            ) > 0
        ),

    CleanColumns =
        Table.RemoveColumns(
            CleanRows,
            List.Select(
                Table.ColumnNames(CleanRows),
                each List.NonNullCount(
                    List.Select(Table.Column(CleanRows, _), each _ <> "" and _ <> null)
                ) = 0
            )
        ),

    AddNullCount =
        Table.AddColumn(
            CleanColumns,
            "NullCount",
            each List.Count(List.Select(Record.FieldValues(_), each _ = null)),
            Int64.Type
        ),

    FirstValidRowIndex = List.PositionOf(AddNullCount[NullCount], 0),

    CleanedForRetry =
        if FirstValidRowIndex = -1 then
            let
                RowCount = Table.RowCount(CleanColumns),
                ColumnsToRemove =
                    List.Select(
                        Table.ColumnNames(CleanColumns),
                        (col) =>
                            List.Count(
                                List.Select(Table.Column(CleanColumns, col), each _ = null or _ = "")
                            ) / RowCount > 0.95
                    )
            in
                Table.AddColumn(
                    Table.RemoveColumns(CleanColumns, ColumnsToRemove),
                    "NullCount",
                    each List.Count(List.Select(Record.FieldValues(_), each _ = null)),
                    Int64.Type
                )
        else
            AddNullCount,

    FinalFirstValidRowIndex =
        List.PositionOf(CleanedForRetry[NullCount], 0),

    KeepFromFirstValid =
        if FinalFirstValidRowIndex <> -1 then
            Table.Skip(CleanedForRetry, FinalFirstValidRowIndex)
        else
            #table({}, {}),

    #"Promoted Headers" =
        Table.PromoteHeaders(KeepFromFirstValid, [PromoteAllScalars = true]),
    #"ToText" = Table.TransformColumnTypes(#"Promoted Headers", List.Transform(Table.ColumnNames(#"Promoted Headers"),each {_, type text}))
in
    #"ToText";
shared ExcelUrl = "https://spherea-my.sharepoint.com/personal/willy_kinfoussia_external_spherea_com/Documents/Documents/PowerQuery%20Project/CA%20Spherea%20SP%26I/Source/Dim%20Client%26Projet%20-%2003-2026.xlsx" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
