// 1. Base Query (Source Data with Initial Transformations)
let
    Source = AzureStorage.DataLake(
        "https://goodreadsreviews60104758.dfs.core.windows.net/lakehouse/gold/curated_reviews/",
        [HierarchicalNavigation=true]
    ),
    DeltaTable = DeltaLake.Table(Source),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(DeltaTable, "date_added_iso", each try
  Text.End([date_added], 4) & "-" &
  (if Text.Middle([date_added], 4, 3) = "Jan" then "01"
   else if Text.Middle([date_added], 4, 3) = "Feb" then "02"
   else if Text.Middle([date_added], 4, 3) = "Mar" then "03"
   else if Text.Middle([date_added], 4, 3) = "Apr" then "04"
   else if Text.Middle([date_added], 4, 3) = "May" then "05"
   else if Text.Middle([date_added], 4, 3) = "Jun" then "06"
   else if Text.Middle([date_added], 4, 3) = "Jul" then "07"
   else if Text.Middle([date_added], 4, 3) = "Aug" then "08"
   else if Text.Middle([date_added], 4, 3) = "Sep" then "09"
   else if Text.Middle([date_added], 4, 3) = "Oct" then "10"
   else if Text.Middle([date_added], 4, 3) = "Nov" then "11"
   else if Text.Middle([date_added], 4, 3) = "Dec" then "12"
   else "00") & "-" &
  Text.Middle([date_added], 8, 2)
otherwise
  null), {{"date_added_iso", type date}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Added custom 1", {{"review_id", type text}, {"book_id", type text}, {"title", type text}, {"author_id", type text}, {"name", type text}, {"user_id", type text}, {"rating", Int64.Type}, {"review_text", type text}, {"language_code", type text}, {"n_votes", Int64.Type}, {"date_added", type text}, {"date_added_iso", type date}}),
  #"Removed blank rows" = Table.SelectRows(#"Changed column type", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed blank rows", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 2" = Table.SelectRows(#"Removed blank rows 1", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 3" = Table.SelectRows(#"Removed blank rows 2", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 4" = Table.SelectRows(#"Removed blank rows 3", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed blank rows 5" = Table.SelectRows(#"Removed blank rows 4", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Removed blank rows 5", "review_length", each if [review_text] <> null then Text.Length(Text.Trim([review_text])) else 0), {{"review_length", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom", each [review_length] >= 10),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Filtered rows", {"date_added_iso"}),
  #"Removed blank rows 6" = Table.SelectRows(#"Removed errors", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Added custom 2" = Table.TransformColumnTypes(Table.AddColumn(#"Removed blank rows 6", "valid_Dates", each if [date_added_iso] <> null and [date_added_iso] <= DateTime.Date(DateTime.LocalNow()) then true else false), {{"valid_Dates", type logical}}),
  #"Removed columns" = Table.RemoveColumns(#"Added custom 2", {"valid_Dates"}),
  #"Replaced value" = Table.ReplaceValue(#"Removed columns", null, 0, Replacer.ReplaceValue, {"n_votes"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "", "Unknown", Replacer.ReplaceValue, {"language_code"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 1", {{"title", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Trimmed text", {{"name", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 2" = Table.TransformColumns(#"Trimmed text 1", {{"review_text", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text 2", {{"title", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"name", each Text.Proper(_), type nullable text}}),
  #"Removed errors 1" = Table.RemoveRowsWithErrors(#"Capitalized each word 1", {"book_id"})
in
    #"Removed errors 1"

// 2.Book Summary Aggregation (agg_book_summary)
let
  Source = Query,
  #"Grouped rows" = Table.Group(Source, {"book_id"}, {{"avg_rating", each List.Average([rating]), type nullable number}, {"num_reviews", each Table.RowCount(_), Int64.Type}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Grouped rows", {{"book_id", type text}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Changed column type", {"book_id"}),
  #"Removed blank rows" = Table.SelectRows(#"Removed errors", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null})))
in
  #"Removed blank rows"

// 3. Author Summary Aggregation (avg_author_summary)
let
  Source = Query,
  #"Grouped rows" = Table.Group(Source, {"name"}, {{"avg_author_rating", each List.Average([rating]), type nullable number}})
in
  #"Grouped rows"

// 4. Review Statistics (book_review_stats)
let
  Source = Query,
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(Source, "review_word_count", each if [review_text] <> null then List.Count(Text.Split(Text.Trim([review_text]), " ")) else 0), {{"review_word_count", Int64.Type}}),
  #"Grouped rows" = Table.Group(#"Added custom", {"book_id"}, {{"avg_review_words", each List.Average([review_word_count]), type nullable number}, {"min_review_words", each List.Min([review_word_count]), type nullable Int64.Type}, {"max_review_words", each List.Max([review_word_count]), type nullable Int64.Type}})
in
  #"Grouped rows"

// 5.Final cleaned dataset where the aggregated columns are merged:
let
  Source = Table.NestedJoin(Query, {"book_id"}, agg_book_summary, {"book_id"}, "agg_book_summary", JoinKind.LeftOuter),
  #"Expanded agg_book_summary" = Table.ExpandTableColumn(Source, "agg_book_summary", {"avg_rating", "num_reviews"}, {"avg_rating", "num_reviews"}),
  #"Merged queries 1" = Table.NestedJoin(#"Expanded agg_book_summary", {"book_id"}, book_review_stats, {"book_id"}, "book_review_stats", JoinKind.LeftOuter),
  #"Expanded book_review_stats" = Table.ExpandTableColumn(#"Merged queries 1", "book_review_stats", {"avg_review_words", "min_review_words", "max_review_words"}, {"avg_review_words", "min_review_words", "max_review_words"}),
  #"Removed columns" = Table.RemoveColumns(#"Expanded book_review_stats", {"review_length", "date_added"}),
  #"Lowercased text" = Table.TransformColumns(#"Removed columns", {{"review_text", each Text.Lower(_), type nullable text}}),
  #"Removed duplicates" = Table.Distinct(#"Lowercased text", {"review_id"}),
  #"Removed duplicates 1" = Table.Distinct(#"Removed duplicates", {"book_id", "author_id", "review_id"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed duplicates 1", {{"avg_rating", "avg_book_rating"}}),
  #"Merged queries" = Table.NestedJoin(#"Renamed columns", {"name"}, avg_author_summary, {"name"}, "avg_author_summary", JoinKind.LeftOuter),
  #"Expanded avg_author_summary" = Table.ExpandTableColumn(#"Merged queries", "avg_author_summary", {"avg_author_rating"}, {"avg_author_rating"})

in
  #"Expanded avg_author_summary"