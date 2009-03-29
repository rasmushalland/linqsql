#light

module SqlGeneration

open System
open System.Linq
open System.Linq.Expressions
open System.Reflection
open System.Collections.Generic
open System.Data.Linq.Mapping
open Microsoft.FSharp.Collections.Tagged



open LinqTypes
open Microsoft.FSharp.Collections.Tagged



// **************************************************************************************
// **************************************************************************************
// SQL generation.
// **************************************************************************************
// **************************************************************************************

let internal ObjComparer = new ComparisonComparer<obj>(defaultComparer)

let internal TableExpressionTokenComparer = new ComparisonComparer<TableExpressionToken>(defaultComparer)




let internal GetColumnSql(columnPropertyInfo : PropertyInfo, tableAlias : string, includeColumnAliasIfNecessary : bool) =
    let attArray = columnPropertyInfo.GetCustomAttributes(typeof<ColumnAttribute>, false)
    if attArray.Length = 1 then 
        let colAtt = attArray.[0] :?> ColumnAttribute
        let colName = if String.IsNullOrEmpty(colAtt.Name) = false then colAtt.Name else columnPropertyInfo.Name
        if String.IsNullOrEmpty(colAtt.Expression) = false then
            let expressionString = String.Format(colAtt.Expression, tableAlias)
            if includeColumnAliasIfNecessary then expressionString ^ " " ^ colName else expressionString
        else tableAlias ^ "." ^ colName
    else tableAlias ^ "." ^ columnPropertyInfo.Name


let rec internal GetAlias(tableNames : Map<TableExpressionToken, string>, tableToken : TableExpressionToken, aliasHint : string) : string * Map<TableExpressionToken, string> =
    let rec tryAlias i =
        let aliasAttempt = if i = 1 then aliasHint else aliasHint ^ "_" ^ (i.ToString())
        match tableNames |> Seq.tryfind (fun kvp -> kvp.Value = aliasAttempt)  with
        | Some(_) -> if i < 20  then tryAlias (i+1) else failwith "wtf: > 20 table instances?"
        | None -> aliasAttempt
    let alias = tryAlias 1
    alias, tableNames.Add(tableToken, alias)

let rec internal SqlValueToString(v : SqlValue, tablenames : Map<TableExpressionToken, string>, settings : SqlSettings) : string * Map<TableExpressionToken, string> =
    match v with
    | ConstSqlValue(c, _) -> 
        // NB: This is probably open to sql injection...
        let sqlstring = 
            match c with
            | null -> "NULL"
            | _ when c.GetType() = typeof<string> -> "'" ^ c.ToString().Replace("'", "''") ^ "'"
            | _ -> c.ToString()
        sqlstring, tablenames
    | BindVariable(name) -> ":" ^ name, tablenames
    | BinarySqlValue(op, vLeft, vRight) ->
        let sqlLeft, t2 = SqlValueToString(vLeft, tablenames, settings)
        match op, vRight with
        | Equal, ConstSqlValue(objRight, _) when objRight = null -> sqlLeft ^ " IS NULL", t2
        | NotEqual, ConstSqlValue(objRight, _) when objRight = null -> sqlLeft ^ " IS NOT NULL", t2
        | _, _ -> 
            let opname = 
                match op with 
                | Add -> "+" | Subtract -> "-" | GreaterThan -> ">" | GreaterThanOrEqual -> ">=" | AndAlso -> "AND" | OrElse -> "OR" 
                | LessThan -> "<" | LessThanOrEqual -> "<=" | Equal -> "=" | NotEqual -> "<>" | StringConcat -> "||"
                | Other -> failwith "Binary \"Other\" should not make it to this place."
            let sqlRight, t3 = SqlValueToString(vRight, t2, settings)
            ("(" ^ sqlLeft ^ " " ^ opname ^ " " ^ sqlRight ^ ")"), t3
    | SqlConstruct(construct, argsSqlValues) ->
        match construct, argsSqlValues with
        | CaseWhen, [test; ifTrue; ifFalse] ->
            let sqlTest, t2 = SqlValueToString(test, tablenames, settings)
            let sqlIfTrue, t3 = SqlValueToString(ifTrue, t2, settings)
            let sqlIfFalse, t4 = SqlValueToString(ifFalse, t3, settings)
            "CASE WHEN " ^ sqlTest ^ " THEN " ^ sqlIfTrue ^ " ELSE " ^ sqlIfFalse ^ " END", t4
        | CaseWhen, _ -> failwith "CaseWhen ???"
    | CallSqlValue(functionName, argsSqlValues) ->
        let (argsSql : string list), t2 = mapWithAccumulator((fun (arg, state) -> SqlValueToString(arg, state, settings)), tablenames, argsSqlValues)
        functionName ^ "(" ^ (String.concat ", " argsSql) ^ ")", t2
    | ColumnAccessSqlValue(table, colPropertyInfo) -> 
        let tableAlias, t2 = SqlValueToString(table, tablenames, settings)
        let columnSql = GetColumnSql(colPropertyInfo, tableAlias, false)
        columnSql, t2
    | LogicalTableSqlValue(LogicalTable(_, tableName, tableToken, alias)) ->
        match tablenames.TryFind(tableToken) with
        | Some(alias) -> alias, tablenames
        | None -> 
            let alias, tablenames = GetAlias(tablenames, tableToken, alias)
            tableName ^ " " ^ alias, tablenames
    | VirtualTableSqlValue(_) -> failwith "virtual table??"
    | SelectClauseSqlValue(_) -> raise <| new NotSupportedException("select clause")

and internal FromClauseToSql(from : TableExpression list, tablenames : Map<TableExpressionToken, string>, settings : SqlSettings) : string * Map<TableExpressionToken, string> =
    match from with
    | [] -> "", tablenames
    | item :: itemtail ->
        let previousFromSql, tableNamesAfterTail = FromClauseToSql(itemtail, tablenames, settings)
        let tableSql, tableNamesAfterCurrent = 
            match item.Content with
            | LogicalTableContent(lt) -> SqlValueToString(LogicalTableSqlValue(lt), tableNamesAfterTail, settings)
            | SelectClauseContent(selectclause) -> 
                let sql, tablenames3_2 = SelectToStringImpl(selectclause, tableNamesAfterTail,  { settings with SelectStyle = SelectStyle.OnlyFrom })
                "(" ^ sql ^ ")", tablenames3_2
        match item.JoinType with 
        | Some (jointype) -> 
            let joinword = match jointype with | JoinType.Inner -> "INNER" | JoinType.LeftOuter -> "LEFT" | Cross -> "CROSS"
            let sql, tableNamesAfterJoin = 
                match jointype with
                | Inner | LeftOuter -> 
                    let joinConditionSql, tableNamesAfterJoin = SqlValueToString((item.Condition.Value), tableNamesAfterCurrent, settings)
                    previousFromSql ^ "\r\n\t" ^ joinword ^ " JOIN " ^ tableSql ^ " ON " ^ joinConditionSql, tableNamesAfterJoin
                | Cross -> 
                    previousFromSql ^ "\r\n\t" ^ joinword ^ " JOIN " ^ tableSql, tableNamesAfterCurrent
            sql, tableNamesAfterJoin
        | None -> tableSql, tableNamesAfterCurrent

and internal SelectToStringImpl(select : SelectClause, tablenames : Map<TableExpressionToken, string>, settings : SqlSettings) : string * Map<TableExpressionToken, string> =
    let fromsql, tableNamesAfterFrom = FromClauseToSql(select.FromClause, tablenames, settings)
    let wheresql, tableNamesAfterWhere = 
        match select.WhereClause with
        | Some(where) -> 
            let wheresql2, tablenames2 = SqlValueToString(where, tableNamesAfterFrom, settings)
            "\r\nWHERE " ^ wheresql2, tablenames2
        | None -> "", tableNamesAfterFrom
    let orderbysql =
        if select.OrderBy.IsEmpty then "" else 
            let partmapper part =
                let orderSqlValue, direction = part
                let partSql, _ = SqlValueToString(orderSqlValue, tableNamesAfterWhere, settings)
                let directionWord = if direction = Ascending then "" else " DESC"
                partSql ^ directionWord
            let parts = select.OrderBy |> List.map partmapper |> List.to_seq |> String.concat ", "
            "\r\nORDER BY " ^ parts
    let getClassColNames(itemType : Type, tableAlias : string) =
        let getClassColNamesDefaultImpl() = 
            let requireColumnAttribute = false
            let propertyinfos = itemType.GetProperties()
            propertyinfos 
                |> Seq.filter (fun pi -> not(requireColumnAttribute) || pi.IsDefined(typeof<ColumnAttribute>, false) ) 
                |> Seq.map (fun pi -> GetColumnSql(pi, tableAlias, true))
        match settings.GetColumnsForSelect(itemType, tableAlias, "") with
        | None -> getClassColNamesDefaultImpl() |> String.concat ", "
        | Some(s) -> s
    let getColumnsSql sqlTableVal = 
        match sqlTableVal with
        | VirtualTableSqlValue(map) -> 
            map |> Seq.map (fun kvp -> SqlValueToString(kvp.Value, tableNamesAfterWhere, settings)) 
                |> Seq.map fst
                |> String.concat ", "
        | LogicalTableSqlValue(LogicalTable(itemType, tableToken, tableName, tableAlias)) -> getClassColNames(itemType, tableAlias)
        | _ -> failwith ("notsup: " ^ sqlTableVal.GetType().ToString())
    let columnsSql = getColumnsSql (select.VirtualTableSqlValue)
    match settings.SelectStyle with
    | ColumnList -> "SELECT " ^ columnsSql ^ "\r\nFROM " ^ fromsql ^ wheresql ^ orderbysql, tableNamesAfterWhere
    | OnlyFrom -> fromsql ^ wheresql ^ orderbysql, tableNamesAfterWhere

and internal SelectToString(select : SelectClause, settings : SqlSettings) : string =
    let sql, _ = SelectToStringImpl(select, (Map<_,_>.Empty(TableExpressionTokenComparer)), settings)
    match select.Next with
    | Some(construct, nextSelect) ->
        match construct with
        | Union | UnionAll ->
            let unionWords = match construct with | Union -> "UNION" | UnionAll -> "UNION ALL"
            let nextSql = SelectToString(nextSelect, settings)
            sql ^ "\r\n" ^ unionWords ^ "\r\n" ^ nextSql
    | None -> sql


let internal DeleteToString(select : SelectClause, tablenames : Map<TableExpressionToken, string>, settings : SqlSettings) : string =
    let getTableSql sqlTableVal = 
        match sqlTableVal with
        | LogicalTableSqlValue(LogicalTable(_, tableName, tableToken, tableAlias)) -> tableName ^ " " ^ tableAlias, tablenames.Add(tableToken, tableAlias)
        | _ -> failwith ("notsup: " ^ sqlTableVal.GetType().ToString())
    let tableSql, tableNamesAfterFrom = getTableSql (select.VirtualTableSqlValue)
    let whereSql, tableNamesAfterWhere = 
        match select.WhereClause with
        | Some(where) -> 
            let wheresql2, tablenames3_2 = SqlValueToString(where, tableNamesAfterFrom, settings)
            "\r\nWHERE " ^ wheresql2, tablenames3_2
        | None -> "", tablenames
    "DELETE FROM " ^ tableSql ^ whereSql

let internal UpdateToString(select : SelectClause, tablenames : Map<TableExpressionToken, string>, settings : SqlSettings) : string =
    let tableSql, tablenames =
        // We need to get the table declared with alias up front ("UPDATE Table myalias ..."), and we can't use the new row for this.
        if List.is_empty select.FromClause then failwith "Empty FROM clause for update?"
        else
            let tableExpr = List.nth select.FromClause ((List.length select.FromClause) - 1)
            match tableExpr.Content with
            | LogicalTableContent(lt) -> SqlValueToString(LogicalTableSqlValue(lt), tablenames, settings)
            | _ -> failwith "First UPDATE table expression must be a table."
        
//        match select.VirtualTableSqlValue with
//        | VirtualTableSqlValue(map) ->
//            if map.IsEmpty then failwith "Empty column map??"
//            else 
//                let pi = (Seq.hd map).Key
//                GetTableName(pi.DeclaringType), tablenames
//        | _ -> failwith ("notsup: " ^ select.VirtualTableSqlValue.GetType().ToString())
    let setSql, tablenames =
        match select.VirtualTableSqlValue with
        | VirtualTableSqlValue(map) -> 
            let createOneColumnUpdate(pi, newValue) =
                let col = GetColumnSql(pi, "XXXX", false)
                let newValueSql, _ = SqlValueToString(newValue, tablenames, settings)
                col ^ " = " ^ newValueSql
            let setSql =
                Seq.map (fun (kvp : KeyValuePair<_, _>) -> createOneColumnUpdate(kvp.Key, kvp.Value)) map
                    |> String.concat ", "
            setSql, tablenames
        | _ -> failwith ("notsup: " ^ select.VirtualTableSqlValue.GetType().ToString())
    let whereSql, tablenames = 
        match select.WhereClause with
        | Some(where) -> 
            let wheresql2, tablenames3_2 = SqlValueToString(where, tablenames, settings)
            "\r\nWHERE " ^ wheresql2, tablenames3_2
        | None -> "", tablenames
    "UPDATE "  ^ tableSql ^ "\r\nSET " ^ setSql ^ whereSql



