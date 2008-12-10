﻿#light

module LinqModule

open System.Linq
open System.Linq.Expressions


type internal SortDirection = Ascending | Descending


let internal (|Call|_|) (expr : Expression) =
    match expr with
    | :? MethodCallExpression as call -> Some(call.Method.Name, call)
    | _ -> None

let internal (|LambdaExpr|_|) (expr : Expression) =
    match expr with
    | Call(_, call) -> Some(call)
    | _ -> None

let internal (|LambdaFromUnary|_|) (expr : Expression) =
    match expr with
    | :? UnaryExpression as unary ->
        match unary.Operand with
        | :? LambdaExpression as lambda -> Some(lambda)
        | _ -> None
    | _ -> None

let internal getlambdafromunary (expr : Expression) =
    match expr with
    | LambdaFromUnary(lambda) -> lambda
    | _ -> failwith "Cannot get lambda."

let internal (|Select|_|) (expr : Expression) =
    match expr with
    | Call("Select", call) -> 
        match call.Arguments.[1] with 
        | LambdaFromUnary(lambda) -> Some(call.Arguments.[0], lambda)
        | _ -> None
    | _ -> None

let internal (|Where|_|) (expr : Expression) =
    match expr with
    | Call("Where", call) -> 
        match call.Arguments.[1] with 
        | LambdaFromUnary(lambda) -> Some(call.Arguments.[0], lambda) 
        | _ -> None
    | _ -> None

let internal (|SelectMany|_|) (expr : Expression) =
    match expr with
    | Call("SelectMany", call) ->
        let input = call.Arguments.[0]
        let collSelector = getlambdafromunary(call.Arguments.[1])
        let resultSelector = getlambdafromunary(call.Arguments.[2])
        let resultSelector2 = if call.Arguments.Count = 3 then Some(getlambdafromunary(call.Arguments.[2])) else None
        Some(input, collSelector, resultSelector2)
    | _ -> None

let internal (|EqualityJoin|_|) (expr : Expression) =
    match expr with
    | Call("Join", call) ->
        let inputLeft = call.Arguments.[0]
        let inputRight = call.Arguments.[1]
        let leftSelector = getlambdafromunary(call.Arguments.[2])
        let rightSelector = getlambdafromunary(call.Arguments.[3])
        let resultSelector = getlambdafromunary(call.Arguments.[4])
        Some(inputLeft, inputRight, leftSelector, rightSelector, resultSelector)
    | _ -> None
    
let internal (|OrderBy|_|) (expr : Expression) =
    match expr with
    | Call(methodname, call) when 
            methodname = "OrderBy" or methodname = "OrderByDescending" or
            methodname = "ThenBy" or methodname = "ThenByDescending" 
        ->
        let input = call.Arguments.[0]
        let keySelector = getlambdafromunary(call.Arguments.[1])
        let dir = if methodname.EndsWith("Descending") then Descending else Ascending
        Some(input, keySelector, dir)
    | _ -> None

let internal (|DefaultIfEmpty|_|) (expr : Expression) =
    match expr with
    | Call("DefaultIfEmpty", call) -> Some(call.Arguments.[0])
    | _ -> None


// TODO:
// - Use bind variables instead of immediate values. V
// - LINQ join syntax. v
// - Respect ColumnAttribute and TableAttribute. v
// - Parameterized views. v
// - Generate DELETE and UPDATE.
// - Some support for executing and retrieving records.
// - Relations via properties. Implement as parameterized views or as SQL in ColumnAttribute?
// - Nested queries.
// - group by.
// - Aggregate functions.
// - "numlist2table".

// Lectio:
// - Use LectioDbAccess.
// - Use OracleFields.
// - Use RowReader.



open System.Reflection
open System.Collections.Generic
open Microsoft.FSharp.Collections.Tagged

open System.Data.Linq.Mapping

type internal ComparisonComparer<'a>(cmp : 'a * 'a -> int) =
    interface System.Collections.Generic.IComparer<'a> with
        override this.Compare(x, y) = cmp(x, y)
// TODO: handle hash collisions better.
let defaultComparer ((x : 'a), (y : 'a )) =
    match x.GetHashCode() - y.GetHashCode() with
    | 0 when not (System.Object.ReferenceEquals(x, y)) -> failwith "Hash collision."
    | diff -> diff
let internal ExpressionComparer = new ComparisonComparer<Expression>(defaultComparer)
let internal ParameterExpressionComparer = new ComparisonComparer<ParameterExpression>(defaultComparer)
let internal MethodInfoComparer = new ComparisonComparer<MethodInfo>(defaultComparer)




open System.Reflection
open System.Linq.Expressions


type JoinType = Inner | LeftOuter
type BinaryOperator = | AndAlso | OrElse | Add | Subtract | GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual | Equal
    /// An atomic expression that usually represents a physical table.
    /// table name * alias hint.
type internal LogicalTable = LogicalTable of Expression * string * string
and internal SqlValue =
    /// VirtualTableSqlValue represents the result of a Select call, ie. often an anonymous type.
    /// The map keys are property get methods for generated values.
    | VirtualTableSqlValue of Map<MethodInfo, SqlValue>
    | LogicalTableSqlValue of LogicalTable
    /// The string option is a bind variable name suggestion.
    | ConstSqlValue of obj * string option
    | BindVariable of string
    | BinarySqlValue of BinaryOperator * SqlValue * SqlValue
    | ColumnAccessSqlValue of SqlValue * PropertyInfo
    | SelectClauseSqlValue of SelectClause
and internal JoinClause = { Table : FromItem; JoinType : JoinType option; Condition : SqlValue option }
and internal FromItem =
    | TableFrom of LogicalTable
    | SelectClauseFrom of SelectClause
and internal SelectClause = { 
    FromClause : JoinClause list; 
    WhereClause : SqlValue option; 
    VirtualTableSqlValue : SqlValue; 
    OrderBy : (SqlValue * SortDirection) list
}

type internal SelectStyle = ColumnList | OnlyFrom



let internal (|TableAccess|_|) (aliasHint : string option) (expr : Expression) : LogicalTable option =
    let GetTableName(tabletype : System.Type) : string =
        let attArray = tabletype.GetCustomAttributes(typeof<TableAttribute>, false)
        if attArray.Length = 1 then
            let att = attArray.[0] :?> TableAttribute
            if System.String.IsNullOrEmpty(att.Name) = false then att.Name else tabletype.Name
        else tabletype.Name
    match expr with
         // Første, konstante IQ.
    | :? ConstantExpression when typeof<IQueryable>.IsAssignableFrom(expr.Type) -> 
        let tableName = 
            let itemType = expr.Type.GetGenericArguments().[0]
            GetTableName(itemType)
        Some(LogicalTable(expr, tableName, aliasHint.Value))
    | _ -> None


let internal ProcessExpression (expr : Expression) : SelectClause =
    let rec processExpressionImplAsSelect (expr : Expression, tables : Map<ParameterExpression, SqlValue>, tableAliasHint : string option, colPropMap : Map<string, SqlValue>) : SelectClause =
        let selValue = processExpressionImpl(expr, tables, tableAliasHint, colPropMap)
        match selValue with
        | SelectClauseSqlValue(sel) -> sel
        | _ -> failwith ("not sel, but " ^ (any_to_string selValue)  ^ "??")

    and mergeSelect (owner : SelectClause, toBeMerged : SelectClause, joinType : JoinType option, joinCondition : SqlValue option) : SelectClause =
        match 1 with
        | _ when List.is_empty toBeMerged.OrderBy = false -> failwith "Can't have ORDER BY on sub-select."
        | _ ->
            let condition =
                match toBeMerged.WhereClause, joinCondition with
                | Some(where), Some(condition) -> Some(BinarySqlValue(BinaryOperator.AndAlso, where, condition))
                | Some(where), None -> Some(where)
                | None, Some(condition) -> Some(condition)
                | None, None -> None
            let newFromItem =
                match toBeMerged.FromClause with
                | [join] ->
                    match join.Table with
                    | TableFrom(logicalTable) -> TableFrom(logicalTable)
                    | _ -> failwith "First table not a TableFrom??"
                | _ -> SelectClauseFrom({toBeMerged with WhereClause = None })
            let newFromItem = { Table = newFromItem; JoinType = joinType; Condition = condition }
            { owner with FromClause = newFromItem :: owner.FromClause; VirtualTableSqlValue = toBeMerged.VirtualTableSqlValue }

    and processExpressionImpl (expr : Expression, tables : Map<ParameterExpression, SqlValue>, tableAliasHint : string option, colPropMap : Map<string, SqlValue>) : SqlValue =
        match expr with
        | TableAccess tableAliasHint t -> 
            SelectClauseSqlValue(
                { FromClause = [{ Table = TableFrom(t); JoinType = None; Condition = None }]; 
                  WhereClause = None; VirtualTableSqlValue = LogicalTableSqlValue t; OrderBy = [] })
        | Select(input, selector) ->
            let inputAliasHint = Some(selector.Parameters.[0].Name)
            let inputselectclause = processExpressionImplAsSelect(input, tables, inputAliasHint, colPropMap)
            let lambdatable = processExpressionImpl(selector.Body, tables.Add(selector.Parameters.[0], inputselectclause.VirtualTableSqlValue), inputAliasHint, colPropMap)
            SelectClauseSqlValue({ inputselectclause with VirtualTableSqlValue = lambdatable })
        | Where(input, predicate) ->
            let inputAliasHint = Some(predicate.Parameters.[0].Name)
            let inputselectclause = processExpressionImplAsSelect(input, tables, inputAliasHint, colPropMap)
            let predicateScalar = processExpressionImpl(predicate.Body, tables.Add(predicate.Parameters.[0], inputselectclause.VirtualTableSqlValue), inputAliasHint, colPropMap)
            let newwhere =
                match inputselectclause.WhereClause with
                | Some(where) -> BinarySqlValue(AndAlso, where, predicateScalar)
                | None -> predicateScalar
            SelectClauseSqlValue({ inputselectclause with WhereClause = Some(newwhere); VirtualTableSqlValue = inputselectclause.VirtualTableSqlValue })
        | OrderBy(input, keySelector, direction) -> 
            let inputAliasHint = Some(keySelector.Parameters.[0].Name)
            let inputselectclause = processExpressionImplAsSelect(input, tables, inputAliasHint, colPropMap)
            let keyScalar = processExpressionImpl(keySelector.Body, tables.Add(keySelector.Parameters.[0], inputselectclause.VirtualTableSqlValue), inputAliasHint, colPropMap)
            SelectClauseSqlValue({ inputselectclause with OrderBy = (keyScalar, direction) :: inputselectclause.OrderBy })
        | SelectMany(input, collSelector, resultSelector) ->
            let newhint = Some(collSelector.Parameters.[0].Name)
            let inputselectclause = processExpressionImplAsSelect(input, tables, newhint, colPropMap)

            let jointype, collExpr =
                match collSelector.Body with
                | DefaultIfEmpty(collExpr) -> JoinType.LeftOuter, collExpr
                | collExpr -> JoinType.Inner, collExpr
            let collSelectClause = 
                let tablesForJoin = tables.Add(collSelector.Parameters.[0], inputselectclause.VirtualTableSqlValue)
                processExpressionImplAsSelect(collExpr, tablesForJoin, None, colPropMap)

            let mergedSelect = mergeSelect(inputselectclause, collSelectClause, Some(jointype), None)

            let tAfterSelect = match resultSelector with 
                                | Some(sel) -> 
                                    let joinedToTable =
                                        let lastFromClause = List.hd collSelectClause.FromClause
                                        match lastFromClause.Table with
                                        | TableFrom(logicalTable) -> logicalTable
                                        | SelectClauseFrom(_) -> failwith "Can't have joined to a select clause."
                                    let tablesForResultSelector = tables.Add(sel.Parameters.[0], inputselectclause.VirtualTableSqlValue).Add(sel.Parameters.[1], LogicalTableSqlValue joinedToTable)
                                    processExpressionImpl(sel.Body, tablesForResultSelector, None, colPropMap)
                                | None -> inputselectclause.VirtualTableSqlValue

            SelectClauseSqlValue({ mergedSelect with VirtualTableSqlValue = tAfterSelect })

        | EqualityJoin(leftInput, rightInput, leftSelector, rightSelector, resultSelector) ->
            let leftSelectClause = 
                let leftHint = Some(leftSelector.Parameters.[0].Name)
                processExpressionImplAsSelect(leftInput, tables, leftHint, colPropMap)
            
            let rightSelectClause = 
                let rightHint = Some(rightSelector.Parameters.[0].Name)
                processExpressionImplAsSelect(rightInput, tables, rightHint, colPropMap)

            let condition =
                let getOrderedJoinValues vt (keySelector : LambdaExpression) =
                    let expr = processExpressionImpl(keySelector.Body, tables.Add(keySelector.Parameters.[0], vt), Some(keySelector.Parameters.[0].Name), colPropMap)
                    match expr with
                    | VirtualTableSqlValue(colmap) -> colmap |> Seq.map (fun kvp -> kvp.Value) |> Seq.to_list
                    | _ -> [expr]
                let leftCols = getOrderedJoinValues leftSelectClause.VirtualTableSqlValue leftSelector
                let rightCols = getOrderedJoinValues rightSelectClause.VirtualTableSqlValue rightSelector
                List.map2 (fun leftSqlValue rightSqlValue -> BinarySqlValue(BinaryOperator.Equal, leftSqlValue, rightSqlValue)) leftCols rightCols
                |> List.reduce_left (fun l r -> BinarySqlValue(BinaryOperator.AndAlso, l, r))

            let combinedSelectClause = mergeSelect(leftSelectClause, rightSelectClause, Some(JoinType.Inner), Some(condition))

            // Is this correct?: To keep only the vtable from the result selector? Should we keep more?
            let resultVirtualTable = 
                let resultTableMap = tables.Add(resultSelector.Parameters.[0], leftSelectClause.VirtualTableSqlValue).Add(resultSelector.Parameters.[1], rightSelectClause.VirtualTableSqlValue)
                processExpressionImpl(resultSelector.Body, resultTableMap, None, colPropMap)

            SelectClauseSqlValue({ combinedSelectClause with VirtualTableSqlValue = resultVirtualTable })

        | :? ParameterExpression as p -> tables.[p]
        | :? BinaryExpression as binexp ->
            let binop = 
                match binexp.NodeType with 
                | ExpressionType.Add -> Add | ExpressionType.Subtract -> Subtract | ExpressionType.GreaterThan -> GreaterThan | ExpressionType.GreaterThanOrEqual -> GreaterThanOrEqual 
                | ExpressionType.AndAlso -> AndAlso | ExpressionType.OrElse -> OrElse | ExpressionType.LessThan -> LessThan | ExpressionType.LessThanOrEqual -> LessThanOrEqual | ExpressionType.Equal -> Equal
                | _ -> failwith ("Bad binop: " ^ binexp.NodeType.ToString())
            BinarySqlValue(binop, processExpressionImpl(binexp.Left, tables, tableAliasHint, colPropMap), processExpressionImpl(binexp.Right, tables, tableAliasHint, colPropMap))
        | :? UnaryExpression as unary when unary.Method = null && unary.IsLiftedToNull -> processExpressionImpl(unary.Operand, tables, tableAliasHint, colPropMap)
        | :? MemberExpression as ma ->
            let inputinstancevalue =
                if (box ma.Expression) <> null then processExpressionImpl(ma.Expression, tables, tableAliasHint, colPropMap)
                else ConstSqlValue(box None, None) // e.g. table properties and DateTime.Now.
            match inputinstancevalue with
            | VirtualTableSqlValue(vt) -> vt.[(ma.Member :?> System.Reflection.PropertyInfo).GetGetMethod()]
            | LogicalTableSqlValue(_) -> ColumnAccessSqlValue(inputinstancevalue, (ma.Member :?> PropertyInfo))
            | ConstSqlValue(instance, _) -> 
                let v, name, preCookedSqlValue = 
                    match ma.Member with
                    | :? FieldInfo as fi -> // Closure field, which may represent a column.
                        match colPropMap.TryFind(fi.Name) with
                        | Some(sqlValue) -> box None, fi.Name, Some(sqlValue)
                        | None -> fi.GetValue(instance), fi.Name, None
                    | :? PropertyInfo as pi -> pi.GetValue(instance, Array.empty), pi.Name, None // Table properties and DateTime.Now.
                    | _ -> failwith ("Member of const, but not field: " ^ instance.GetType().ToString())
                match v, preCookedSqlValue with
                | :? IQueryable as queryable, _ -> processExpressionImpl(queryable.Expression, tables, tableAliasHint, colPropMap)
                | _, Some(sqlValue) -> sqlValue
                | _ -> ConstSqlValue(v, Some(name))
            | v -> failwith ("Member access on non-virtual table or const: " ^ v.ToString() ^ " - Member:" ^ ma.Member.Name)
        | :? ConstantExpression as ce -> ConstSqlValue(ce.Value, None)

        // Lambda
        | :? NewExpression as newExpr ->
            let pairs = List.zip (newExpr.Members |> Seq.map (fun memberinfo -> memberinfo :?> System.Reflection.MethodInfo) |> Seq.to_list) (newExpr.Arguments |> Seq.to_list)
            let foldFunc (statemap : Map<MethodInfo, SqlValue>) (m, v) = statemap.Add(m, processExpressionImpl(v, tables, tableAliasHint, colPropMap))
            let m2 = List.fold_left foldFunc (Map<_,_>.Empty(MethodInfoComparer)) pairs
            VirtualTableSqlValue(m2)
        | Call(_, callExpr) when typeof<IQueryable>.IsAssignableFrom(callExpr.Method.ReturnType) ->
            let argSqlValues = callExpr.Arguments |> Seq.map (fun arg -> processExpressionImpl(arg, tables, tableAliasHint, colPropMap))
            let argPairs = Seq.zip (callExpr.Method.GetParameters()) argSqlValues
            let queryable =
                let makeArgument (param : ParameterInfo, sqlValue) : obj =
                    match sqlValue with
                    | ConstSqlValue(v, _) -> v
                    | _ when param.ParameterType.IsValueType -> System.Activator.CreateInstance(param.ParameterType)
                    | _ -> box None
                let argsArray = argPairs |> Seq.map makeArgument |> Seq.to_array
                callExpr.Method.Invoke(None, argsArray) :?> IQueryable
            let newColPropMap =
                let makeColPairs (param : ParameterInfo, sqlValue) : (string * SqlValue) option = 
                    match sqlValue with
                    | ConstSqlValue(_, _) -> None
                    | ColumnAccessSqlValue(_, _) -> Some(param.Name, sqlValue)
                    | _ -> failwith ("Can only parameterize views with constants and column properties.")
                let emptyColPropMap = Map<string, SqlValue>.Empty(System.StringComparer.Ordinal)
                Seq.choose makeColPairs argPairs 
                |> Seq.fold (fun (colPropMap2 : Map<string, SqlValue>) (propname, sqlValue) -> colPropMap2.Add(propname, sqlValue)) emptyColPropMap

            let select = processExpressionImplAsSelect(queryable.Expression, tables, None, newColPropMap)

            SelectClauseSqlValue select

        | _ -> failwith ("argh12: " ^ expr.NodeType.ToString() ^ ": " ^ expr.ToString())

    let s = processExpressionImpl(expr, Map<_,_>.Empty(ParameterExpressionComparer), None, Map<string, SqlValue>.Empty(System.StringComparer.Ordinal))
    match s with
    | SelectClauseSqlValue(sel) -> sel
    | _ -> failwith "not select??"


/// A simple, immutable map which does not require an ordering of it's keys.
type SimpleMap<'key, 'value> (items : ('key * 'value) list) =
    static member Empty() = new SimpleMap<'key,'value>([])
    member this.ContainsKey(key) = List.mem_assoc key items
    member this.ContainsValue(value) = List.exists (fun (k, v) -> v = value) items
    member this.Add(key, value) =
        let newitems = List.filter (fun (k, v) -> k <> key) items
        new SimpleMap<_,_>((key, value) :: newitems)
    member this.Item with get(key) = List.assoc key items
    member this.Count = List.length items
    
    interface IEnumerable<KeyValuePair<'key, 'value>> with
        member this.GetEnumerator() = 
            let seq = items |> Seq.of_list |> Seq.map (fun (k, v) -> new KeyValuePair<_,_>(k, v))
            seq.GetEnumerator()
    
    interface System.Collections.IEnumerable with
        member this.GetEnumerator() = (this :> IEnumerable<_>).GetEnumerator() :> System.Collections.IEnumerator

// Bind variables.

let rec internal FindBindVariablesInSqlValue (v : SqlValue) (binds : SimpleMap<obj, string>) : SqlValue * SimpleMap<obj, string> =
    match v with
    | ConstSqlValue(c, nameSuggestion) ->
        let name = 
            if binds.ContainsKey(c) then binds.[c] 
            else if nameSuggestion.IsSome && binds.ContainsValue(nameSuggestion.Value) = false then nameSuggestion.Value
            else "p" ^ binds.Count.ToString()
        BindVariable(name), binds.Add(c, name)
    | BindVariable(_) -> v, binds
    | BinarySqlValue(op, l, r) ->
        let l2, b1 = FindBindVariablesInSqlValue l binds
        let r2, b2 = FindBindVariablesInSqlValue r b1
        BinarySqlValue(op, l2, r2), b2
    | ColumnAccessSqlValue(sv, colname) -> 
        let sv2, b2 = FindBindVariablesInSqlValue sv binds
        ColumnAccessSqlValue(sv2, colname), b2
    | LogicalTableSqlValue(_) -> v, binds
    | VirtualTableSqlValue(_) -> failwith "virtual table??"
    | SelectClauseSqlValue(_) -> raise <| new System.NotSupportedException("select clause")

and internal FindBindVariablesInFromClause (from : JoinClause list) (binds : SimpleMap<obj, string>) : JoinClause list * SimpleMap<obj, string> =
    match from with 
    | [] -> from, binds
    | join :: jointail ->
        let newtail, b2 = FindBindVariablesInFromClause jointail binds
        let newjoin, b3 =
            match join.Table with
            | TableFrom(_) -> join.Table, b2
            | SelectClauseFrom(selectclause) -> 
                let newselectclause, b2_2 = FindBindVariablesInSelectClause selectclause b2
                SelectClauseFrom(newselectclause), b2_2
        let newcondition, b4 =
            match join.Condition with
            | Some(condition) ->
                let newcondTmp, b4Tmp = FindBindVariablesInSqlValue condition b3
                Some(newcondTmp), b4Tmp
            | None -> None, b3
        { join with Table = newjoin; Condition = newcondition } :: newtail, b4

and internal FindBindVariablesInSelectClause (select : SelectClause) (binds : SimpleMap<obj, string>) : SelectClause * SimpleMap<obj, string> =
    let newfrom, bindsAfterFrom = FindBindVariablesInFromClause select.FromClause binds
    let (newwhere : SqlValue option), bindsAfterWhere = 
        if select.WhereClause.IsSome then 
            let newwhere, bindsAfterWhere = FindBindVariablesInSqlValue select.WhereClause.Value bindsAfterFrom
            Some(newwhere), bindsAfterWhere
        else None, bindsAfterFrom
    let neworderby, bindsAfterOrderby =
        let rec searchInOrderByPairs orderbypairs binds =
            match orderbypairs with
            | (sv, dir) :: tail ->
                let sv2, b2 = FindBindVariablesInSqlValue sv binds
                let newtail, b3 = searchInOrderByPairs tail b2
                (sv2, dir) :: newtail, b3
            | [] -> [], binds
        searchInOrderByPairs select.OrderBy bindsAfterWhere
    {select with FromClause = newfrom; WhereClause = newwhere; OrderBy = neworderby }, bindsAfterOrderby


// SQL generation.

let internal ObjComparer = new ComparisonComparer<obj>(defaultComparer)



type internal SqlSettings = {
    GetColumnsForSelect :  System.Type * string * string  -> string option;
    SelectStyle : SelectStyle;
}



let internal GetColumnSql(columnPropertyInfo : PropertyInfo, tableAlias : string, includeColumnAliasIfNecessary : bool) =
    let attArray = columnPropertyInfo.GetCustomAttributes(typeof<ColumnAttribute>, false)
    if attArray.Length = 1 then 
        let colAtt = attArray.[0] :?> ColumnAttribute
        let colName = if System.String.IsNullOrEmpty(colAtt.Name) = false then colAtt.Name else columnPropertyInfo.Name
        if System.String.IsNullOrEmpty(colAtt.Expression) = false then
            let expressionString = System.String.Format(colAtt.Expression, tableAlias)
            if includeColumnAliasIfNecessary then expressionString ^ " " ^ colName else expressionString
        else tableAlias ^ "." ^ colName
    else tableAlias ^ "." ^ columnPropertyInfo.Name





let rec internal SqlValueToString(v : SqlValue, tablenames : Map<Expression, string>, settings : SqlSettings) : string * Map<Expression, string> =
    match v with
    | ConstSqlValue(c, _) -> 
        // NB: This is probably open to sql injection...
        let sqlstring = if c.GetType() = typeof<string> then "'" ^ c.ToString().Replace("'", "''") ^ "'" else c.ToString()
        sqlstring, tablenames
    | BindVariable(name) -> ":" ^ name, tablenames
    | BinarySqlValue(op, v1, v2) ->
        let opname = 
            match op with 
                | Add -> "+" | Subtract -> "-" | GreaterThan -> ">" | GreaterThanOrEqual -> ">=" | AndAlso -> "AND" | OrElse -> "OR" 
                | LessThan -> "<" | LessThanOrEqual -> "<=" | Equal -> "="
        let sqlLeft, t2 = SqlValueToString(v1, tablenames, settings)
        let sqlRight, t3 = SqlValueToString(v2, t2, settings)
        (sqlLeft ^ " " ^ opname ^ " " ^ sqlRight), t3
    | ColumnAccessSqlValue(table, colPropertyInfo) -> 
        let tableAlias, t2 = SqlValueToString(table, tablenames, settings)
        let columnSql = GetColumnSql(colPropertyInfo, tableAlias, false)
        columnSql, t2
    | LogicalTableSqlValue(LogicalTable(tableExpression, tableName, tableAliasHint)) ->
        match tablenames.TryFind(tableExpression) with
        | Some(name) -> 
            name, tablenames
        | None -> 
            let tableAlias =
                let rec tryAlias i =
                    let aliasAttempt = if i = 1 then tableAliasHint else tableAliasHint ^ "_" ^ (i.ToString())
                    match tablenames |> Seq.tryfind (fun kvp -> kvp.Value = aliasAttempt)  with
                    | Some(_) -> if i < 20  then tryAlias (i+1) else failwith "wtf: > 20 table instances?"
                    | None -> aliasAttempt
                tryAlias 1
            tableName ^ " " ^ tableAliasHint, tablenames.Add(tableExpression, tableAlias)
    | VirtualTableSqlValue(_) -> failwith "virtual table??"
    | SelectClauseSqlValue(_) -> raise <| new System.NotSupportedException("select clause")

and internal FromClauseToSql(from : JoinClause list, tablenames : Map<Expression, string>, settings : SqlSettings) : string * Map<Expression, string> =
    match from with
    | [] -> "", tablenames
    | item :: itemtail ->
        let previousFromSql, tableNamesAfterTail = FromClauseToSql(itemtail, tablenames, settings)
        let tableSql, tableNamesAfterCurrent = 
            match item.Table with
            | TableFrom(lt) -> SqlValueToString(LogicalTableSqlValue(lt), tableNamesAfterTail, settings)
            | SelectClauseFrom(selectclause) -> 
                let sql, tablenames3_2 = SelectToString(selectclause, tableNamesAfterTail,  { settings with SelectStyle = SelectStyle.OnlyFrom })
                "(" ^ sql ^ ")", tablenames3_2
        match item.JoinType with 
        | Some (jointype) -> 
            let joinword = match jointype with | JoinType.Inner -> "INNER" | JoinType.LeftOuter -> "LEFT"
            let joinConditionSql, tableNamesAfterJoin = SqlValueToString((item.Condition.Value), tableNamesAfterCurrent, settings)
            let sql = previousFromSql ^ "\r\n\t" ^ joinword ^ " JOIN " ^ tableSql ^ " ON " ^ joinConditionSql
            sql, tableNamesAfterJoin
        | None -> tableSql, tableNamesAfterCurrent

and internal SelectToString(select : SelectClause, tablenames : Map<Expression, string>, settings : SqlSettings) : string * Map<Expression, string> =
    let fromsql, tableNamesAfterFrom = FromClauseToSql(select.FromClause, tablenames, settings)
    let wheresql, tableNamesAfterWhere = 
        match select.WhereClause with
        | Some(where) -> 
            let wheresql2, tablenames3_2 = SqlValueToString(where, tableNamesAfterFrom, settings)
            "\r\nWHERE " ^ wheresql2, tablenames3_2
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
    let getClassColNames(expr : Expression, tableAlias : string) =
        let getClassColNamesDefaultImpl() = 
            let requireColumnAttribute = false
            match expr with
            | :? ConstantExpression as c when (typeof<IQueryable>).IsAssignableFrom(c.Type) ->
                let propertyinfos = c.Value.GetType().GetGenericArguments().[0].GetProperties()
                propertyinfos 
                    |> Seq.filter (fun pi -> not(requireColumnAttribute) || pi.IsDefined(typeof<ColumnAttribute>, false) ) 
                    |> Seq.map (fun pi -> GetColumnSql(pi, tableAlias, true))
            | _ -> failwith ("Bad expr: " ^ expr.ToString())
        let tableType = expr.Type.GetGenericArguments().[0]
        match settings.GetColumnsForSelect(tableType, tableAlias, "") with
        | None -> 
            let colnames = getClassColNamesDefaultImpl()
            colnames |> Seq.reduce (fun s1 s2 -> s1 ^ ", " ^ s2)
        | Some(s) -> s
    let getColumnsSql sqlTableVal = 
        match sqlTableVal with
        | VirtualTableSqlValue(map) -> 
            map |> Seq.map (fun kvp -> SqlValueToString(kvp.Value, tableNamesAfterWhere, settings)) 
                |> Seq.map fst
                |> Seq.reduce (fun s1 s2 -> s1 ^ ", " ^ s2)
        | LogicalTableSqlValue(LogicalTable(expr, tableName, tableAlias)) -> getClassColNames(expr, tableAlias)
        | _ -> failwith ("notsup: " ^ sqlTableVal.GetType().ToString())
    let columnsSql = getColumnsSql (select.VirtualTableSqlValue)
    match settings.SelectStyle with
    | ColumnList -> "SELECT " ^ columnsSql ^ "\r\nFROM " ^ fromsql ^ wheresql ^ orderbysql, tableNamesAfterWhere
    | OnlyFrom -> fromsql ^ wheresql ^ orderbysql, tableNamesAfterWhere



