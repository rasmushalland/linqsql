﻿#light

module LinqModule

open System
open System.Linq
open System.Linq.Expressions


type internal SortDirection = Ascending | Descending

module internal LinqPatterns =

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

    let internal (|Join|_|) (expr : Expression) =
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

    let internal (|Union|_|) (expr : Expression) =
        match expr with
        | Call("Union", call) ->
            let inputLeft = call.Arguments.[0]
            let inputRight = call.Arguments.[1]
            Some(false, inputLeft, inputRight)
        | Call("Concat", call) ->
            let inputLeft = call.Arguments.[0]
            let inputRight = call.Arguments.[1]
            Some(true, inputLeft, inputRight)
        | _ -> None


// TODO:
// - Use bind variables instead of immediate values. V
// - LINQ join syntax. v
// - Respect ColumnAttribute and TableAttribute. v
// - Parameterized views. v
// - Generate DELETE. v
// - Generate UPDATE.
// - Custom application handling of method calls, e.g. "String.ToUpper()" -> oracle "upper()". v
// - Some support for executing and retrieving records:
//   - Let the application help. v
//   - Automatic - figure out how to populate classes.
// - Relations via properties. Implement as parameterized views or as SQL in ColumnAttribute?
// - Nested queries.
// - group by.
// - Aggregate functions.
// - "numlist2table".
// - UNION and friends currently cannot be composed.

// Lectio:
// - Use LectioDbAccess. v
// - Use OracleFields. v
// - Use RowReader. v



open System.Reflection
open System.Collections.Generic
open Microsoft.FSharp.Collections.Tagged

open System.Data.Linq.Mapping

type internal ComparisonComparer<'a>(cmp : 'a * 'a -> int) =
    interface Collections.Generic.IComparer<'a> with
        override this.Compare(x, y) = cmp(x, y)
// TODO: handle hash collisions better.
let defaultComparer ((x : 'a), (y : 'a )) =
    match x.GetHashCode() - y.GetHashCode() with
    | 0 when not (Object.ReferenceEquals(x, y)) -> failwith "Hash collision."
    | diff -> diff
let internal ExpressionComparer = new ComparisonComparer<Expression>(defaultComparer)
let internal ParameterExpressionComparer = new ComparisonComparer<ParameterExpression>(defaultComparer)
let internal MethodInfoComparer = new ComparisonComparer<MethodInfo>(defaultComparer)
let internal PropertyInfoComparer = new ComparisonComparer<PropertyInfo>(defaultComparer)

let internal mapWithAccumulator<'a, 'b, 'c>(f : ('a * 'c) -> ('b * 'c), initialState : 'c, s : 'a list ) : 'b list * 'c = 
    let rec ff(itemlist, state) =
        match itemlist with
        | item :: itemTail -> 
            let newTail, newState = ff(itemTail, state)
            let newItem, newState2 = f(item, newState)
            (newItem :: newTail), newState2
        | [] -> [], state
    ff(s, initialState)




open System.Reflection
open System.Linq.Expressions

type internal TableExpressionToken = TableExpressionToken of obj

// Justification for the special SqlValues:
// 
// LogicalTableSqlValue:
// This is a simple table value, and it is the first SqlValue to be created during processing.
// It needs to somehow store a token and an alias.
// After construction of a SelectClause it is hardly needed?
//
// VirtualTable:
// Needed to support the anonymous types which are constructed.
//
// SelectClauseSqlValue:
// This value is actually kept and used. Its VirtualTableSqlValue is used during construction (it actually
// declared as an SqlValue rather than as an SqlValue
// 


type internal JoinType = Inner | LeftOuter | Cross
type internal BinaryOperator = | AndAlso | OrElse | Add | Subtract | GreaterThan | GreaterThanOrEqual | LessThan | LessThanOrEqual | Equal | NotEqual | StringConcat | Other
type internal SqlConstruct = CaseWhen
type internal RowSetSqlConstruct = Union | UnionAll
    /// An atomic expression that usually represents a physical table.
    /// Item type * table name * table instance token * alias hint.
type internal LogicalTable = LogicalTable of Type * string * TableExpressionToken * string
and internal SqlValue =
    /// VirtualTableSqlValue represents the result of a Select call, ie. often an anonymous type.
    /// The map keys are property get methods for generated values.
    | VirtualTableSqlValue of Map<PropertyInfo, SqlValue>
    | LogicalTableSqlValue of LogicalTable
//    | LogicalTableSqlValue of LogicalTable
    /// The string option is a bind variable name suggestion.
    | ConstSqlValue of obj * string option
    | BindVariable of string
    | BinarySqlValue of BinaryOperator * SqlValue * SqlValue
    | SqlConstruct of SqlConstruct * SqlValue list
    | CallSqlValue of string * SqlValue list
    | ColumnAccessSqlValue of SqlValue * PropertyInfo
    | SelectClauseSqlValue of SelectClause
    | TableExpressionSqlValue of SelectClause
and internal TableExpression = {
    Content : TableExpressionContent; 
    Token : TableExpressionToken
    Alias : string; 
    JoinType : JoinType option; 
    Condition : SqlValue option;
}
and internal TableExpressionContent = 
    | LogicalTableContent of LogicalTable
    | SelectClauseContent of SelectClause
and internal SelectClause = { 
    FromClause : TableExpression list; 
    WhereClause : SqlValue option; 
    VirtualTableSqlValue : SqlValue; 
    OrderBy : (SqlValue * SortDirection) list
    Next : (RowSetSqlConstruct * SelectClause) option
}

type internal SelectStyle = ColumnList | OnlyFrom


type internal SqlSettings = {
    GetColumnsForSelect :  Type * string * string  -> string option;
    TranslateCall : MethodCallExpression -> string option
    SelectStyle : SelectStyle;
}


// **************************************************************************************
// **************************************************************************************
// Expression tree processing.
// **************************************************************************************
// **************************************************************************************


let internal GetTableName(tabletype : Type) : string =
    let attArray = tabletype.GetCustomAttributes(typeof<TableAttribute>, false)
    if attArray.Length = 1 then
        let att = attArray.[0] :?> TableAttribute
        if String.IsNullOrEmpty(att.Name) = false then att.Name else tabletype.Name
    else tabletype.Name

let internal (|TableAccess|_|) (alias : string option) (expr : Expression) : LogicalTable option =
    match expr with
         // Første, konstante IQ.
    | :? ConstantExpression when typeof<IQueryable>.IsAssignableFrom(expr.Type) -> 
        let itemType = expr.Type.GetGenericArguments().[0]
        let tableName = GetTableName(itemType)
        Some(LogicalTable(itemType, tableName, TableExpressionToken(expr), alias.Value))
    | _ -> None


let internal ProcessExpression (expr : Expression, settings : SqlSettings) : SelectClause =
    let rec processExpressionImplAsSelect (expr : Expression, tables : Map<ParameterExpression, SqlValue>, tableAliasHint : string option, colPropMap : Map<string, SqlValue>, settings : SqlSettings) : SelectClause =
        let selValue = processExpressionImpl(expr, tables, tableAliasHint, colPropMap, settings)
        match selValue with
        | SelectClauseSqlValue(sel) -> sel
        | _ -> failwith ("not sel, but " ^ (any_to_string selValue)  ^ "??")

    and mergeSelect (owner : SelectClause, toBeMerged : SelectClause, joinTypeHint : JoinType option, joinCondition : SqlValue option, settings : SqlSettings) : SelectClause =
        match 1 with
        | _ when List.is_empty toBeMerged.OrderBy = false -> failwith "Can't have ORDER BY on sub-select."
        | _ ->
            let condition =
                match toBeMerged.WhereClause, joinCondition with
                | Some(where), Some(condition) -> Some(BinarySqlValue(BinaryOperator.AndAlso, where, condition))
                | Some(where), None -> Some(where)
                | None, Some(condition) -> Some(condition)
                | None, None -> None
            let newContent, token, alias =
                match toBeMerged.FromClause with
                | [join] ->
                    match join.Content with
                    | LogicalTableContent(logicalTable) -> LogicalTableContent(logicalTable), join.Token, join.Alias
                    | _ -> failwith "First RowSet not a TableRowSet??"
                | _ -> 
                    let lastTable = List.hd toBeMerged.FromClause
                    SelectClauseContent({toBeMerged with WhereClause = None }), lastTable.Token, lastTable.Alias
            let joinType =
                match joinTypeHint, condition with
                | Some(Cross), Some(cond) -> Some(Inner)
                | _, None -> Some(Cross)
                | _, _ -> joinTypeHint
            let newTableExpression = { Content = newContent; Alias = alias; Token = token; JoinType = joinType; Condition = condition }
            { owner with FromClause = newTableExpression :: owner.FromClause; VirtualTableSqlValue = toBeMerged.VirtualTableSqlValue }

    and processExpressionImpl (expr : Expression, tables : Map<ParameterExpression, SqlValue>, tableAliasHint : string option, colPropMap : Map<string, SqlValue>, settings : SqlSettings) : SqlValue =
        match expr with
        | TableAccess tableAliasHint t -> 
            match t with
                | LogicalTable(itemType, tableName, token, alias) ->
                    SelectClauseSqlValue(
                        { FromClause = [{ Content = LogicalTableContent(t); Token = token; Alias = alias; JoinType = None; Condition = None; }]; 
                          WhereClause = None; VirtualTableSqlValue = LogicalTableSqlValue t; OrderBy = []; Next = None })

        | :? ParameterExpression as p -> tables.[p]
        | :? BinaryExpression as binexp ->
            let binop = 
                match binexp.NodeType with 
                | ExpressionType.Add -> if binexp.Type = typeof<String> then StringConcat else Add 
                | ExpressionType.Subtract -> Subtract | ExpressionType.GreaterThan -> GreaterThan | ExpressionType.GreaterThanOrEqual -> GreaterThanOrEqual 
                | ExpressionType.AndAlso -> AndAlso | ExpressionType.OrElse -> OrElse | ExpressionType.LessThan -> LessThan | ExpressionType.LessThanOrEqual -> LessThanOrEqual 
                | ExpressionType.Equal -> Equal | ExpressionType.NotEqual -> NotEqual | ExpressionType.Coalesce -> Other
                | _ -> failwith ("Bad binop: " ^ binexp.NodeType.ToString())
            let newLeft = processExpressionImpl(binexp.Left, tables, tableAliasHint, colPropMap, settings)
            let newRight = processExpressionImpl(binexp.Right, tables, tableAliasHint, colPropMap, settings)
            match binop with
            | Other ->
                match binexp.NodeType with
                | ExpressionType.Coalesce -> 
                    match newRight with 
                    | CallSqlValue("coalesce", rightArgs) -> CallSqlValue("coalesce", newLeft :: rightArgs)
                    | _ -> CallSqlValue("coalesce", [newLeft; newRight])
                | _ -> failwith <| sprintf "binary huh?: %s" (binexp.NodeType.ToString())
            | _ -> BinarySqlValue(binop, newLeft, newRight)
//        | :? UnaryExpression as unary when unary.Method = null && unary.IsLiftedToNull -> processExpressionImpl(unary.Operand, tables, tableAliasHint, colPropMap, settings)
        | :? UnaryExpression as unary  -> processExpressionImpl(unary.Operand, tables, tableAliasHint, colPropMap, settings)
        | :? MemberExpression as ma ->
            let inputinstancevalue =
                if (box ma.Expression) <> null then processExpressionImpl(ma.Expression, tables, tableAliasHint, colPropMap, settings)
                else ConstSqlValue(box None, None) // e.g. table properties, DateTime.Now and null.
            match inputinstancevalue with
            | VirtualTableSqlValue(vt) -> vt.[ma.Member :?> Reflection.PropertyInfo]
            | LogicalTableSqlValue(_) -> ColumnAccessSqlValue(inputinstancevalue, (ma.Member :?> PropertyInfo))
            | ConstSqlValue(instance, _) -> 
                let v, name, preCookedSqlValue = 
                    match ma.Member with
                    | :? FieldInfo as fi -> // Closure field, which may represent a column passed as an function argument.
                        match colPropMap.TryFind(fi.Name) with
                        | Some(sqlValue) -> box None, fi.Name, Some(sqlValue)
                        | None -> fi.GetValue(instance), fi.Name, None
                    | :? PropertyInfo as pi -> pi.GetValue(instance, Array.empty), pi.Name, None // Table properties and DateTime.Now.
                    | _ -> failwith ("Member of const, but not field: " ^ instance.GetType().ToString())
                match v, preCookedSqlValue with
                | :? IQueryable as queryable, _ -> processExpressionImpl(queryable.Expression, tables, tableAliasHint, colPropMap, settings)
                | _, Some(sqlValue) -> sqlValue
                | _ -> ConstSqlValue(v, Some(name))
            | v -> failwith ("Member access on non-virtual table or const: " ^ v.ToString() ^ " - Member:" ^ ma.Member.Name)
        | :? ConstantExpression as ce -> ConstSqlValue(ce.Value, None)
        | :? NewExpression as newExpr ->
            let aa = newExpr.Members.First().DeclaringType
            let propertyInfos =
                let getpi mi = aa.GetProperties() |> Seq.find (fun pi -> pi.GetGetMethod() = mi)
                (newExpr.Members |> Seq.map (fun memberinfo -> getpi (memberinfo :?> Reflection.MethodInfo)) |> Seq.to_list)
            let pairs = List.zip propertyInfos (newExpr.Arguments |> Seq.to_list)
            let foldFunc (statemap : Map<PropertyInfo, SqlValue>) (m, v) = statemap.Add(m, processExpressionImpl(v, tables, tableAliasHint, colPropMap, settings))
            let m2 = List.fold_left foldFunc (Map<_,_>.Empty(PropertyInfoComparer)) pairs
            VirtualTableSqlValue(m2)
        | :? MemberInitExpression as mi ->
            // Throw away the NewExpression - since there is a MemberInitExpression, the constructur call can reasonably be ignored.
//            let newExprSqlValue = processExpressionImpl(mi, tables, tableAliasHint, colPropMap, settings)
            let memberAssBindings = mi.Bindings |> Seq.map (fun binding -> binding :?> MemberAssignment) |> Seq.to_list
            let foldFunc (statemap : Map<PropertyInfo, SqlValue>) (ma : MemberAssignment) = 
                statemap.Add((ma.Member :?> PropertyInfo), processExpressionImpl(ma.Expression, tables, tableAliasHint, colPropMap, settings))
            let m2 = List.fold_left foldFunc (Map<_,_>.Empty(PropertyInfoComparer)) memberAssBindings
            VirtualTableSqlValue(m2)
        | :? ConditionalExpression as ce ->
            let test = processExpressionImpl(ce.Test, tables, tableAliasHint, colPropMap, settings)
            let ifTrue = processExpressionImpl(ce.IfTrue, tables, tableAliasHint, colPropMap, settings)
            let ifFalse = processExpressionImpl(ce.IfFalse, tables, tableAliasHint, colPropMap, settings)
            SqlConstruct(CaseWhen, [test; ifTrue; ifFalse])

        | LinqPatterns.Call(_, _) ->
            match expr with
            | LinqPatterns.Select(input, selector) ->
                let inputAliasHint = Some(selector.Parameters.[0].Name)
                let inputselectclause = processExpressionImplAsSelect(input, tables, inputAliasHint, colPropMap, settings)
                let lambdatable = processExpressionImpl(selector.Body, tables.Add(selector.Parameters.[0], inputselectclause.VirtualTableSqlValue), inputAliasHint, colPropMap, settings)
                SelectClauseSqlValue({ inputselectclause with VirtualTableSqlValue = lambdatable })
            | LinqPatterns.Where(input, predicate) ->
                let inputAliasHint = Some(predicate.Parameters.[0].Name)
                let inputselectclause = processExpressionImplAsSelect(input, tables, inputAliasHint, colPropMap, settings)
                let predicateScalar = processExpressionImpl(predicate.Body, tables.Add(predicate.Parameters.[0], inputselectclause.VirtualTableSqlValue), inputAliasHint, colPropMap, settings)
                let newwhere =
                    match inputselectclause.WhereClause with
                    | Some(where) -> BinarySqlValue(AndAlso, where, predicateScalar)
                    | None -> predicateScalar
                SelectClauseSqlValue({ inputselectclause with WhereClause = Some(newwhere); VirtualTableSqlValue = inputselectclause.VirtualTableSqlValue })
            | LinqPatterns.OrderBy(input, keySelector, direction) -> 
                let inputAliasHint = Some(keySelector.Parameters.[0].Name)
                let inputselectclause = processExpressionImplAsSelect(input, tables, inputAliasHint, colPropMap, settings)
                let keyScalar = processExpressionImpl(keySelector.Body, tables.Add(keySelector.Parameters.[0], inputselectclause.VirtualTableSqlValue), inputAliasHint, colPropMap, settings)
                SelectClauseSqlValue({ inputselectclause with OrderBy = (keyScalar, direction) :: inputselectclause.OrderBy })
            | LinqPatterns.SelectMany(input, collSelector, resultSelector) ->
                let newhint = Some(collSelector.Parameters.[0].Name)
                let inputselectclause = processExpressionImplAsSelect(input, tables, newhint, colPropMap, settings)

                let jointype, collExpr =
                    match collSelector.Body with
                    | LinqPatterns.DefaultIfEmpty(collExpr) -> JoinType.LeftOuter, collExpr
                    | collExpr -> JoinType.Inner, collExpr
                let collSelectClause = 
                    let tablesForJoin = tables.Add(collSelector.Parameters.[0], inputselectclause.VirtualTableSqlValue)
                    let alias =
                        match resultSelector with
                        | Some(lambda) -> Some(lambda.Parameters.[1].Name)
                        | None -> None
                    processExpressionImplAsSelect(collExpr, tablesForJoin, alias, colPropMap, settings)

                let mergedSelect = mergeSelect(inputselectclause, collSelectClause, Some(jointype), None, settings)

                let tAfterSelect = match resultSelector with 
                                    | Some(sel) -> 
                                        let joinedToTable =
                                            let lastFromClause = List.hd collSelectClause.FromClause
                                            match lastFromClause.Content with
                                            | LogicalTableContent(logicalTable) -> logicalTable
                                            | SelectClauseContent(_) -> failwith "Can't have joined to a select clause."
                                        let tablesForResultSelector = tables.Add(sel.Parameters.[0], inputselectclause.VirtualTableSqlValue).Add(sel.Parameters.[1], LogicalTableSqlValue joinedToTable)
                                        processExpressionImpl(sel.Body, tablesForResultSelector, None, colPropMap, settings)
                                    | None -> inputselectclause.VirtualTableSqlValue

//                let tAfterSelect = match resultSelector with 
//                                    | Some(sel) -> 
//                                        let resultTableMap = tables.Add(sel.Parameters.[0], inputselectclause.VirtualTableSqlValue)
//                                        let resultTableMap = tables.Add(sel.Parameters.[1], collSelectClause.VirtualTableSqlValue)
//                                        processExpressionImpl(sel.Body, resultTableMap, None, colPropMap, settings)
//                                    | None -> inputselectclause.VirtualTableSqlValue

                SelectClauseSqlValue({ mergedSelect with VirtualTableSqlValue = tAfterSelect })

            | LinqPatterns.Join(leftInput, rightInput, leftSelector, rightSelector, resultSelector) ->
                let leftSelectClause = 
                    let leftHint = Some(leftSelector.Parameters.[0].Name)
                    processExpressionImplAsSelect(leftInput, tables, leftHint, colPropMap, settings)

                let rightSelectClause = 
                    let rightHint = Some(rightSelector.Parameters.[0].Name)
                    processExpressionImplAsSelect(rightInput, tables, rightHint, colPropMap, settings)

                let condition =
                    let getOrderedJoinValues vt (keySelector : LambdaExpression) =
                        let expr = processExpressionImpl(keySelector.Body, tables.Add(keySelector.Parameters.[0], vt), Some(keySelector.Parameters.[0].Name), colPropMap, settings)
                        match expr with
                        | VirtualTableSqlValue(colmap) -> colmap |> Seq.map (fun kvp -> kvp.Value) |> Seq.to_list
                        | _ -> [expr]
                    let leftCols = getOrderedJoinValues leftSelectClause.VirtualTableSqlValue leftSelector
                    let rightCols = getOrderedJoinValues rightSelectClause.VirtualTableSqlValue rightSelector
                    List.map2 (fun leftSqlValue rightSqlValue -> BinarySqlValue(BinaryOperator.Equal, leftSqlValue, rightSqlValue)) leftCols rightCols
                    |> List.reduce_left (fun l r -> BinarySqlValue(BinaryOperator.AndAlso, l, r))

                let combinedSelectClause = mergeSelect(leftSelectClause, rightSelectClause, Some(JoinType.Inner), Some(condition), settings)

                // Is this correct?: To keep only the vtable from the result selector? Should we keep more?
                let resultVirtualTable = 
                    let resultTableMap = tables.Add(resultSelector.Parameters.[0], leftSelectClause.VirtualTableSqlValue).Add(resultSelector.Parameters.[1], rightSelectClause.VirtualTableSqlValue)
                    processExpressionImpl(resultSelector.Body, resultTableMap, None, colPropMap, settings)

                SelectClauseSqlValue({ combinedSelectClause with VirtualTableSqlValue = resultVirtualTable })

            | LinqPatterns.Union(isUnionAll, leftInput, rightInput) ->
                let l = processExpressionImplAsSelect(leftInput, tables, None, colPropMap, settings)
                let r = processExpressionImplAsSelect(rightInput, tables, None, colPropMap, settings)
                let construct = if isUnionAll then RowSetSqlConstruct.UnionAll else RowSetSqlConstruct.Union
                SelectClauseSqlValue({ l with Next = Some(construct, r) })

            | LinqPatterns.Call(_, callExpr) when typeof<IQueryable>.IsAssignableFrom(callExpr.Method.ReturnType) ->
                let argSqlValues = callExpr.Arguments |> Seq.map (fun arg -> processExpressionImpl(arg, tables, tableAliasHint, colPropMap, settings))
                let argPairs = Seq.zip (callExpr.Method.GetParameters()) argSqlValues
                let queryable =
                    let makeArgument (param : ParameterInfo, sqlValue) : obj =
                        match sqlValue with
                        | ConstSqlValue(v, _) -> v
                        | _ when param.ParameterType.IsValueType -> Activator.CreateInstance(param.ParameterType)
                        | _ -> box None
                    let argsArray = argPairs |> Seq.map makeArgument |> Seq.to_array
                    callExpr.Method.Invoke(None, argsArray) :?> IQueryable
                let newColPropMap =
                    let makeColPairs (param : ParameterInfo, sqlValue) : (string * SqlValue) option = 
                        match sqlValue with
                        | ConstSqlValue(_, _) -> None
                        | ColumnAccessSqlValue(_, _) -> Some(param.Name, sqlValue)
                        | _ -> failwith ("Can only parameterize views with constants and column properties.")
                    let emptyColPropMap = Map<string, SqlValue>.Empty(StringComparer.Ordinal)
                    Seq.choose makeColPairs argPairs 
                    |> Seq.fold (fun (colPropMap2 : Map<string, SqlValue>) (propname, sqlValue) -> colPropMap2.Add(propname, sqlValue)) emptyColPropMap

                let select = processExpressionImplAsSelect(queryable.Expression, tables, None, newColPropMap, settings)

                SelectClauseSqlValue select
            | LinqPatterns.Call(methodName, callExpr) ->
                let methodName = match settings.TranslateCall(callExpr) with | Some(n) -> n | None -> methodName
                let allSqlValues = 
                    let argsSqlValues = callExpr.Arguments |> Seq.map (fun argExpr -> processExpressionImpl(argExpr, tables, None, colPropMap, settings)) |> Seq.to_list
                    match callExpr.Method.IsStatic with
                    | true -> argsSqlValues
                    | false ->
                        let instanceSqlValue = processExpressionImpl(callExpr.Object, tables, None, colPropMap, settings)
                        instanceSqlValue :: argsSqlValues
                CallSqlValue(methodName, allSqlValues)
            | _ -> failwith ("Unknown call??: " ^ expr.ToString())

        | _ -> failwith ("argh12: " ^ expr.NodeType.ToString() ^ ": " ^ expr.ToString())

    let s = processExpressionImpl(expr, Map<_,_>.Empty(ParameterExpressionComparer), None, Map<string, SqlValue>.Empty(StringComparer.Ordinal), settings)
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
    
    interface Collections.IEnumerable with
        member this.GetEnumerator() = (this :> IEnumerable<_>).GetEnumerator() :> Collections.IEnumerator


// **************************************************************************************
// **************************************************************************************
// Bind variables.
// **************************************************************************************
// **************************************************************************************


let rec internal FindBindVariablesInSqlValue (v : SqlValue, binds : SimpleMap<obj, string>) : SqlValue * SimpleMap<obj, string> =
    match v with
    | ConstSqlValue(c, _) when (box c) = null -> v, binds
    | ConstSqlValue(c, nameSuggestion) ->
        let name = 
            if binds.ContainsKey(c) then binds.[c] 
            else if nameSuggestion.IsSome && binds.ContainsValue(nameSuggestion.Value) = false then nameSuggestion.Value
            else "p" ^ binds.Count.ToString()
        BindVariable(name), binds.Add(c, name)
    | BindVariable(_) -> v, binds
    | CallSqlValue(functionName, argsSqlValues) ->
        let newArgs, newBinds = mapWithAccumulator((fun (arg, binds) -> FindBindVariablesInSqlValue(arg, binds)), binds, argsSqlValues)
        CallSqlValue(functionName, newArgs), newBinds
    | BinarySqlValue(op, l, r) ->
        let l2, b1 = FindBindVariablesInSqlValue(l, binds)
        let r2, b2 = FindBindVariablesInSqlValue(r, b1)
        BinarySqlValue(op, l2, r2), b2
    | SqlConstruct(construct, argsSqlValues) ->
        let newArgs, newBinds = mapWithAccumulator((fun (arg, binds) -> FindBindVariablesInSqlValue(arg, binds)), binds, argsSqlValues)
        SqlConstruct(construct, newArgs), newBinds
    | ColumnAccessSqlValue(sv, colname) -> 
        let sv2, b2 = FindBindVariablesInSqlValue(sv, binds)
        ColumnAccessSqlValue(sv2, colname), b2
    | LogicalTableSqlValue(_) -> v, binds
    | VirtualTableSqlValue(_) -> failwith "virtual table??"
    | SelectClauseSqlValue(_) -> failwith "select clause??"

and internal FindBindVariablesInFromClause (from : TableExpression list) (binds : SimpleMap<obj, string>) : TableExpression list * SimpleMap<obj, string> =
    match from with 
    | [] -> from, binds
    | join :: jointail ->
        let newtail, b2 = FindBindVariablesInFromClause jointail binds
        let newjoin, b3 =
            match join.Content with
            | LogicalTableContent(_) -> join.Content, b2
            | SelectClauseContent(selectclause) -> 
                let newselectclause, b2_2 = FindBindVariablesInSelectClause(selectclause, b2)
                SelectClauseContent(newselectclause), b2_2
        let newcondition, b4 =
            match join.Condition with
            | Some(condition) ->
                let newcondTmp, b4Tmp = FindBindVariablesInSqlValue(condition, b3)
                Some(newcondTmp), b4Tmp
            | None -> None, b3
        { join with Content = newjoin; Condition = newcondition } :: newtail, b4

and internal FindBindVariablesInSelectClause (select : SelectClause, binds : SimpleMap<obj, string>) : SelectClause * SimpleMap<obj, string> =
    let newfrom, bindsAfterFrom = FindBindVariablesInFromClause select.FromClause binds
    let (newwhere : SqlValue option), bindsAfterWhere = 
        if select.WhereClause.IsSome then 
            let newwhere, bindsAfterWhere = FindBindVariablesInSqlValue(select.WhereClause.Value, bindsAfterFrom)
            Some(newwhere), bindsAfterWhere
        else None, bindsAfterFrom
    let neworderby, bindsAfterOrderby =
        let rec searchInOrderByPairs orderbypairs binds =
            match orderbypairs with
            | (sv, dir) :: tail ->
                let sv2, b2 = FindBindVariablesInSqlValue(sv, binds)
                let newtail, b3 = searchInOrderByPairs tail b2
                (sv2, dir) :: newtail, b3
            | [] -> [], binds
        searchInOrderByPairs select.OrderBy bindsAfterWhere
    let newNext, binds =
        match select.Next with
        | None -> None, bindsAfterOrderby
        | Some(construct, nextSelect) -> 
            let adf, binds = FindBindVariablesInSelectClause(nextSelect, bindsAfterOrderby)
            Some(construct, adf), binds
    {select with FromClause = newfrom; WhereClause = newwhere; OrderBy = neworderby; Next = newNext; }, bindsAfterOrderby


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


