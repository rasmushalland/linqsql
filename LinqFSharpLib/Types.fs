#light

module LinqTypes

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
open Microsoft.FSharp.Collections.Tagged

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
