#light

namespace LinqTranslation

open System
open System.Linq
open System.Linq.Expressions
open System.Collections.Generic
open Microsoft.FSharp.Collections.Tagged

open SqlGeneration



type LinqApplicationHelper() =
    abstract member GetColumnsForSelect : 
        tableType : Type * 
        tableAlias : string *
        columnNamePrefix : string 
            -> string
    default this.GetColumnsForSelect(tabletype, tableAlias, columnNamePrefix) =
        null

    abstract member TranslateCall : MethodCallExpression -> string
    default this.TranslateCall(call) = null
        

module internal LLL =
    open LinqModule

    let internal createSettings(helper) : SqlSettings =
        let helper = match box helper with | null -> new LinqApplicationHelper() | _  -> helper
        let getcolsforselect(tableType, tableAlias, columnNamePrefix) =
            match helper.GetColumnsForSelect(tableType, tableAlias, columnNamePrefix) with | null -> None | s -> Some(s)
        let translatecall callExpr = match helper.TranslateCall(callExpr) with | v when String.IsNullOrEmpty(v) -> None | c -> Some(c)
        {
            GetColumnsForSelect = getcolsforselect;
            TranslateCall = translatecall;
            SelectStyle = LinqModule.SelectStyle.ColumnList; 
        }

    let makeBindsDict(kvpEnumerable : IEnumerable<KeyValuePair<obj, string>>) = 
        let binds = new Dictionary<string, obj>()
        for kvp in kvpEnumerable do
            binds.Add(kvp.Value, kvp.Key)
        binds


type LinqProvider =

    val internal sql : string
    val internal binds : Dictionary<string, obj>

    private new(sqlArg, binds) =
        { sql = sqlArg; binds = binds }

    static member CreateSelect(expr : Expression, helper : LinqApplicationHelper) : LinqProvider =
        let settings = LLL.createSettings helper
        let tmpSelectClause = Some(LinqModule.ProcessExpression(expr, settings))
        let sel, tmpBinds = LinqModule.FindBindVariablesInSelectClause(tmpSelectClause.Value, (LinqModule.SimpleMap.Empty()))
        let sql = SqlGeneration.SelectToString(sel, settings)
        new LinqProvider(sql, LLL.makeBindsDict tmpBinds)

    static member CreateDelete(expr : Expression, helper : LinqApplicationHelper) : LinqProvider =
        let settings = LLL.createSettings helper
        let tmpSelectClause = Some(LinqModule.ProcessExpression(expr, settings))
        let sel, tmpBinds = LinqModule.FindBindVariablesInSelectClause(tmpSelectClause.Value, (LinqModule.SimpleMap.Empty()))
        let sql =  SqlGeneration.DeleteToString(sel, (Map<_,_>.Empty(SqlGeneration.TableExpressionTokenComparer)), settings)
        new LinqProvider(sql, LLL.makeBindsDict tmpBinds)

    static member CreateUpdate(expr : Expression, helper : LinqApplicationHelper) : LinqProvider =
        let settings = LLL.createSettings helper
        let tmpSelectClause = Some(LinqModule.ProcessExpression(expr, settings))
        let sel, tmpBinds = LinqModule.FindBindVariablesInSelectClause(tmpSelectClause.Value, (LinqModule.SimpleMap.Empty()))
        let sql =  SqlGeneration.UpdateToString(sel, (Map<_,_>.Empty(SqlGeneration.TableExpressionTokenComparer)), settings)
        new LinqProvider(sql, LLL.makeBindsDict tmpBinds)

    member this.Sql : string = this.sql
    member this.Binds : Dictionary<string, obj> = this.binds

