#light

namespace LinqTranslation

open System.Linq
open System.Linq.Expressions
open System.Collections.Generic
open Microsoft.FSharp.Collections.Tagged




type LinqApplicationHelper() =
    abstract member GetColumnsForSelect : 
        tableType : System.Type * 
        tableAlias : string *
        columnNamePrefix : string 
            -> string
    default this.GetColumnsForSelect(tabletype, tableAlias, columnNamePrefix) =
        null

type LinqProvider =

    val sql : string
//    val selectClause : SelectClause option
    val binds : Dictionary<string, obj>
    
    private new(sqlArg, bindsArg) =
        { sql = sqlArg; binds = bindsArg }

    static member CreateSelect(expr : Expression) : LinqProvider =
        let tmpSelectClause = Some(LinqModule.ProcessExpression expr)
        let sel, tmpBinds = LinqModule.FindBindVariablesInSelectClause tmpSelectClause.Value (LinqModule.SimpleMap.Empty())
        let sql, _ = LinqModule.SelectToString(sel, (Map<_,_>.Empty(LinqModule.ExpressionComparer)), LinqModule.SelectStyle.ColumnList)
        let binds = new Dictionary<string, obj>()
        for kvp in tmpBinds do
            binds.Add(kvp.Value, kvp.Key)
        new LinqProvider(sql, binds)

    member this.Sql : string = this.sql
    member this.Binds : Dictionary<string, obj> = this.binds

