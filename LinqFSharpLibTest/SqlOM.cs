using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Linq.Mapping;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using NUnit.Framework;

namespace LinqTestConsoleApp.SqlOM
{
	[TestFixture]
	public class SqlOMTest
	{
		[Test]
		public void TestSelectStar()
		{
			IQueryable q =
					from o in DBX.Orders
					select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestSelectStarWhere()
		{
			IQueryable q =
				from o in DBX.Orders
				where o.ShipVia > 1980
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestConvertWithImplicitCast()
		{
			var q =
				from o in DBX.Orders
				let id = 10248
				where o.OrderID == id
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestSelectStarWhere2()
		{
			IQueryable q =
				from o in DBX.Orders
				where o.ShipVia > 1980 && o.ShipVia < 1990
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithInnerJoin()
		{
			IQueryable q =
					from o in DBX.Orders
					from p in DBX.Product.Where(p => p.ProductName != o.ShipName)
					select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithInnerJoinDouble()
		{
			IQueryable q =
					from o in DBX.Orders
					from p1 in DBX.Product.Where(p => p.ProductName != o.ShipName)
					from p in DBX.Product.Where(he2 => he2.ID > p1.ID)
					select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithOrderBy()
		{
			IQueryable q =
					from o in DBX.Orders
					orderby o.Title
					select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithOrderByDescending()
		{
			IQueryable q =
					from o in DBX.Orders
					orderby o.Title descending
					select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithOrderByThenBy()
		{
			IQueryable q =
					from o in DBX.Orders
					orderby o.Title, o.OrderID
					select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithLeftOuterJoin()
		{
			IQueryable q =
					from o in DBX.Orders
					from p in DBX.Product.Where(p => p.ProductName != o.ShipName).DefaultIfEmpty()
					select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithLeftOuterJoinAndWhere()
		{
			IQueryable q =
				from o in DBX.Orders
				from p in DBX.Product.Where(p => p.ProductName != o.ShipName).DefaultIfEmpty()
				where p.ID == 0 || p.ID > 1000
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithJoinSyntax_OneColumn()
		{
			IQueryable q =
				from o in DBX.Orders
				join od in DBX.OrderDetails on o.OrderID equals od.OrderID
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithJoinSyntax_AnonymousType()
		{
			IQueryable q =
				from o in DBX.Orders
				join od in DBX.OrderDetails on new { o.OrderID } equals new { od.OrderID }
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithJoinSyntax_AnonymousType2()
		{
			IQueryable q =
				from o in DBX.Orders
				join od in DBX.OrderDetails on new { o.OrderID, Name = o.Title } equals new { od.OrderID, Name = "charles" }
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithViewJoinSelectMany()
		{
			IQueryable q =
				from o in DBX.Orders
				from od in DBX.GetOrderDetails(o.OrderID)
				select od;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithViewJoinSelectMany_ComplexView()
		{
			IQueryable q =
				from o in DBX.Orders
				from p in DBX.GetProductsFromOrder(o.OrderID)
				select p;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithViewJoin()
		{
			IQueryable q =
				from orderDetail in DBX.OrderDetails
				join p in DBX.GetProductsWithStock(10) on orderDetail.ProductID equals p.ID
				select p;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithViewFromJoin()
		{
			IQueryable q =
				from orderDetail in DBX.OrderDetails
				from p in DBX.GetProductsWithStock(20).Where(product => orderDetail.ProductID == product.ID)
				select p;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithTablePropertyPredicate()
		{
			IQueryable q =
				from o in DBX.OrdersWithLargeEmployeeID
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithTablePropertyPredicateOnJoinedTable()
		{
			IQueryable q =
				from o in DBX.Orders
				join od in DBX.OrderDetailsWithLargeEmployeeID on o.OrderID equals od.OrderID
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithTablePropertyPredicateOnJoinedTable_SelectMany()
		{
			IQueryable q =
				from o in DBX.Orders
				from od in DBX.OrderDetailsWithLargeEmployeeID.Where(she => o.OrderID == she.OrderID)
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithLet()
		{
			IQueryable q =
				from o in DBX.Orders
				let xx = o.OrderID + 10
				where o.ShipVia > 50 && xx > 100
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestQuerySimpleWhere()
		{
			ProcessQuery((new Orders[0]).AsQueryable().Where(s => s.OrderID == 123));
		}

		[Test]
		public void TestFunction_ToUpper()
		{
			var q =
				from s in DBX.Orders
				where s.Title.ToUpper() == "Hans"
				select s;
			ProcessQuery(q);
		}

		[Test]
		public void TestCrossJoin()
		{
			var q =
				from o1 in DBX.Orders
				from o2 in DBX.Orders
				select o2;
			ProcessQuery(q);
		}

		[Test]
		public void TestCrossJoin_TurnedToInnerJoin()
		{
			var q =
				from o1 in DBX.Orders
				from o2 in DBX.OrdersWithLargeEmployeeID
				select o2;
			ProcessQuery(q);
		}

		[Test]
		public void TestUnion()
		{
			var q =
				(from o in DBX.Orders
				where o.OrderID < 100
				select o)
				.Union(
				(from o in DBX.Orders
				 where o.OrderID > 101
				 select o)
				);
			ProcessQuery(q);
		}

		[Test]
		public void TestUnion_Alias()
		{
			var q =
				from o in 
				(from o in DBX.Orders
				where o.OrderID < 100
				select o)
				.Union(
				(from o2 in DBX.Orders
				 where o2.OrderID > 101
				 select o2)
				)
				where o.OrderID == 100
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestUnion_Multiple()
		{
			var q =
				(from o in DBX.Orders
				where o.OrderID < 100
				select o)
				.Union(
				(from o in DBX.Orders
				 where o.OrderID > 100
				 select o)
				)
				.Union(
				(from o in DBX.Orders
				 where o.OrderID == 100
				 select o)
				);
			ProcessQuery(q);
		}

		[Test]
		public void TestUnionAll()
		{
			var q =
				(from o in DBX.Orders
				where o.OrderID == 100
				select o)
				.Concat(
				(from o in DBX.Orders
				 where o.OrderID == 101
				 select o)
				);
			ProcessQuery(q);
		}

		[Test]
		public void TestEnumerableTable()
		{
			var ids = new[] {100, 200, 300};
			var q =
				from id in ids.AsQueryable()
				join o in DBX.Orders on id equals o.OrderID
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestEnumerableTable2()
		{
			var ids = new List<long> { 100, 200, 300 };
			var q =
				from o in DBX.Orders
				join id in ids on o.OrderID equals id
				select o;
			ProcessQuery(q);
		}

		[Test(Description = "Should generate \"WHERE ... IN (...)\".")]
		public void TestEnumerableTable3()
		{
			var ids = new List<long> { 100, 200, 300 };
			var q =
				from o in DBX.Orders
				where ids.Contains(o.OrderID)
				select o;
			ProcessQuery(q);
		}

		[Test]
		public void TestUpdate()
		{
			var q =
				from o in DBX.Orders
				where o.OrderID == 123
				select new Orders { ShipName = "New shipname" };
			ProcessUpdate(q);
		}

		[Test]
		public void TestUpdateWithColumnValueUse()
		{
			var q =
				from o in DBX.Orders
				where o.OrderID == 123
				select new Orders { ShipName = o.Title + o.OrderID };
			ProcessUpdate(q);
		}

		private void ProcessUpdate<T>(IQueryable<T> q)
		{
			var lp = LinqTranslation.LinqProvider.CreateUpdate(q.Expression, null);
			Console.WriteLine("SQL: " + lp.Sql);
			if (lp.Binds.Any())
				Console.WriteLine("Binds: " + string.Join(", ", lp.Binds.Select(kvp => kvp.Key + ": " + kvp.Value).ToArray()));
		}


		private static void ProcessQuery(IQueryable q)
		{
			var lp = LinqTranslation.LinqProvider.CreateSelect(q.Expression, null);
			Console.WriteLine("SQL: " + lp.Sql);
			if (lp.Binds.Any())
				Console.WriteLine("Binds: " + string.Join(", ", lp.Binds.Select(kvp => kvp.Key + ": " + kvp.Value).ToArray()));
		}
	}


	[Table(Name = "orders")]
	class Orders
	{
		public int OrderID { get; private set; }
		public int EmployeeID { get; private set; }

		[Column(Expression = "'Title: ' + cast({0}.requireddate as varchar(50))")]
		public string Title { get; set; }
		public int ShipVia { get; set; }
		public string ShipName { get; set; }

		public int? SecondaryID { get; internal set; }
	}

	class Products
	{
		public long ID { get; private set; }
		public string ProductName { get; private set; }
		public int UnitsOnOrder { get; private set; }
		public int UnitsInStock { get; private set; }
	}

	[Table(Name = "Order Details")]
	class OrderDetail
	{
		public int OrderID { get; private set; }
		public int ProductID { get; private set; }
	}

	static class DBX
	{
		public static IQueryable<OrderDetail> OrderDetails
		{
			get { return (new OrderDetail[0]).AsQueryable(); }
		}
		public static IQueryable<Orders> Orders
		{
			get { return (new Orders[0]).AsQueryable(); }
		}

		public static IQueryable<Orders> OrdersWithLargeEmployeeID
		{
			get { return (new Orders[0]).AsQueryable().Where(order => order.EmployeeID > 1000); }
		}
		public static IQueryable<OrderDetail> OrderDetailsWithLargeEmployeeID
		{
			get { return from orderDetail in OrderDetails where orderDetail.OrderID > 1001 select orderDetail; }
		}

		public static IQueryable<Products> Product
		{
			get { return (new Products[0]).AsQueryable(); }
		}

		public static IQueryable<OrderDetail> GetOrderDetails(long studentID)
		{
			return
				from orderDetail in OrderDetails
				where orderDetail.OrderID == studentID
				select orderDetail;
		}

		public static IQueryable<Products> GetProductsWithStock(int minInStock)
		{
			return
				from p in Product
				where p.UnitsInStock >= minInStock
				select p;
		}

		public static IQueryable<Products> GetProductsFromOrder(long studentID)
		{
			return
				from orderDetail in OrderDetails
				where orderDetail.OrderID == studentID
				join p in Product on orderDetail.ProductID equals p.ID
				select p;
		}
	}
}
