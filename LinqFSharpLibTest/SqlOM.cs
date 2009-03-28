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
					from s1 in DBX.PurchaseOrder
					select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestSelectStarWhere()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				where s1.SomeInteger > 1980
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestConvertWithImplicitCast()
		{
			var q =
				from s in DBX.PurchaseOrder
				let id = 123
				where s.ID == id
				select s;
			ProcessQuery(q);
		}

		[Test]
		public void TestSelectStarWhere2()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				where s1.SomeInteger > 1980 && s1.SomeInteger < 1990
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithInnerJoin()
		{
			IQueryable q =
					from s1 in DBX.PurchaseOrder
					from he in DBX.Product.Where(he => he.FirstManufacturedYear > s1.SecondaryID)
					select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithInnerJoinDouble()
		{
			IQueryable q =
					from s1 in DBX.PurchaseOrder
					from he1 in DBX.Product.Where(he1 => he1.FirstManufacturedYear > s1.SecondaryID)
					from he2 in DBX.Product.Where(he2 => he2.ID > he1.ID)
					select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithOrderBy()
		{
			IQueryable q =
					from s1 in DBX.PurchaseOrder
					orderby s1.Title
					select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithOrderByDescending()
		{
			IQueryable q =
					from s1 in DBX.PurchaseOrder
					orderby s1.Title descending
					select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithOrderByThenBy()
		{
			IQueryable q =
					from s1 in DBX.PurchaseOrder
					orderby s1.Title, s1.ID
					select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithLeftOuterJoin()
		{
			IQueryable q =
					from s1 in DBX.PurchaseOrder
					from he in DBX.Product.Where(he => he.FirstManufacturedYear > s1.SecondaryID).DefaultIfEmpty()
					select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithLeftOuterJoinAndWhere()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				from he in DBX.Product.Where(he => he.FirstManufacturedYear > s1.SecondaryID).DefaultIfEmpty()
				where he.ID == 0 || he.ID > 1000
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithJoinSyntax_OneColumn()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				join she in DBX.OrderDetail on s1.ID equals she.OrderID
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithJoinSyntax_AnonymousType()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				join she in DBX.OrderDetail on new { StudentID = s1.ID } equals new { StudentID = she.OrderID }
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithJoinSyntax_AnonymousType2()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				join she in DBX.OrderDetail on new { StudentID = s1.ID, Name = s1.Title } equals new { StudentID = she.OrderID, Name = "charles" }
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithViewJoinSelectMany()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				from she in DBX.GetStudentHoldElementer(s1.ID)
				select she;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithViewJoinSelectMany_ComplexView()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				from he in DBX.GetHoldElementerForStudent(s1.ID)
				select he;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithViewJoin()
		{
			IQueryable q =
				from orderDetail in DBX.OrderDetail
				join p in DBX.GetProductsActive(DateTime.Now) on orderDetail.ProductID equals p.ID
				select p;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithViewFromJoin()
		{
			IQueryable q =
				from orderDetail in DBX.OrderDetail
				from she in DBX.GetProductsActive(DateTime.Now).Where(product => orderDetail.ProductID == product.ID)
				select she;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithTablePropertyPredicate()
		{
			IQueryable q =
				from s1 in DBX.OrdersWithLargeID
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithTablePropertyPredicateOnJoinedTable()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				join she in DBX.OrderDetailsWithLargeOrderID on s1.ID equals she.OrderID
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithTablePropertyPredicateOnJoinedTable_SelectMany()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				from she in DBX.OrderDetailsWithLargeOrderID.Where(she => s1.ID == she.OrderID)
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQueryWithLet()
		{
			IQueryable q =
				from s1 in DBX.PurchaseOrder
				let xx = s1.ID + 10
				where s1.SomeInteger > 50 && xx > 100
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestQuerySimpleWhere()
		{
			ProcessQuery((new PurchaseOrder[0]).AsQueryable().Where(s => s.ID == 123));
		}

		[Test]
		public void TestFunction_ToUpper()
		{
			var q =
				from s in DBX.PurchaseOrder
				where s.Title.ToUpper() == "Hans"
				select s;
			ProcessQuery(q);
		}

		[Test]
		public void TestCrossJoin()
		{
			var q =
				from s1 in DBX.PurchaseOrder
				from s2 in DBX.PurchaseOrder
				select s2;
			ProcessQuery(q);
		}

		[Test]
		public void TestCrossJoin_TurnedToInnerJoin()
		{
			var q =
				from s1 in DBX.PurchaseOrder
				from s2 in DBX.OrdersWithLargeID
				select s2;
			ProcessQuery(q);
		}

		[Test]
		public void TestUnion()
		{
			var q =
				(from s1 in DBX.PurchaseOrder
				where s1.ID < 100
				select s1)
				.Union(
				(from s1 in DBX.PurchaseOrder
				 where s1.ID > 101
				 select s1)
				);
			ProcessQuery(q);
		}

		[Test]
		public void TestUnion_Alias()
		{
			var q =
				from stud in 
				(from s1 in DBX.PurchaseOrder
				where s1.ID < 100
				select s1)
				.Union(
				(from s2 in DBX.PurchaseOrder
				 where s2.ID > 101
				 select s2)
				)
				where stud.ID == 100
				select stud;
			ProcessQuery(q);
		}

		[Test]
		public void TestUnion_Multiple()
		{
			var q =
				(from s1 in DBX.PurchaseOrder
				where s1.ID < 100
				select s1)
				.Union(
				(from s1 in DBX.PurchaseOrder
				 where s1.ID > 100
				 select s1)
				)
				.Union(
				(from s1 in DBX.PurchaseOrder
				 where s1.ID == 100
				 select s1)
				);
			ProcessQuery(q);
		}

		[Test]
		public void TestUnionAll()
		{
			var q =
				(from s1 in DBX.PurchaseOrder
				where s1.ID == 100
				select s1)
				.Concat(
				(from s1 in DBX.PurchaseOrder
				 where s1.ID == 101
				 select s1)
				);
			ProcessQuery(q);
		}

		[Test]
		public void TestEnumerableTable()
		{
			var ids = new[] {100, 200, 300};
			var q =
				from id in ids.AsQueryable()
				join s2 in DBX.PurchaseOrder on id equals s2.ID
				select s2;
			ProcessQuery(q);
		}

		[Test]
		public void TestEnumerableTable2()
		{
			var ids = new[] {100, 200, 300};
			var q =
				from s1 in DBX.PurchaseOrder
				join id in ids on s1.ID equals id
				select s1;
			ProcessQuery(q);
		}

		[Test(Description = "Should generate \"WHERE ... IN (...)\".")]
		public void TestEnumerableTable3()
		{
			var ids = new long[] { 100, 200, 300 };
			var q =
				from s1 in DBX.PurchaseOrder
				where ids.Contains(s1.ID)
				select s1;
			ProcessQuery(q);
		}

		[Test]
		public void TestUpdate()
		{
			var q =
				from s in DBX.PurchaseOrder
				where s.ID == 123
				select new PurchaseOrder { Title = "New student name" };
			ProcessUpdate(q);
		}

		[Test]
		public void TestUpdateWithColumnValueUse()
		{
			var q =
				from s in DBX.PurchaseOrder
				where s.ID == 123
				select new PurchaseOrder { Title = s.Title + s.ID };
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


	[Table(Name = "PurchaseOrder")]
	class PurchaseOrder
	{
		[Column(Name = "id")]
		public long ID { get; private set; }
		public string Title { get; set; }
		[Column(Name = "some_integer")]
		public int SomeInteger { get; set; }

		[Column(Expression = "title || some_integer")]
		public int ComputedTitle { get; set; }

		public int? SecondaryID { get; internal set; }
	}

	class Product
	{
		public long ID { get; private set; }

		public int FirstManufacturedYear { get; private set; }

		public DateTime AvailabilityFrom { get; private set; }
		public DateTime AvailabilityTo { get; private set; }
	}

	[Table(Name = "OrderDetail")]
	class OrderDetail
	{
		[Column(Name = "Order_ID")]
		public long OrderID { get; private set; }
		[Column(Name = "Product_ID")]
		public long ProductID { get; private set; }

		public DateTime StartDate { get; set; }
		public DateTime EndDate { get; set; }
	}

	static class DBX
	{
		public static IQueryable<OrderDetail> OrderDetail
		{
			get { return (new OrderDetail[0]).AsQueryable(); }
		}
		public static IQueryable<PurchaseOrder> PurchaseOrder
		{
			get { return (new PurchaseOrder[0]).AsQueryable(); }
		}

		public static IQueryable<PurchaseOrder> OrdersWithLargeID
		{
			get { return (new PurchaseOrder[0]).AsQueryable().Where(order => order.ID > 1000); }
		}
		public static IQueryable<OrderDetail> OrderDetailsWithLargeOrderID
		{
			get { return from orderDetail in OrderDetail where orderDetail.OrderID > 1001 select orderDetail; }
		}

		public static IQueryable<Product> Product
		{
			get { return (new Product[0]).AsQueryable(); }
		}

		public static IQueryable<OrderDetail> GetStudentHoldElementer(long studentID)
		{
			return
				from orderDetail in OrderDetail
				where orderDetail.OrderID == studentID
				select orderDetail;
		}

		public static IQueryable<Product> GetProductsActive(DateTime asof)
		{
			return
				from p in Product
				where p.AvailabilityFrom <= asof && p.AvailabilityTo  > asof
				select p;
		}

		public static IQueryable<Product> GetHoldElementerForStudent(long studentID)
		{
			return
				from orderDetail in OrderDetail
				where orderDetail.OrderID == studentID
				join p in Product on orderDetail.ProductID equals p.ID
				select p;
		}
	}
}
