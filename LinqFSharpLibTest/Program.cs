using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace LinqFSharpConsole
{
	class Program
	{
		static void Main(string[] args)
		{
			var q = from i in Enumerable.Range(0, 10).AsQueryable()
			        where i+2 == 0
			        select i;


			var lp = LinqTranslation.LinqProvider.CreateSelect(q.Expression, null);

			var mce = (MethodCallExpression) q.Expression;
			var c = (ConstantExpression) mce.Arguments.First();
			var v = c.Value;
			Console.WriteLine("iq: {0}", v is IQueryable);
			Console.WriteLine("enum: {0}", v is IEnumerable);
			

		}

		private static void CountLinesInFolder(string dir, Dictionary<string, int> fileDict)
		{
			foreach (var file in Directory.GetFiles(dir))
			{
				if (file.EndsWith(".cs") || file.EndsWith(".aspx"))
				{
					int linecount = File.ReadAllLines(file).Length;
					fileDict[file] = linecount;
				}
			}

			foreach (string directory in Directory.GetDirectories(dir))
			{
				if (directory.StartsWith(@"C:\Projects\VSS\LectioVS2005\ExternalDlls"))
					continue;

				CountLinesInFolder(directory, fileDict);
			}
		}
	}
}
