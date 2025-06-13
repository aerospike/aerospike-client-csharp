/* 
 * Copyright 2012-2025 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using Aerospike.Client;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestQueryExpressionComplex : TestSync
	{
		private static readonly string setName = SuiteHelpers.set + "country";
		private const string indexName = "exp_index";
		private const string keyPrefix = "country";
		private static Expression exp = Exp.Build(
			Exp.Cond(
				Exp.And(
					Exp.GE(   // Is the age 18 or older?
						Exp.IntBin("age"),
						Exp.Val(18)
					),
					Exp.Or( // Do they live in a target country?
						Exp.EQ(Exp.StringBin("country"), Exp.Val("Australia")),
						Exp.EQ(Exp.StringBin("country"), Exp.Val("Canada")),
						Exp.EQ(Exp.StringBin("country"), Exp.Val("USA"))
					)
				),
				Exp.Val(1),
				Exp.Unknown()
		));

		[ClassInitialize()]
		public static void Prepare(TestContext testContext)
		{
			Policy policy = new()
			{
				totalTimeout = 0 // Do not timeout on index create.
			};

			try
			{
				IndexTask itask = client.CreateIndex(policy, SuiteHelpers.ns, setName, indexName, IndexType.NUMERIC, IndexCollectionType.DEFAULT, exp);
				itask.Wait();
			}
			catch (AerospikeException ae)
			{
				if (ae.Result != ResultCode.INDEX_ALREADY_EXISTS)
				{
					throw;
				}
			}

			// Write records
			InsertData();
		}

		[ClassCleanup(ClassCleanupBehavior.EndOfClass)]
		public static void Destroy()
		{
			client.DropIndex(null, SuiteHelpers.ns, setName, indexName);
		}

		[TestMethod]
		public void QueryExpressionComplexExpression()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.Range(exp, 1, 1));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					//Console.WriteLine(rs.Record);
					count++;
				}
				Assert.AreEqual(6, count);
			}
			finally
			{
				rs.Close();
			}
		}

		[TestMethod]
		public void QueryExpressionComplexIndexName()
		{
			Statement stmt = new();
			stmt.SetNamespace(SuiteHelpers.ns);
			stmt.SetSetName(setName);
			stmt.SetFilter(Filter.RangeByIndex(indexName, 1, 1));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					//Console.WriteLine(rs.Record);
					count++;
				}
				Assert.AreEqual(6, count);
			}
			finally
			{
				rs.Close();
			}
		}

		private static void Insert(int key, string name, int age, string country)
		{
			client.Put(null,
				new Key(SuiteHelpers.ns, setName, keyPrefix + key),
						new Bin("name", name),
						new Bin("age", age),
						new Bin("country", country));
		}

		private static void InsertData()
		{
			Insert(1, "Tim", 312, "Australia");
			Insert(2, "Bob", 47, "Canada");
			Insert(3, "Jo", 15, "USA");
			Insert(4, "Steven", 23, "Botswana");
			Insert(5, "Susan", 32, "Canada");
			Insert(6, "Jess", 17, "USA");
			Insert(7, "Sam", 18, "USA");
			Insert(8, "Alex", 47, "Canada");
			Insert(9, "Pam", 56, "Australia");
			Insert(10, "Vivek", 12, "India");
			Insert(11, "Kiril", 22, "Sweden");
			Insert(12, "Bill", 23, "UK");
		}
	}
}
