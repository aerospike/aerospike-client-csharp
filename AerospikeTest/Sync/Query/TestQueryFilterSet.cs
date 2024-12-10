/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
	public class TestQueryFilterSet : TestSync
	{
		private const string set1 = "tqps1";
		private const string set2 = "tqps2";
		private const string set3 = "tqps3";
		private const string binA = "a";
		private const string binB = "b";

		[ClassInitialize()]
		public static async Task Prepare(TestContext testContext)
		{
			WritePolicy policy = new WritePolicy();

			// Write records in set p1.
			for (int i = 1; i <= 5; i++)
			{
				policy.expiration = i * 60;

				Key key = new Key(args.ns, set1, i);
				if (!args.testAsyncAwait)
				{
					client.Put(policy, key, new Bin(binA, i));
				}
				else
				{
					await asyncAwaitClient.Put(policy, key, new[] { new Bin(binA, i) }, CancellationToken.None);
				}
			}

			// Write records in set p2.
			for (int i = 20; i <= 22; i++)
			{
				Key key = new Key(args.ns, set2, i);
				if (!args.testAsyncAwait)
				{
					client.Put(null, key, new Bin(binA, i), new Bin(binB, (double)i));
				}
				else
				{
					await asyncAwaitClient.Put(null, key, new[] { new Bin(binA, i), new Bin(binB, (double)i) }, CancellationToken.None);
				}
			}

			// Write records in set p3 with send key.
			policy = new WritePolicy();
			policy.sendKey = true;

			for (int i = 31; i <= 40; i++)
			{
				if (!args.testAsyncAwait)
				{
					Key intKey = new Key(args.ns, set3, i);
					client.Put(policy, intKey, new Bin(binA, i));

					Key strKey = new Key(args.ns, set3, "key-p3-" + i);
					client.Put(policy, strKey, new Bin(binA, i));
				}
				else
				{
					Key intKey = new Key(args.ns, set3, i);
					await asyncAwaitClient.Put(policy, intKey, new[] { new Bin(binA, i) }, CancellationToken.None);

					Key strKey = new Key(args.ns, set3, "key-p3-" + i);
					await asyncAwaitClient.Put(policy, strKey, new[] { new Bin(binA, i) }, CancellationToken.None);
				}
			}

			// Write one record in set p3 with send key not set.
			if (!args.testAsyncAwait)
			{
				client.Put(null, new Key(args.ns, set3, 25), new Bin(binA, 25));
			}
			else
			{
				await asyncAwaitClient.Put(null, new Key(args.ns, set3, 25), new[] { new Bin(binA, 25) }, CancellationToken.None);
			}
		}

		[TestMethod]
		public void QuerySetName()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.EQ(Exp.SetName(), Exp.Val(set2)));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(3, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryDouble()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(set2);

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.GT(Exp.FloatBin(binB), Exp.Val(21.5)));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(1, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryKeyString()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(set3);

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.RegexCompare("^key-.*-35$", 0, Exp.Key(Exp.Type.STRING)));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(1, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryKeyInt()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(set3);

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.LT(Exp.Key(Exp.Type.INT), Exp.Val(35)));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						//System.out.println(rs.getKey().toString() + " - " + rs.getRecord().toString());
						count++;
					}
					Assert.AreEqual(4, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryKeyExists()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(set3);

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.KeyExists());

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(20, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryVoidTime()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(set1);

			DateTime now = DateTime.UtcNow;
			DateTime end = now.Add(TimeSpan.FromMinutes(2));

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(
				Exp.And(
					Exp.GE(Exp.VoidTime(), Exp.Val(now)),
					Exp.LT(Exp.VoidTime(), Exp.Val(end))));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(2, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else
			{
				throw new NotImplementedException();
			}
		}

		[TestMethod]
		public void QueryTTL()
		{
			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(set1);

			QueryPolicy policy = new QueryPolicy();
			policy.filterExp = Exp.Build(Exp.LE(Exp.TTL(), Exp.Val(60)));

			if (!args.testAsyncAwait)
			{
				RecordSet rs = client.Query(policy, stmt);

				try
				{
					int count = 0;

					while (rs.Next())
					{
						count++;
					}
					Assert.AreEqual(1, count);
				}
				finally
				{
					rs.Close();
				}
			}
			else 
			{ 
				throw new NotImplementedException(); 
			}
		}
	}
}
