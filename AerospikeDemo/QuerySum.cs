/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System.IO;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QuerySum : SyncExample
	{
		public QuerySum(Console console) : base(console)
		{
		}

		/// <summary>
		/// Create secondary index and query on it and apply aggregation user defined function.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Query functions are not supported by the connected Aerospike server.");
				return;
			}
			string indexName = "aggindex";
			string keyPrefix = "aggkey";
			string binName = args.GetBinName("aggbin");
			int size = 10;

			Register(client, args);
			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, binName, size);
			RunQuery(client, args, indexName, binName);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void Register(AerospikeClient client, Arguments args)
		{
			string packageName = "sum_example.lua";
			console.Info("Register: " + packageName);
			LuaExample.Register(client, args.policy, packageName);
		}

		private void CreateIndex(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.NUMERIC);
			task.Wait();
		}

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, int size)
		{
			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, i);

				console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, bin.value);

				client.Put(args.writePolicy, key, bin);
			}
		}

		private void RunQuery(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			int begin = 4;
			int end = 7;

			console.Info("Query for:ns={0} set={1} index={2} bin={3} >= {4} <= {5}", 
				args.ns, args.set, indexName, binName, begin, end);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.Range(binName, begin, end));

			ResultSet rs = client.QueryAggregate(null, stmt, "sum_example", "sum_single_bin", Value.Get(binName));

			try
			{
				int expected = 22; // 4 + 5 + 6 + 7
				int count = 0;

				while (rs.Next())
				{
					object obj = rs.Object;
					double sum;

					if (obj is double)
					{
						sum = (double)rs.Object;
					}
					else
					{
						console.Error("Return value not a double: " + obj);
						continue;
					}

					if (expected == (int)sum)
					{
						console.Info("Sum matched: value=" + expected);
					}
					else
					{
						console.Error("Sum mismatch: Expected {0}. Received {1}.", expected, sum);
					}
					count++;
				}

				if (count == 0)
				{
					console.Error("Query failed. No records returned.");
				}
			}
			finally
			{
				rs.Close();
			}
		}
	}
}
