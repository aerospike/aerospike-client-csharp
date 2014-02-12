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
using System.Collections.Generic;

namespace Aerospike.Demo
{
	public class QueryFilter : SyncExample
	{
		public QueryFilter(Console console) : base(console)
		{
		}

		/// <summary>
		/// Query on a secondary index with a filter and then apply an additional filter in the 
		/// user defined function.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Query functions are not supported by the connected Aerospike server.");
				return;
			}
			string indexName = "profileindex";
			string keyPrefix = "profilekey";
			string binName = args.GetBinName("name");

			Register(client, args);
			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, binName);
			RunQuery(client, args, indexName, binName);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
		}

		private void Register(AerospikeClient client, Arguments args)
		{
			string packageName = "filter_example.lua";
			console.Info("Register: " + packageName);
			LuaExample.Register(client, args.policy, packageName);
		}

		private void CreateIndex(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			console.Info("Create index: ns={0} set={1} index={2} bin={3}",
				args.ns, args.set, indexName, binName);

			Policy policy = new Policy();
			policy.timeout = 0; // Do not timeout on index create.
			IndexTask task = client.CreateIndex(policy, args.ns, args.set, indexName, binName, IndexType.STRING);
			task.Wait();
		}

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName)
		{
			WriteRecord(client, args, keyPrefix + 1, "Charlie", "cpass");
			WriteRecord(client, args, keyPrefix + 2, "Bill", "hknfpkj");
			WriteRecord(client, args, keyPrefix + 3, "Doug", "dj6554");
		}

		private void WriteRecord(AerospikeClient client, Arguments args, string userKey, string name, string password)
		{
			Key key = new Key(args.ns, args.set, userKey);
			Bin bin1 = new Bin("name", name);
			Bin bin2 = new Bin("password", password);
			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}",
				key.ns, key.setName, key.userKey, bin1.name, bin1.value);

			client.Put(args.writePolicy, key, bin1, bin2);
		}

		private void RunQuery(AerospikeClient client, Arguments args, string indexName, string binName)
		{
			string nameFilter = "Bill";
			string passFilter = "hknfpkj";

			console.Info("Query for: ns=%s set=%s index=%s name=%s pass=%s", args.ns, args.set, indexName, nameFilter, passFilter);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetFilters(Filter.Equal(binName, nameFilter));

			// passFilter will be applied in filter_example.lua.
			ResultSet rs = client.QueryAggregate(null, stmt, "filter_example", "profile_filter", Value.Get(passFilter));

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Dictionary<object, object> map = (Dictionary<object, object>)rs.Object;
					Validate(map, "name", nameFilter);
					Validate(map, "password", passFilter);
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

		private void Validate(Dictionary<object, object> map, string name, object expected)
		{
			object val = null;
			map.TryGetValue(name, out val);

			if (val != null && val.Equals(expected))
			{
				console.Info("Data matched: value={0}", expected);
			}
			else
			{
				console.Error("Data mismatch: Expected {0}. Received {1}.", expected, val);
			}
		}
	}
}
