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
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class QueryString : SyncExample
	{
		public QueryString(Console console) : base(console)
		{
		}

		/// <summary>
		/// Create secondary index and query on it.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasUdf)
			{
				console.Info("Query functions are not supported by the connected Aerospike server.");
				return;
			}

			string indexName = "queryindex";
			string keyPrefix = "querykey";
			string valuePrefix = "queryvalue";
			string binName = args.GetBinName("querybin");
			int size = 5;

			CreateIndex(client, args, indexName, binName);
			WriteRecords(client, args, keyPrefix, binName, valuePrefix, size);
			RunQuery(client, args, indexName, binName, valuePrefix);
			client.DropIndex(args.policy, args.ns, args.set, indexName);
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

		private void WriteRecords(AerospikeClient client, Arguments args, string keyPrefix, string binName, string valuePrefix, int size)
		{
			for (int i = 1; i <= size; i++)
			{
				Key key = new Key(args.ns, args.set, keyPrefix + i);
				Bin bin = new Bin(binName, valuePrefix + i);

				console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, bin.value);

				client.Put(args.writePolicy, key, bin);
			}
		}

		private void RunQuery(AerospikeClient client, Arguments args, string indexName, string binName, string valuePrefix)
		{
			string filter = valuePrefix + 3;

			console.Info("Query for: ns={0} set={1} index={2} bin={3} filter={4}", 
				args.ns, args.set, indexName, binName, filter);

			Statement stmt = new Statement();
			stmt.SetNamespace(args.ns);
			stmt.SetSetName(args.set);
			stmt.SetBinNames(binName);
			stmt.SetFilters(Filter.Equal(binName, filter));

			RecordSet rs = client.Query(null, stmt);

			try
			{
				int count = 0;

				while (rs.Next())
				{
					Key key = rs.Key;
					Record record = rs.Record;
					string result = (string)record.GetValue(binName);

					if (result.Equals(filter))
					{
						console.Info("Record found: namespace={0} set={1} digest={2} bin={3} value={4}", 
							key.ns, key.setName, ByteUtil.BytesToHexString(key.digest), binName, result);
					}
					else
					{
						console.Error("Query mismatch: Expected {0}. Received {1}.", filter, result);
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
