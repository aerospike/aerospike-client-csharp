/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Batch : SyncExample
	{
		public Batch(Console console) : base(console)
		{
		}

		/// <summary>
		/// Batch multiple gets in one call to the server.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			string keyPrefix = "batchkey";
			string valuePrefix = "batchvalue";
			string binName = args.GetBinName("batchbin");
			int size = 8;

			WriteRecords(client, args, keyPrefix, binName, valuePrefix, size);
			BatchExists(client, args, keyPrefix, size);
			BatchReads(client, args, keyPrefix, binName, size);
			BatchReadHeaders(client, args, keyPrefix, size);

			try
			{
				BatchReadComplex(client, args, keyPrefix, binName);
			}
			catch (Exception ex)
			{
				// Server version may not yet support new batch protocol.
				Node[] nodes = client.Nodes;

				foreach (Node node in nodes)
				{
					if (!node.HasBatchIndex)
					{
						Log.Warn("Server does not support new batch protocol");
						return;
					}
				}
				throw ex;
			}
		}

		/// <summary>
		/// Write records individually.
		/// </summary>
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

		/// <summary>
		/// Check existence of records in one batch.
		/// </summary>
		private void BatchExists(AerospikeClient client, Arguments args, string keyPrefix, int size)
		{
			// Batch into one call.
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			bool[] existsArray = client.Exists(null, keys);

			for (int i = 0; i < existsArray.Length; i++)
			{
				Key key = keys[i];
				bool exists = existsArray[i];
				console.Info("Record: namespace={0} set={1} key={2} exists={3}", 
					key.ns, key.setName, key.userKey, exists);
			}
		}

		/// <summary>
		/// Read records in one batch.
		/// </summary>
		private void BatchReads(AerospikeClient client, Arguments args, string keyPrefix, string binName, int size)
		{
			// Batch gets into one call.
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			Record[] records = client.Get(null, keys, binName);

			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];
				Log.Level level = Log.Level.ERROR;
				object value = null;

				if (record != null)
				{
					level = Log.Level.INFO;
					value = record.GetValue(binName);
				}
				console.Write(level, "Record: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, binName, value);
			}

			if (records.Length != size)
			{
				console.Error("Record size mismatch. Expected {0}. Received {1}.", size, records.Length);
			}
		}

		/// <summary>
		/// Read record header data in one batch.
		/// </summary>
		private void BatchReadHeaders(AerospikeClient client, Arguments args, string keyPrefix, int size)
		{
			// Batch gets into one call.
			Key[] keys = new Key[size];
			for (int i = 0; i < size; i++)
			{
				keys[i] = new Key(args.ns, args.set, keyPrefix + (i + 1));
			}

			Record[] records = client.GetHeader(null, keys);

			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];
				Log.Level level = Log.Level.ERROR;
				int generation = 0;
				int expiration = 0;

				if (record != null && (record.generation > 0 || record.expiration > 0))
				{
					level = Log.Level.INFO;
					generation = record.generation;
					expiration = record.expiration;
				}
				console.Write(level, "Record: namespace={0} set={1} key={2} generation={3} expiration={4}", 
					key.ns, key.setName, key.userKey, generation, expiration);
			}

			if (records.Length != size)
			{
				console.Error("Record size mismatch. Expected %d. Received %d.", size, records.Length);
			}
		}

		/// <summary>
		/// Read records with varying namespaces, bin names and read types in one batch.
		/// This requires Aerospike Server version >= 3.6.0.
		/// </summary>
		private void BatchReadComplex(AerospikeClient client, Arguments args, string keyPrefix, string binName)
		{
			// Batch gets into one call.
			// Batch allows multiple namespaces in one call, but example test environment may only have one namespace.
			string[] bins = new string[] {binName};
			List<BatchRead> records = new List<BatchRead>();
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 1), bins));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 2), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 3), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 4), false));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 5), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 6), true));
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 7), bins));

			// This record should be found, but the requested bin will not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, keyPrefix + 8), new string[] { "binnotfound" }));

			// This record should not be found.
			records.Add(new BatchRead(new Key(args.ns, args.set, "keynotfound"), bins));

			// Execute batch.
			client.Get(null, records);

			// Show results.
			int found = 0;
			foreach (BatchRead record in records)
			{
				Key key = record.key;
				Record rec = record.record;

				if (rec != null)
				{
					found++;
					console.Info("Record: ns={0} set={1} key={2} bin={3} value={4}", 
						key.ns, key.setName, key.userKey, binName, rec.GetValue(binName));
				}
				else
				{
					console.Info("Record not found: ns={0} set={1} key={2} bin={3}",
						key.ns, key.setName, key.userKey, binName);
				}
			}

			if (found != 8)
			{
				console.Error("Records found mismatch. Expected %d. Received %d.", 8, found);
			}
		}
	}
}
