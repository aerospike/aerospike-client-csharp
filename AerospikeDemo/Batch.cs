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

			bool[] existsArray = client.Exists(args.policy, keys);

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

			Record[] records = client.Get(args.policy, keys, binName);

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

			Record[] records = client.GetHeader(args.policy, keys);

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
	}
}
