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
using System;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Serialize : SyncExample
	{
		public Serialize(Console console) : base(console)
		{
		}

		/// <summary>
		/// Write complex objects using standard C# serializer.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
#if BINARY_FORMATTER
			TestArray(client, args);
			TestList(client, args);
			TestComplex(client, args);
#endif
		}

#if BINARY_FORMATTER
		/// <summary>
		/// Write array of integers using standard C# serializer.
		/// </summary>
		public virtual void TestArray(IAerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "serialarraykey");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			console.Info("Initialize array");

			int[] array = new int[10000];

			for (int i = 0; i < 10000; i++)
			{
				array[i] = i * i;
			}

			Bin bin = new Bin(args.GetBinName("serialbin"), new Value.BlobValue(array));

			// Do a test that pushes this complex object through the serializer
			console.Info("Write array using serializer.");
			client.Put(args.writePolicy, key, bin);

			console.Info("Read array using serializer.");
			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			int[] received;

			try
			{
				received = (int[])record.GetValue(bin.name);
			}
			catch (Exception)
			{
				throw new Exception(string.Format("Failed to parse returned value: namespace={0} set={1} key={2} bin={3}", 
					key.ns, key.setName, key.userKey, bin.name));
			}

			if (received.Length != 10000)
			{
				throw new Exception(string.Format("Array length mismatch: Expected={0:D} Received={1:D}", 
					10000, received.Length));
			}

			for (int i = 0; i < 10000; i++)
			{
				if (received[i] != i * i)
				{
					throw new Exception(string.Format("Mismatch: index={0:D} expected={1:D} received={2:D}", 
						i, i * i, received[i]));
				}
			}

			console.Info("Read array successful.");
		}

		/// <summary>
		/// Write list object using standard C# serializer.
		/// </summary>
		public virtual void TestList(IAerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "seriallistkey");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			console.Info("Initialize list");

			List<string> list = new List<string>();
			list.Add("string1");
			list.Add("string2");
			list.Add("string3");

			Bin bin = new Bin(args.GetBinName("serialbin"), new Value.BlobValue(list));

			console.Info("Write list using serializer.");
			client.Put(args.writePolicy, key, bin);

			console.Info("Read list using serializer.");
			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			List<string> received;

			try
			{
				received = (List<string>)record.GetValue(bin.name);
			}
			catch (Exception e)
			{
				throw new Exception(string.Format("Failed to parse returned value: namespace={0} set={1} key={2} bin={3}", 
					key.ns, key.setName, key.userKey, bin.name), e);
			}

			if (received.Count != 3)
			{
				throw new Exception(string.Format("Array length mismatch: Expected={0:D} Received={1:D}", 
					3, received.Count));
			}

			for (int i = 0; i < received.Count; i++)
			{
				string expected = "string" + (i + 1);
				if (!received[i].Equals(expected))
				{
					object obj = received[i];
					throw new Exception(string.Format("Mismatch: index={0:D} expected={1} received={2}", 
						i, expected, obj));
				}
			}

			console.Info("Read list successful.");
		}

		/// <summary>
		/// Write complex object using standard C# serializer.
		/// </summary>
		public virtual void TestComplex(IAerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "serialcomplexkey");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			console.Info("Initialize complex object");

			List<object> inner = new List<object>();
			inner.Add("string2");
			inner.Add(8);

			Dictionary<object, object> innerMap = new Dictionary<object, object>();
			innerMap["a"] = 1;
			innerMap[2] = "b";
			innerMap["list"] = inner;

			List<object> list = new List<object>();
			list.Add("string1");
			list.Add(4);
			list.Add(inner);
			list.Add(innerMap);

			Bin bin = new Bin(args.GetBinName("complexbin"), (object)list);

			console.Info("Write complex object using serializer.");
			client.Put(args.writePolicy, key, bin);

			console.Info("Read complex object using serializer.");
			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			string expected = Util.ListToString(list);
			string received;

			try
			{
				object val = record.GetValue(bin.name);
				received = Util.ObjectToString(val);
			}
			catch (Exception)
			{
				throw new Exception(string.Format("Failed to parse returned value: namespace={0} set={1} key={2} bin={3}", 
					key.ns, key.setName, key.userKey, bin.name));
			}

			if (received != null && received.Equals(expected))
			{
				console.Info("Data matched: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Data mismatch");
				console.Error("Expected " + expected);
				console.Error("Received " + received);
			}
			console.Info("Read complex object successful.");
		}
#endif
	}
}
