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
using System.IO;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class UserDefinedFunction : SyncExample
	{
		public UserDefinedFunction(Console console) : base(console)
		{
		}

		/// <summary>
		/// Register user defined function and call it.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Register(client, args);
			WriteUsingUdf(client, args);
			WriteIfGenerationNotChanged(client, args);
			WriteIfNotExists(client, args);
			WriteWithValidation(client, args);
			//WriteListMapUsingUdf(client, args);
#if BINARY_FORMATTER
			WriteBlobUsingUdf(client, args);
#endif
			ServerSideExists(client, args);
		}

		private void Register(AerospikeClient client, Arguments args)
		{
			string packageName = "record_example.lua";
			console.Info("Register: " + packageName);
			LuaExample.Register(client, args.policy, packageName);
		}

		private void WriteUsingUdf(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "udfkey1");
			Bin bin = new Bin(args.GetBinName("udfbin1"), "string value");

			client.Execute(args.writePolicy, key, "record_example", "writeBin", Value.Get(bin.name), bin.value);

			Record record = client.Get(args.policy, key, bin.name);
			string expected = bin.value.ToString();
			string received = (string)record.GetValue(bin.name);

			if (received != null && received.Equals(expected))
			{
				console.Info("Data matched: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Data mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}

		private void WriteIfGenerationNotChanged(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "udfkey2");
			Bin bin = new Bin(args.GetBinName("udfbin2"), "string value");

			// Seed record.
			client.Put(args.writePolicy, key, bin);

			// Get record generation.
			long gen = (long)client.Execute(args.writePolicy, key, "record_example", "getGeneration");

			// Write record if generation has not changed.
			client.Execute(args.writePolicy, key, "record_example", "writeIfGenerationNotChanged", Value.Get(bin.name), bin.value, Value.Get(gen));
			console.Info("Record written.");
		}

		private void WriteIfNotExists(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "udfkey3");
			string binName = "udfbin3";

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Write record only if not already exists. This should succeed.
			client.Execute(args.writePolicy, key, "record_example", "writeUnique", Value.Get(binName), Value.Get("first"));

			// Verify record written.
			Record record = client.Get(args.policy, key, binName);
			string expected = "first";
			string received = (string)record.GetValue(binName);

			if (received != null && received.Equals(expected))
			{
				console.Info("Record written: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, binName, received);
			}
			else
			{
				console.Error("Data mismatch: Expected {0}. Received {1}.", expected, received);
			}

			// Write record second time. This should fail.
			console.Info("Attempt second write.");
			client.Execute(args.writePolicy, key, "record_example", "writeUnique", Value.Get(binName), Value.Get("second"));

			// Verify record not written.
			record = client.Get(args.policy, key, binName);
			received = (string)record.GetValue(binName);

			if (received != null && received.Equals(expected))
			{
				console.Info("Success. Record remained unchanged: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, binName, received);
			}
			else
			{
				console.Error("Data mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}

		private void WriteWithValidation(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "udfkey4");
			string binName = "udfbin4";

			// Lua function writeWithValidation accepts number between 1 and 10.
			// Write record with valid value.
			console.Info("Write with valid value.");
			client.Execute(args.writePolicy, key, "record_example", "writeWithValidation", Value.Get(binName), Value.Get(4));

			// Write record with invalid value.
			console.Info("Write with invalid value.");

			try
			{
				client.Execute(args.writePolicy, key, "record_example", "writeWithValidation", Value.Get(binName), Value.Get(11));
				console.Error("UDF should not have succeeded!");
			}
			catch (Exception)
			{
				console.Info("Success. UDF resulted in exception as expected.");
			}
		}

		private void WriteListMapUsingUdf(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "udfkey5");

			List<object> inner = new List<object>();
			inner.Add("string2");
			inner.Add(8L);

			Dictionary<object, object> innerMap = new Dictionary<object, object>();
			innerMap["a"] = 1L;
			innerMap[2L] = "b";
			innerMap["list"] = inner;

			List<object> list = new List<object>();
			list.Add("string1");
			list.Add(4L);
			list.Add(inner);
			list.Add(innerMap);

			string binName = args.GetBinName("udfbin5");

			client.Execute(args.writePolicy, key, "record_example", "writeBin", Value.Get(binName), Value.Get(list));

			object received = client.Execute(args.writePolicy, key, "record_example", "readBin", Value.Get(binName));
			string receivedString = Util.ListToString((List<object>)received);
			string expectedString = Util.ListToString(list);

			if (receivedString.Equals(expectedString))
			{
				console.Info("UDF data matched: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, binName, received);
			}
			else
			{
				console.Error("UDF data mismatch");
				console.Error("Expected " + list);
				console.Error("Received " + received);
			}
		}

#if BINARY_FORMATTER
		private void WriteBlobUsingUdf(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "udfkey6");
			string binName = args.GetBinName("udfbin6");
			byte[] blob;

			// Create packed blob using standard C# tools.
			using (MemoryStream ms = new MemoryStream())
			{
				Formatter.Default.Serialize(ms, 9845);
				Formatter.Default.Serialize(ms, "Hello world.");
				blob = ms.ToArray();
			}

			client.Execute(args.writePolicy, key, "record_example", "writeBin", Value.Get(binName), Value.Get(blob));
			byte[] received = (byte[])client.Execute(args.writePolicy, key, "record_example", "readBin", Value.Get(binName));
			string receivedString = Util.BytesToString(received);
			string expectedString = Util.BytesToString(blob);

			if (receivedString.Equals(expectedString))
			{
				console.Info("Blob data matched: namespace={0} set={1} key={2} bin={3} value={4}",
					key.ns, key.setName, key.userKey, binName, receivedString);
			}
			else
			{
				throw new Exception(string.Format("Mismatch: expected={0} received={1}", expectedString, receivedString));
			}
		}
#endif

		private void ServerSideExists(AerospikeClient client, Arguments args)
		{
			console.Info("Write list.");
			List<int> list = new List<int>();
			list.Add(64);
			list.Add(3702);
			list.Add(-5);

			Key key = new Key(args.ns, args.set, "udfkey7");
			Bin bin = new Bin("udfbin7", list);
			client.Put(args.writePolicy, key, bin);

			ServerSideExists(client, args.writePolicy, key, bin, 3702, true);
			ServerSideExists(client, args.writePolicy, key, bin, 65, false);
		}

		private void ServerSideExists(AerospikeClient client, WritePolicy policy, Key key, Bin bin, int search, bool expected)
		{
			long lexists = (long)client.Execute(policy, key, "record_example", "valueExists", Value.Get(bin.name), Value.Get(search));
			bool exists = (lexists != 0);

			if (expected && exists)
			{
				console.Info("Value found as expected.");
				return;
			}

			if (!expected && !exists)
			{
				console.Info("Value not found as expected.");
				return;
			}

			console.Error("Data mismatch. Expected " + expected + " Received " + exists);
		}
	}
}
