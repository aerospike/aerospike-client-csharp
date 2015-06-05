/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class LargeSet : SyncExample
	{
		public LargeSet(Console console) : base(console)
		{
		}

		/// <summary>
		/// Perform operations on a list within a single bin.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			if (!args.hasLargeDataTypes)
			{
				console.Info("Large set functions are not supported by the connected Aerospike server.");
				return;
			}

			Key key = new Key(args.ns, args.set, "setkey");
			string binName = args.GetBinName("setbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Initialize large set operator.
			Aerospike.Client.LargeSet set = client.GetLargeSet(args.writePolicy, key, binName, null);

			// Write values.
			set.Add(Value.Get("setvalue1"));
			set.Add(Value.Get("setvalue2"));
			set.Add(Value.Get("setvalue3"));

			// Verify large set was created with default configuration.
			IDictionary map = set.GetConfig();

			foreach (DictionaryEntry entry in map)
			{
				console.Info(entry.Key.ToString() + ',' + entry.Value);
			}

			// Remove last value.
			set.Remove(Value.Get("setvalue3"));

			int size = set.Size();

			if (size != 2)
			{
				throw new Exception("Size mismatch. Expected 2 Received " + size);
			}

			string received = (string)set.Get(Value.Get("setvalue2"));
			string expected = "setvalue2";

			if (received != null && received.Equals(expected))
			{
				console.Info("Data matched: namespace={0} set={1} key={2} value={3}", key.ns, key.setName, key.userKey, received);
			}
			else
			{
				console.Error("Data mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}
