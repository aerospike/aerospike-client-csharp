/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
using Aerospike.Client;

namespace Aerospike.Demo
{
	public class Add : SyncExample
	{
		public Add(Console console) : base(console)
		{
		}

		/// <summary>
		/// Add integer values.
		/// </summary>
		public override void RunExample(AerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "addkey");
			string binName = args.GetBinName("addbin");

			// Delete record if it already exists.
			client.Delete(args.writePolicy, key);

			// Perform some adds and check results.
			Bin bin = new Bin(binName, 10);
			console.Info("Initial add will create record.  Initial value is " + bin.value + '.');
			client.Add(args.writePolicy, key, bin);

			bin = new Bin(binName, 5);
			console.Info("Add " + bin.value + " to existing record.");
			client.Add(args.writePolicy, key, bin);

			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}", 
					key.ns, key.setName, key.userKey));
			}

			// The value received from the server is an unsigned byte stream.
			// Convert to an integer before comparing with expected.
			object obj = record.GetValue(bin.name);
			int received = (int)(long)obj;		
			int expected = 15;

			if (received == expected)
			{
				console.Info("Add successful: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Add mismatch: Expected {0}. Received {1}.", expected, received);
			}

			// Demonstrate add and get combined.
			bin = new Bin(binName, 30);
			console.Info("Add " + bin.value + " to existing record.");
			record = client.Operate(args.writePolicy, key, Operation.Add(bin), Operation.Get(bin.name));

			expected = 45;
			received = (int)(long)record.GetValue(bin.name);

			if (received == expected)
			{
				console.Info("Add successful: namespace={0} set={1} key={2} bin={3} value={4}", 
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				console.Error("Add mismatch: Expected {0}. Received {1}.", expected, received);
			}
		}
	}
}
