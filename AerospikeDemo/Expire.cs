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
using System;
using System.Threading;

namespace Aerospike.Demo
{
	public class Expire : SyncExample
	{
		public Expire(Console console) : base(console)
		{
		}

		/// <summary>
		/// Write and twice read a bin value, demonstrating record expiration.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "expirekey");
			Bin bin = new Bin(args.GetBinName("expirebin"), "expirevalue");

			console.Info("Put: namespace={0} set={1} key={2} bin={3} value={4} expiration=2",
				key.ns, key.setName, key.userKey, bin.name, bin.value);

			// Specify that record expires 2 seconds after it's written.
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.expiration = 2;
			client.Put(writePolicy, key, bin);

			// Read the record before it expires, showing it's there.
			console.Info("Get: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

			Record record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}",
					key.ns, key.setName, key.userKey));
			}

			object received = record.GetValue(bin.name);
			string expected = bin.value.ToString();

			if (received.Equals(expected))
			{
				console.Info("Get successful: namespace={0} set={1} key={2} bin={3} value={4}",
					key.ns, key.setName, key.userKey, bin.name, received);
			}
			else
			{
				throw new Exception(string.Format("Expire mismatch: Expected {0}. Received {1}.", expected, received));
			}

			// Read the record after it expires, showing it's gone.
			console.Info("Sleeping for 3 seconds ...");
			Thread.Sleep(3 * 1000);
			record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				console.Info("Expiry successful. Record not found.");
			}
			else
			{
				console.Error("Found record when it should have expired.");
			}
		}
	}
}
