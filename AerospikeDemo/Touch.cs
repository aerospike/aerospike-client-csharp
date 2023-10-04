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
	public class Touch : SyncExample
	{
		public Touch(Console console) : base(console)
		{
		}

		/// <summary>
		/// Demonstrate touch command.
		/// </summary>
		public override void RunExample(IAerospikeClient client, Arguments args)
		{
			Key key = new Key(args.ns, args.set, "touchkey");
			Bin bin = new Bin(args.GetBinName("touchbin"), "touchvalue");

			console.Info("Create record with 2 second expiration.");
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.expiration = 2;
			client.Put(writePolicy, key, bin);

			console.Info("Touch same record with 5 second expiration.");
			writePolicy.expiration = 5;
			Record record = client.Operate(writePolicy, key, Operation.Touch(), Operation.GetHeader());

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2} bin={3} value={4}",
					key.ns, key.setName, key.userKey, bin.name, null));
			}

			if (record.expiration == 0)
			{
				throw new Exception(string.Format("Failed to get record expiration: namespace={0} set={1} key={2}",
					key.ns, key.setName, key.userKey));
			}

			console.Info("Sleep 3 seconds.");
			Thread.Sleep(3000);

			record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				throw new Exception(string.Format("Failed to get: namespace={0} set={1} key={2}",
					key.ns, key.setName, key.userKey));
			}

			console.Info("Success. Record still exists.");
			console.Info("Sleep 4 seconds.");
			Thread.Sleep(4000);

			record = client.Get(args.policy, key, bin.name);

			if (record == null)
			{
				console.Info("Success. Record expired as expected.");
			}
			else
			{
				console.Error("Found record when it should have expired.");
			}
		}
	}
}
