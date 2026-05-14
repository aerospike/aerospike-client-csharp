/* 
 * Copyright 2012-2026 Aerospike, Inc.
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

namespace Aerospike.Example;

public class Exists(Console console) : SyncExample(console)
{
	/// <summary>
	/// Demonstrate record existence checking.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "existskey");
		string binName = args.GetBinName("existsbin");

		client.Delete(args.writePolicy, key);

		bool exists = client.Exists(args.policy, key);
		console.Info("Exists before put: {0}", exists);

		if (exists)
		{
			throw new Exception("Record should not exist.");
		}

		var bin = new Bin(binName, "existsvalue");
		client.Put(args.writePolicy, key, bin);
		console.Info("Put: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

		exists = client.Exists(args.policy, key);
		console.Info("Exists after put: {0}", exists);

		if (!exists)
		{
			throw new Exception("Record should exist.");
		}

		client.Delete(args.writePolicy, key);

		exists = client.Exists(args.policy, key);
		console.Info("Exists after delete: {0}", exists);

		if (exists)
		{
			throw new Exception("Record should not exist after delete.");
		}

		console.Info("Exists example completed successfully.");
	}
}
