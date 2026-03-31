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

public class Delete(Console console) : SyncExample(console)
{
	/// <summary>
	/// Demonstrate record deletion including durable delete.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		RunDefaultDelete(client, args);
		RunDurableDelete(client, args);

		var deleteKey = new Key(args.ns, args.set, "deletekey");
		if (client.Exists(null, deleteKey))
		{
			throw new Exception("Delete verification failed: deletekey still exists.");
		}
		var durableDeleteKey = new Key(args.ns, args.set, "durabledeletekey");
		if (client.Exists(null, durableDeleteKey))
		{
			throw new Exception("Delete verification failed: durabledeletekey still exists.");
		}
		console.Info("Delete verified successfully.");
	}

	private void RunDefaultDelete(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "deletekey");
		var bin = new Bin(args.GetBinName("bin"), "value");

		client.Put(args.writePolicy, key, bin);
		console.Info("Put: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

		bool existed = client.Delete(args.writePolicy, key);
		console.Info("Delete: namespace={0} set={1} key={2} existed={3}",
			key.ns, key.setName, key.userKey, existed);

		bool exists = client.Exists(args.policy, key);

		if (!exists)
		{
			console.Info("Record deleted successfully.");
		}
		else
		{
			console.Error("Record still exists after delete.");
		}

		existed = client.Delete(args.writePolicy, key);
		console.Info("Delete non-existent: existed={0}", existed);
	}

	/// <summary>
	/// Durable delete leaves a tombstone so the delete cannot be undone
	/// by a conflicting write from another data center (XDR).
	/// Requires Enterprise edition.
	/// </summary>
	private void RunDurableDelete(IAerospikeClient client, Arguments args)
	{
		RequireEnterprise(args);

		var key = new Key(args.ns, args.set, "durabledeletekey");
		var bin = new Bin(args.GetBinName("bin"), "durablevalue");

		client.Put(args.writePolicy, key, bin);
		console.Info("Put: namespace={0} set={1} key={2}", key.ns, key.setName, key.userKey);

		var durablePolicy = new WritePolicy(args.writePolicy)
		{
			durableDelete = true
		};

		bool existed = client.Delete(durablePolicy, key);
		console.Info("Durable delete: namespace={0} set={1} key={2} existed={3}",
			key.ns, key.setName, key.userKey, existed);

		bool exists = client.Exists(args.policy, key);

		if (!exists)
		{
			console.Info("Durable delete successful.");
		}
		else
		{
			console.Error("Record still exists after durable delete.");
		}
	}
}
