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
using System.Collections;

namespace Aerospike.Example;

public class OperateBit(Console console) : SyncExample(console)
{

	/// <summary>
	/// Perform operations on a blob bin.
	/// </summary>
	public override void RunExample(IAerospikeClient client, Arguments args)
	{
		RunSimpleExample(client, args);

		string verifyBin = args.GetBinName("bitbin");
		Record verifyRec = client.Get(null, new Key(args.ns, args.set, "bitkey"));
		if (verifyRec == null)
		{
			throw new Exception("OperateBit verification failed: record not found.");
		}
		if (verifyRec.GetValue(verifyBin) == null)
		{
			throw new Exception("OperateBit verification failed: bit bin is null.");
		}
		console.Info("OperateBit verified successfully.");
	}

	/// <summary>
	/// Simple example of bit functionality.
	/// </summary>
	public void RunSimpleExample(IAerospikeClient client, Arguments args)
	{
		var key = new Key(args.ns, args.set, "bitkey");
		string binName = args.GetBinName("bitbin");

		// Delete record if it already exists.
		client.Delete(args.writePolicy, key);

		byte[] bytes = [0x01, 0x02, 0x03, 0x04, 0x05];

		client.Put(args.writePolicy, key, new Bin(binName, bytes));

		// Set last 3 bits of bitmap to true.
		Record record = client.Operate(args.writePolicy, key,
			BitOperation.Set(BitPolicy.Default, binName, -3, 3, new byte[] { 0xE0 }),
			Operation.Get(binName)
			);

		IList list = record.GetList(binName);

		var val = (byte[])list[1];

		foreach (byte b in val)
		{
			console.Info(Convert.ToString(b));
		}
	}
}
