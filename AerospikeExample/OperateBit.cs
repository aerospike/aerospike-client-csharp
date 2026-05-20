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

public sealed class OperateBit : SyncExample
{
	/// <summary>
	/// Perform a bitwise operation on a blob bin.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "bitkey");
		string binName = "bitbin";

		Record record = client.Operate(writePolicy, key,
			BitOperation.Set(BitPolicy.Default, binName, -3, 3, [0xE0]),
			Operation.Get(binName));

		// The operate call returns one result per operation on the same bin (Set + Get),
		// so the bin result is a list of two entries.
		IList results = record.GetList(binName);
		byte[] bytes = (byte[])results[1];

		foreach (byte b in bytes)
		{
			console.Info(Convert.ToString(b));
		}
	}
}
