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

public sealed class Operate : SyncExample
{
	/// <summary>
	/// Demonstrate multiple operations against a single record in one call.
	/// </summary>
	public override void RunExample()
	{
		Key key = new(ns, set, "opkey");
		Bin intBin = new("optintbin", 7);
		Bin stringBin = new("optstringbin", "string value");
		console.Info(
			$"Initial record: namespace={key.ns} set={key.setName} key={key.userKey} " +
			$"binname1={intBin.name} binvalue1={intBin.value} " +
			$"binname2={stringBin.name} binvalue2={stringBin.value}");

		Bin addBin = new(intBin.name, 4);
		Bin replaceBin = new(stringBin.name, "new string");
		console.Info($"Add: {addBin.value}");
		console.Info($"Write: {replaceBin.value}");
		console.Info("Read:");

		Record record = client.Operate(writePolicy, key, Operation.Add(addBin), Operation.Put(replaceBin), Operation.Get());
		console.Info($"Record: {record}");
	}
}
