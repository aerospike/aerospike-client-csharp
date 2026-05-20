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

public sealed class Replace : SyncExample
{
	/// <summary>
	/// Demonstrate writing bins with replace semantics. Replace overwrites all bins,
	/// dropping any bin not referenced in the put. Replace performs better than the
	/// default put because the server does not have to read the existing record first.
	/// </summary>
	public override void RunExample()
	{
		RunReplaceExample();
		RunReplaceOnlyExample();
	}

	private void RunReplaceExample()
	{
		Key key = new(ns, set, "replacekey");
		Bin bin3 = new("bin3", "value3");

		console.Info($"Replace with: namespace={key.ns} set={key.setName} key={key.userKey} bin={bin3.name} value={bin3.value}");

		WritePolicy replacePolicy = new(writePolicy)
		{
			recordExistsAction = RecordExistsAction.REPLACE
		};
		client.Put(replacePolicy, key, bin3);
	}

	private void RunReplaceOnlyExample()
	{
		Key key = new(ns, set, "replaceonlykey");
		Bin bin = new("bin", "value");

		console.Info($"Replace record requiring that it exists: namespace={key.ns} set={key.setName} key={key.userKey}");

		WritePolicy replaceOnlyPolicy = new(writePolicy)
		{
			recordExistsAction = RecordExistsAction.REPLACE_ONLY
		};

		try
		{
			client.Put(replaceOnlyPolicy, key, bin);
			console.Info("Replace-only put completed.");
		}
		catch (AerospikeException ae) when (ae.Result is ResultCode.KEY_NOT_FOUND_ERROR or ResultCode.FILTERED_OUT)
		{
			console.Info("Success. Replace-only rejected because the record does not exist.");
		}
	}
}
