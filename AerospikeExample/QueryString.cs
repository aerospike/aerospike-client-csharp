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

public sealed class QueryString : SyncExample
{
	/// <summary>
	/// Run a secondary-index equality query on a string bin.
	/// </summary>
	public override void RunExample()
	{
		const string indexName = "queryindex";
		const string binName = "querybin";
		const string filterValue = "queryvalue3";

		console.Info($"Query for: ns={ns} set={set} index={indexName} bin={binName} filter={filterValue}");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			BinNames = [binName],
			Filter = Filter.Equal(binName, filterValue)
		};

		using RecordSet rs = client.Query(null, stmt);

		while (rs.Next())
		{
			Key key = rs.Key;
			string result = (string)rs.Record.GetValue(binName);

			console.Info(
				$"Record found: namespace={key.ns} set={key.setName} " +
				$"digest={ByteUtil.BytesToHexString(key.digest)} bin={binName} value={result}");
		}
	}
}
