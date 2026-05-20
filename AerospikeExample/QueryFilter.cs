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

public sealed class QueryFilter : SyncExample
{
	/// <summary>
	/// Query a secondary index and apply an additional server-side filter inside a UDF.
	/// </summary>
	public override void RunExample()
	{
		const string indexName = "profileindex";
		const string binName = "name";
		const string nameFilter = "Bill";
		const string passwordFilter = "hknfpkj";

		console.Info($"Query for: ns={ns} set={set} index={indexName} name={nameFilter} pass={passwordFilter}");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			Filter = Filter.Equal(binName, nameFilter)
		};
		stmt.SetAggregateFunction("filter_example", "profile_filter", Value.Get(passwordFilter));

		// passwordFilter is applied inside filter_example.lua.
		using ResultSet rs = client.QueryAggregate(null, stmt);

		while (rs.Next())
		{
			Dictionary<object, object> map = (Dictionary<object, object>)rs.Object;
			console.Info($"Record found: name={map["name"]} password={map["password"]}");
		}
	}
}
