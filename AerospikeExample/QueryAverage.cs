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

public sealed class QueryAverage : SyncExample
{
	/// <summary>
	/// Compute an average on the server using a stream UDF over a secondary-index query.
	/// </summary>
	public override void RunExample()
	{
		const string indexName = "avgindex";
		const string binName = "l2";

		console.Info($"Query for: ns={ns} set={set} index={indexName} bin={binName}");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			Filter = Filter.Equal(binName, 1)
		};

		using ResultSet rs = client.QueryAggregate(null, stmt, "average_example", "average");

		if (rs.Next() && rs.Object is Dictionary<object, object> map)
		{
			double sum = Convert.ToDouble((long)map["sum"]);
			double count = Convert.ToDouble((long)map["count"]);
			double avg = sum / count;
			console.Info($"Sum={sum} Count={count} Average={avg}");
		}
	}
}
