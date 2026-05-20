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

public sealed class QueryExecute : SyncExample
{
	/// <summary>
	/// Run a user-defined function against every record matching a query filter.
	/// </summary>
	public override void RunExample()
	{
		const string indexName = "qeindex1";
		const string binName1 = "qebin1";
		const string binName2 = "qebin2";
		const int begin = 3;
		const int end = 9;

		console.Info($"For ns={ns} set={set} index={indexName} bin={binName1} >= {begin} <= {end}");
		console.Info($"Even integers: add 100 to existing {binName1}");
		console.Info($"Multiple of 5: delete {binName2} bin");
		console.Info("Multiple of 9: delete record");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			Filter = Filter.Range(binName1, begin, end)
		};

		ExecuteTask task = client.Execute(
			writePolicy, stmt,
			"record_example", "processRecord",
			Value.Get(binName1), Value.Get(binName2), Value.Get(100));
		task.Wait(3000, 3000);
	}
}
