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

public sealed class QueryPrimary : SyncExample
{
	internal const string SetName = "query-primary";

	public override void RunExample()
	{
		QuerySet();
		QueryWithRecordSizeFilter();
	}

	private void QuerySet()
	{
		Statement stmt = new()
		{
			Namespace = ns,
			SetName = SetName,
			// MaxRecords is a cluster-wide target that the client divides among
			// nodes, so an uneven distribution can return fewer records.
			MaxRecords = 20
		};

		using (RecordSet recordSet = client.Query(null, stmt))
		{
			while (recordSet.Next())
			{
				console.Info($"Key: {recordSet.Key.userKey} | Record: {recordSet.Record}");
			}
		}
	}

	// Exp.RecordSize() requires server 7.0+. Exp.DeviceSize() and Exp.MemorySize()
	// are the pre-7.0 equivalents and are deprecated as of server 8.1.
	private void QueryWithRecordSizeFilter()
	{
		RequireMinServerVersion(new Version(7, 0));

		QueryPolicy queryPolicy = new()
		{
			filterExp = Exp.Build(Exp.GT(Exp.RecordSize(), Exp.Val(1024 * 16)))
		};

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = SetName
		};

		using (RecordSet recordSet = client.Query(queryPolicy, stmt))
		{
			while (recordSet.Next())
			{
				console.Info($"Key: {recordSet.Key.userKey} | Record: {recordSet.Record}");
			}
		}
	}
}
