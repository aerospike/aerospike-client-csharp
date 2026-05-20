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

public sealed class QueryList : SyncExample
{
	/// <summary>
	/// Query a list-typed secondary index by element value.
	/// </summary>
	public override void RunExample()
	{
		const string binName = "listbin";

		console.Info("Query list bins");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			BinNames = [binName],
			Filter = Filter.Contains(binName, IndexCollectionType.LIST, "905")
		};

		using RecordSet rs = client.Query(null, stmt);

		int count = 0;

		while (rs.Next())
		{
			count++;
			IList list = rs.Record.GetList(binName);

			console.Info($"Record {count}");

			foreach (object item in list)
			{
				console.Info(item.ToString());
			}
		}
	}
}
