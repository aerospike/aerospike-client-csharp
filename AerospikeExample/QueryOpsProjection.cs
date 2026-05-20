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

public sealed class QueryOpsProjection : SyncExample
{
	/// <summary>
	/// Run a query and project bins and computed expressions in a single round trip.
	/// </summary>
	public override void RunExample()
	{
		RequireMinServerVersion(Node.SERVER_VERSION_8_1_2);

		SimpleBinProjection();
		ExpressionBasedBinProjection();
	}

	private void SimpleBinProjection()
	{
		Statement statement = new()
		{
			Namespace = ns,
			SetName = set,
			Operations =
			[
				Operation.Get("test-bin-1"),
				Operation.Get("test-bin-2"),
				MapOperation.GetByKey("test-map-bin", Value.Get("a"), MapReturnType.VALUE)
			]
		};

		using RecordSet recordSet = client.Query(null, statement);

		while (recordSet.Next())
		{
			Key key = recordSet.Key;
			Record record = recordSet.Record;

			console.Info($"Key: {key.userKey}");
			console.Info($"  test-bin-1: {record.GetValue("test-bin-1")}");
			console.Info($"  test-bin-2: {record.GetValue("test-bin-2")}");
			console.Info($"  test-map-bin: {record.GetValue("test-map-bin")}");
		}
	}

	private void ExpressionBasedBinProjection()
	{
		Statement statement = new()
		{
			Namespace = ns,
			SetName = set,
			Operations =
			[
				ExpOperation.Read(
					"original-value",
					Exp.Build(Exp.IntBin("counter")),
					ExpReadFlags.DEFAULT),

				ExpOperation.Read(
					"doubled-value",
					Exp.Build(Exp.Mul(Exp.IntBin("counter"), Exp.Val(2))),
					ExpReadFlags.DEFAULT),

				ExpOperation.Read(
					"status",
					Exp.Build(
						Exp.Cond(
							Exp.GE(Exp.IntBin("counter"), Exp.Val(100)), Exp.Val("high"),
							Exp.Val("low"))),
					ExpReadFlags.DEFAULT)
			]
		};

		using RecordSet recordSet = client.Query(null, statement);

		while (recordSet.Next())
		{
			Record record = recordSet.Record;
			console.Info($"Original: {record.GetValue("original-value")}");
			console.Info($"Doubled: {record.GetValue("doubled-value")}");
			console.Info($"Status: {record.GetValue("status")}");
		}
	}
}
