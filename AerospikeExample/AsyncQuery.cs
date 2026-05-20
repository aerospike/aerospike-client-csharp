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

public sealed class AsyncQuery : AsyncExample
{
	private readonly ManualResetEventSlim completed = new();

	/// <summary>
	/// Run a secondary-index range query asynchronously and stream results to a listener.
	/// </summary>
	public override void RunExample()
	{
		const string binName = "asqbin";
		const int begin = 26;
		const int end = 34;

		completed.Reset();

		console.Info($"Query for: ns={ns} set={set} bin={binName} >= {begin} <= {end}");

		Statement stmt = new()
		{
			Namespace = ns,
			SetName = set,
			BinNames = [binName],
			Filter = Filter.Range(binName, begin, end)
		};

		QueryPolicy qp = new()
		{
			failOnClusterChange = true
		};

		client.Query(qp, new RecordSequenceHandler(this, binName), stmt);
		completed.Wait();
	}

	private void NotifyCompleted() => completed.Set();

	private sealed class RecordSequenceHandler(AsyncQuery parent, string binName) : RecordSequenceListener
	{
		public void OnRecord(Key key, Record record)
		{
			parent.console.Info($"Result: {record.GetInt(binName)}");
		}

		public void OnSuccess() => parent.NotifyCompleted();

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Query failed: {Util.GetErrorMessage(e)}");
			parent.NotifyCompleted();
		}
	}
}
