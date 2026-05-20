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

public sealed class ScanResume : SyncExample
{
	private const string SetName = "resume";

	private int recordCount;
	private int recordMax;

	/// <summary>
	/// Terminate a scan partway through and resume it from where it left off.
	/// </summary>
	public override void RunExample()
	{
		// Serialize node scans so scan callback atomics are not necessary.
		ScanPolicy scanPolicy = new()
		{
			concurrentNodes = false
		};

		PartitionFilter filter = PartitionFilter.All();
		recordCount = 0;
		recordMax = 50;

		console.Info("Start scan terminate");

		try
		{
			client.ScanPartitions(scanPolicy, filter, ns, SetName, ScanCallback);
		}
		catch (AerospikeException ae) when (
			ae is AerospikeException.ScanTerminated ||
			ae.InnerException is AerospikeException.ScanTerminated)
		{
			console.Info("Scan terminated as expected");
		}

		console.Info($"Records returned: {recordCount}");

		// PartitionFilter could be serialized at this point. Resume the scan now.
		recordCount = 0;
		recordMax = 0;

		console.Info("Start scan resume");
		client.ScanPartitions(scanPolicy, filter, ns, SetName, ScanCallback);
		console.Info($"Records returned: {recordCount}");
	}

	private void ScanCallback(Key key, Record record)
	{
		recordCount++;

		if (recordMax > 0 && recordCount >= recordMax)
		{
			// Terminating the scan rolls back the last-seen digest so the current record
			// is returned again when the scan is resumed later.
			throw new AerospikeException.ScanTerminated();
		}
	}
}
