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

public sealed class AsyncBatch : AsyncExample
{
	private const string KeyPrefix = "batchkey";
	private const string BinName = "batchbin";
	private const int BatchSize = 8;
	private const int TotalTasks = 6;

	private readonly ManualResetEventSlim completed = new();
	private Key[] sendKeys;
	private int tasksComplete;

	/// <summary>
	/// Demonstrate asynchronous batch operations: exists, get, headers, and complex reads.
	/// </summary>
	public override void RunExample()
	{
		completed.Reset();
		tasksComplete = 0;
		sendKeys = BuildKeys();

		BatchExistsArray();
		BatchExistsSequence();
		BatchGetArray();
		BatchGetSequence();
		BatchGetHeaders();
		BatchReadComplex();

		completed.Wait();
	}

	private Key[] BuildKeys()
	{
		Key[] keys = new Key[BatchSize];

		for (int i = 0; i < BatchSize; i++)
		{
			keys[i] = new Key(ns, set, $"{KeyPrefix}{i + 1}");
		}

		return keys;
	}

	private void BatchExistsArray() =>
		client.Exists(null, new ExistsArrayHandler(this), sendKeys);

	private void BatchExistsSequence() =>
		client.Exists(null, new ExistsSequenceHandler(this), sendKeys);

	private void BatchGetArray() =>
		client.Get(null, new RecordArrayHandler(this), sendKeys);

	private void BatchGetSequence() =>
		client.Get(null, new RecordSequenceHandler(this), sendKeys);

	private void BatchGetHeaders() =>
		client.GetHeader(null, new RecordHeaderArrayHandler(this), sendKeys);

	private void BatchReadComplex()
	{
		string[] bins = [BinName];
		List<BatchRead> records =
		[
			new(new Key(ns, set, $"{KeyPrefix}1"), bins),
			new(new Key(ns, set, $"{KeyPrefix}2"), true),
			new(new Key(ns, set, $"{KeyPrefix}3"), true),
			new(new Key(ns, set, $"{KeyPrefix}4"), false),
			new(new Key(ns, set, $"{KeyPrefix}5"), true),
			new(new Key(ns, set, $"{KeyPrefix}6"), true),
			new(new Key(ns, set, $"{KeyPrefix}7"), bins),
			// This record exists, but the requested bin will not.
			new(new Key(ns, set, $"{KeyPrefix}8"), ["binnotfound"]),
			// This record will not be found.
			new(new Key(ns, set, "keynotfound"), bins),
		];

		client.Get(null, new BatchListHandler(this), records);
	}

	private void TaskComplete()
	{
		if (Interlocked.Increment(ref tasksComplete) >= TotalTasks)
		{
			completed.Set();
		}
	}

	private sealed class ExistsArrayHandler(AsyncBatch parent) : ExistsArrayListener
	{
		public void OnSuccess(Key[] keys, bool[] existsArray)
		{
			for (int i = 0; i < existsArray.Length; i++)
			{
				Key key = keys[i];
				parent.console.Info($"Record: namespace={key.ns} set={key.setName} key={key.userKey} exists={existsArray[i]}");
			}

			parent.TaskComplete();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Batch exists array failed: {Util.GetErrorMessage(e)}");
			parent.TaskComplete();
		}
	}

	private sealed class ExistsSequenceHandler(AsyncBatch parent) : ExistsSequenceListener
	{
		public void OnExists(Key key, bool exists)
		{
			parent.console.Info($"Record: namespace={key.ns} set={key.setName} digest={ByteUtil.BytesToHexString(key.digest)} exists={exists}");
		}

		public void OnSuccess() => parent.TaskComplete();

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Batch exists sequence failed: {Util.GetErrorMessage(e)}");
			parent.TaskComplete();
		}
	}

	private sealed class RecordArrayHandler(AsyncBatch parent) : RecordArrayListener
	{
		public void OnSuccess(Key[] keys, Record[] records)
		{
			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];

				if (record == null)
				{
					parent.console.Error($"Record not found: namespace={key.ns} set={key.setName} key={key.userKey} bin={BinName}");
				}
				else
				{
					parent.console.Info($"Record: namespace={key.ns} set={key.setName} key={key.userKey} bin={BinName} value={record.GetValue(BinName)}");
				}
			}

			parent.console.Info($"Records returned: {records.Length}");
			parent.TaskComplete();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Batch get array failed: {Util.GetErrorMessage(e)}");
			parent.TaskComplete();
		}
	}

	private sealed class RecordSequenceHandler(AsyncBatch parent) : RecordSequenceListener
	{
		public void OnRecord(Key key, Record record)
		{
			if (record == null)
			{
				parent.console.Error($"Record not found: namespace={key.ns} set={key.setName} digest={ByteUtil.BytesToHexString(key.digest)} bin={BinName}");
			}
			else
			{
				parent.console.Info($"Record: namespace={key.ns} set={key.setName} digest={ByteUtil.BytesToHexString(key.digest)} bin={BinName} value={record.GetValue(BinName)}");
			}
		}

		public void OnSuccess() => parent.TaskComplete();

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Batch get sequence failed: {Util.GetErrorMessage(e)}");
			parent.TaskComplete();
		}
	}

	private sealed class RecordHeaderArrayHandler(AsyncBatch parent) : RecordArrayListener
	{
		public void OnSuccess(Key[] keys, Record[] records)
		{
			for (int i = 0; i < records.Length; i++)
			{
				Key key = keys[i];
				Record record = records[i];

				if (record == null || (record.generation == 0 && record.expiration == 0))
				{
					parent.console.Error($"Record not found: namespace={key.ns} set={key.setName} key={key.userKey}");
				}
				else
				{
					parent.console.Info($"Record: namespace={key.ns} set={key.setName} key={key.userKey} generation={record.generation} expiration={record.expiration}");
				}
			}

			parent.console.Info($"Headers returned: {records.Length}");
			parent.TaskComplete();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Batch get headers failed: {Util.GetErrorMessage(e)}");
			parent.TaskComplete();
		}
	}

	private sealed class BatchListHandler(AsyncBatch parent) : BatchListListener
	{
		public void OnSuccess(List<BatchRead> records)
		{
			int found = 0;

			foreach (BatchRead record in records)
			{
				Key key = record.key;
				Record rec = record.record;

				if (rec != null)
				{
					found++;
					parent.console.Info($"Record: ns={key.ns} set={key.setName} key={key.userKey} bin={BinName} value={rec.GetValue(BinName)}");
				}
				else
				{
					parent.console.Info($"Record not found: ns={key.ns} set={key.setName} key={key.userKey} bin={BinName}");
				}
			}

			parent.console.Info($"Records found: {found}");
			parent.TaskComplete();
		}

		public void OnFailure(AerospikeException e)
		{
			parent.console.Error($"Batch read complex failed: {Util.GetErrorMessage(e)}");
			parent.TaskComplete();
		}
	}
}
