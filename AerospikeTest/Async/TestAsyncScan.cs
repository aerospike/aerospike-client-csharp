﻿/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncScan : TestAsync
	{
		private int recordCount;

		[TestMethod]
		public void AsyncScan()
		{
			recordCount = 0;

			ScanPolicy policy = new();
			client.ScanAll(policy, new RecordSequenceHandler(this), SuiteHelpers.ns, SuiteHelpers.set);
			WaitTillComplete();
		}

		private class RecordSequenceHandler(TestAsyncScan parent) : RecordSequenceListener
		{
			public void OnRecord(Key key, Record record)
			{
				parent.recordCount++;

				if ((parent.recordCount % 10000) == 0)
				{
					;
				}
			}

			public void OnSuccess()
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}
	}
}
