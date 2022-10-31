/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class ScanCommand : MultiCommand
	{
		private readonly ScanPolicy scanPolicy;
		private readonly string setName;
		private readonly string[] binNames;
		private readonly ScanCallback callback;
		private readonly ulong taskId;

		public ScanCommand
		(
			Cluster cluster,
			Node node,
			ScanPolicy scanPolicy,
			string ns,
			string setName,
			string[] binNames,
			ScanCallback callback,
			ulong taskId,
			ulong clusterKey,
			bool first
		) : base(cluster, scanPolicy, node, ns, clusterKey, first, LatencyType.NONE)
		{
			this.scanPolicy = scanPolicy;
			this.setName = setName;
			this.binNames = binNames;
			this.callback = callback;
			this.taskId = taskId;
		}

		public override void Execute()
		{
			ExecuteAndValidate();
		}
		
		protected internal override void WriteBuffer()
		{
			SetScan(scanPolicy, ns, setName, binNames, taskId, null);
		}

		protected internal override void ParseRow(Key key)
		{
			Record record = ParseRecord();

			if (!valid)
			{
				throw new AerospikeException.ScanTerminated();
			}

			callback(key, record);
		}
	}
}
