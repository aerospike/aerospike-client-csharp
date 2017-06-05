/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
namespace Aerospike.Client
{
	public sealed class AsyncScan : AsyncMultiCommand
	{
		private readonly ScanPolicy scanPolicy;
		private readonly RecordSequenceListener listener;
		private readonly string ns;
		private readonly string setName;
		private readonly string[] binNamesScan;
		private readonly ulong taskId;

		public AsyncScan
		(
			AsyncMultiExecutor parent,
			AsyncCluster cluster,
			AsyncNode node,
			ScanPolicy scanPolicy,
			RecordSequenceListener listener,
			string ns,
			string setName,
			string[] binNames,
			ulong taskId
		) : base(parent, cluster, scanPolicy, node, true)
		{
			this.scanPolicy = scanPolicy;
			this.listener = listener;
			this.ns = ns;
			this.setName = setName;
			this.binNamesScan = binNames;
			this.taskId = taskId;
		}

		protected internal override void WriteBuffer()
		{
			SetScan(scanPolicy, ns, setName, binNamesScan, taskId);
		}

		protected internal override void ParseRow(Key key)
		{
			Record record = ParseRecord();
			listener.OnRecord(key, record);
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return null;
		}
	}
}
