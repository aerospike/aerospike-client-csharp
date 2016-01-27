/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
		private readonly ScanPolicy policy;
		private readonly string ns;
		private readonly string setName;
		private readonly ScanCallback callback;
		private readonly string[] binNames;
		private readonly ulong taskId;

		public ScanCommand
		(
			Node node, 
			ScanPolicy policy,
			string ns,
			string setName,
			ScanCallback callback,
			string[] binNames,
			ulong taskId
		) : base(node, true)
		{
			this.policy = policy;
			this.ns = ns;
			this.setName = setName;
			this.callback = callback;
			this.binNames = binNames;
			this.taskId = taskId;
		}

		protected internal override Policy GetPolicy()
		{
			return policy;
		}

		protected internal override void WriteBuffer()
		{
			SetScan(policy, ns, setName, binNames, taskId);
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
