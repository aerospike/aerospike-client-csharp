/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncInfo : AsyncCommand
	{
		private readonly InfoListener listener;
		private readonly Node serverNode;
		private readonly string[] commands;
		private Dictionary<string,string> map;

		public AsyncInfo(AsyncCluster cluster, InfoPolicy policy, InfoListener listener, AsyncNode node, params string[] commands)
			: base(cluster, CreatePolicy(policy))
		{
			this.listener = listener;
			this.serverNode = node;
			this.commands = commands;
		}

		public AsyncInfo(AsyncInfo other)
			: base(other)
		{
			this.listener = other.listener;
			this.serverNode = other.serverNode;
			this.commands = other.commands;
		}

		private static Policy CreatePolicy(InfoPolicy policy)
		{
			Policy p = new Policy();

			if (policy == null) {
				p.SetTimeout(1000);
			}
			else {
				p.SetTimeout(policy.timeout);
			}
			return p;
		}

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncInfo(this);
		}

		protected internal override bool IsWrite()
		{
			return true;
		}

		protected internal override Node GetNode(Cluster cluster)
		{
			return serverNode;
		}

		protected override Latency.LatencyType GetLatencyType()
		{
			return Latency.LatencyType.NONE;
		}

		protected internal override void WriteBuffer()
		{
			dataOffset = 8;
		
			foreach (string command in commands)
			{
				dataOffset += ByteUtil.EstimateSizeUtf8(command) + 1;
			}
		
			SizeBuffer();
			dataOffset += 8; // Skip size field.

			// The command format is: <name1>\n<name2>\n...
			foreach (string command in commands)
			{
				dataOffset += ByteUtil.StringToUtf8(command, dataBuffer, dataOffset);
				dataBuffer[dataOffset++] = (byte)'\n';
			}
			EndInfo();
		}

		protected internal sealed override void ParseCommand()
		{
			Info info = new Info(dataBuffer, dataLength, dataOffset);
			map = info.ParseMultiResponse();
			Finish();
		}

		protected internal override bool PrepareRetry(bool timeout)
		{
			return true;
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(map);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
		}
	}
}
