/* 
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
using System;

namespace Aerospike.Client
{
	[Serializable]
	public sealed class PartitionStatus
	{
		public ulong bval;
		public byte[] digest;
		public readonly int id;
		[NonSerialized] public Node node;
		[NonSerialized] public int sequence;
		public bool retry;

		public PartitionStatus(int id)
		{
			this.id = id;
			this.retry = true;
		}
	}
}
