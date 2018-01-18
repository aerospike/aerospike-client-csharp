/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
	public sealed class Partition
	{
		public readonly string ns;
		public readonly uint partitionId;

		public Partition(Key key)
		{
			this.ns = key.ns;
			this.partitionId = BitConverter.ToUInt32(key.digest, 0) % Node.PARTITIONS;
		}

		public Partition(string ns, uint partitionId)
		{
			this.ns = ns;
			this.partitionId = partitionId;
		}

		public override string ToString()
		{
			return ns + ':' + partitionId;
		}

		public override int GetHashCode()
		{
			const int prime = 31;
			int result = prime + ns.GetHashCode();
			result = prime * result + (int)partitionId;
			return result;
		}

		public override bool Equals(object obj)
		{
			Partition other = (Partition) obj;
			return this.ns.Equals(other.ns) && this.partitionId == other.partitionId;
		}
	}
}
