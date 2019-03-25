/* 
 * Copyright 2012-2019 Aerospike, Inc.
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
	public sealed class Partitions
	{
		internal readonly Node[][] replicas;
		internal readonly int[] regimes;
		internal readonly bool scMode;

		public Partitions(int partitionCount, int replicaCount, bool scMode)
		{
			this.replicas = new Node[replicaCount][];

			for (int i = 0; i < replicaCount; i++)
			{
				this.replicas[i] = new Node[partitionCount];
			}
			this.regimes = new int[partitionCount];
			this.scMode = scMode;
		}

		/// <summary>
		/// Copy partition map while reserving space for a new replica count.
		/// </summary>
		public Partitions(Partitions other, int replicaCount)
		{
			this.replicas = new Node[replicaCount][];

			if (other.replicas.Length < replicaCount)
			{
				int i = 0;

				// Copy existing entries.
				for (; i < other.replicas.Length; i++)
				{
					this.replicas[i] = other.replicas[i];
				}

				// Create new entries.
				for (; i < replicaCount; i++)
				{
					this.replicas[i] = new Node[other.regimes.Length];
				}
			}
			else
			{
				// Copy existing entries.
				for (int i = 0; i < replicaCount; i++)
				{
					this.replicas[i] = other.replicas[i];
				}
			}
			this.regimes = other.regimes;
			this.scMode = other.scMode;
		}
	}
}
