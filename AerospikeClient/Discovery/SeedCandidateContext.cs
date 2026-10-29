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
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Aerospike.Client
{
	/// <summary>
	/// Read-only context passed to <see cref="ISeedCandidateProvider.RefreshSeedCandidates"/>
	/// on each call.
	/// </summary>
	public sealed class SeedCandidateContext
	{
		/// <summary>
		/// Expected cluster name from <see cref="ClientPolicy.clusterName"/>.
		/// May be null.
		/// </summary>
		public readonly string clusterName;

		/// <summary>
		/// Static seed hosts passed to the client constructor. Unmodifiable.
		/// </summary>
		public readonly IList<Host> configuredSeeds;

		/// <summary>
		/// Cluster tend count when the provider is called: 0 at client initialization, then the number
		/// of completed tend iterations. Informational only.
		/// </summary>
		public readonly long tendIteration;

		/// <summary>
		/// Initialize seed candidate context.
		/// </summary>
		public SeedCandidateContext(string clusterName, Host[] configuredSeeds, long tendIteration)
		{
			this.clusterName = clusterName;
			Host[] copy = (Host[])configuredSeeds.Clone();
			this.configuredSeeds = new ReadOnlyCollection<Host>(copy);
			this.tendIteration = tendIteration;
		}

		/// <inheritdoc/>
		public override string ToString()
		{
			return clusterName + ' ' + configuredSeeds + ' ' + tendIteration;
		}
	}
}
