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

namespace Aerospike.Client
{
	/// <summary>
	/// Seed candidate provider that returns a fixed list of hosts. This provider does no I/O and
	/// needs no periodic refresh.
	/// </summary>
	public sealed class StaticSeedCandidateProvider : ISeedCandidateProvider
	{
		private readonly List<Host> hosts;

		/// <summary>
		/// Initialize provider with fixed seed hosts.
		/// </summary>
		public StaticSeedCandidateProvider(params Host[] hosts)
		{
			this.hosts = new List<Host>(hosts ?? System.Array.Empty<Host>());
		}

		/// <summary>
		/// Return the fixed seed hosts.
		/// </summary>
		public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
		{
			return new List<Host>(hosts);
		}

		/// <summary>
		/// Return false. The seed hosts never change.
		/// </summary>
		public bool NeedsPeriodicRefresh()
		{
			return false;
		}

		/// <summary>
		/// Return true. The provider does no I/O.
		/// </summary>
		public bool SupportsDeadline()
		{
			return true;
		}
	}
}
