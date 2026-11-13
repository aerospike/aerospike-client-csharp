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
	/// Source of seed hosts used to discover the cluster. The client consults the provider when
	/// the cluster is initialized and when it must re-seed because no nodes are reachable.
	/// <para>
	/// When no provider is configured, <see cref="StaticSeedCandidateProvider"/> is used with the hosts
	/// passed to the client constructor.
	/// </para>
	/// </summary>
	public interface ISeedCandidateProvider
	{
		/// <summary>
		/// Return the current seed candidates. Returned hosts are used as seeds, so they must carry the
		/// TLS name when TLS is enabled; a null TLS name is defaulted the same way as for configured seeds.
		/// <para>
		/// Called once at client initialization on the application thread that constructs the client,
		/// with <see cref="SeedCandidateContext.tendIteration"/> 0, and on each periodic refresh
		/// (background thread when <see cref="DiscoveryExecutionMode.THREAD"/>, or tend thread when
		/// <see cref="DiscoveryExecutionMode.TEND"/>). An exception at initialization is logged at warning
		/// level and the client continues with the seeds left by
		/// <see cref="ClientPolicy.seedMergePolicy"/>; when none are left, client construction fails with
		/// <see cref="ResultCode.SEED_DISCOVERY_FAILED"/>. An exception on a periodic refresh is logged
		/// at warning level and the previous seed list is kept.
		/// </para>
		/// </summary>
		/// <param name="ctx">cluster name, configured seeds and tend count for this call</param>
		List<Host> RefreshSeedCandidates(SeedCandidateContext ctx);

		/// <summary>
		/// Return true if the provider's candidates can change and must be refreshed periodically.
		/// Return false if the candidates are fixed, in which case the client schedules no periodic
		/// work for this provider. Called once on the application thread that constructs the client.
		/// </summary>
		bool NeedsPeriodicRefresh();

		/// <summary>
		/// Return true if <see cref="RefreshSeedCandidates"/> honors a deadline: it either does no I/O
		/// or bounds all of its I/O so that it can return within a caller-supplied time limit.
		/// Only providers that return true are eligible for <see cref="DiscoveryExecutionMode.TEND"/>.
		/// Called once on the application thread that constructs the client.
		/// </summary>
		bool SupportsDeadline();

		/// <summary>
		/// Release resources held by the provider. The default does nothing.
		/// </summary>
		void Close()
		{
		}
	}
}
