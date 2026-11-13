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
namespace Aerospike.Client
{
	/// <summary>
	/// How the static seeds (the hosts passed to the client constructor) and the discovered seeds
	/// (the hosts returned by <see cref="ISeedCandidateProvider.RefreshSeedCandidates"/>)
	/// are combined into the seed list the client tries when it initializes the cluster or re-seeds
	/// because no nodes are active. The policy only orders seed attempts; it does not change node
	/// identity.
	/// <para>
	/// The seed list is rebuilt only when the provider is called: at client initialization and on each
	/// periodic refresh. Duplicate hosts (same name and port) are removed, keeping the first occurrence.
	/// A failed refresh keeps the previous seed list, and no refresh leaves the seed list empty.
	/// </para>
	/// <para>Default: <see cref="MERGE"/></para>
	/// </summary>
	public enum SeedMergePolicy
	{
		/// <summary>
		/// Try static seeds first, then discovered seeds. An empty discovery result leaves only the
		/// static seeds.
		/// <para>
		/// Use for migration to discovery, for resilience, and to keep a known-good endpoint while
		/// discovery catches up. This is the default.
		/// </para>
		/// </summary>
		MERGE,

		/// <summary>
		/// At client initialization, try static seeds first, then discovered seeds (as <see cref="MERGE"/>).
		/// After initialization, the discovered seeds of each successful refresh replace the seed list
		/// and static seeds are no longer tried. An empty discovery result after initialization keeps
		/// the previous seed list.
		/// <para>
		/// Use for greenfield cloud-native deployments where the static seeds are legacy.
		/// </para>
		/// </summary>
		REPLACE,

		/// <summary>
		/// Try discovered seeds only. Static seeds are used only when discovery returns an empty
		/// result, at client initialization or on a refresh.
		/// <para>
		/// Use when the static seeds are an emergency fallback only.
		/// </para>
		/// </summary>
		DISCOVERY_ONLY
	}
}
