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
	/// Rewrite of a server advertised address into the address the client connects to. The client
	/// applies the translator to every address returned by peers and service info commands.
	/// <para>
	/// When no translator is configured, <see cref="StaticMapAddressTranslator"/> is used with
	/// <see cref="ClientPolicy.ipMap"/>.
	/// </para>
	/// </summary>
	public interface IAddressTranslator
	{
		/// <summary>
		/// Return the host the client should connect to for the advertised endpoint, or null when
		/// the translator has no mapping for it (a miss). A non-null value is a hit, even when it
		/// equals the advertised host and port. The client applies
		/// <see cref="ClientPolicy.translationOnMiss"/> to a miss.
		/// <para>
		/// This method is called on the cluster tend thread and must be in-memory only. It must not
		/// perform I/O of any kind, including DNS resolution.
		/// </para>
		/// <para>
		/// The TLS name is never rewritten. The client always uses the advertised endpoint's TLS name,
		/// regardless of the TLS name on the returned host.
		/// </para>
		/// </summary>
		Host Translate(Endpoint advertised);

		/// <summary>
		/// Notify the translator that the cluster view changed, so it can drop cached state.
		/// The default does nothing.
		/// <para>
		/// The client keeps one counter per client instance, starting at 0, and adds one each time a peers
		/// refresh adds or removes a node or changes the address a node advertises. This method is then
		/// called with the new counter value. The cluster view established at client initialization is
		/// the starting view and is not notified.
		/// </para>
		/// <para>
		/// This method is called on the cluster tend thread and must be in-memory only. An exception is
		/// logged at warning level and does not stop the cluster tend.
		/// </para>
		/// </summary>
		/// <param name="viewChange">counter value: 1 for the first change, then 2, 3, ...</param>
		void OnClusterViewChanged(int viewChange)
		{
		}
	}
}
