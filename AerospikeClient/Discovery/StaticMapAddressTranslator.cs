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
	/// Address translator that replaces the advertised host name with the value mapped to it.
	/// The port is kept. A host name that is not mapped, or any host name when the map is null, is a
	/// miss.
	/// </summary>
	public sealed class StaticMapAddressTranslator : IAddressTranslator
	{
		private readonly Dictionary<string, string> ipMap;

		/// <summary>
		/// Initialize translator with a map of advertised host name to the host name to connect to.
		/// The map is referenced, not copied. It may be null.
		/// </summary>
		public StaticMapAddressTranslator(Dictionary<string, string> ipMap)
		{
			this.ipMap = ipMap;
		}

		/// <summary>
		/// Return the advertised endpoint with its host name replaced when mapped, or null when not
		/// mapped. In-memory lookup only.
		/// </summary>
		public Host Translate(Endpoint advertised)
		{
			if (ipMap == null)
			{
				return null;
			}

			string host;

			if (!ipMap.TryGetValue(advertised.host, out host) || host == null)
			{
				return null;
			}

			return new Host(host, advertised.tlsName, advertised.port);
		}
	}
}
