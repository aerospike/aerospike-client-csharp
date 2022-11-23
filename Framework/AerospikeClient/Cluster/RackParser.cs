/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using System.Collections.Generic;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Parse rack-ids info command.
	/// </summary>
	public sealed class RackParser : Info
	{
		internal const string RebalanceGeneration = "rebalance-generation";
		internal const string RackIds = "rack-ids";

		private readonly Dictionary<string,int> racks;
		private readonly int generation;

		public RackParser(Connection conn, Node node)
			: base(conn, RebalanceGeneration, RackIds)
		{
			// Send format: rebalance-generation\nrack-ids\n
			if (length == 0)
			{
				throw new AerospikeException.Parse("rack-ids response is empty");
			}

			this.racks = new Dictionary<string, int>();
			generation = ParseGeneration();
			ParseRacks(node);
		}

		public int Generation
		{
			get {return generation;}
		}

		public Dictionary<string,int> Racks
		{
			get {return racks;}
		}

		private int ParseGeneration()
		{
			ParseName(RebalanceGeneration);
			int gen = ParseInt();
			Expect('\n');
			return gen;
		}

		private void ParseRacks(Node node)
		{
			// Use low-level info methods and parse byte array directly for maximum performance.
			// Receive format: rack-ids\t<ns1>:<rack1>;<ns2>:<rack2>...\n
			ParseName(RackIds);

			int begin = offset;

			while (offset < length)
			{
				if (buffer[offset] == ':')
				{
					// Parse namespace.
					string ns = ByteUtil.Utf8ToString(buffer, begin, offset - begin).Trim();

					if (ns.Length <= 0 || ns.Length >= 32)
					{
						string response = GetTruncatedResponse();
						throw new AerospikeException.Parse("Invalid racks namespace " + ns + ". Response=" + response);
					}
					begin = ++offset;

					// Parse rack.
					while (offset < length)
					{
						byte b = buffer[offset];

						if (b == ';' || b == '\n')
						{
							break;
						}
						offset++;
					}
					int rack = Convert.ToInt32(Encoding.UTF8.GetString(buffer, begin, offset - begin));

					racks[ns] = rack;
					begin = ++offset;
				}
				else
				{
					offset++;
				}
			}
		}
	}
}
