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
using System.Reflection;
using System.Text.RegularExpressions;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// U-11: public discovery identifiers must not match /nlb/i.
	/// </summary>
	[TestClass]
	public class TestDiscoveryNaming
	{
		private static readonly Regex Nlb = new Regex("nlb", RegexOptions.IgnoreCase | RegexOptions.Compiled);

		[TestMethod]
		public void PublicDiscoverySurfaceHasNoNlbNames()
		{
			Assembly assembly = typeof(AerospikeClient).Assembly;
			List<string> hits = new List<string>();

			foreach (Type type in assembly.GetExportedTypes())
			{
				if (!IsDiscoverySurface(type))
				{
					continue;
				}

				Collect(hits, type.FullName);

				foreach (MemberInfo member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
				{
					Collect(hits, type.FullName + "." + member.Name);
				}
			}

			Assert.AreEqual(0, hits.Count, string.Join(", ", hits));
		}

		private static bool IsDiscoverySurface(Type type)
		{
			string ns = type.Namespace ?? "";
			return ns.StartsWith("Aerospike.Client", StringComparison.Ordinal) &&
				(ns.Contains("Discovery", StringComparison.Ordinal) ||
				 type.Name.Contains("Seed", StringComparison.Ordinal) ||
				 type.Name.Contains("Translat", StringComparison.Ordinal) ||
				 type.Name.Contains("Endpoint", StringComparison.Ordinal) ||
				 type.Name.Contains("Discovery", StringComparison.Ordinal));
		}

		private static void Collect(List<string> hits, string name)
		{
			if (name != null && Nlb.IsMatch(name))
			{
				hits.Add(name);
			}
		}
	}
}
