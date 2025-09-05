/* 
 * Copyright 2012-2025 Aerospike, Inc.
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

using Aerospike.Client.Config;

namespace Aerospike.Client
{
	public interface IConfigProvider
	{
		/// <summary>
		/// Default interval in milliseconds between dynamic configuration check for file modifications.
		/// </summary>
		internal const int DEFAULT_CONFIG_INTERVAL = 60000;

		public IConfigurationData ConfigurationData { get; }

		/// <summary>
		/// Milliseconds between dynamic configuration check for file modifications.
		/// </summary>
		public int Interval { get; }

		public bool LoadConfig();

		public static void LogStringChange(string configValue, string policyValue, string section, string field)
		{
			if (configValue != policyValue)
			{
				Log.Info($"Set {section}.{field} = {configValue}");
			}
		}

		public static void LogIntChange(int? configValue, int policyValue, string section, string field)
		{
			if (configValue.HasValue)
			{
				if (configValue.Value != policyValue)
				{
					Log.Info($"Set {section}.{field} = {configValue}");
				}
			}
		}

		public static void LogBoolChange(bool? configValue, bool policyValue, string section, string field)
		{
			if (configValue.HasValue)
			{
				if (configValue.Value != policyValue)
				{
					Log.Info($"Set {section}.{field} = {configValue}");
				}
			}
		}

		public static void LogRackIdsChange(int[] configValue, List<int> policyValue, string section, string field)
		{
			if (!Cluster.RackIdsEqual(policyValue, configValue))
			{
				Log.Info($"Set {section}.{field} = {String.Join(",", configValue)}");
			}
		}

		public static void LogReadModeAPChange(ReadModeAP? configValue, ReadModeAP policyValue, string section, string field)
		{
			if (configValue.HasValue)
			{
				if (configValue.Value != policyValue)
				{
					Log.Info($"Set {section}.{field} = {configValue}");
				}
			}
		}

		public static void LogReadModeSCChange(ReadModeSC? configValue, ReadModeSC policyValue, string section, string field)
		{
			if (configValue.HasValue)
			{
				if (configValue.Value != policyValue)
				{
					Log.Info($"Set {section}.{field} = {configValue}");
				}
			}
		}

		public static void LogReplicaChange(Replica? configValue, Replica policyValue, string section, string field)
		{
			if (configValue.HasValue)
			{
				if (configValue.Value != policyValue)
				{
					Log.Info($"Set {section}.{field} = {configValue}");
				}
			}
		}

		public static void LogQueryDurationChange(QueryDuration? configValue, QueryDuration policyValue, string section, string field)
		{
			if (configValue.HasValue)
			{
				if (configValue.Value != policyValue)
				{
					Log.Info($"Set {section}.{field} = {configValue}");
				}
			}
		}

		public static void LogStringDictionaryChange(Dictionary<string, string> configValue, Dictionary<string, string> policyValue, string section, string field)
		{
			if (!AreStringDictionaryEqual(configValue, policyValue))
			{
				Log.Info($"Set {section}.{field} = {String.Join(", ", configValue)}");
			}
		}

		public static bool AreStringDictionaryEqual(Dictionary<string, string> dict1, Dictionary<string, string> dict2)
		{
			if (dict1 == null)
			{
				return dict2 == null;
			}
			else if (dict2 == null)
			{
				return false;
			}

			if (dict1.Count != dict2.Count)
			{
				return false;
			}

			foreach (var kvp in dict1)
			{
				if (!dict2.TryGetValue(kvp.Key, out string value) || !value.Equals(kvp.Value))
				{
					return false;
				}
			}
			return true;
		}
	}
}
