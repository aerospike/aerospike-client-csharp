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

using Microsoft.Extensions.Configuration;
using Aerospike.Client.Config;

namespace Aerospike.Client
{
    public class AerospikeYamlConfigProvider : IAerospikeConfigProvider
    {
        private IConfigurationRoot configRoot;

		public ConfigurationData ConfigurationData { get; private set; }

        public string YamlFilePath;

		public AerospikeYamlConfigProvider()
        {
            YamlFilePath = "." + Path.DirectorySeparatorChar + "aerospikeconfig.yaml";
            configRoot = new ConfigurationBuilder()
				.AddYamlFile(YamlFilePath, optional: false, reloadOnChange: true)
                .Build();
            ConfigurationData = new()
            {
                metaData = new MetaData(),
                staticProperties = new StaticProperties(),
                dynamicProperties = new DynamicProperties()
            };
        }

		public AerospikeYamlConfigProvider(string path)
        {
            YamlFilePath = path;
            configRoot = new ConfigurationBuilder()
				.AddYamlFile(YamlFilePath, optional: false, reloadOnChange: true)
				.Build();
            ConfigurationData = new()
            {
                metaData = new MetaData(),
                staticProperties = new StaticProperties(),
                dynamicProperties = new DynamicProperties()
            };
        }

        public void Watch()
        {
            _ = this.configRoot.GetReloadToken().RegisterChangeCallback(_ => {
                ProcessDynamicConfig();
                Watch();
            }, null);
        }

        public void InitalizeConfig()
		{
            ProcessStaticConfig();
            ProcessDynamicConfig();
        }

        private void ProcessStaticConfig()
        {
            configRoot.GetSection("metadata").Bind(ConfigurationData.metaData);
            configRoot.GetSection("static").Bind(ConfigurationData.staticProperties);
        }

        private void ProcessDynamicConfig()
        {
            configRoot.GetSection("dynamic").Bind(ConfigurationData.dynamicProperties);
        }

		private static int GetInt(IConfigurationSection section, string propertyName)
		{
			string value = section.GetSection(propertyName).Value;
			if (value != null)
			{
				return Int32.Parse(value);
			}
			return 0;
		}

		private static bool GetBool(IConfigurationSection section, string propertyName)
		{
			string value = section.GetSection(propertyName).Value;
			if (value != null)
			{
				return Boolean.Parse(value);
			}
			return false;
		}

		private static int[] GetIntArray(IConfigurationSection section, string propertyName)
		{
			var arraySection = section.GetSection(propertyName);
			int count = arraySection.GetChildren().Count();
			var integers = new int[count];
			if (count > 0)
			{
				for (int i=0; i<count; i++)
				{
					integers[i] = Int32.Parse(arraySection.GetSection(i.ToString()).Value);
				}
			}

			return integers;
		}

		private static ReadModeAP GetReadModeAP(IConfigurationSection section)
		{
			string value = section.GetSection(READ_MODE_AP).Value;
			if (value != null)
			{
				return value switch
				{
					"ONE" => ReadModeAP.ONE,
					"ALL" => ReadModeAP.ALL,
					_ => ReadModeAP.ONE,
				};
			}
			return ReadModeAP.ONE;
		}

		private static ReadModeSC GetReadModeSC(IConfigurationSection section)
		{
			string value = section.GetSection(READ_MODE_SC).Value;
			if (value != null)
			{
				return value switch
				{
					"SESSION" => ReadModeSC.SESSION,
					"LINEARIZE" => ReadModeSC.LINEARIZE,
					"ALLOW_REPLICA" => ReadModeSC.ALLOW_REPLICA,
					"ALLOW_UNAVAILABLE" => ReadModeSC.ALLOW_UNAVAILABLE,
					_ => ReadModeSC.SESSION
				};
			}
			return ReadModeSC.SESSION;
		}

		private static Replica GetReplica(IConfigurationSection section)
		{
			string value = section.GetSection(REPLICA).Value;
			if (value != null)
			{
				return value switch
				{
					"MASTER" => Replica.MASTER,
					"MASTER_PROLES" => Replica.MASTER_PROLES,
					"SEQUENCE" => Replica.SEQUENCE,
					"PREFER_RACK" => Replica.PREFER_RACK,
					"RANDOM" => Replica.RANDOM,
					_ => Replica.MASTER
				};
			}
			return Replica.MASTER;
		}

		private static QueryDuration GetQueryDuration(IConfigurationSection section)
		{
			string value = section.GetSection("expected_duration").Value;
			if (value != null)
			{
				return value switch
				{
					"LONG" => QueryDuration.LONG,
					"SHORT" => QueryDuration.SHORT,
					"LONG_RELAX_AP" => QueryDuration.LONG_RELAX_AP,
					_ => QueryDuration.LONG
				};
			}
			return QueryDuration.LONG;
		}
	}
}
