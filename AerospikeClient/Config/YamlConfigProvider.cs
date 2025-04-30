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
	public class YamlConfigProvider : IConfigProvider
	{
		private const string CONFIG_PATH_ENV = "AEROSPIKE_CLIENT_CONFIG_URL";

		private IConfigurationRoot configRoot;

		public ConfigurationData ConfigurationData { get; private set; }

		public int Interval { get; private set; }

		private string yamlFilePath;

		private volatile bool modified = false;

		public YamlConfigProvider()
		{
			ConfigurationData = new()
			{
				staticConfig = new StaticConfig(),
				dynamicConfig = new DynamicConfig()
			};

			Interval = IConfigProvider.DEFAULT_INTERVAL;

			SetYamlFilePath();
			LoadConfig(); 
		}

		private void Watch()
		{
			_ = this.configRoot.GetReloadToken().RegisterChangeCallback(_ => {
				ProcessDynamicConfig();
				Watch();
			}, null);
		}

		public bool LoadConfig()
		{
			if (yamlFilePath == null)
			{
				Log.Error("The YAML config file path has not been set. Check the " + CONFIG_PATH_ENV + " env variable");
				return false;
			}

			try
			{
				if (configRoot == null)
				{
					InitalizeConfig();
					modified = false;
					return true;
				}
				else if (modified) // Modified is set in the callback from the watch if file changes
				{
					modified = false;
					return true;
				}
			}
			catch (FileNotFoundException e)
			{
				Log.Error("YAML configuration file could not be found at: " + yamlFilePath + e.Message);
			}
			catch (IOException e)
			{
				Log.Error("YAML Configuration file could not be read from: " + yamlFilePath + ". " + e.Message);
			}
			catch (Exception e)
			{
				Log.Error("Unable to parse YAML file: " + e.Message);
			}
			return false;

		}

		private void InitalizeConfig()
		{
			configRoot = new ConfigurationBuilder()
			   .AddYamlFile(yamlFilePath, optional: false, reloadOnChange: true)
			   .Build();

			ProcessStaticConfig();
			ProcessDynamicConfig();

			Watch();
		}

		private void ProcessStaticConfig()
		{
			configRoot.GetSection("static").Bind(ConfigurationData.staticConfig);

			if (ConfigurationData.staticConfig.client.config_interval.HasValue)
			{
				Interval = ConfigurationData.staticConfig.client.config_interval.Value;
			}
			else
			{
				Interval = IConfigProvider.DEFAULT_INTERVAL;
			}
		}

		private void ProcessDynamicConfig()
		{
			var dynamicSection = configRoot.GetSection("dynamic");
			if (!dynamicSection.Exists())
			{
				// disable dynamic config
				ConfigurationData = null;
			}
			else
			{
				dynamicSection.Bind(ConfigurationData.dynamicConfig);
			}
			modified = true;
		}

		private void SetYamlFilePath()
		{
			try
			{
				string configPath = Environment.GetEnvironmentVariable(CONFIG_PATH_ENV);
				Uri envUri = new(configPath);
				if (envUri.IsFile)
				{
					yamlFilePath = envUri.AbsolutePath;
				}
				else
				{
					Log.Error("Could not parse the " + CONFIG_PATH_ENV + " env var");
				}
			}
			catch (Exception e)
			{
				Log.Error("Error setting config path " + e);
			}
		}
	}
}
