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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Configuration;
using YamlDotNet.RepresentationModel;

namespace Aerospike.Client.Config
{
	/// <summary>
	/// Validation and helpers for <c>static.client.discovery</c> / <c>static.client.translation</c> YAML.
	/// </summary>
	internal static class DiscoveryYaml
	{
		internal static readonly string[] YamlSeedProviders =
		[
			StaticDiscoveryConfig.Static, StaticDiscoveryConfig.Dns, StaticDiscoveryConfig.AwsTags
		];
		internal static readonly string[] BuiltInSeedProviders =
		[
			StaticDiscoveryConfig.Static, StaticDiscoveryConfig.Dns, StaticDiscoveryConfig.AwsTags
		];
		internal static readonly string[] BuiltInTranslators =
		[
			StaticTranslationConfig.StaticMap, StaticTranslationConfig.PortMap, StaticTranslationConfig.PassThrough
		];

		private static readonly string[] MergePolicies = ["merge", "replace", "discovery_only"];
		private static readonly string[] OnMiss = ["pass_through", "strict"];
		private static readonly string[] ExecutionModes = ["thread", "tend"];
		private static readonly string[] RecordTypes = ["a", "srv"];
		private static readonly string[] AddressFields = ["private_ip", "public_ip", "private_dns", "public_dns"];

		internal static void FinalizeAndValidate(
			StaticClientConfig client,
			IConfigurationSection clientSection,
			string yamlFilePath)
		{
			if (client == null)
			{
				return;
			}

			// IConfiguration treats ':' as a path separator, so host:port map keys must be read
			// from the YAML document directly.
			LoadColonSafeMaps(client, yamlFilePath);

			IConfigurationSection discoverySection = clientSection?.GetSection("discovery");
			IConfigurationSection translationSection = clientSection?.GetSection("translation");

			if (discoverySection != null && discoverySection.Exists())
			{
				client.discovery ??= new StaticDiscoveryConfig();
				FinalizeDiscovery(client.discovery, discoverySection);
			}

			if (translationSection != null && translationSection.Exists())
			{
				client.translation ??= new StaticTranslationConfig();
				FinalizeTranslation(client.translation, translationSection);
			}

			if (client.translation?.static_map != null && client.ip_map != null)
			{
				throw new AerospikeException(
					"static.client.translation.static_map and static.client.ip_map are both set");
			}
		}

		private static void LoadColonSafeMaps(StaticClientConfig client, string yamlFilePath)
		{
			if (string.IsNullOrEmpty(yamlFilePath) || !File.Exists(yamlFilePath))
			{
				return;
			}

			YamlMappingNode root;

			using (StreamReader reader = new StreamReader(yamlFilePath))
			{
				YamlStream yaml = new YamlStream();
				yaml.Load(reader);

				if (yaml.Documents.Count == 0 || yaml.Documents[0].RootNode is not YamlMappingNode mapping)
				{
					return;
				}

				root = mapping;
			}

			YamlMappingNode clientNode = Map(Map(root, "static"), "client");

			if (clientNode == null)
			{
				return;
			}

			Dictionary<string, string> ipMap = StringMap(Map(clientNode, "ip_map"), "static.client.ip_map");

			if (ipMap != null)
			{
				client.ip_map = ipMap;
			}

			YamlMappingNode translation = Map(clientNode, "translation");

			if (translation != null)
			{
				client.translation ??= new StaticTranslationConfig();
				Dictionary<string, string> staticMap =
					StringMap(Map(translation, "static_map"), "static.client.translation.static_map");
				Dictionary<string, string> portMap =
					StringMap(Map(translation, "port_map"), "static.client.translation.port_map");

				if (staticMap != null)
				{
					client.translation.static_map = staticMap;
				}

				if (portMap != null)
				{
					client.translation.port_map = portMap;
				}
			}
		}

		private static YamlMappingNode Map(YamlMappingNode parent, string key)
		{
			if (parent == null)
			{
				return null;
			}

			foreach (KeyValuePair<YamlNode, YamlNode> entry in parent.Children)
			{
				if (entry.Key is YamlScalarNode scalar &&
					scalar.Value != null &&
					scalar.Value.Equals(key, StringComparison.Ordinal) &&
					entry.Value is YamlMappingNode child)
				{
					return child;
				}
			}

			return null;
		}

		private static Dictionary<string, string> StringMap(YamlMappingNode node, string path)
		{
			if (node == null)
			{
				return null;
			}

			Dictionary<string, string> map = new Dictionary<string, string>();

			foreach (KeyValuePair<YamlNode, YamlNode> entry in node.Children)
			{
				if (entry.Key is not YamlScalarNode keyNode || keyNode.Value == null)
				{
					throw new AerospikeException("Invalid " + path + " value: expected a string key");
				}

				if (entry.Value is not YamlScalarNode valueNode || valueNode.Value == null)
				{
					throw new AerospikeException(
						"Invalid " + path + '.' + keyNode.Value + " value: expected a string");
				}

				map[keyNode.Value] = valueNode.Value;
			}

			return map;
		}

		private static void FinalizeDiscovery(StaticDiscoveryConfig discovery, IConfigurationSection section)
		{
			string provider = discovery.provider;

			if (provider != null)
			{
				if (!YamlSeedProviders.Contains(provider))
				{
					discovery.options = Options(section);
				}
				else
				{
					EnumValue("static.client.discovery.provider", provider, YamlSeedProviders);
				}
			}

			if (discovery.dns != null)
			{
				if (provider == null || !provider.Equals(StaticDiscoveryConfig.Dns, StringComparison.Ordinal))
				{
					throw new AerospikeException(
						"static.client.discovery.dns is only valid with provider dns");
				}
			}
			else if (provider != null && provider.Equals(StaticDiscoveryConfig.Dns, StringComparison.Ordinal))
			{
				throw new AerospikeException("static.client.discovery.dns is required with provider dns");
			}

			if (discovery.aws != null)
			{
				if (provider == null || !provider.Equals(StaticDiscoveryConfig.AwsTags, StringComparison.Ordinal))
				{
					throw new AerospikeException(
						"static.client.discovery.aws is only valid with provider aws-tags");
				}
			}
			else if (provider != null && provider.Equals(StaticDiscoveryConfig.AwsTags, StringComparison.Ordinal))
			{
				throw new AerospikeException("static.client.discovery.aws is required with provider aws-tags");
			}

			if (discovery.dns != null)
			{
				ValidateDns(discovery.dns);
			}

			if (discovery.aws != null)
			{
				ValidateAws(discovery.aws);
			}

			if (discovery.merge_policy != null)
			{
				discovery.merge_policy = EnumValue(
					"static.client.discovery.merge_policy", discovery.merge_policy, MergePolicies);
			}

			if (discovery.execution_mode != null)
			{
				discovery.execution_mode = EnumValue(
					"static.client.discovery.execution_mode", discovery.execution_mode, ExecutionModes);
			}
		}

		private static void ValidateDns(DnsDiscoveryConfig dns)
		{
			const string path = "static.client.discovery.dns";

			if (dns.names == null || dns.names.Length == 0)
			{
				throw new AerospikeException(path + ".names is required");
			}

			if (dns.record_type != null)
			{
				dns.record_type = EnumValue(path + ".record_type", dns.record_type, RecordTypes);
			}
		}

		private static void ValidateAws(AwsDiscoveryConfig aws)
		{
			const string path = "static.client.discovery.aws";

			if (string.IsNullOrEmpty(aws.region))
			{
				throw new AerospikeException(path + ".region is required");
			}

			if (aws.filters == null || aws.filters.Length == 0)
			{
				throw new AerospikeException(
					"Invalid " + path + ".filters value: expected a non-empty list of name and values");
			}

			for (int i = 0; i < aws.filters.Length; i++)
			{
				string filter = path + ".filters[" + i + ']';
				AwsDiscoveryFilterConfig entry = aws.filters[i];

				if (entry == null || string.IsNullOrEmpty(entry.name))
				{
					throw new AerospikeException(filter + ".name is required");
				}

				if (entry.values == null || entry.values.Length == 0)
				{
					throw new AerospikeException(filter + ".values is required");
				}
			}

			if (aws.address_field != null)
			{
				aws.address_field = EnumValue(path + ".address_field", aws.address_field, AddressFields);
			}
		}

		private static void FinalizeTranslation(StaticTranslationConfig translation, IConfigurationSection section)
		{
			string provider = translation.provider;

			if (provider != null && !BuiltInTranslators.Contains(provider))
			{
				translation.options = Options(section);
			}
			else if (provider != null)
			{
				provider = EnumValue("static.client.translation.provider", provider, BuiltInTranslators);
				translation.provider = provider;
			}
			else if (translation.static_map != null)
			{
				provider = StaticTranslationConfig.StaticMap;
				translation.provider = provider;
			}

			if (provider == null)
			{
				if (translation.on_miss != null)
				{
					translation.on_miss = EnumValue(
						"static.client.translation.on_miss", translation.on_miss, OnMiss);
				}
				return;
			}

			if (translation.static_map != null &&
				!provider.Equals(StaticTranslationConfig.StaticMap, StringComparison.Ordinal))
			{
				throw new AerospikeException(
					"static.client.translation.static_map is only valid with provider static-map, not '" +
					provider + "'");
			}

			if (translation.port_map != null &&
				!provider.Equals(StaticTranslationConfig.PortMap, StringComparison.Ordinal))
			{
				throw new AerospikeException(
					"static.client.translation.port_map is only valid with provider port-map, not '" +
					provider + "'");
			}

			if (provider.Equals(StaticTranslationConfig.PortMap, StringComparison.Ordinal))
			{
				if (translation.port_map == null)
				{
					throw new AerospikeException(
						"static.client.translation.port_map is required with provider port-map");
				}

				try
				{
					_ = new PortMapAddressTranslator(translation.port_map);
				}
				catch (AerospikeException e)
				{
					throw new AerospikeException(
						"static.client.translation.port_map: " + e.BaseMessage, e);
				}
			}

			if (translation.on_miss != null)
			{
				translation.on_miss = EnumValue(
					"static.client.translation.on_miss", translation.on_miss, OnMiss);
			}
		}

		internal static Dictionary<string, object> Options(IConfigurationSection section)
		{
			Dictionary<string, object> options = new Dictionary<string, object>();

			foreach (IConfigurationSection child in section.GetChildren())
			{
				if (child.Key.Equals("provider", StringComparison.OrdinalIgnoreCase))
				{
					continue;
				}

				IEnumerable<IConfigurationSection> nested = child.GetChildren();

				if (nested.Any())
				{
					options[child.Key] = Options(child);
				}
				else
				{
					options[child.Key] = child.Value;
				}
			}

			return options;
		}

		internal static string EnumValue(string path, string value, string[] valid)
		{
			if (value == null)
			{
				throw new AerospikeException("Invalid " + path + " value: null");
			}

			foreach (string item in valid)
			{
				if (item.Equals(value, StringComparison.OrdinalIgnoreCase))
				{
					return item;
				}
			}

			throw new AerospikeException(
				"Invalid " + path + " value '" + value + "': valid values: " + string.Join(", ", valid));
		}

		internal static SeedMergePolicy ParseMergePolicy(string value)
		{
			return value.ToLowerInvariant() switch
			{
				"merge" => SeedMergePolicy.MERGE,
				"replace" => SeedMergePolicy.REPLACE,
				"discovery_only" => SeedMergePolicy.DISCOVERY_ONLY,
				_ => throw new AerospikeException(
					"Invalid static.client.discovery.merge_policy value '" + value + "'")
			};
		}

		internal static TranslationMissMode ParseOnMiss(string value)
		{
			return value.ToLowerInvariant() switch
			{
				"pass_through" => TranslationMissMode.PASS_THROUGH,
				"strict" => TranslationMissMode.STRICT,
				_ => throw new AerospikeException(
					"Invalid static.client.translation.on_miss value '" + value + "'")
			};
		}

		internal static DiscoveryExecutionMode ParseExecutionMode(string value)
		{
			return value.ToLowerInvariant() switch
			{
				"thread" => DiscoveryExecutionMode.THREAD,
				"tend" => DiscoveryExecutionMode.TEND,
				_ => throw new AerospikeException(
					"Invalid static.client.discovery.execution_mode value '" + value + "'")
			};
		}

		internal static DnsSeedCandidateProvider BuildDns(DnsDiscoveryConfig dns)
		{
			DnsSeedCandidateProvider.Builder builder = DnsSeedCandidateProvider.CreateBuilder()
				.Names(dns.names);

			if (dns.record_type != null)
			{
				builder.RecordType(dns.record_type.Equals("srv", StringComparison.OrdinalIgnoreCase) ?
					DnsSeedCandidateProvider.RecordType.SRV :
					DnsSeedCandidateProvider.RecordType.A);
			}

			if (dns.port.HasValue)
			{
				builder.Port(dns.port.Value);
			}

			if (dns.tls_name != null)
			{
				builder.TlsName(dns.tls_name);
			}

			if (dns.timeout_ms.HasValue)
			{
				builder.Timeout(dns.timeout_ms.Value);
			}

			if (dns.servers != null)
			{
				builder.Servers(dns.servers);
			}

			try
			{
				return builder.Build();
			}
			catch (AerospikeException e)
			{
				throw new AerospikeException("Invalid static.client.discovery.dns: " + e.BaseMessage, e);
			}
		}

		internal static AwsEc2TagSeedCandidateProvider BuildAws(AwsDiscoveryConfig aws)
		{
			AwsEc2TagSeedCandidateProvider.Builder builder = AwsEc2TagSeedCandidateProvider.CreateBuilder()
				.Region(aws.region);

			foreach (AwsDiscoveryFilterConfig filter in aws.filters)
			{
				builder.Filter(filter.name, filter.values);
			}

			if (aws.address_field != null)
			{
				builder.AddressField(ParseAddressField(aws.address_field));
			}

			if (aws.port.HasValue)
			{
				builder.Port(aws.port.Value);
			}

			if (aws.tls_name != null)
			{
				builder.TlsName(aws.tls_name);
			}

			if (aws.timeout_ms.HasValue)
			{
				builder.Timeout(aws.timeout_ms.Value);
			}

			if (aws.init_timeout_ms.HasValue)
			{
				builder.InitTimeout(aws.init_timeout_ms.Value);
			}

			if (aws.endpoint != null)
			{
				builder.Endpoint(aws.endpoint);
			}

			try
			{
				return builder.Build();
			}
			catch (AerospikeException e)
			{
				throw new AerospikeException("Invalid static.client.discovery.aws: " + e.BaseMessage, e);
			}
		}

		private static AwsEc2TagSeedCandidateProvider.AddressField ParseAddressField(string value)
		{
			return value.ToLowerInvariant() switch
			{
				"private_ip" => AwsEc2TagSeedCandidateProvider.AddressField.PRIVATE_IP,
				"public_ip" => AwsEc2TagSeedCandidateProvider.AddressField.PUBLIC_IP,
				"private_dns" => AwsEc2TagSeedCandidateProvider.AddressField.PRIVATE_DNS,
				"public_dns" => AwsEc2TagSeedCandidateProvider.AddressField.PUBLIC_DNS,
				_ => throw new AerospikeException(
					"Invalid static.client.discovery.aws.address_field value '" + value + "'")
			};
		}
	}
}
