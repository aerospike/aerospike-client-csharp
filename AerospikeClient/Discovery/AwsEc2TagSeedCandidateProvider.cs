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
using System.Collections.ObjectModel;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using Aerospike.Client.Config;

namespace Aerospike.Client
{
	/// <summary>
	/// Seed candidate provider that lists running EC2 instances matching a set of filters, usually a
	/// tag, through the EC2 <c>DescribeInstances</c> API, and returns one seed host per instance. Use
	/// it for a self-managed Aerospike cluster on EC2 whose nodes carry a known tag.
	/// <para>
	/// Each refresh sends one SigV4-signed EC2 Query API <c>DescribeInstances</c> request (API version
	/// 2016-11-15) with <see cref="Builder.Filters"/> plus <c>instance-state-name=running</c>, and
	/// follows <c>NextToken</c> pagination. It returns one host per instance with a non-empty
	/// <see cref="Builder.AddressField"/>, with <see cref="Builder.Port"/> and
	/// <see cref="Builder.TlsName"/>, deduplicated and sorted by address. An instance without that
	/// field is skipped and the count is logged at debug level. Any API, credential or timeout failure
	/// throws an <see cref="AerospikeException"/> with the AWS error code and request id when there is
	/// one, so the client keeps the previous seed list. No access key secret, session token, web
	/// identity token or signature is logged or put in a message.
	/// </para>
	/// <para>
	/// Credentials: <see cref="Builder.CredentialsHook"/>, when set, replaces the chain below.
	/// Otherwise the first source found, in this order, is used: configured keys; environment
	/// variables; shared credentials file; EKS web identity; ECS container credentials; EC2 instance
	/// profile through IMDSv2 (unless <c>AWS_EC2_METADATA_DISABLED</c> is <c>true</c>). SSO,
	/// <c>credential_process</c>, <c>role_arn</c> profiles and cross-account roles are not in the chain;
	/// use the hook. The chain resolves once, at the first refresh; later refreshes reuse the source
	/// that answered. Credentials are cached with their expiry and reloaded on the refreshing thread
	/// when they are within five minutes of expiry, never on a timer.
	/// </para>
	/// <para>
	/// All I/O of one refresh, credentials included, is bounded by <see cref="Builder.Timeout"/>, or
	/// <see cref="Builder.InitTimeout"/> for the first refresh. The provider starts no thread and uses
	/// no HTTP proxy. Eligible for <see cref="DiscoveryExecutionMode.TEND"/> when timeout does not
	/// exceed <see cref="ClientPolicy.discoveryTendRefreshDeadlineMs"/>.
	/// </para>
	/// </summary>
	public sealed class AwsEc2TagSeedCandidateProvider : ISeedCandidateProvider
	{
		/// <summary>
		/// Instance field returned as the seed address.
		/// </summary>
		public enum AddressField
		{
			/// <summary>Primary private IPv4 address (<c>privateIpAddress</c>).</summary>
			PRIVATE_IP,

			/// <summary>Public IPv4 address (<c>ipAddress</c>).</summary>
			PUBLIC_IP,

			/// <summary>Private DNS name (<c>privateDnsName</c>).</summary>
			PRIVATE_DNS,

			/// <summary>Public DNS name (<c>dnsName</c>).</summary>
			PUBLIC_DNS
		}

		/// <summary>
		/// AWS credentials used to sign requests. <see cref="ToString"/> shows only the access key id.
		/// </summary>
		public sealed class Credentials
		{
			internal readonly string accessKeyId;
			internal readonly string secretAccessKey;
			internal readonly string sessionToken;
			internal readonly long expiration;

			/// <summary>
			/// Credentials without expiry.
			/// </summary>
			public Credentials(string accessKeyId, string secretAccessKey, string sessionToken)
				: this(accessKeyId, secretAccessKey, sessionToken, 0)
			{
			}

			/// <summary>
			/// Credentials with an expiry.
			/// </summary>
			/// <param name="accessKeyId">access key id, required</param>
			/// <param name="secretAccessKey">secret access key, required</param>
			/// <param name="sessionToken">session token of temporary credentials, may be null</param>
			/// <param name="expiration">expiry in milliseconds since the epoch, or 0 for none</param>
			public Credentials(string accessKeyId, string secretAccessKey, string sessionToken, long expiration)
			{
				if (string.IsNullOrEmpty(accessKeyId))
				{
					throw new AerospikeException(
						"Invalid AWS EC2 seed provider credentials: access key id is empty");
				}

				if (string.IsNullOrEmpty(secretAccessKey))
				{
					throw new AerospikeException(
						"Invalid AWS EC2 seed provider credentials: secret access key is empty");
				}

				this.accessKeyId = accessKeyId;
				this.secretAccessKey = secretAccessKey;
				this.sessionToken = string.IsNullOrEmpty(sessionToken) ? null : sessionToken;
				this.expiration = expiration;
			}

			/// <summary>Access key id.</summary>
			public string AccessKeyId => accessKeyId;

			/// <summary>Secret access key.</summary>
			public string SecretAccessKey => secretAccessKey;

			/// <summary>Session token of temporary credentials. May be null.</summary>
			public string SessionToken => sessionToken;

			/// <summary>Expiry in milliseconds since the epoch, or 0 for none.</summary>
			public long Expiration => expiration;

			public override string ToString()
			{
				return "Credentials{accessKeyId=" + accessKeyId + ", expiration=" + expiration + '}';
			}
		}

		/// <summary>
		/// Source of credentials that replaces the built-in credential chain. Called on the thread that
		/// refreshes the provider. Must return quickly; an implementation that does I/O should cache.
		/// </summary>
		public delegate Credentials CredentialsHook();

		/// <summary>Default port set on every host.</summary>
		public const int DefaultPort = 3000;

		/// <summary>Default tend refresh timeout in milliseconds.</summary>
		public const int DefaultTimeout = 80;

		/// <summary>Default first refresh timeout in milliseconds.</summary>
		public const int DefaultInitTimeout = 10000;

		internal const string ApiVersion = "2016-11-15";
		internal const string StateFilter = "instance-state-name";

		private static readonly Regex RegionPattern = new Regex("^[a-z0-9]+(-[a-z0-9]+)+$", RegexOptions.Compiled);

		private const string Form = "application/x-www-form-urlencoded; charset=utf-8";
		private const string Service = "ec2";

		private readonly string region;
		private readonly IReadOnlyDictionary<string, IReadOnlyList<string>> filters;
		private readonly AddressField addressField;
		private readonly int port;
		private readonly string tlsName;
		private readonly int timeout;
		private readonly int initTimeout;
		private readonly string endpoint;
		private readonly Uri uri;
		private readonly string hostHeader;
		private readonly string path;
		private readonly string query;
		private readonly AwsEnvironment env;
		private readonly AwsXml xml;
		private readonly AwsCredentialChain chain;

		/// <summary>
		/// Return a builder.
		/// </summary>
		public static Builder CreateBuilder()
		{
			return new Builder();
		}

		private AwsEc2TagSeedCandidateProvider(Builder builder)
		{
			if (builder.region == null || !RegionPattern.IsMatch(builder.region))
			{
				throw new AerospikeException("Invalid AWS EC2 seed provider region: '" + builder.region + "'");
			}

			if (builder.filters.Count == 0)
			{
				throw new AerospikeException(
					"Invalid AWS EC2 seed provider filters: at least one filter is required");
			}

			foreach (KeyValuePair<string, List<string>> entry in builder.filters)
			{
				string name = entry.Key;

				if (string.IsNullOrEmpty(name))
				{
					throw new AerospikeException("Invalid AWS EC2 seed provider filter name: '" + name + "'");
				}

				if (name.Equals(StateFilter, StringComparison.Ordinal))
				{
					throw new AerospikeException("Invalid AWS EC2 seed provider filter name: '" + name +
						"': the provider adds " + StateFilter + "=running");
				}

				List<string> values = entry.Value;

				if (values == null || values.Count == 0)
				{
					throw new AerospikeException(
						"Invalid AWS EC2 seed provider filter '" + name + "' values: empty");
				}

				foreach (string value in values)
				{
					if (string.IsNullOrEmpty(value))
					{
						throw new AerospikeException(
							"Invalid AWS EC2 seed provider filter '" + name + "' value: '" + value + "'");
					}
				}
			}

			if (!Enum.IsDefined(typeof(AddressField), builder.addressField))
			{
				throw new AerospikeException("Invalid AWS EC2 seed provider addressField: " + builder.addressField);
			}

			if (builder.port <= 0 || builder.port > 65535)
			{
				throw new AerospikeException("Invalid AWS EC2 seed provider port: " + builder.port);
			}

			if (builder.tlsName != null && builder.tlsName.Length == 0)
			{
				throw new AerospikeException("Invalid AWS EC2 seed provider tlsName: empty");
			}

			if (builder.timeout <= 0)
			{
				throw new AerospikeException("Invalid AWS EC2 seed provider timeout: " + builder.timeout);
			}

			if (builder.initTimeout <= 0)
			{
				throw new AerospikeException("Invalid AWS EC2 seed provider initTimeout: " + builder.initTimeout);
			}

			if (builder.credentials != null && builder.credentialsHook != null)
			{
				throw new AerospikeException(
					"Invalid AWS EC2 seed provider credentials: credentials and credentialsHook cannot both be set");
			}

			this.env = builder.env;
			string target = builder.endpoint ?? env.Ec2Endpoint(builder.region);

			if (!Uri.TryCreate(target, UriKind.Absolute, out this.uri) ||
				string.IsNullOrEmpty(uri.Host) ||
				!string.IsNullOrEmpty(uri.Query) ||
				!string.IsNullOrEmpty(uri.Fragment) ||
				!(uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase) ||
					uri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase)))
			{
				throw new AerospikeException("Invalid AWS EC2 seed provider endpoint: '" + target +
					"': expected http or https URL with a host and no query");
			}

			this.region = builder.region;
			this.filters = Copy(builder.filters);
			this.addressField = builder.addressField;
			this.port = builder.port;
			this.tlsName = builder.tlsName;
			this.timeout = builder.timeout;
			this.initTimeout = builder.initTimeout;
			this.endpoint = builder.endpoint;
			this.hostHeader = AwsHttp.Host(uri);
			this.path = string.IsNullOrEmpty(uri.AbsolutePath) ? "/" : uri.AbsolutePath;
			this.query = Query(filters);
			this.xml = new AwsXml();
			this.chain = new AwsCredentialChain(builder.credentials, builder.credentialsHook, env, region, xml);
		}

		private static IReadOnlyDictionary<string, IReadOnlyList<string>> Copy(
			Dictionary<string, List<string>> filters)
		{
			Dictionary<string, IReadOnlyList<string>> map = new Dictionary<string, IReadOnlyList<string>>();

			foreach (KeyValuePair<string, List<string>> entry in filters)
			{
				map[entry.Key] = new ReadOnlyCollection<string>(new List<string>(entry.Value));
			}

			return new ReadOnlyDictionary<string, IReadOnlyList<string>>(map);
		}

		private static string Query(IReadOnlyDictionary<string, IReadOnlyList<string>> filters)
		{
			StringBuilder sb = new StringBuilder("Action=DescribeInstances&Version=").Append(ApiVersion);
			int n = 0;

			foreach (KeyValuePair<string, IReadOnlyList<string>> entry in filters)
			{
				Append(sb, ++n, entry.Key, entry.Value);
			}

			Append(sb, ++n, StateFilter, new[] { "running" });
			return sb.ToString();
		}

		private static void Append(StringBuilder sb, int n, string name, IReadOnlyList<string> values)
		{
			sb.Append("&Filter.").Append(n).Append(".Name=").Append(AwsSigV4.Encode(name));
			int v = 0;

			foreach (string value in values)
			{
				sb.Append("&Filter.").Append(n).Append(".Value.").Append(++v).Append('=')
					.Append(AwsSigV4.Encode(value));
			}
		}

		private static string ElementName(AddressField field)
		{
			return field switch
			{
				AddressField.PRIVATE_IP => "privateIpAddress",
				AddressField.PUBLIC_IP => "ipAddress",
				AddressField.PRIVATE_DNS => "privateDnsName",
				AddressField.PUBLIC_DNS => "dnsName",
				_ => throw new AerospikeException("Invalid AWS EC2 seed provider addressField: " + field)
			};
		}

		/// <inheritdoc/>
		public List<Host> RefreshSeedCandidates(SeedCandidateContext ctx)
		{
			int budget = ctx.tendIteration == 0 ? initTimeout : timeout;
			long deadline = AwsHttp.Deadline(budget);
			Credentials credentials = chain.Get(deadline);
			SortedSet<string> addresses = new SortedSet<string>(StringComparer.Ordinal);
			string nextToken = null;
			int instances = 0;
			int skipped = 0;
			int pages = 0;
			string element = ElementName(addressField);

			do
			{
				XmlElement root = Describe(credentials, nextToken, deadline, budget);
				pages++;

				foreach (XmlElement reservation in AwsXml.Children(AwsXml.Child(root, "reservationSet"), "item"))
				{
					foreach (XmlElement instance in AwsXml.Children(AwsXml.Child(reservation, "instancesSet"), "item"))
					{
						instances++;
						string address = AwsXml.Text(instance, element);

						if (string.IsNullOrEmpty(address))
						{
							skipped++;
						}
						else
						{
							addresses.Add(address);
						}
					}
				}

				nextToken = AwsXml.Text(root, "nextToken");
			}
			while (!string.IsNullOrEmpty(nextToken));

			List<Host> hosts = new List<Host>(addresses.Count);

			foreach (string address in addresses)
			{
				hosts.Add(new Host(address, tlsName, port));
			}

			if (Log.DebugEnabled())
			{
				if (skipped > 0)
				{
					Log.Debug("AWS EC2 seed provider skipped " + skipped + " instances without " + element);
				}

				Log.Debug("AWS EC2 seed provider " + region + " returned [" + string.Join(", ", hosts) +
					"] from " + instances + " instances in " + pages + " pages");
			}

			return hosts;
		}

		private XmlElement Describe(Credentials credentials, string nextToken, long deadline, int budget)
		{
			string body = nextToken == null ? query : query + "&NextToken=" + AwsSigV4.Encode(nextToken);
			byte[] payload = Encoding.UTF8.GetBytes(body);
			string amzDate = AwsSigV4.AmzDate(env.CurrentTimeMillis());
			SortedDictionary<string, string> headers = new SortedDictionary<string, string>(StringComparer.Ordinal);
			headers["content-type"] = Form;
			headers["host"] = hostHeader;
			headers["x-amz-date"] = amzDate;

			if (credentials.sessionToken != null)
			{
				headers["x-amz-security-token"] = credentials.sessionToken;
			}

			string authorization = AwsSigV4.Authorization(credentials.accessKeyId, credentials.secretAccessKey,
				"POST", path, "", headers, payload, region, Service, amzDate);
			string signature = authorization.Substring(authorization.LastIndexOf('=') + 1);
			headers["authorization"] = authorization;

			AwsHttp.Response response;

			try
			{
				response = AwsHttp.Request("POST", uri, headers, payload, deadline);
			}
			catch (TimeoutException)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: DescribeInstances timed out after " + budget + "ms");
			}
			catch (IOException e)
			{
				throw new AerospikeException("AWS EC2 seed provider: DescribeInstances failed: " +
					AwsXml.Redact(e.ToString(), credentials.secretAccessKey, credentials.sessionToken, signature));
			}

			if (response.Status != 200)
			{
				throw xml.Error("DescribeInstances", response, credentials.secretAccessKey,
					credentials.sessionToken, signature);
			}

			try
			{
				return xml.Parse(response.Body);
			}
			catch (Exception e)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: invalid DescribeInstances response: " + e.Message);
			}
		}

		internal string CredentialSource()
		{
			AwsCredentialChain.Source source = chain.SourceResolved();
			return source?.Label;
		}

		/// <summary>AWS region.</summary>
		public string Region => region;

		/// <summary>Filters, by name, in configured order. Unmodifiable.</summary>
		public IReadOnlyDictionary<string, IReadOnlyList<string>> Filters => filters;

		/// <summary>Instance field returned as the seed address.</summary>
		public AddressField Address => addressField;

		/// <summary>Port set on every host.</summary>
		public int Port => port;

		/// <summary>TLS name set on every host. May be null.</summary>
		public string TlsName => tlsName;

		/// <summary>Total timeout in milliseconds for one tend refresh, credentials included.</summary>
		public int Timeout => timeout;

		/// <summary>Total timeout in milliseconds for the first refresh, credentials included.</summary>
		public int InitTimeout => initTimeout;

		/// <summary>Configured EC2 endpoint, or null for the regional endpoint.</summary>
		public string Endpoint => endpoint;

		/// <inheritdoc/>
		public bool NeedsPeriodicRefresh() => true;

		/// <inheritdoc/>
		public bool SupportsDeadline() => true;

		public override string ToString()
		{
			return StaticDiscoveryConfig.AwsTags + ' ' + region + ' ' + filters;
		}

		/// <summary>
		/// Builder for <see cref="AwsEc2TagSeedCandidateProvider"/>. Values are validated by
		/// <see cref="Build"/>.
		/// </summary>
		public sealed class Builder
		{
			internal string region;
			internal readonly Dictionary<string, List<string>> filters = new Dictionary<string, List<string>>();
			internal AwsEc2TagSeedCandidateProvider.AddressField addressField =
				AwsEc2TagSeedCandidateProvider.AddressField.PRIVATE_IP;
			internal int port = DefaultPort;
			internal string tlsName;
			internal int timeout = DefaultTimeout;
			internal int initTimeout = DefaultInitTimeout;
			internal string endpoint;
			internal Credentials credentials;
			internal CredentialsHook credentialsHook;
			internal AwsEnvironment env = AwsEnvironment.System;

			internal Builder()
			{
			}

			/// <summary>
			/// AWS region of the instances and of the EC2 and STS endpoints, such as <c>us-east-1</c>.
			/// Required.
			/// </summary>
			public Builder Region(string region)
			{
				this.region = region;
				return this;
			}

			/// <summary>
			/// Add a <c>DescribeInstances</c> filter. Adding a name again replaces its values.
			/// <c>instance-state-name=running</c> is always added and cannot be set. At least one filter
			/// is required.
			/// </summary>
			public Builder Filter(string name, params string[] values)
			{
				filters[name] = values != null ? new List<string>(values) : null;
				return this;
			}

			/// <summary>
			/// Add a <c>DescribeInstances</c> filter. See <see cref="Filter(string, string[])"/>.
			/// </summary>
			public Builder Filter(string name, IList<string> values)
			{
				filters[name] = values != null ? new List<string>(values) : null;
				return this;
			}

			/// <summary>
			/// Add filters by name. See <see cref="Filter(string, string[])"/>.
			/// </summary>
			public Builder Filters(IDictionary<string, IList<string>> filters)
			{
				if (filters != null)
				{
					foreach (KeyValuePair<string, IList<string>> entry in filters)
					{
						Filter(entry.Key, entry.Value);
					}
				}

				return this;
			}

			/// <summary>
			/// Add filter <c>tag:&lt;key&gt;</c> with the given values.
			/// </summary>
			public Builder TagFilter(string key, params string[] values)
			{
				return Filter("tag:" + key, values);
			}

			/// <summary>
			/// Instance field returned as the seed address. Default: <see cref="AddressField.PRIVATE_IP"/>.
			/// </summary>
			public Builder AddressField(AwsEc2TagSeedCandidateProvider.AddressField addressField)
			{
				this.addressField = addressField;
				return this;
			}

			/// <summary>
			/// Port set on every host. Default: <see cref="DefaultPort"/>.
			/// </summary>
			public Builder Port(int port)
			{
				this.port = port;
				return this;
			}

			/// <summary>
			/// TLS name set on every host. Null leaves the TLS name to be defaulted the same way as for
			/// configured seeds. Default: null.
			/// </summary>
			public Builder TlsName(string tlsName)
			{
				this.tlsName = tlsName;
				return this;
			}

			/// <summary>
			/// Total time in milliseconds for one tend refresh, credentials and every page included.
			/// Must be greater than zero, and must not exceed
			/// <see cref="ClientPolicy.discoveryTendRefreshDeadlineMs"/> when
			/// <see cref="DiscoveryExecutionMode.TEND"/> is used. Default: <see cref="DefaultTimeout"/>.
			/// </summary>
			public Builder Timeout(int timeout)
			{
				this.timeout = timeout;
				return this;
			}

			/// <summary>
			/// Total time in milliseconds for the first refresh, which runs in the client constructor and
			/// resolves the credential chain. Must be greater than zero. Default:
			/// <see cref="DefaultInitTimeout"/>.
			/// </summary>
			public Builder InitTimeout(int initTimeout)
			{
				this.initTimeout = initTimeout;
				return this;
			}

			/// <summary>
			/// EC2 endpoint URL, such as a VPC interface endpoint. Null uses the regional endpoint
			/// <c>https://ec2.&lt;region&gt;.amazonaws.com</c>. Default: null.
			/// </summary>
			public Builder Endpoint(string endpoint)
			{
				this.endpoint = endpoint;
				return this;
			}

			/// <summary>
			/// Static credentials, the first source of the credential chain. Cannot be combined with
			/// <see cref="CredentialsHook"/>.
			/// </summary>
			public Builder Credentials(string accessKeyId, string secretAccessKey, string sessionToken)
			{
				this.credentials = new Credentials(accessKeyId, secretAccessKey, sessionToken);
				return this;
			}

			/// <summary>
			/// Credentials hook that replaces the built-in credential chain. Cannot be combined with
			/// <see cref="Credentials(string, string, string)"/>.
			/// </summary>
			public Builder CredentialsHook(CredentialsHook credentialsHook)
			{
				this.credentialsHook = credentialsHook;
				return this;
			}

			internal Builder Environment(AwsEnvironment env)
			{
				this.env = env;
				return this;
			}

			/// <summary>
			/// Validate the settings and build the provider.
			/// </summary>
			public AwsEc2TagSeedCandidateProvider Build()
			{
				return new AwsEc2TagSeedCandidateProvider(this);
			}
		}
	}
}
