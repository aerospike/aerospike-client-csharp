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
using System.Text;
using System.Xml;
using static Aerospike.Client.AwsEc2TagSeedCandidateProvider;

namespace Aerospike.Client
{
	/// <summary>
	/// AWS credential chain of <see cref="AwsEc2TagSeedCandidateProvider"/>. Sources, first found wins:
	/// configured keys, environment, shared credentials file, web identity, ECS container credentials,
	/// EC2 IMDSv2. A hook replaces the chain. The chain resolves once; later calls reuse the source that
	/// answered. Credentials are cached and reloaded on the calling thread when they are within
	/// <see cref="RefreshMargin"/> of expiry. Used by one provider instance on one thread at a time.
	/// </summary>
	internal sealed class AwsCredentialChain
	{
		internal const long RefreshMargin = 5 * 60 * 1000;

		private const string Form = "application/x-www-form-urlencoded; charset=utf-8";
		private const string ImdsTokenTtl = "21600";
		private const string ImdsCredentials = "/latest/meta-data/iam/security-credentials/";

		internal sealed class Source
		{
			internal static readonly Source Config = new Source("config");
			internal static readonly Source Environment = new Source("environment");
			internal static readonly Source Profile = new Source("profile");
			internal static readonly Source WebIdentity = new Source("web-identity");
			internal static readonly Source Ecs = new Source("ecs");
			internal static readonly Source Imds = new Source("imds");
			internal static readonly Source Hook = new Source("hook");

			internal readonly string Label;

			private Source(string label)
			{
				Label = label;
			}
		}

		private readonly Credentials config;
		private readonly CredentialsHook hook;
		private readonly AwsEnvironment env;
		private readonly string region;
		private readonly AwsXml xml;
		private Source source;
		private Credentials cached;
		private string imdsRole;

		internal AwsCredentialChain(
			Credentials config,
			CredentialsHook hook,
			AwsEnvironment env,
			string region,
			AwsXml xml)
		{
			this.config = config;
			this.hook = hook;
			this.env = env;
			this.region = region;
			this.xml = xml;
		}

		/// <summary>
		/// Return credentials, loading them when none are cached or the cached ones are within
		/// <see cref="RefreshMargin"/> of expiry. <paramref name="deadline"/> is a
		/// <see cref="System.Diagnostics.Stopwatch.GetTimestamp"/> value.
		/// </summary>
		internal Credentials Get(long deadline)
		{
			long now = env.CurrentTimeMillis();

			if (cached != null && (cached.expiration == 0 || now < cached.expiration - RefreshMargin))
			{
				return cached;
			}

			try
			{
				Credentials credentials = source == null ? Resolve(deadline) : Load(source, deadline);
				cached = credentials;
				return credentials;
			}
			catch (AerospikeException ae)
			{
				if (cached != null && now < cached.expiration)
				{
					if (Log.WarnEnabled())
					{
						Log.Warn("AWS EC2 seed provider: credentials refresh from " + source.Label +
							" failed, using cached credentials until they expire: " + ae.BaseMessage);
					}

					return cached;
				}

				throw;
			}
		}

		/// <summary>
		/// Return the source that answered, or null before the chain has resolved.
		/// </summary>
		internal Source SourceResolved()
		{
			return source;
		}

		private Credentials Resolve(long deadline)
		{
			Source found;

			if (hook != null)
			{
				found = Source.Hook;
			}
			else if (config != null)
			{
				found = Source.Config;
			}
			else if (FromEnvironment() != null)
			{
				found = Source.Environment;
			}
			else if (FromProfile() != null)
			{
				found = Source.Profile;
			}
			else if (Present("AWS_WEB_IDENTITY_TOKEN_FILE") && Present("AWS_ROLE_ARN"))
			{
				found = Source.WebIdentity;
			}
			else if (Present("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") ||
				Present("AWS_CONTAINER_CREDENTIALS_FULL_URI"))
			{
				found = Source.Ecs;
			}
			else if (!"true".Equals(env.GetEnv("AWS_EC2_METADATA_DISABLED"), StringComparison.OrdinalIgnoreCase))
			{
				found = Source.Imds;
			}
			else
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: no AWS credentials found in config, environment, " +
					"shared credentials file, web identity or ECS, and IMDS is disabled");
			}

			Credentials credentials;

			try
			{
				credentials = Load(found, deadline);
			}
			catch (AerospikeException ae)
			{
				if (found == Source.Imds)
				{
					throw new AerospikeException(
						"AWS EC2 seed provider: no AWS credentials found in config, " +
						"environment, shared credentials file, web identity or ECS, and " +
						ae.BaseMessage.Replace("AWS EC2 seed provider: ", "", StringComparison.Ordinal), ae);
				}

				throw;
			}

			source = found;

			if (Log.InfoEnabled())
			{
				Log.Info("AWS EC2 seed provider credentials from " + found.Label + ": access key " +
					credentials.accessKeyId);
			}

			return credentials;
		}

		private Credentials Load(Source from, long deadline)
		{
			if (from == Source.Hook)
			{
				return FromHook();
			}

			if (from == Source.Config)
			{
				return config;
			}

			if (from == Source.Environment)
			{
				return FromEnvironment();
			}

			if (from == Source.Profile)
			{
				return FromProfile();
			}

			if (from == Source.WebIdentity)
			{
				return FromWebIdentity(deadline);
			}

			if (from == Source.Ecs)
			{
				return FromEcs(deadline);
			}

			return FromImds(deadline);
		}

		private Credentials FromHook()
		{
			Credentials credentials;

			try
			{
				credentials = hook();
			}
			catch (Exception e)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: credentials hook failed: " + e.GetType().FullName, e);
			}

			if (credentials == null)
			{
				throw new AerospikeException("AWS EC2 seed provider: credentials hook returned null");
			}

			return credentials;
		}

		private Credentials FromEnvironment()
		{
			string key = env.GetEnv("AWS_ACCESS_KEY_ID");
			string secret = env.GetEnv("AWS_SECRET_ACCESS_KEY");

			if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(secret))
			{
				return null;
			}

			return new Credentials(key, secret, Empty(env.GetEnv("AWS_SESSION_TOKEN")));
		}

		private Credentials FromProfile()
		{
			string file = env.GetEnv("AWS_SHARED_CREDENTIALS_FILE");
			string path;

			if (string.IsNullOrEmpty(file))
			{
				path = Path.Combine(env.UserHome(), ".aws", "credentials");
			}
			else if (file.StartsWith("~/", StringComparison.Ordinal) || file.StartsWith("~\\", StringComparison.Ordinal))
			{
				path = Path.Combine(env.UserHome(), file.Substring(2));
			}
			else
			{
				path = file;
			}

			if (!File.Exists(path))
			{
				return null;
			}

			string profile = env.GetEnv("AWS_PROFILE");

			if (string.IsNullOrEmpty(profile))
			{
				profile = "default";
			}

			string[] lines;

			try
			{
				lines = File.ReadAllLines(path, Encoding.UTF8);
			}
			catch (IOException e)
			{
				throw new AerospikeException("AWS EC2 seed provider: failed to read " + path + ": " + e, e);
			}

			Dictionary<string, string> keys = new Dictionary<string, string>();
			bool inProfile = false;

			foreach (string raw in lines)
			{
				string line = raw.Trim();

				if (line.Length == 0 || line.StartsWith('#') || line.StartsWith(';'))
				{
					continue;
				}

				if (line.StartsWith('[') && line.EndsWith(']'))
				{
					inProfile = line.Substring(1, line.Length - 2).Trim().Equals(profile, StringComparison.Ordinal);
					continue;
				}

				int i = line.IndexOf('=');

				if (inProfile && i > 0)
				{
					keys[line.Substring(0, i).Trim()] = line.Substring(i + 1).Trim();
				}
			}

			keys.TryGetValue("aws_access_key_id", out string key);
			keys.TryGetValue("aws_secret_access_key", out string secret);

			if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(secret))
			{
				return null;
			}

			keys.TryGetValue("aws_session_token", out string token);
			return new Credentials(key, secret, Empty(token));
		}

		private Credentials FromWebIdentity(long deadline)
		{
			string token;

			try
			{
				token = File.ReadAllText(env.GetEnv("AWS_WEB_IDENTITY_TOKEN_FILE"), Encoding.UTF8).Trim();
			}
			catch (IOException e)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: failed to read AWS_WEB_IDENTITY_TOKEN_FILE: " + e, e);
			}

			string session = env.GetEnv("AWS_ROLE_SESSION_NAME");

			if (string.IsNullOrEmpty(session))
			{
				session = "aerospike-client-" + env.CurrentTimeMillis();
			}

			string body = "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn=" +
				AwsSigV4.Encode(env.GetEnv("AWS_ROLE_ARN")) + "&RoleSessionName=" + AwsSigV4.Encode(session) +
				"&WebIdentityToken=" + AwsSigV4.Encode(token);
			SortedDictionary<string, string> headers = new SortedDictionary<string, string>(StringComparer.Ordinal);
			headers["content-type"] = Form;

			AwsHttp.Response response = Send("STS AssumeRoleWithWebIdentity", "POST", env.StsEndpoint(region),
				headers, Encoding.UTF8.GetBytes(body), deadline, token);

			if (response.Status != 200)
			{
				throw xml.Error("STS AssumeRoleWithWebIdentity", response, token);
			}

			XmlElement credentials;

			try
			{
				credentials = (XmlElement)xml.Parse(response.Body).GetElementsByTagName("Credentials")[0];
			}
			catch (Exception)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: invalid STS AssumeRoleWithWebIdentity response");
			}

			if (credentials == null)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: STS AssumeRoleWithWebIdentity response has no credentials");
			}

			return Temporary("STS AssumeRoleWithWebIdentity", AwsXml.Text(credentials, "AccessKeyId"),
				AwsXml.Text(credentials, "SecretAccessKey"), AwsXml.Text(credentials, "SessionToken"),
				AwsXml.Text(credentials, "Expiration"));
		}

		private Credentials FromEcs(long deadline)
		{
			string relative = env.GetEnv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
			string uri = !string.IsNullOrEmpty(relative) ? env.EcsEndpoint() + relative :
				env.GetEnv("AWS_CONTAINER_CREDENTIALS_FULL_URI");
			string authorization = null;
			string file = env.GetEnv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE");

			if (!string.IsNullOrEmpty(file))
			{
				try
				{
					authorization = File.ReadAllText(file, Encoding.UTF8).Trim();
				}
				catch (IOException e)
				{
					throw new AerospikeException(
						"AWS EC2 seed provider: failed to read AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: " + e, e);
				}
			}
			else
			{
				authorization = Empty(env.GetEnv("AWS_CONTAINER_AUTHORIZATION_TOKEN"));
			}

			SortedDictionary<string, string> headers = new SortedDictionary<string, string>(StringComparer.Ordinal);

			if (authorization != null)
			{
				headers["authorization"] = authorization;
			}

			AwsHttp.Response response = Send("ECS credentials", "GET", uri, headers, null, deadline, authorization);

			if (response.Status != 200)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: ECS credentials failed: HTTP " + response.Status);
			}

			Dictionary<string, string> json = ParseJson(response, "ECS credentials");
			json.TryGetValue("AccessKeyId", out string key);
			json.TryGetValue("SecretAccessKey", out string secret);
			json.TryGetValue("Token", out string token);
			json.TryGetValue("Expiration", out string expiration);
			return Temporary("ECS credentials", key, secret, token, expiration);
		}

		private Credentials FromImds(long deadline)
		{
			string baseUrl = env.ImdsEndpoint();
			SortedDictionary<string, string> headers = new SortedDictionary<string, string>(StringComparer.Ordinal);
			headers["x-aws-ec2-metadata-token-ttl-seconds"] = ImdsTokenTtl;

			AwsHttp.Response response = Send("IMDS token", "PUT", baseUrl + "/latest/api/token", headers,
				Array.Empty<byte>(), deadline);

			if (response.Status != 200)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: IMDS token failed: HTTP " + response.Status);
			}

			string token = response.Text().Trim();
			headers.Clear();
			headers["x-aws-ec2-metadata-token"] = token;

			if (imdsRole == null)
			{
				response = Send("IMDS role", "GET", baseUrl + ImdsCredentials, headers, null, deadline, token);

				if (response.Status != 200)
				{
					throw new AerospikeException(
						"AWS EC2 seed provider: IMDS role failed: HTTP " + response.Status +
						" (no instance profile attached?)");
				}

				string role = response.Text().Trim();
				int i = role.IndexOf('\n');
				role = i >= 0 ? role.Substring(0, i).Trim() : role;

				if (role.Length == 0)
				{
					throw new AerospikeException(
						"AWS EC2 seed provider: IMDS returned no instance profile role");
				}

				imdsRole = role;
			}

			response = Send("IMDS credentials", "GET", baseUrl + ImdsCredentials + imdsRole, headers, null,
				deadline, token);

			if (response.Status != 200)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: IMDS credentials failed: HTTP " + response.Status);
			}

			Dictionary<string, string> json = ParseJson(response, "IMDS credentials");
			json.TryGetValue("Code", out string code);

			if (code != null && !code.Equals("Success", StringComparison.Ordinal))
			{
				throw new AerospikeException("AWS EC2 seed provider: IMDS credentials failed: " + code);
			}

			json.TryGetValue("AccessKeyId", out string key);
			json.TryGetValue("SecretAccessKey", out string secret);
			json.TryGetValue("Token", out string session);
			json.TryGetValue("Expiration", out string expiration);
			return Temporary("IMDS credentials", key, secret, session, expiration);
		}

		private AwsHttp.Response Send(
			string action,
			string method,
			string uri,
			IDictionary<string, string> headers,
			byte[] body,
			long deadline,
			params string[] secrets)
		{
			try
			{
				return AwsHttp.Request(method, new Uri(uri), headers, body, deadline);
			}
			catch (TimeoutException)
			{
				throw new AerospikeException("AWS EC2 seed provider: " + action + " timed out");
			}
			catch (Exception e) when (e is IOException || e is ArgumentException || e is UriFormatException)
			{
				throw new AerospikeException("AWS EC2 seed provider: " + action + " failed: " +
					AwsXml.Redact(e.ToString(), secrets));
			}
		}

		private static Credentials Temporary(
			string action,
			string key,
			string secret,
			string token,
			string expiration)
		{
			if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(secret) || expiration == null)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: " + action + " response has no credentials");
			}

			long expires;

			try
			{
				expires = DateTimeOffset.Parse(expiration, null,
					System.Globalization.DateTimeStyles.RoundtripKind).ToUnixTimeMilliseconds();
			}
			catch (Exception)
			{
				throw new AerospikeException(
					"AWS EC2 seed provider: " + action + " returned an invalid expiration: " + expiration);
			}

			return new Credentials(key, secret, Empty(token), expires);
		}

		private static Dictionary<string, string> ParseJson(AwsHttp.Response response, string action)
		{
			try
			{
				return Json.Object(response.Text());
			}
			catch (Exception)
			{
				throw new AerospikeException("AWS EC2 seed provider: invalid " + action + " response");
			}
		}

		private bool Present(string name)
		{
			string value = env.GetEnv(name);
			return !string.IsNullOrEmpty(value);
		}

		private static string Empty(string value)
		{
			return string.IsNullOrEmpty(value) ? null : value;
		}

		/// <summary>
		/// Parser for the flat JSON objects returned by the ECS and IMDS credential endpoints.
		/// </summary>
		internal sealed class Json
		{
			private readonly string s;
			private int i;

			private Json(string s)
			{
				this.s = s;
			}

			internal static Dictionary<string, string> Object(string text)
			{
				Json json = new Json(text);
				Dictionary<string, string> map = new Dictionary<string, string>();
				json.Expect('{');

				if (json.Peek() == '}')
				{
					json.i++;
					return map;
				}

				while (true)
				{
					string key = json.String();
					json.Expect(':');
					string value = json.Value();

					if (value != null)
					{
						map[key] = value;
					}

					char c = json.Next();

					if (c == '}')
					{
						return map;
					}

					if (c != ',')
					{
						throw new ArgumentException("expected , or }");
					}
				}
			}

			private char Peek()
			{
				while (i < s.Length && char.IsWhiteSpace(s[i]))
				{
					i++;
				}

				if (i >= s.Length)
				{
					throw new ArgumentException("unexpected end");
				}

				return s[i];
			}

			private char Next()
			{
				char c = Peek();
				i++;
				return c;
			}

			private void Expect(char c)
			{
				if (Next() != c)
				{
					throw new ArgumentException("expected " + c);
				}
			}

			private string Value()
			{
				char c = Peek();

				if (c == '"')
				{
					return String();
				}

				if (c == '{' || c == '[')
				{
					Skip();
					return null;
				}

				int begin = i;

				while (i < s.Length && ",}] \t\r\n".IndexOf(s[i]) < 0)
				{
					i++;
				}

				string literal = s.Substring(begin, i - begin);
				return literal.Equals("null", StringComparison.Ordinal) ? null : literal;
			}

			private void Skip()
			{
				int depth = 0;

				do
				{
					char c = Peek();

					if (c == '"')
					{
						String();
						continue;
					}

					if (c == '{' || c == '[')
					{
						depth++;
					}
					else if (c == '}' || c == ']')
					{
						depth--;
					}

					i++;
				}
				while (depth > 0);
			}

			private string String()
			{
				Expect('"');
				StringBuilder sb = new StringBuilder();

				while (true)
				{
					if (i >= s.Length)
					{
						throw new ArgumentException("unterminated string");
					}

					char c = s[i++];

					if (c == '"')
					{
						return sb.ToString();
					}

					if (c != '\\')
					{
						sb.Append(c);
						continue;
					}

					c = s[i++];

					switch (c)
					{
						case 'b': sb.Append('\b'); break;
						case 'f': sb.Append('\f'); break;
						case 'n': sb.Append('\n'); break;
						case 'r': sb.Append('\r'); break;
						case 't': sb.Append('\t'); break;
						case 'u':
							sb.Append((char)Convert.ToInt32(s.Substring(i, 4), 16));
							i += 4;
							break;
						default: sb.Append(c); break;
					}
				}
			}
		}
	}
}
