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
using System.Text;
using Aerospike.Client;
using static Aerospike.Client.AwsEc2TagSeedCandidateProvider;
using Request = Aerospike.Test.StubAwsServer.Request;
using Reply = Aerospike.Test.StubAwsServer.Reply;

namespace Aerospike.Test
{
	/// <summary>
	/// Credential sources of the AWS chain against <see cref="StubAwsServer"/>: order, expiry, hook.
	/// Mirrors Java AwsCredentialChainTest (Mirza L0 checklist).
	/// </summary>
	[TestClass]
	public class TestAwsCredentialChain
	{
		private static readonly SeedCandidateContext Init =
			new SeedCandidateContext(null, Array.Empty<Host>(), 0);

		private const string RoleArn = "arn:aws:iam::123456789012:role/seed-role";
		private const string WebToken = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVYQU1QTEUifQ.web-identity-token";
		private const string Key = TestAwsEc2TagSeedCandidateProvider.Key;
		private const string Secret = TestAwsEc2TagSeedCandidateProvider.Secret;
		private const string Token = TestAwsEc2TagSeedCandidateProvider.Token;

		private readonly List<string> messages = new List<string>();
		private StubAwsServer stub;
		private TestAwsEnvironment env;
		private string dir;

		[TestInitialize]
		public void SetUp()
		{
			stub = new StubAwsServer();
			env = new TestAwsEnvironment(stub.Url);
			dir = Path.Combine(Path.GetTempPath(), "aws-chain-" + Guid.NewGuid().ToString("N"));
			Directory.CreateDirectory(dir);
			env.Home = dir;
			Log.SetLevel(Log.Level.INFO);
			Log.SetCallback((level, message) => messages.Add(level + " " + message));
		}

		[TestCleanup]
		public void TearDown()
		{
			stub?.Dispose();
			Log.SetCallback(null);

			if (dir != null && Directory.Exists(dir))
			{
				Directory.Delete(dir, true);
			}
		}

		private Builder Builder()
		{
			return AwsEc2TagSeedCandidateProvider.CreateBuilder()
				.Region("us-east-1")
				.TagFilter("aerospike-cluster", "prod-a")
				.Environment(env);
		}

		private Request LastEc2()
		{
			Request last = null;

			foreach (Request request in stub.Requests())
			{
				if (request.Path.Equals("/", StringComparison.Ordinal))
				{
					last = request;
				}
			}

			return last;
		}

		private static string AccessKey(Request request)
		{
			string authorization = request.Headers["authorization"];
			const string marker = "Credential=";
			int start = authorization.IndexOf(marker, StringComparison.Ordinal) + marker.Length;
			int end = authorization.IndexOf('/', start);
			return authorization.Substring(start, end - start);
		}

		private string Refresh(AwsEc2TagSeedCandidateProvider provider)
		{
			provider.RefreshSeedCandidates(Init);
			Request request = LastEc2();
			string accessKey = AccessKey(request);
			Console.WriteLine("AWS-TAGS-CREDENTIALS source=" + provider.CredentialSource() +
				" accessKey=" + accessKey +
				" sessionTokenHeader=" + request.Headers.ContainsKey("x-amz-security-token"));
			return accessKey;
		}

		private void EnvKeys(string key)
		{
			env.Set("AWS_ACCESS_KEY_ID", key)
				.Set("AWS_SECRET_ACCESS_KEY", Secret)
				.Set("AWS_SESSION_TOKEN", Token);
		}

		private string ProfileFile(string name)
		{
			string file = Path.Combine(dir, name.Replace('/', Path.DirectorySeparatorChar));
			Directory.CreateDirectory(Path.GetDirectoryName(file)!);
			File.WriteAllText(file,
				"# shared credentials\n[default]\naws_access_key_id = AKIADEFAULT\n" +
				"aws_secret_access_key = " + Secret + "\n\n[seed]\naws_access_key_id=AKIASEEDPROFILE\n" +
				"aws_secret_access_key=" + Secret + "\naws_session_token=" + Token + "\n\n[sso-only]\n" +
				"sso_session = corp\n", Encoding.UTF8);
			return file;
		}

		private void WebIdentity()
		{
			string tokenFile = Path.Combine(dir, "web-identity-token");
			File.WriteAllText(tokenFile, WebToken + "\n", Encoding.UTF8);
			env.Set("AWS_WEB_IDENTITY_TOKEN_FILE", tokenFile)
				.Set("AWS_ROLE_ARN", RoleArn)
				.Set("AWS_ROLE_SESSION_NAME", "seed-session");
			stub.Sts(new Reply(200,
				StubAwsServer.AssumeRoleWithWebIdentity("ASIASTS", Secret, Token, "2099-01-01T00:00:00Z")));
		}

		[TestMethod]
		public void ConfigKeys()
		{
			EnvKeys("AKIAENV");
			Assert.AreEqual(Key, Refresh(Builder().Credentials(Key, Secret, null).Build()));
			Assert.IsFalse(LastEc2().Headers.ContainsKey("x-amz-security-token"));
		}

		[TestMethod]
		public void Environment()
		{
			EnvKeys("AKIAENV");
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("AKIAENV", Refresh(provider));
			Assert.AreEqual("environment", provider.CredentialSource());
			Assert.AreEqual(Token, LastEc2().Headers["x-amz-security-token"]);
			Assert.IsTrue(messages.Any(m =>
				m.Contains("INFO AWS EC2 seed provider credentials from environment: access key AKIAENV")));
			Assert.IsFalse(string.Join('\n', messages).Contains(Secret));
		}

		[TestMethod]
		public void ProfileFileWithAwsProfile()
		{
			env.Set("AWS_SHARED_CREDENTIALS_FILE", ProfileFile("credentials")).Set("AWS_PROFILE", "seed");
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("AKIASEEDPROFILE", Refresh(provider));
			Assert.AreEqual("profile", provider.CredentialSource());
			Assert.AreEqual(Token, LastEc2().Headers["x-amz-security-token"]);
		}

		[TestMethod]
		public void ProfileFileDefaultInHome()
		{
			ProfileFile(".aws/credentials");
			Assert.AreEqual("AKIADEFAULT", Refresh(Builder().Build()));
		}

		[TestMethod]
		public void ProfileWithoutStaticKeysFallsThroughToImds()
		{
			env.Set("AWS_SHARED_CREDENTIALS_FILE", ProfileFile("credentials")).Set("AWS_PROFILE", "sso-only");
			stub.Credentials(StubAwsServer.CredentialsJson("ASIAIMDS", Secret, Token, "2099-01-01T00:00:00Z"));
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("ASIAIMDS", Refresh(provider));
			Assert.AreEqual("imds", provider.CredentialSource());
		}

		[TestMethod]
		public void WebIdentityThroughStubSts()
		{
			WebIdentity();
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("ASIASTS", Refresh(provider));
			Assert.AreEqual("web-identity", provider.CredentialSource());

			Request sts = stub.Requests()[0];
			Assert.AreEqual("/sts", sts.Path);
			Assert.AreEqual("AssumeRoleWithWebIdentity", sts.Params["Action"]);
			Assert.AreEqual("2011-06-15", sts.Params["Version"]);
			Assert.AreEqual(RoleArn, sts.Params["RoleArn"]);
			Assert.AreEqual("seed-session", sts.Params["RoleSessionName"]);
			Assert.AreEqual(WebToken, sts.Params["WebIdentityToken"]);
			Assert.IsFalse(sts.Headers.ContainsKey("authorization"));
			Assert.AreEqual(Token, LastEc2().Headers["x-amz-security-token"]);
		}

		[TestMethod]
		public void EcsWithAuthorizationToken()
		{
			env.Set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1")
				.Set("AWS_CONTAINER_AUTHORIZATION_TOKEN", StubAwsServer.EcsAuthorization);
			stub.Credentials(StubAwsServer.CredentialsJson("ASIAECS", Secret, Token, "2099-01-01T00:00:00Z"));
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("ASIAECS", Refresh(provider));
			Assert.AreEqual("ecs", provider.CredentialSource());
			Assert.AreEqual("/ecs/v2/credentials/task-1", stub.Requests()[0].Path);
		}

		[TestMethod]
		public void ImdsV2TokenHandshake()
		{
			stub.Credentials(StubAwsServer.CredentialsJson("ASIAIMDS", Secret, Token, "2099-01-01T00:00:00Z"));
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("ASIAIMDS", Refresh(provider));
			Assert.AreEqual("imds", provider.CredentialSource());

			List<Request> requests = stub.Requests();
			Assert.AreEqual("PUT", requests[0].Method);
			Assert.AreEqual("/latest/api/token", requests[0].Path);
			Assert.AreEqual("21600", requests[0].Headers["x-aws-ec2-metadata-token-ttl-seconds"]);
			Assert.AreEqual("/latest/meta-data/iam/security-credentials/", requests[1].Path);
			Assert.AreEqual("/latest/meta-data/iam/security-credentials/" + StubAwsServer.ImdsRole, requests[2].Path);
			Assert.AreEqual(StubAwsServer.ImdsToken, requests[2].Headers["x-aws-ec2-metadata-token"]);
		}

		[TestMethod]
		public void NoSourceFound()
		{
			env.Set("AWS_EC2_METADATA_DISABLED", "true");
			AerospikeException ae = Assert.Throws<AerospikeException>(() =>
				Builder().Build().RefreshSeedCandidates(Init));
			StringAssert.Contains(ae.BaseMessage, "no AWS credentials found");
			Assert.AreEqual(0, stub.Requests().Count);
		}

		[TestMethod]
		public void ChainOrderHigherSourceWins()
		{
			stub.Credentials(StubAwsServer.CredentialsJson("ASIAIMDS", Secret, Token, "2099-01-01T00:00:00Z"));
			env.Set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task-1")
				.Set("AWS_CONTAINER_AUTHORIZATION_TOKEN", StubAwsServer.EcsAuthorization);
			// Same stub credentials JSON serves ECS and IMDS; ECS is preferred so ASIAIMDS is the ECS key.
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("ASIAIMDS", Refresh(provider), "ecs over imds");
			Assert.AreEqual("ecs", provider.CredentialSource());

			WebIdentity();
			provider = Builder().Build();
			Assert.AreEqual("ASIASTS", Refresh(provider), "web identity over ecs");
			Assert.AreEqual("web-identity", provider.CredentialSource());

			env.Set("AWS_SHARED_CREDENTIALS_FILE", ProfileFile("credentials")).Set("AWS_PROFILE", "seed");
			provider = Builder().Build();
			Assert.AreEqual("AKIASEEDPROFILE", Refresh(provider), "profile over web identity");
			Assert.AreEqual("profile", provider.CredentialSource());

			EnvKeys("AKIAENV");
			provider = Builder().Build();
			Assert.AreEqual("AKIAENV", Refresh(provider), "environment over profile");
			Assert.AreEqual("environment", provider.CredentialSource());

			provider = Builder().Credentials(Key, Secret, null).Build();
			Assert.AreEqual(Key, Refresh(provider), "config over environment");
			Assert.AreEqual("config", provider.CredentialSource());

			provider = Builder().CredentialsHook(() => new Credentials("AKIAHOOK", Secret, null)).Build();
			Assert.AreEqual("AKIAHOOK", Refresh(provider), "hook over config");
			Assert.AreEqual("hook", provider.CredentialSource());
			Assert.AreEqual(1, stub.Count("/sts"));
			Assert.AreEqual(0, stub.Count("/latest/api/token"));
		}

		[TestMethod]
		public void ExpiryRefreshesNearExpiryAndNotBefore()
		{
			string expiration = DateTimeOffset.UtcNow.AddHours(1).ToString("o");
			stub.Credentials(StubAwsServer.CredentialsJson("ASIAIMDS1", Secret, Token, expiration));
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("ASIAIMDS1", Refresh(provider));
			Assert.AreEqual(1, stub.Count("/latest/api/token"));

			stub.Credentials(StubAwsServer.CredentialsJson("ASIAIMDS2", Secret, Token, "2099-01-01T00:00:00Z"));
			env.ClockOffset = (long)TimeSpan.FromMinutes(54).TotalMilliseconds;
			Assert.AreEqual("ASIAIMDS1", Refresh(provider), "6 minutes before expiry");
			Assert.AreEqual(1, stub.Count("/latest/api/token"));

			env.ClockOffset = (long)TimeSpan.FromMinutes(56).TotalMilliseconds;
			Assert.AreEqual("ASIAIMDS2", Refresh(provider), "4 minutes before expiry");
			Assert.AreEqual(2, stub.Count("/latest/api/token"));
			Assert.AreEqual(1, stub.Count("/latest/meta-data/iam/security-credentials/"), "role name cached");
		}

		[TestMethod]
		public void FailedReloadKeepsUnexpiredCredentials()
		{
			string expiration = DateTimeOffset.UtcNow.AddHours(1).ToString("o");
			stub.Credentials(StubAwsServer.CredentialsJson("ASIAIMDS1", Secret, Token, expiration));
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			Assert.AreEqual("ASIAIMDS1", Refresh(provider));

			stub.Credentials("{\"Code\":\"Failure\"}");
			env.ClockOffset = (long)TimeSpan.FromMinutes(58).TotalMilliseconds;
			Assert.AreEqual("ASIAIMDS1", Refresh(provider));
			Assert.IsTrue(messages.Any(m => m.StartsWith(
				"WARN AWS EC2 seed provider: credentials refresh from imds failed, using cached credentials until they expire",
				StringComparison.Ordinal)));

			env.ClockOffset = (long)TimeSpan.FromMinutes(61).TotalMilliseconds;
			AerospikeException ae = Assert.Throws<AerospikeException>(() => provider.RefreshSeedCandidates(Init));
			StringAssert.Contains(ae.BaseMessage, "IMDS credentials failed: Failure");
		}

		[TestMethod]
		public void HookReplacesChain()
		{
			EnvKeys("AKIAENV");
			int calls = 0;
			AwsEc2TagSeedCandidateProvider provider = Builder().CredentialsHook(() =>
			{
				calls++;
				long ttl = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() +
					(long)TimeSpan.FromMinutes(calls == 1 ? 4 : 60).TotalMilliseconds;
				return new Credentials("ASIAHOOK", Secret, Token, ttl);
			}).Build();

			Assert.AreEqual("ASIAHOOK", Refresh(provider));
			Assert.AreEqual("hook", provider.CredentialSource());
			Assert.AreEqual(Token, LastEc2().Headers["x-amz-security-token"]);
			Refresh(provider);
			Refresh(provider);
			Assert.AreEqual(2, calls);
		}

		[TestMethod]
		public void JsonParser()
		{
			Dictionary<string, string> map = AwsCredentialChain.Json.Object(
				"{\"a\":\"x\\\"y\", \"n\": 12, \"o\": {\"k\": [1, {\"z\": \"}\"}]}, \"b\":true, \"nil\": null, " +
				"\"s\": \"\\u00e9\\n\"}");
			Assert.AreEqual("x\"y", map["a"]);
			Assert.AreEqual("12", map["n"]);
			Assert.AreEqual("true", map["b"]);
			Assert.AreEqual("é\n", map["s"]);
			Assert.AreEqual(0, AwsCredentialChain.Json.Object(" { } ").Count);
			Assert.Throws<ArgumentException>(() => AwsCredentialChain.Json.Object("{\"a\" 1}"));
		}
	}
}
