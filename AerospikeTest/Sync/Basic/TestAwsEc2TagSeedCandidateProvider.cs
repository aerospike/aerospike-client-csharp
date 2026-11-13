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
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Aerospike.Client;
using static Aerospike.Client.AwsEc2TagSeedCandidateProvider;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAwsEc2TagSeedCandidateProvider
	{
		internal const string Key = "AKIDEXAMPLE";
		internal const string Secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
		internal const string Token = "FwoGZXIvYXdzEXAMPLESESSIONTOKEN";

		private static readonly SeedCandidateContext Tend = new SeedCandidateContext(null, Array.Empty<Host>(), 7);

		private StubAwsServer stub;
		private TestAwsEnvironment env;

		[TestInitialize]
		public void SetUp()
		{
			stub = new StubAwsServer();
			env = new TestAwsEnvironment(stub.Url)
				.Set("AWS_ACCESS_KEY_ID", Key)
				.Set("AWS_SECRET_ACCESS_KEY", Secret)
				.Set("AWS_SESSION_TOKEN", Token);
		}

		[TestCleanup]
		public void TearDown()
		{
			stub?.Dispose();
		}

		private Builder Builder()
		{
			return AwsEc2TagSeedCandidateProvider.CreateBuilder()
				.Region("us-east-1")
				.TagFilter("aerospike-cluster", "prod-a")
				.Environment(env);
		}

		private static string All()
		{
			return StubAwsServer.Describe(null,
				StubAwsServer.Instance("i-0b", "10.0.0.2", "198.51.100.2", "ip-10-0-0-2.ec2.internal",
					"ec2-198-51-100-2.compute-1.amazonaws.com"),
				StubAwsServer.Instance("i-0a", "10.0.0.1", null, "ip-10-0-0-1.ec2.internal", ""),
				StubAwsServer.Instance("i-0c", "10.0.0.3", "198.51.100.3", "ip-10-0-0-3.ec2.internal",
					"ec2-198-51-100-3.compute-1.amazonaws.com"),
				StubAwsServer.Instance("i-0d", "10.0.0.1", null, "ip-10-0-0-1.ec2.internal", null));
		}

		private static List<string> Names(List<Host> hosts)
		{
			return hosts.Select(h => h.name + ':' + h.port + (h.tlsName != null ? '/' + h.tlsName : "")).ToList();
		}

		[TestMethod]
		public void EachAddressFieldAndMissingField()
		{
			Dictionary<AddressField, List<string>> expected = new Dictionary<AddressField, List<string>>
			{
				[AddressField.PRIVATE_IP] = ["10.0.0.1:3000", "10.0.0.2:3000", "10.0.0.3:3000"],
				[AddressField.PUBLIC_IP] = ["198.51.100.2:3000", "198.51.100.3:3000"],
				[AddressField.PRIVATE_DNS] =
				[
					"ip-10-0-0-1.ec2.internal:3000",
					"ip-10-0-0-2.ec2.internal:3000",
					"ip-10-0-0-3.ec2.internal:3000"
				],
				[AddressField.PUBLIC_DNS] =
				[
					"ec2-198-51-100-2.compute-1.amazonaws.com:3000",
					"ec2-198-51-100-3.compute-1.amazonaws.com:3000"
				]
			};
			stub.Ec2Default(new StubAwsServer.Reply(200, All()));

			foreach (KeyValuePair<AddressField, List<string>> entry in expected)
			{
				List<Host> hosts = Builder().AddressField(entry.Key).Build().RefreshSeedCandidates(Tend);
				CollectionAssert.AreEqual(entry.Value, Names(hosts));
			}
		}

		[TestMethod]
		public void PortAndTlsNameCarried()
		{
			stub.Ec2Default(new StubAwsServer.Reply(200, All()));
			List<Host> hosts = Builder().Port(4333).TlsName("aerospike-tls").Build().RefreshSeedCandidates(Tend);
			CollectionAssert.AreEqual(new List<string>
			{
				"10.0.0.1:4333/aerospike-tls",
				"10.0.0.2:4333/aerospike-tls",
				"10.0.0.3:4333/aerospike-tls"
			}, Names(hosts));
		}

		[TestMethod]
		public void FiltersSentAsConfiguredWithRunningAddedAndSigned()
		{
			AwsEc2TagSeedCandidateProvider provider = Builder()
				.Filter("subnet-id", "subnet-1", "subnet-2")
				.Build();
			provider.RefreshSeedCandidates(Tend);

			Assert.AreEqual(1, stub.Requests().Count);
			StubAwsServer.Request request = stub.Requests()[0];

			Dictionary<string, string> expected = new Dictionary<string, string>
			{
				["Action"] = "DescribeInstances",
				["Version"] = "2016-11-15",
				["Filter.1.Name"] = "tag:aerospike-cluster",
				["Filter.1.Value.1"] = "prod-a",
				["Filter.2.Name"] = "subnet-id",
				["Filter.2.Value.1"] = "subnet-1",
				["Filter.2.Value.2"] = "subnet-2",
				["Filter.3.Name"] = "instance-state-name",
				["Filter.3.Value.1"] = "running"
			};
			Assert.AreEqual(expected.Count, request.Params.Count);

			foreach (KeyValuePair<string, string> entry in expected)
			{
				Assert.IsTrue(request.Params.TryGetValue(entry.Key, out string value), entry.Key);
				Assert.AreEqual(entry.Value, value, entry.Key);
			}
			Assert.AreEqual("POST", request.Method);
			Assert.AreEqual(Token, request.Headers["x-amz-security-token"]);

			string date = request.Headers["x-amz-date"];
			string authorization = request.Headers["authorization"];
			Assert.IsTrue(Regex.IsMatch(authorization,
				"^AWS4-HMAC-SHA256 Credential=" + Key + "/\\d{8}/us-east-1/ec2/" +
				"aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-security-token, Signature=[0-9a-f]{64}$"));

			SortedDictionary<string, string> headers = new SortedDictionary<string, string>(StringComparer.Ordinal)
			{
				["content-type"] = request.Headers["content-type"],
				["host"] = request.Headers["host"],
				["x-amz-date"] = date,
				["x-amz-security-token"] = Token
			};
			StringBuilder body = new StringBuilder();

			foreach (KeyValuePair<string, string> entry in request.Params)
			{
				if (body.Length > 0)
				{
					body.Append('&');
				}

				body.Append(AwsSigV4.Encode(entry.Key)).Append('=').Append(AwsSigV4.Encode(entry.Value));
			}

			Assert.AreEqual(AwsSigV4.Authorization(Key, Secret, "POST", "/", "", headers,
				Encoding.UTF8.GetBytes(body.ToString()), "us-east-1", "ec2", date), authorization);
		}

		[TestMethod]
		public void PaginationFollowsNextToken()
		{
			stub.Ec2(
				new StubAwsServer.Reply(200, StubAwsServer.Describe("page-1",
					StubAwsServer.Instance("i-0a", "10.0.0.1", null, null, null))),
				new StubAwsServer.Reply(200, StubAwsServer.Describe(null,
					StubAwsServer.Instance("i-0b", "10.0.0.2", null, null, null))));

			List<Host> hosts = Builder().Build().RefreshSeedCandidates(Tend);
			CollectionAssert.AreEqual(new List<string> { "10.0.0.1:3000", "10.0.0.2:3000" }, Names(hosts));
			Assert.AreEqual(2, stub.Requests().Count);
			Assert.IsFalse(stub.Requests()[0].Params.ContainsKey("NextToken"));
			Assert.AreEqual("page-1", stub.Requests()[1].Params["NextToken"]);
		}

		[TestMethod]
		public void ApiErrorIncludesCodeMessageAndRequestIdWithoutSecrets()
		{
			stub.Ec2Default(new StubAwsServer.Reply(403,
				StubAwsServer.Error("UnauthorizedOperation", "You are not authorized to perform this operation.",
					"req-123")));

			AerospikeException e = Assert.Throws<AerospikeException>(() =>
				Builder().Build().RefreshSeedCandidates(Tend));
			StringAssert.Contains(e.BaseMessage, "DescribeInstances failed: HTTP 403 UnauthorizedOperation");
			StringAssert.Contains(e.BaseMessage, "request id req-123");
			Assert.IsFalse(e.BaseMessage.Contains(Secret));
			Assert.IsFalse(e.BaseMessage.Contains(Token));
		}

		[TestMethod]
		public void InvalidBuilderSettingsRejected()
		{
			Assert.Throws<AerospikeException>(() =>
				AwsEc2TagSeedCandidateProvider.CreateBuilder().TagFilter("a", "b").Build());
			Assert.Throws<AerospikeException>(() =>
				AwsEc2TagSeedCandidateProvider.CreateBuilder().Region("us-east-1").Build());
			Assert.Throws<AerospikeException>(() =>
				Builder().Filter("instance-state-name", "running").Build());
			Assert.Throws<AerospikeException>(() =>
				Builder().Credentials(Key, Secret, null).CredentialsHook(() => new Credentials(Key, Secret, null))
					.Build());
		}

		[TestMethod]
		public void CredentialsToStringHidesSecret()
		{
			Assert.IsFalse(new Credentials(Key, Secret, Token).ToString().Contains(Secret));
		}

		[TestMethod]
		public void EnvironmentCredentialsUsed()
		{
			stub.Ec2Default(new StubAwsServer.Reply(200, All()));
			AwsEc2TagSeedCandidateProvider provider = Builder().Build();
			provider.RefreshSeedCandidates(Tend);
			Assert.AreEqual("environment", provider.CredentialSource());
		}

		[TestMethod]
		public void ConfigCredentialsPreferred()
		{
			stub.Ec2Default(new StubAwsServer.Reply(200, All()));
			AwsEc2TagSeedCandidateProvider provider = Builder()
				.Credentials("AKIDCONFIG", Secret, null)
				.Build();
			provider.RefreshSeedCandidates(Tend);
			Assert.AreEqual("config", provider.CredentialSource());
			Assert.IsTrue(stub.Requests()[0].Headers["authorization"].Contains("AKIDCONFIG"));
		}
	}
}
