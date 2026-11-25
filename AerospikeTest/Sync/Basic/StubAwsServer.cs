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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Aerospike.Test
{
	/// <summary>
	/// In-process AWS endpoints on one loopback TCP port, for tests: EC2 Query API at <c>/</c>, STS
	/// at <c>/sts</c>, ECS container credentials at <c>/ecs/...</c> and IMDSv2 at <c>/latest/...</c>.
	/// </summary>
	internal sealed class StubAwsServer : IDisposable
	{
		internal const string ImdsToken = "imds-session-token";
		internal const string ImdsRole = "aerospike-seed-role";
		internal const string EcsAuthorization = "ecs-authorization-token";

		internal sealed class Request
		{
			internal readonly string Method;
			internal readonly string Path;
			internal readonly Dictionary<string, string> Headers;
			internal readonly Dictionary<string, string> Params;

			internal Request(
				string method,
				string path,
				Dictionary<string, string> headers,
				Dictionary<string, string> parameters)
			{
				Method = method;
				Path = path;
				Headers = headers;
				Params = parameters;
			}
		}

		internal sealed class Reply
		{
			internal readonly int Status;
			internal readonly string Body;

			internal Reply(int status, string body)
			{
				Status = status;
				Body = body;
			}
		}

		private readonly TcpListener listener;
		private readonly Thread thread;
		private readonly List<Request> requests = new List<Request>();
		private readonly Queue<Reply> ec2 = new Queue<Reply>();
		private readonly ConcurrentDictionary<string, int> counts = new ConcurrentDictionary<string, int>();
		private readonly object sync = new object();
		private volatile Reply ec2Default = new Reply(200, Describe(null));
		private volatile Reply sts;
		private volatile string credentialsJson;
		private volatile bool closed;

		internal StubAwsServer()
		{
			listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			thread = new Thread(AcceptLoop)
			{
				IsBackground = true,
				Name = "stub-aws"
			};
			thread.Start();
		}

		internal string Url => "http://127.0.0.1:" + ((IPEndPoint)listener.LocalEndpoint).Port;

		internal void Ec2(params Reply[] replies)
		{
			lock (sync)
			{
				foreach (Reply reply in replies)
				{
					ec2.Enqueue(reply);
				}
			}
		}

		internal void Ec2Default(Reply reply)
		{
			ec2Default = reply;
		}

		internal void Sts(Reply reply)
		{
			sts = reply;
		}

		internal void Credentials(string json)
		{
			credentialsJson = json;
		}

		internal List<Request> Requests()
		{
			lock (sync)
			{
				return new List<Request>(requests);
			}
		}

		internal int Count(string path)
		{
			return counts.TryGetValue(path, out int n) ? n : 0;
		}

		private Reply NextEc2()
		{
			lock (sync)
			{
				return ec2.Count > 0 ? ec2.Dequeue() : ec2Default;
			}
		}

		private void AcceptLoop()
		{
			while (!closed)
			{
				TcpClient client;

				try
				{
					client = listener.AcceptTcpClient();
				}
				catch (Exception)
				{
					return;
				}

				ThreadPool.QueueUserWorkItem(_ => Handle(client));
			}
		}

		private void Handle(TcpClient client)
		{
			try
			{
				using (client)
				using (NetworkStream stream = client.GetStream())
				{
					client.ReceiveTimeout = 5000;
					client.SendTimeout = 5000;
					string requestLine = ReadLine(stream);

					if (string.IsNullOrEmpty(requestLine))
					{
						return;
					}

					string[] parts = requestLine.Split(' ');
					string method = parts[0];
					string target = parts.Length > 1 ? parts[1] : "/";
					int q = target.IndexOf('?');
					string path = q >= 0 ? target.Substring(0, q) : target;
					Dictionary<string, string> headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

					while (true)
					{
						string line = ReadLine(stream);

						if (line.Length == 0)
						{
							break;
						}

						int i = line.IndexOf(':');

						if (i > 0)
						{
							headers[line.Substring(0, i).Trim().ToLowerInvariant()] = line.Substring(i + 1).Trim();
						}
					}

					byte[] bodyBytes = Array.Empty<byte>();

					if (headers.TryGetValue("content-length", out string length) &&
						int.TryParse(length, out int n) && n > 0)
					{
						bodyBytes = new byte[n];
						int offset = 0;

						while (offset < n)
						{
							int read = stream.Read(bodyBytes, offset, n - offset);

							if (read <= 0)
							{
								break;
							}

							offset += read;
						}
					}

					string body = Encoding.UTF8.GetString(bodyBytes);
					Request recorded = new Request(method, path, headers, Form(body));

					lock (sync)
					{
						requests.Add(recorded);
					}

					counts.AddOrUpdate(path, 1, (_, c) => c + 1);
					Reply reply = Resolve(method, path, headers);

					if (reply == null)
					{
						reply = new Reply(500, "");
					}

					byte[] bytes = Encoding.UTF8.GetBytes(reply.Body ?? "");
					string contentType = path.Equals("/", StringComparison.Ordinal) ||
						path.Equals("/sts", StringComparison.Ordinal) ? "text/xml" : "text/plain";
					string response =
						"HTTP/1.1 " + reply.Status + " OK\r\n" +
						"Content-Type: " + contentType + "\r\n" +
						"Content-Length: " + bytes.Length + "\r\n" +
						"Connection: close\r\n\r\n";
					byte[] head = Encoding.ASCII.GetBytes(response);
					stream.Write(head, 0, head.Length);
					stream.Write(bytes, 0, bytes.Length);
					stream.Flush();
				}
			}
			catch (Exception)
			{
			}
		}

		private Reply Resolve(string method, string path, Dictionary<string, string> headers)
		{
			if (path.Equals("/", StringComparison.Ordinal))
			{
				return NextEc2();
			}

			if (path.Equals("/sts", StringComparison.Ordinal))
			{
				return sts;
			}

			if (path.StartsWith("/ecs/", StringComparison.Ordinal))
			{
				headers.TryGetValue("authorization", out string auth);
				return EcsAuthorization.Equals(auth, StringComparison.Ordinal) ?
					new Reply(200, credentialsJson) :
					new Reply(401, "{\"message\":\"unauthorized\"}");
			}

			if (path.Equals("/latest/api/token", StringComparison.Ordinal))
			{
				return method.Equals("PUT", StringComparison.Ordinal) &&
					headers.ContainsKey("x-aws-ec2-metadata-token-ttl-seconds") ?
					new Reply(200, ImdsToken) : new Reply(400, "");
			}

			if (path.StartsWith("/latest/meta-data/iam/security-credentials/", StringComparison.Ordinal))
			{
				headers.TryGetValue("x-aws-ec2-metadata-token", out string token);

				if (!ImdsToken.Equals(token, StringComparison.Ordinal))
				{
					return new Reply(401, "");
				}

				if (path.EndsWith('/'))
				{
					return new Reply(200, ImdsRole + "\n");
				}

				return path.EndsWith("/" + ImdsRole, StringComparison.Ordinal) ?
					new Reply(200, credentialsJson) : new Reply(404, "");
			}

			return new Reply(404, "");
		}

		private static string ReadLine(Stream stream)
		{
			MemoryStream buffer = new MemoryStream(128);

			while (true)
			{
				int b = stream.ReadByte();

				if (b < 0)
				{
					break;
				}

				if (b == '\n')
				{
					break;
				}

				if (b != '\r')
				{
					buffer.WriteByte((byte)b);
				}
			}

			return Encoding.ASCII.GetString(buffer.ToArray());
		}

		private static Dictionary<string, string> Form(string body)
		{
			Dictionary<string, string> map = new Dictionary<string, string>();

			if (string.IsNullOrEmpty(body))
			{
				return map;
			}

			foreach (string pair in body.Split('&'))
			{
				int i = pair.IndexOf('=');
				string key = i < 0 ? pair : pair.Substring(0, i);
				string value = i < 0 ? "" : pair.Substring(i + 1);
				map[WebUtility.UrlDecode(key)] = WebUtility.UrlDecode(value);
			}

			return map;
		}

		internal static string Describe(string nextToken, params string[] instances)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
			sb.Append("<DescribeInstancesResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">\n");
			sb.Append("  <requestId>8f7724cf-496f-496e-8fe3-example</requestId>\n");
			sb.Append("  <reservationSet>\n");

			if (instances.Length > 0)
			{
				sb.Append("    <item>\n      <reservationId>r-1234567890abcdef0</reservationId>\n");
				sb.Append("      <ownerId>123456789012</ownerId>\n      <groupSet/>\n      <instancesSet>\n");

				foreach (string instance in instances)
				{
					sb.Append(instance);
				}

				sb.Append("      </instancesSet>\n    </item>\n");
			}

			sb.Append("  </reservationSet>\n");

			if (nextToken != null)
			{
				sb.Append("  <nextToken>").Append(nextToken).Append("</nextToken>\n");
			}

			sb.Append("</DescribeInstancesResponse>\n");
			return sb.ToString();
		}

		internal static string Instance(
			string id,
			string privateIp,
			string publicIp,
			string privateDns,
			string publicDns)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("        <item>\n          <instanceId>").Append(id).Append("</instanceId>\n");
			sb.Append("          <imageId>ami-0abcdef1234567890</imageId>\n");
			sb.Append("          <instanceState>\n            <code>16</code>\n            <name>running</name>\n");
			sb.Append("          </instanceState>\n");
			Field(sb, "privateDnsName", privateDns);
			Field(sb, "dnsName", publicDns);
			sb.Append("          <instanceType>c5.large</instanceType>\n");
			Field(sb, "privateIpAddress", privateIp);
			Field(sb, "ipAddress", publicIp);
			sb.Append("          <networkInterfaceSet>\n            <item>\n");
			sb.Append("              <privateIpAddress>192.0.2.254</privateIpAddress>\n");
			sb.Append("            </item>\n          </networkInterfaceSet>\n");
			sb.Append("          <tagSet>\n            <item>\n              <key>aerospike-cluster</key>\n");
			sb.Append("              <value>prod-a</value>\n            </item>\n          </tagSet>\n");
			sb.Append("        </item>\n");
			return sb.ToString();
		}

		private static void Field(StringBuilder sb, string name, string value)
		{
			if (value != null)
			{
				sb.Append("          <").Append(name).Append('>').Append(value).Append("</").Append(name)
					.Append(">\n");
			}
		}

		internal static string Error(string code, string message, string requestId)
		{
			return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response><Errors><Error><Code>" + code +
				"</Code><Message>" + message + "</Message></Error></Errors><RequestID>" + requestId +
				"</RequestID></Response>";
		}

		internal static string AssumeRoleWithWebIdentity(string key, string secret, string token, string expiration)
		{
			return "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n" +
				"  <AssumeRoleWithWebIdentityResult>\n" +
				"    <Credentials>\n" +
				"      <SessionToken>" + token + "</SessionToken>\n" +
				"      <SecretAccessKey>" + secret + "</SecretAccessKey>\n" +
				"      <Expiration>" + expiration + "</Expiration>\n" +
				"      <AccessKeyId>" + key + "</AccessKeyId>\n" +
				"    </Credentials>\n" +
				"  </AssumeRoleWithWebIdentityResult>\n" +
				"</AssumeRoleWithWebIdentityResponse>\n";
		}

		internal static string CredentialsJson(string key, string secret, string token, string expiration)
		{
			return "{\n  \"Code\" : \"Success\",\n  \"LastUpdated\" : \"2012-04-26T16:39:16Z\",\n" +
				"  \"Type\" : \"AWS-HMAC\",\n  \"AccessKeyId\" : \"" + key + "\",\n  \"SecretAccessKey\" : \"" +
				secret + "\",\n  \"Token\" : \"" + token + "\",\n  \"Expiration\" : \"" + expiration + "\"\n}";
		}

		public void Dispose()
		{
			closed = true;

			try
			{
				listener.Stop();
			}
			catch (Exception)
			{
			}

			thread.Join(2000);
		}
	}
}
