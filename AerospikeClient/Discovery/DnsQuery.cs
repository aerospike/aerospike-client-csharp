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
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// Minimal DNS client for A / AAAA / SRV queries used by <see cref="DnsSeedCandidateProvider"/>.
	/// No external dependency; UDP with TCP fallback on truncated answers.
	/// </summary>
	internal static class DnsQuery
	{
		internal const ushort TypeA = 1;
		internal const ushort TypeAAAA = 28;
		internal const ushort TypeSRV = 33;
		internal const ushort ClassIN = 1;

		internal static List<string> Query(IPEndPoint server, string name, ushort type, int timeoutMs)
		{
			byte[] request = BuildQuery(name, type);
			byte[] response = Exchange(server, request, timeoutMs);
			return ParseAnswers(response, type);
		}

		private static byte[] BuildQuery(string name, ushort type)
		{
			List<byte> packet = new List<byte>(64);
			ushort id = (ushort)Environment.TickCount;
			packet.Add((byte)(id >> 8));
			packet.Add((byte)id);
			packet.Add(0x01); // RD
			packet.Add(0x00);
			packet.Add(0x00);
			packet.Add(0x01); // QDCOUNT = 1
			packet.Add(0x00);
			packet.Add(0x00); // ANCOUNT
			packet.Add(0x00);
			packet.Add(0x00); // NSCOUNT
			packet.Add(0x00);
			packet.Add(0x00); // ARCOUNT
			WriteName(packet, name);
			packet.Add((byte)(type >> 8));
			packet.Add((byte)type);
			packet.Add((byte)(ClassIN >> 8));
			packet.Add((byte)ClassIN);
			return packet.ToArray();
		}

		private static void WriteName(List<byte> packet, string name)
		{
			string n = name.TrimEnd('.');

			if (n.Length == 0)
			{
				packet.Add(0);
				return;
			}

			foreach (string label in n.Split('.'))
			{
				byte[] bytes = Encoding.ASCII.GetBytes(label);

				if (bytes.Length == 0 || bytes.Length > 63)
				{
					throw new AerospikeException("Invalid DNS name label in '" + name + "'");
				}

				packet.Add((byte)bytes.Length);
				packet.AddRange(bytes);
			}

			packet.Add(0);
		}

		private static byte[] Exchange(IPEndPoint server, byte[] request, int timeoutMs)
		{
			using (UdpClient udp = new UdpClient())
			{
				udp.Client.ReceiveTimeout = Math.Max(1, timeoutMs);
				udp.Client.SendTimeout = Math.Max(1, timeoutMs);
				udp.Connect(server);
				udp.Send(request, request.Length);

				IPEndPoint remote = new IPEndPoint(IPAddress.Any, 0);
				byte[] response;

				try
				{
					response = udp.Receive(ref remote);
				}
				catch (SocketException se) when (se.SocketErrorCode == SocketError.TimedOut)
				{
					throw new TimeoutException("DNS UDP query timed out");
				}

				if (response.Length < 12)
				{
					throw new AerospikeException("DNS response too short");
				}

				bool truncated = (response[2] & 0x02) != 0;

				if (!truncated)
				{
					return response;
				}
			}

			return ExchangeTcp(server, request, timeoutMs);
		}

		private static byte[] ExchangeTcp(IPEndPoint server, byte[] request, int timeoutMs)
		{
			using (TcpClient tcp = new TcpClient())
			{
				IAsyncResult ar = tcp.BeginConnect(server.Address, server.Port, null, null);

				if (!ar.AsyncWaitHandle.WaitOne(timeoutMs))
				{
					throw new TimeoutException("DNS TCP connect timed out");
				}

				tcp.EndConnect(ar);
				tcp.ReceiveTimeout = Math.Max(1, timeoutMs);
				tcp.SendTimeout = Math.Max(1, timeoutMs);

				NetworkStream stream = tcp.GetStream();
				byte[] len = [(byte)(request.Length >> 8), (byte)request.Length];
				stream.Write(len, 0, 2);
				stream.Write(request, 0, request.Length);

				byte[] lenBuf = new byte[2];
				ReadFully(stream, lenBuf, 2);
				int length = (lenBuf[0] << 8) | lenBuf[1];
				byte[] response = new byte[length];
				ReadFully(stream, response, length);
				return response;
			}
		}

		private static void ReadFully(NetworkStream stream, byte[] buffer, int length)
		{
			int offset = 0;

			while (offset < length)
			{
				int n = stream.Read(buffer, offset, length - offset);

				if (n <= 0)
				{
					throw new AerospikeException("DNS TCP response truncated");
				}

				offset += n;
			}
		}

		private static List<string> ParseAnswers(byte[] response, ushort expectedType)
		{
			if (response.Length < 12)
			{
				throw new AerospikeException("DNS response too short");
			}

			int rcode = response[3] & 0x0f;

			if (rcode == 3)
			{
				throw new DnsNxDomainException();
			}

			if (rcode == 2)
			{
				throw new DnsServFailException();
			}

			if (rcode != 0)
			{
				throw new AerospikeException("DNS response rcode " + rcode);
			}

			int qdCount = (response[4] << 8) | response[5];
			int anCount = (response[6] << 8) | response[7];
			int offset = 12;

			for (int i = 0; i < qdCount; i++)
			{
				offset = SkipName(response, offset);
				offset += 4; // type + class
			}

			List<string> answers = new List<string>();

			for (int i = 0; i < anCount; i++)
			{
				offset = SkipName(response, offset);

				if (offset + 10 > response.Length)
				{
					break;
				}

				ushort type = (ushort)((response[offset] << 8) | response[offset + 1]);
				offset += 2;
				offset += 2; // class
				offset += 4; // ttl
				int rdLength = (response[offset] << 8) | response[offset + 1];
				offset += 2;

				if (offset + rdLength > response.Length)
				{
					break;
				}

				if (type == expectedType)
				{
					string value = FormatRdata(response, offset, rdLength, type);

					if (value != null)
					{
						answers.Add(value);
					}
				}

				offset += rdLength;
			}

			return answers;
		}

		private static string FormatRdata(byte[] response, int offset, int rdLength, ushort type)
		{
			switch (type)
			{
				case TypeA:
					if (rdLength != 4)
					{
						return null;
					}
					return response[offset] + "." + response[offset + 1] + "." + response[offset + 2] + "." + response[offset + 3];

				case TypeAAAA:
					if (rdLength != 16)
					{
						return null;
					}
					return new IPAddress(Slice(response, offset, 16)).ToString();

				case TypeSRV:
					if (rdLength < 7)
					{
						return null;
					}
					int priority = (response[offset] << 8) | response[offset + 1];
					int weight = (response[offset + 2] << 8) | response[offset + 3];
					int port = (response[offset + 4] << 8) | response[offset + 5];
					string target = ReadName(response, offset + 6, out _);
					return priority + " " + weight + " " + port + " " + target;

				default:
					return null;
			}
		}

		private static byte[] Slice(byte[] source, int offset, int length)
		{
			byte[] copy = new byte[length];
			Buffer.BlockCopy(source, offset, copy, 0, length);
			return copy;
		}

		private static int SkipName(byte[] message, int offset)
		{
			while (offset < message.Length)
			{
				byte len = message[offset];

				if (len == 0)
				{
					return offset + 1;
				}

				if ((len & 0xc0) == 0xc0)
				{
					return offset + 2;
				}

				offset += 1 + len;
			}

			throw new AerospikeException("Invalid DNS name in response");
		}

		private static string ReadName(byte[] message, int offset, out int nextOffset)
		{
			StringBuilder sb = new StringBuilder();
			int jumped = 0;
			int pos = offset;
			nextOffset = offset;

			while (pos < message.Length)
			{
				byte len = message[pos];

				if (len == 0)
				{
					if (jumped == 0)
					{
						nextOffset = pos + 1;
					}
					break;
				}

				if ((len & 0xc0) == 0xc0)
				{
					int pointer = ((len & 0x3f) << 8) | message[pos + 1];

					if (jumped == 0)
					{
						nextOffset = pos + 2;
					}

					pos = pointer;
					jumped++;

					if (jumped > 20)
					{
						throw new AerospikeException("DNS name pointer loop");
					}

					continue;
				}

				pos++;

				if (sb.Length > 0)
				{
					sb.Append('.');
				}

				sb.Append(Encoding.ASCII.GetString(message, pos, len));
				pos += len;

				if (jumped == 0)
				{
					nextOffset = pos;
				}
			}

			return sb.ToString().ToLowerInvariant();
		}

		internal sealed class DnsNxDomainException : Exception
		{
			internal DnsNxDomainException() : base("NXDOMAIN") { }
		}

		internal sealed class DnsServFailException : Exception
		{
			internal DnsServFailException() : base("SERVFAIL") { }
		}
	}
}
