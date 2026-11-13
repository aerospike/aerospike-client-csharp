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
	/// In-process DNS server on a loopback UDP port, for tests. Answers A, AAAA and SRV queries from
	/// in-memory records; unknown names get NXDOMAIN.
	/// </summary>
	internal sealed class StubDnsServer : IDisposable
	{
		internal const int A = 1;
		internal const int AAAA = 28;
		internal const int SRV = 33;
		internal const int NoError = 0;
		internal const int ServFail = 2;
		internal const int NxDomain = 3;
		internal const int NoAnswer = -1;

		private readonly UdpClient socket;
		private readonly Thread thread;
		private readonly ConcurrentDictionary<string, List<byte[]>> answers = new ConcurrentDictionary<string, List<byte[]>>();
		private readonly ConcurrentDictionary<string, List<byte[]>> additional = new ConcurrentDictionary<string, List<byte[]>>();
		private readonly ConcurrentDictionary<string, int> rcodes = new ConcurrentDictionary<string, int>();
		private readonly List<string> queries = new List<string>();
		private readonly object queryLock = new object();
		private volatile bool silent;
		private volatile bool truncated;
		private volatile bool closed;
		private TcpListener tcp;

		internal StubDnsServer() : this(false)
		{
		}

		internal StubDnsServer(bool silent)
		{
			this.silent = silent;
			socket = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
			thread = new Thread(Run)
			{
				Name = "stub-dns",
				IsBackground = true
			};
			thread.Start();
		}

		internal string Address => "127.0.0.1:" + ((IPEndPoint)socket.Client.LocalEndPoint).Port;

		internal StubDnsServer Truncate()
		{
			truncated = true;
			return this;
		}

		internal StubDnsServer HangTcp()
		{
			int port = ((IPEndPoint)socket.Client.LocalEndPoint).Port;
			tcp = new TcpListener(IPAddress.Loopback, port);
			tcp.Start(50);
			return this;
		}

		internal StubDnsServer ARecord(string name, params string[] addresses)
		{
			foreach (string address in addresses)
			{
				Answer(name, A, IPAddress.Parse(address).GetAddressBytes());
			}
			return this;
		}

		internal StubDnsServer Aaaa(string name, params string[] addresses)
		{
			foreach (string address in addresses)
			{
				Answer(name, AAAA, IPAddress.Parse(address).GetAddressBytes());
			}
			return this;
		}

		internal StubDnsServer Srv(string name, int priority, int weight, int port, string target)
		{
			using (MemoryStream ms = new MemoryStream())
			{
				ms.WriteByte((byte)(priority >> 8));
				ms.WriteByte((byte)priority);
				ms.WriteByte((byte)(weight >> 8));
				ms.WriteByte((byte)weight);
				ms.WriteByte((byte)(port >> 8));
				ms.WriteByte((byte)port);
				byte[] encoded = EncodeName(target);
				ms.Write(encoded, 0, encoded.Length);
				Answer(name, SRV, ms.ToArray());
			}
			return this;
		}

		internal StubDnsServer AdditionalA(string name, string target, string address)
		{
			string k = Key(name, SRV);
			List<byte[]> list = additional.GetOrAdd(k, _ => new List<byte[]>());
			lock (list)
			{
				list.Add(Record(target, A, IPAddress.Parse(address).GetAddressBytes()));
			}
			return this;
		}

		internal StubDnsServer Rcode(string name, int type, int rcode)
		{
			rcodes[Key(name, type)] = rcode;
			return this;
		}

		internal IList<string> Queries
		{
			get
			{
				lock (queryLock)
				{
					return new List<string>(queries);
				}
			}
		}

		private void Answer(string name, int type, byte[] rdata)
		{
			string k = Key(name, type);
			List<byte[]> list = answers.GetOrAdd(k, _ => new List<byte[]>());
			lock (list)
			{
				list.Add(Record(name, type, rdata));
			}
		}

		private static string Key(string name, int type)
		{
			string n = name.ToLowerInvariant();
			return (n.EndsWith(".") ? n : n + ".") + '/' + type;
		}

		private void Run()
		{
			while (!closed)
			{
				try
				{
					IPEndPoint remote = new IPEndPoint(IPAddress.Any, 0);
					byte[] data = socket.Receive(ref remote);

					if (data.Length < 12)
					{
						continue;
					}

					int offset = 0;
					ushort id = ReadU16(data, ref offset);
					ushort flags = ReadU16(data, ref offset);
					offset += 8; // counts
					int qstart = offset;
					string qname = DecodeName(data, ref offset);
					int qtype = ReadU16(data, ref offset);
					offset += 2; // class
					int qend = offset;
					string key = Key(qname, qtype);

					lock (queryLock)
					{
						queries.Add(key);
					}

					if (silent)
					{
						continue;
					}

					List<byte[]> an = GetList(answers, key);
					List<byte[]> ar = GetList(additional, key);
					int rcode = rcodes.TryGetValue(key, out int code) ? code :
						(an.Count == 0 && !HasName(qname) ? NxDomain : NoError);

					if (rcode == NoAnswer)
					{
						continue;
					}

					if (rcode != NoError || truncated)
					{
						an = [];
						ar = [];
					}

					using (MemoryStream ms = new MemoryStream())
					{
						WriteU16(ms, id);
						int responseFlags = 0x8000 | 0x0400 | (truncated ? 0x0200 : 0) | (flags & 0x0100) | 0x0080 | rcode;
						WriteU16(ms, (ushort)responseFlags);
						WriteU16(ms, 1);
						WriteU16(ms, (ushort)an.Count);
						WriteU16(ms, 0);
						WriteU16(ms, (ushort)ar.Count);
						ms.Write(data, qstart, qend - qstart);

						foreach (byte[] rr in an)
						{
							ms.Write(rr, 0, rr.Length);
						}

						foreach (byte[] rr in ar)
						{
							ms.Write(rr, 0, rr.Length);
						}

						byte[] response = ms.ToArray();
						socket.Send(response, response.Length, remote);
					}
				}
				catch (SocketException)
				{
					if (closed)
					{
						return;
					}
				}
				catch (ObjectDisposedException)
				{
					return;
				}
			}
		}

		private static List<byte[]> GetList(ConcurrentDictionary<string, List<byte[]>> map, string key)
		{
			if (!map.TryGetValue(key, out List<byte[]> list))
			{
				return [];
			}

			lock (list)
			{
				return new List<byte[]>(list);
			}
		}

		private bool HasName(string name)
		{
			string prefix = Key(name, 0);
			prefix = prefix.Substring(0, prefix.Length - 1);

			foreach (string key in answers.Keys)
			{
				if (key.StartsWith(prefix))
				{
					return true;
				}
			}

			return false;
		}

		private static byte[] Record(string name, int type, byte[] rdata)
		{
			using (MemoryStream ms = new MemoryStream())
			{
				byte[] encoded = EncodeName(name);
				ms.Write(encoded, 0, encoded.Length);
				WriteU16(ms, (ushort)type);
				WriteU16(ms, 1);
				WriteU32(ms, 60);
				WriteU16(ms, (ushort)rdata.Length);
				ms.Write(rdata, 0, rdata.Length);
				return ms.ToArray();
			}
		}

		private static byte[] EncodeName(string name)
		{
			using (MemoryStream ms = new MemoryStream())
			{
				foreach (string label in name.Split('.'))
				{
					if (label.Length == 0)
					{
						continue;
					}

					byte[] bytes = Encoding.ASCII.GetBytes(label);
					ms.WriteByte((byte)bytes.Length);
					ms.Write(bytes, 0, bytes.Length);
				}

				ms.WriteByte(0);
				return ms.ToArray();
			}
		}

		private static string DecodeName(byte[] data, ref int offset)
		{
			List<string> labels = new List<string>();
			int len;

			while ((len = data[offset++] & 0xff) != 0)
			{
				labels.Add(Encoding.ASCII.GetString(data, offset, len));
				offset += len;
			}

			return string.Join(".", labels) + ".";
		}

		private static ushort ReadU16(byte[] data, ref int offset)
		{
			ushort value = (ushort)((data[offset] << 8) | data[offset + 1]);
			offset += 2;
			return value;
		}

		private static void WriteU16(MemoryStream ms, ushort value)
		{
			ms.WriteByte((byte)(value >> 8));
			ms.WriteByte((byte)value);
		}

		private static void WriteU32(MemoryStream ms, uint value)
		{
			ms.WriteByte((byte)(value >> 24));
			ms.WriteByte((byte)(value >> 16));
			ms.WriteByte((byte)(value >> 8));
			ms.WriteByte((byte)value);
		}

		public void Dispose()
		{
			closed = true;
			tcp?.Stop();
			socket.Close();
			thread.Join(1000);
		}
	}
}
