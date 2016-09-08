/* 
 * Copyright 2012-2016 Aerospike, Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Authentication;
using System.Text;
using System.Threading;

namespace Aerospike.Client
{
	public sealed class Util
	{
		public static void Sleep(int millis)
		{
			try
			{
				Thread.Sleep(millis);
			}
			catch (ThreadInterruptedException)
			{
			}
		}

		public static string GetErrorMessage(Exception e)
		{
			// Find initial cause of exception
			Exception cause = e;
			while (cause.InnerException != null)
			{
				cause = e.InnerException;
			}

			// Connection error messages don't need a stacktrace.
			if (cause is SocketException || cause is AerospikeException.Connection)
			{
				return e.Message;
			}

			// Inner exception stack traces identify the real problem.
			return e.Message + Environment.NewLine + cause.StackTrace;
		}

		public static string ReadFileEncodeBase64(string path)
		{
			try
			{
				byte[] bytes = File.ReadAllBytes(path);
				return Convert.ToBase64String(bytes);
			}
			catch (Exception e)
			{
				throw new AerospikeException("Failed to read " + path, e);
			}
		}

		public static string MapToString(IDictionary<object,object> map)
		{
			StringBuilder sb = new StringBuilder(200);
			MapToString(sb, map);
			return sb.ToString();
		}

		private static void MapToString(StringBuilder sb, IDictionary<object, object> map)
		{
			sb.Append('[');
			int i = 0;

			foreach (KeyValuePair<object, object> pair in map)
			{
				if (i > 0)
				{
					sb.Append(", ");
				}
				sb.Append('{');
				ObjectToString(sb, pair.Key);
				sb.Append(",");
				ObjectToString(sb, pair.Value);
				sb.Append('}');
				i++;
			}
			sb.Append(']');
		}

		public static string ListToString(List<object> list)
		{
			StringBuilder sb = new StringBuilder(200);
			ListToString(sb, list);
			return sb.ToString();
		}

		private static void ListToString(StringBuilder sb, List<object> list)
		{
			sb.Append('[');

			for (int i = 0; i < list.Count; i++)
			{
				if (i > 0)
				{
					sb.Append(", ");
				}
				ObjectToString(sb, list[i]);
			}
			sb.Append(']');
		}

		public static string ArrayToString(object[] list)
		{
			StringBuilder sb = new StringBuilder(200);
			ArrayToString(sb, list);
			return sb.ToString();
		}

		private static void ArrayToString(StringBuilder sb, object[] list)
		{
			sb.Append('[');

			for (int i = 0; i < list.Length; i++)
			{
				if (i > 0)
				{
					sb.Append(", ");
				}
				ObjectToString(sb, list[i]);
			}
			sb.Append(']');
		}

		public static string BytesToString(byte[] bytes)
		{
			StringBuilder sb = new StringBuilder(200);
			sb.Append('[');

			for (int i = 0; i < bytes.Length; i++)
			{
				if (i > 0)
				{
					sb.Append(", ");
				}
				sb.Append(bytes[i]);
			}
			sb.Append(']');
			return sb.ToString();
		}

		public static string ObjectToString(object obj)
		{
			StringBuilder sb = new StringBuilder(200);
			ObjectToString(sb, obj);
			return sb.ToString();
		}

		/// <summary>
		/// String conversion for objects containing List, Dictionary and array.
		/// </summary>
		private static void ObjectToString(StringBuilder sb, object obj)
		{
			if (obj is object[])
			{
				ArrayToString(sb, (object[])obj);
				return;
			}

			if (obj is List<object>)
			{
				ListToString(sb, (List<object>)obj);
				return;
			}

			if (obj is Dictionary<object, object>)
			{
				MapToString(sb, (Dictionary<object, object>)obj);
				return;
			}

			sb.Append(obj);
		}

		public static bool ToBool(object result)
		{
			return (result != null) ? ((long)result != 0) : false;
		}

#if (AS_OPTIMIZE_WINDOWS)
		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
		static extern int memcmp(byte[] b1, byte[] b2, long count);

		public static bool ByteArrayEquals(byte[] b1, byte[] b2)
		{
			return b1.Length == b2.Length && memcmp(b1, b2, b1.Length) == 0;
		}
#else
		public static bool ByteArrayEquals(byte[] b1, byte[] b2)
		{
			if (b1.Length != b2.Length)
			{
				return false;
			}

			for (int i = 0; i < b1.Length; i++)
			{
				if (b1[i] != b2[i])
				{
					return false;
				}
			}
			return true;
		}
#endif	

		public static SslProtocols ParseSslProtocols(string protocolString)
		{
			string str = protocolString.Trim();
			SslProtocols protocols;

			if (protocolString.Length > 0)
			{
				protocols = SslProtocols.None;
				string[] list = str.Split(',');

				foreach (string item in list)
				{
					string s = item.Trim();

					if (s.Length > 0)
					{
						protocols |= (SslProtocols)Enum.Parse(typeof(SslProtocols), s);
					}
				}
			}
			else
			{
				protocols = SslProtocols.Default;
			}
			return protocols;
		}
	}
}
