/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using System.Collections.Generic;
using System.Runtime.InteropServices;

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
			// Connection error messages don't need a stacktrace.
			Exception cause = e.InnerException;
			if (e is SocketException || e is AerospikeException.Connection || cause is SocketException)
			{
				return e.Message;
			}
			// Unexpected exceptions need a stacktrace.
			return e.Message + Environment.NewLine + e.StackTrace;
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

		public static string MapToString(Dictionary<object,object> map)
		{
			StringBuilder sb = new StringBuilder(200);
			MapToString(sb, map);
			return sb.ToString();
		}

		private static void MapToString(StringBuilder sb, Dictionary<object, object> map)
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

		[DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
		static extern int memcmp(byte[] b1, byte[] b2, long count);

		public static bool ByteArrayEquals(byte[] b1, byte[] b2)
		{
			return b1.Length == b2.Length && memcmp(b1, b2, b1.Length) == 0;
		}
	}
}
