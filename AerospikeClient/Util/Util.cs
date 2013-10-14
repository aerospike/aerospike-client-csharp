/*
 * Aerospike Client - C# Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using System.Collections.Generic;

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
	}
}