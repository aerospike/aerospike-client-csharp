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
using Aerospike.Client;
using System.Collections;
using System.Text;

namespace Aerospike.Example;

internal static class ExampleValueFormatter
{
	private const int MaxLength = 1000;

	public static string Format(object value)
	{
		if (value == null)
		{
			return "null";
		}

		if (value is string text)
		{
			return text;
		}

		if (value is byte[] bytes)
		{
			return ByteUtil.BytesToHexString(bytes);
		}

		if (value is Record record)
		{
			return FormatRecord(record);
		}

		StringBuilder builder = new();
		AppendValue(value, builder);
		return builder.ToString();
	}

	public static object[] FormatArgs(object[] args)
	{
		if (args == null || args.Length == 0)
		{
			return args;
		}

		object[] formatted = new object[args.Length];

		for (int i = 0; i < args.Length; i++)
		{
			formatted[i] = Format(args[i]);
		}

		return formatted;
	}

	private static string FormatRecord(Record record)
	{
		if (record.bins == null || record.bins.Count == 0)
		{
			return $"generation={record.generation} expiration={record.expiration}";
		}

		return Format(record.bins);
	}

	private static bool AppendValue(object value, StringBuilder builder)
	{
		if (builder.Length > MaxLength)
		{
			builder.Append("...");
			return false;
		}

		switch (value)
		{
			case null:
				builder.Append("null");
				return true;
			case string text:
				builder.Append(text);
				return true;
			case byte[] bytes:
				builder.Append(ByteUtil.BytesToHexString(bytes));
				return true;
			case IDictionary map:
				return AppendMap(map, builder);
			case IEnumerable list:
				return AppendList(list, builder);
			default:
				builder.Append(value);
				return true;
		}
	}

	private static bool AppendList(IEnumerable list, StringBuilder builder)
	{
		builder.Append('[');
		bool first = true;

		foreach (object item in list)
		{
			if (!first)
			{
				builder.Append(", ");
			}

			if (!AppendValue(item, builder))
			{
				return false;
			}

			first = false;
		}

		builder.Append(']');
		return true;
	}

	private static bool AppendMap(IDictionary map, StringBuilder builder)
	{
		builder.Append('{');
		bool first = true;

		foreach (DictionaryEntry entry in map)
		{
			if (!first)
			{
				builder.Append(", ");
			}

			if (!AppendValue(entry.Key, builder))
			{
				return false;
			}

			builder.Append('=');

			if (!AppendValue(entry.Value, builder))
			{
				return false;
			}

			first = false;
		}

		builder.Append('}');
		return true;
	}
}
