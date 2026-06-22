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
using System.Text;

namespace Aerospike.Example;

public sealed class OperateString : SyncExample
{
	private const string BinName = "text";

	/// <summary>
	/// Demonstrate string operations. Requires server version 8.1.3 or later.
	/// </summary>
	public override void RunExample()
	{
		RequireMinServerVersion(Node.SERVER_VERSION_8_1_3);

		RunReadOps();
		RunModifyOps();
		RunToString();
	}

	private void RunReadOps()
	{
		Key key = new(ns, set, "opstr_read");

		// strlen: codepoint count.
		Put(key, "hello world");
		Record record = client.Operate(writePolicy, key, StringOperation.Strlen(BinName));
		console.Info($"strlen(\"hello world\") = {record.GetLong(BinName)}");

		// substr(start): codepoint slice to end of string.
		record = client.Operate(writePolicy, key, StringOperation.Substr(BinName, 6));
		console.Info($"substr(6) = \"{record.GetString(BinName)}\"");

		// substr(start, end): half-open codepoint range.
		record = client.Operate(writePolicy, key, StringOperation.Substr(BinName, 0, 5));
		console.Info($"substr(0, 5) = \"{record.GetString(BinName)}\"");

		// charAt: single-codepoint slice.
		record = client.Operate(writePolicy, key, StringOperation.CharAt(BinName, 6));
		console.Info($"charAt(6) = \"{record.GetString(BinName)}\"");

		// find(needle): index of first match, -1 if absent.
		record = client.Operate(writePolicy, key, StringOperation.Find(BinName, "world"));
		console.Info($"find(\"world\") = {record.GetLong(BinName)}");

		// find(needle, occurrence): index of nth match.
		Put(key, "ababab");
		record = client.Operate(writePolicy, key, StringOperation.Find(BinName, "ab", 2));
		console.Info($"find(\"ab\", occurrence=2) on \"ababab\" = {record.GetLong(BinName)}");

		Put(key, "hello world");
		record = client.Operate(writePolicy, key, StringOperation.Contains(BinName, "hello"));
		console.Info($"contains(\"hello\") = {record.GetBool(BinName)}");

		record = client.Operate(writePolicy, key, StringOperation.StartsWith(BinName, "hello"));
		console.Info($"startsWith(\"hello\") = {record.GetBool(BinName)}");

		record = client.Operate(writePolicy, key, StringOperation.EndsWith(BinName, "world"));
		console.Info($"endsWith(\"world\") = {record.GetBool(BinName)}");

		Put(key, "12345");
		record = client.Operate(writePolicy, key, StringOperation.ToInteger(BinName));
		console.Info($"toInteger(\"12345\") = {record.GetLong(BinName)}");

		Put(key, "3.14");
		record = client.Operate(writePolicy, key, StringOperation.ToDouble(BinName));
		console.Info($"toDouble(\"3.14\") = {record.GetDouble(BinName)}");

		Put(key, "héllo");
		record = client.Operate(writePolicy, key, StringOperation.ByteLength(BinName));
		console.Info($"byteLength(\"héllo\") = {record.GetLong(BinName)} (5 codepoints, 6 UTF-8 bytes)");

		Put(key, "12345");
		record = client.Operate(writePolicy, key, StringOperation.IsNumeric(BinName));
		console.Info($"isNumeric(\"12345\") = {record.GetBool(BinName)}");

		Put(key, "3.14");
		record = client.Operate(writePolicy, key, StringOperation.IsNumeric(BinName, StringNumericType.INT));
		console.Info($"isNumeric(\"3.14\", INT) = {record.GetBool(BinName)}");

		Put(key, "HELLO");
		record = client.Operate(writePolicy, key, StringOperation.IsUpper(BinName));
		console.Info($"isUpper(\"HELLO\") = {record.GetBool(BinName)}");

		Put(key, "hello");
		record = client.Operate(writePolicy, key, StringOperation.IsLower(BinName));
		console.Info($"isLower(\"hello\") = {record.GetBool(BinName)}");

		record = client.Operate(writePolicy, key, StringOperation.ToBlob(BinName));
		console.Info($"toBlob(\"hello\") = [{string.Join(", ", (byte[])record.GetValue(BinName))}]");

		Put(key, "abc");
		record = client.Operate(writePolicy, key, StringOperation.Split(BinName));
		console.Info($"split() = {ExampleValueFormatter.Format(record.GetList(BinName))}");

		Put(key, "one,two,three");
		record = client.Operate(writePolicy, key, StringOperation.Split(BinName, ","));
		console.Info($"split(\",\") = {ExampleValueFormatter.Format(record.GetList(BinName))}");

		Put(key, "aGVsbG8=");
		record = client.Operate(writePolicy, key, StringOperation.B64Decode(BinName));
		console.Info($"b64Decode(\"aGVsbG8=\") = \"{Encoding.UTF8.GetString((byte[])record.GetValue(BinName))}\"");

		Put(key, "Hello123World");
		record = client.Operate(writePolicy, key, StringOperation.RegexCompare(BinName, "[0-9]+"));
		console.Info($"regexCompare(\"[0-9]+\") = {record.GetBool(BinName)}");

		Put(key, "HELLO");
		record = client.Operate(writePolicy, key,
			StringOperation.RegexCompare(BinName, "hello", StringRegexFlags.CASE_INSENSITIVE));
		console.Info($"regexCompare(\"hello\", CASE_INSENSITIVE) = {record.GetBool(BinName)}");
	}

	private void RunModifyOps()
	{
		Key key = new(ns, set, "opstr_modify");
		StringPolicy stringPolicy = StringPolicy.Default;

		Put(key, "hello world");
		ModifyAndShow(key, "insert(5, \" beautiful\")",
			StringOperation.Insert(stringPolicy, BinName, 5, " beautiful"));

		Put(key, "hello world");
		ModifyAndShow(key, "overwrite(6, \"earth\")",
			StringOperation.Overwrite(stringPolicy, BinName, 6, "earth"));

		Put(key, "hello");
		ModifyAndShow(key, "concat(\"!\")",
			StringOperation.Concat(stringPolicy, BinName, "!"));

		Put(key, "hello");
		ModifyAndShow(key, "concat([\" \", \"big\", \" world\"])",
			StringOperation.Concat(stringPolicy, BinName, new List<string> { " ", "big", " world" }));

		Put(key, "hello");
		ModifyAndShow(key, "append(\"!\")",
			StringOperation.Append(stringPolicy, BinName, "!"));

		Put(key, "world");
		ModifyAndShow(key, "prepend(\"hello \")",
			StringOperation.Prepend(stringPolicy, BinName, "hello "));

		Put(key, "hello beautiful world");
		ModifyAndShow(key, "snip(5, 15)",
			StringOperation.Snip(stringPolicy, BinName, 5, 15));

		Put(key, "hello world world");
		ModifyAndShow(key, "replace(\"world\", \"earth\")",
			StringOperation.Replace(stringPolicy, BinName, "world", "earth"));

		Put(key, "aabaa");
		ModifyAndShow(key, "replaceAll(\"a\", \"x\")",
			StringOperation.ReplaceAll(stringPolicy, BinName, "a", "x"));

		Put(key, "hello world");
		ModifyAndShow(key, "upper()",
			StringOperation.Upper(stringPolicy, BinName));

		Put(key, "HELLO WORLD");
		ModifyAndShow(key, "lower()",
			StringOperation.Lower(stringPolicy, BinName));

		Put(key, "HELLO World");
		ModifyAndShow(key, "caseFold()",
			StringOperation.CaseFold(stringPolicy, BinName));

		Put(key, "café");
		ModifyAndShow(key, "normalizeNFC()",
			StringOperation.NormalizeNFC(stringPolicy, BinName));

		Put(key, " hello ");
		ModifyAndShow(key, "trimStart()",
			StringOperation.TrimStart(stringPolicy, BinName));

		Put(key, " hello ");
		ModifyAndShow(key, "trimEnd()",
			StringOperation.TrimEnd(stringPolicy, BinName));

		Put(key, " hello world ");
		ModifyAndShow(key, "trim()",
			StringOperation.Trim(stringPolicy, BinName));

		Put(key, "hello");
		ModifyAndShow(key, "padStart(10, \"*\")",
			StringOperation.PadStart(stringPolicy, BinName, 10, "*"));

		Put(key, "hello");
		ModifyAndShow(key, "padEnd(10, \".\")",
			StringOperation.PadEnd(stringPolicy, BinName, 10, "."));

		Put(key, "ab");
		ModifyAndShow(key, "repeat(3)",
			StringOperation.Repeat(stringPolicy, BinName, 3));

		Put(key, "abc123def456");
		ModifyAndShow(key, "regexReplace(\"[0-9]+\", \"NUM\", GLOBAL)",
			StringOperation.RegexReplace(stringPolicy, BinName, "[0-9]+", "NUM", StringRegexFlags.GLOBAL));
	}

	private void RunToString()
	{
		Key key = new(ns, set, "opstr_tostring");
		const string numBin = "n";

		client.Delete(writePolicy, key);
		client.Put(writePolicy, key, new Bin(numBin, 42));

		Record record = client.Operate(writePolicy, key, StringOperation.ToString(numBin));
		console.Info($"toString(int 42) = \"{record.GetString(numBin)}\"");
	}

	private void Put(Key key, string value)
	{
		client.Delete(writePolicy, key);
		client.Put(writePolicy, key, new Bin(BinName, value));
	}

	private void ModifyAndShow(Key key, string label, Operation modifyOp)
	{
		client.Operate(writePolicy, key, modifyOp);
		string result = client.Get(policy, key).GetString(BinName);
		console.Info($"{label} -> \"{result}\"");
	}
}
