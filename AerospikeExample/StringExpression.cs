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

public sealed class StringExpression : SyncExample
{
	private const string BinName = "text";
	private const string ResultBinName = "result";

	/// <summary>
	/// Demonstrate string expression builders. Requires server version 8.1.3 or later.
	/// </summary>
	public override void RunExample()
	{
		RequireMinServerVersion(Node.SERVER_VERSION_8_1_3);

		RunReadExpressions();
		RunModifyExpressions();
		RunToString();
	}

	private void RunReadExpressions()
	{
		Key key = new(ns, set, "stringexp_read");

		Put(key, "hello world");
		Record record = Eval(key, StringExp.Strlen(Exp.StringBin(BinName)));
		Console.WriteLine($"strlen(\"hello world\") = {record.GetLong(ResultBinName)}");

		record = Eval(key, StringExp.Substr(Exp.Val(6), Exp.StringBin(BinName)));
		Console.WriteLine($"substr(6) = \"{record.GetString(ResultBinName)}\"");

		record = Eval(key, StringExp.Substr(Exp.Val(0), Exp.Val(5), Exp.StringBin(BinName)));
		Console.WriteLine($"substr(0, 5) = \"{record.GetString(ResultBinName)}\"");

		record = Eval(key, StringExp.CharAt(Exp.Val(6), Exp.StringBin(BinName)));
		Console.WriteLine($"charAt(6) = \"{record.GetString(ResultBinName)}\"");

		record = Eval(key, StringExp.Find(Exp.Val("world"), Exp.StringBin(BinName)));
		Console.WriteLine($"find(\"world\") = {record.GetLong(ResultBinName)}");

		Put(key, "ababab");
		record = Eval(key, StringExp.Find(Exp.Val("ab"), Exp.Val(2), Exp.StringBin(BinName)));
		Console.WriteLine($"find(\"ab\", occurrence=2) on \"ababab\" = {record.GetLong(ResultBinName)}");

		Put(key, "hello world");
		record = Eval(key, StringExp.Contains(Exp.Val("hello"), Exp.StringBin(BinName)));
		Console.WriteLine($"contains(\"hello\") = {record.GetBool(ResultBinName)}");

		record = Eval(key, StringExp.StartsWith(Exp.Val("hello"), Exp.StringBin(BinName)));
		Console.WriteLine($"startsWith(\"hello\") = {record.GetBool(ResultBinName)}");

		record = Eval(key, StringExp.EndsWith(Exp.Val("world"), Exp.StringBin(BinName)));
		Console.WriteLine($"endsWith(\"world\") = {record.GetBool(ResultBinName)}");

		Put(key, "12345");
		record = Eval(key, StringExp.ToInteger(Exp.StringBin(BinName)));
		Console.WriteLine($"toInteger(\"12345\") = {record.GetLong(ResultBinName)}");

		Put(key, "3.14");
		record = Eval(key, StringExp.ToDouble(Exp.StringBin(BinName)));
		Console.WriteLine($"toDouble(\"3.14\") = {record.GetDouble(ResultBinName)}");

		Put(key, "héllo");
		record = Eval(key, StringExp.ByteLength(Exp.StringBin(BinName)));
		Console.WriteLine($"byteLength(\"héllo\") = {record.GetLong(ResultBinName)} (5 codepoints, 6 UTF-8 bytes)");

		Put(key, "12345");
		record = Eval(key, StringExp.IsNumeric(Exp.StringBin(BinName)));
		Console.WriteLine($"isNumeric(\"12345\") = {record.GetBool(ResultBinName)}");

		Put(key, "3.14");
		record = Eval(key, StringExp.IsNumeric(StringNumericType.INT, Exp.StringBin(BinName)));
		Console.WriteLine($"isNumeric(\"3.14\", INT) = {record.GetBool(ResultBinName)}");

		// FLOAT requires a decimal point followed by a digit.
		Put(key, "12345");
		record = Eval(key, StringExp.IsNumeric(StringNumericType.FLOAT, Exp.StringBin(BinName)));
		Console.WriteLine($"isNumeric(\"12345\", FLOAT) = {record.GetBool(ResultBinName)}");

		Put(key, "HELLO");
		record = Eval(key, StringExp.IsUpper(Exp.StringBin(BinName)));
		Console.WriteLine($"isUpper(\"HELLO\") = {record.GetBool(ResultBinName)}");

		Put(key, "hello");
		record = Eval(key, StringExp.IsLower(Exp.StringBin(BinName)));
		Console.WriteLine($"isLower(\"hello\") = {record.GetBool(ResultBinName)}");

		record = Eval(key, StringExp.ToBlob(Exp.StringBin(BinName)));
		Console.WriteLine($"toBlob(\"hello\") = [{string.Join(", ", (byte[])record.GetValue(ResultBinName))}]");

		Put(key, "abc");
		record = Eval(key, StringExp.Split(Exp.StringBin(BinName)));
		Console.WriteLine($"split() = {ExampleValueFormatter.Format(record.GetList(ResultBinName))}");

		Put(key, "one,two,three");
		record = Eval(key, StringExp.Split(Exp.Val(","), Exp.StringBin(BinName)));
		Console.WriteLine($"split(\",\") = {ExampleValueFormatter.Format(record.GetList(ResultBinName))}");

		Put(key, "aGVsbG8=");
		record = Eval(key, StringExp.B64Decode(Exp.StringBin(BinName)));
		Console.WriteLine($"b64Decode(\"aGVsbG8=\") = \"{Encoding.UTF8.GetString((byte[])record.GetValue(ResultBinName))}\"");

		Put(key, "Hello123World");
		record = Eval(key, StringExp.RegexCompare(Exp.Val("[0-9]+"), Exp.StringBin(BinName)));
		Console.WriteLine($"regexCompare(\"[0-9]+\") = {record.GetBool(ResultBinName)}");

		Put(key, "HELLO");
		record = Eval(key, StringExp.RegexCompare(
			Exp.Val("hello"), StringRegexFlags.CASE_INSENSITIVE, Exp.StringBin(BinName)));
		Console.WriteLine($"regexCompare(\"hello\", CASE_INSENSITIVE) = {record.GetBool(ResultBinName)}");
	}

	private void RunModifyExpressions()
	{
		Key key = new(ns, set, "stringexp_modify");
		StringPolicy stringPolicy = StringPolicy.Default;

		Put(key, "hello world");
		Record record = Eval(key, StringExp.Insert(
			stringPolicy, Exp.Val(5), Exp.Val(" beautiful"), Exp.StringBin(BinName)));
		Console.WriteLine($"insert(5, \" beautiful\") -> \"{record.GetString(ResultBinName)}\"");

		record = Eval(key, StringExp.Overwrite(
			stringPolicy, Exp.Val(6), Exp.Val("earth"), Exp.StringBin(BinName)));
		Console.WriteLine($"overwrite(6, \"earth\") -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "hello");
		record = Eval(key, StringExp.Concat(
			stringPolicy,
			Exp.Val(new List<string> { " ", "big", " world" }),
			Exp.StringBin(BinName)));
		Console.WriteLine($"concat([\" \", \"big\", \" world\"]) -> \"{record.GetString(ResultBinName)}\"");

		record = Eval(key, StringExp.Append(stringPolicy, Exp.Val("!"), Exp.StringBin(BinName)));
		Console.WriteLine($"append(\"!\") -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "world");
		record = Eval(key, StringExp.Prepend(stringPolicy, Exp.Val("hello "), Exp.StringBin(BinName)));
		Console.WriteLine($"prepend(\"hello \") -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "hello beautiful world");
		record = Eval(key, StringExp.Snip(stringPolicy, Exp.Val(5), Exp.Val(15), Exp.StringBin(BinName)));
		Console.WriteLine($"snip(5, 15) -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "hello world world");
		record = Eval(key, StringExp.Replace(
			stringPolicy, Exp.Val("world"), Exp.Val("earth"), Exp.StringBin(BinName)));
		Console.WriteLine($"replace(\"world\", \"earth\") -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "aabaa");
		record = Eval(key, StringExp.ReplaceAll(
			stringPolicy, Exp.Val("a"), Exp.Val("x"), Exp.StringBin(BinName)));
		Console.WriteLine($"replaceAll(\"a\", \"x\") -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "hello world");
		record = Eval(key, StringExp.Upper(stringPolicy, Exp.StringBin(BinName)));
		Console.WriteLine($"upper() -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "HELLO WORLD");
		record = Eval(key, StringExp.Lower(stringPolicy, Exp.StringBin(BinName)));
		Console.WriteLine($"lower() -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "HELLO World");
		record = Eval(key, StringExp.CaseFold(stringPolicy, Exp.StringBin(BinName)));
		Console.WriteLine($"caseFold() -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "café");
		record = Eval(key, StringExp.NormalizeNFC(stringPolicy, Exp.StringBin(BinName)));
		Console.WriteLine($"normalizeNFC() -> \"{record.GetString(ResultBinName)}\"");

		Put(key, " hello ");
		record = Eval(key, StringExp.TrimStart(stringPolicy, Exp.StringBin(BinName)));
		Console.WriteLine($"trimStart() -> \"{record.GetString(ResultBinName)}\"");

		record = Eval(key, StringExp.TrimEnd(stringPolicy, Exp.StringBin(BinName)));
		Console.WriteLine($"trimEnd() -> \"{record.GetString(ResultBinName)}\"");

		Put(key, " hello world ");
		record = Eval(key, StringExp.Trim(stringPolicy, Exp.StringBin(BinName)));
		Console.WriteLine($"trim() -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "hello");
		record = Eval(key, StringExp.PadStart(
			stringPolicy, Exp.Val(10), Exp.Val("*"), Exp.StringBin(BinName)));
		Console.WriteLine($"padStart(10, \"*\") -> \"{record.GetString(ResultBinName)}\"");

		record = Eval(key, StringExp.PadEnd(
			stringPolicy, Exp.Val(10), Exp.Val("."), Exp.StringBin(BinName)));
		Console.WriteLine($"padEnd(10, \".\") -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "ab");
		record = Eval(key, StringExp.Repeat(stringPolicy, Exp.Val(3), Exp.StringBin(BinName)));
		Console.WriteLine($"repeat(3) -> \"{record.GetString(ResultBinName)}\"");

		Put(key, "abc123def456");
		record = Eval(key, StringExp.RegexReplace(
			stringPolicy,
			Exp.Val("[0-9]+"),
			Exp.Val("NUM"),
			StringRegexFlags.GLOBAL,
			Exp.StringBin(BinName)));
		Console.WriteLine($"regexReplace(\"[0-9]+\", \"NUM\", GLOBAL) -> \"{record.GetString(ResultBinName)}\"");
	}

	private void RunToString()
	{
		Key key = new(ns, set, "stringexp_tostring");
		const string numBin = "n";

		client.Delete(writePolicy, key);
		client.Put(writePolicy, key, new Bin(numBin, 42));

		Record record = client.Operate(writePolicy, key,
			ExpOperation.Read(ResultBinName, Exp.Build(StringExp.ToString(Exp.IntBin(numBin))), ExpReadFlags.DEFAULT));
		Console.WriteLine($"toString(IntBin(\"n\") = 42) -> \"{record.GetString(ResultBinName)}\"");
	}

	private void Put(Key key, string value)
	{
		client.Delete(writePolicy, key);
		client.Put(writePolicy, key, new Bin(BinName, value));
	}

	private Record Eval(Key key, Exp exp)
	{
		return client.Operate(writePolicy, key,
			ExpOperation.Read(ResultBinName, Exp.Build(exp), ExpReadFlags.DEFAULT));
	}
}
