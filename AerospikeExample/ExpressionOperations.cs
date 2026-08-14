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

namespace Aerospike.Example;

public sealed class ExpressionOperations : SyncExample
{
	private const string UserKey = "expression-operations";

	// Returns score + 5, or unknown when score is negative.
	private static readonly Expression BonusScore = Exp.Build(
		Exp.Let(
			Exp.Def("current", Exp.IntBin("score")),
			Exp.Cond(
				Exp.GE(Exp.Var("current"), Exp.Val(0)),
				Exp.Add(Exp.Var("current"), Exp.Val(5)),
				Exp.Unknown())));

	public override void RunExample()
	{
		RequireMinServerVersion(new Version(5, 6, 0));

		Key key = new(ns, set, UserKey);

		QualifyingScore(key);
		UnknownFailsDefaultRead(key);
		UnknownToleratedWithNoFail(key);
	}

	private void QualifyingScore(Key key)
	{
		client.Put(writePolicy, key, new Bin("score", 10));

		Record result = client.Operate(writePolicy, key,
			ExpOperation.Write("bonus", BonusScore, ExpWriteFlags.EVAL_NO_FAIL),
			ExpOperation.Read("strict", BonusScore, ExpReadFlags.DEFAULT),
			ExpOperation.Read("tolerant", BonusScore, ExpReadFlags.EVAL_NO_FAIL));

		Console.WriteLine($"Bonus score: {result.GetValue("tolerant")}");

		if (result.GetInt("strict") != 15 || client.Get(null, key).GetInt("bonus") != 15)
		{
			throw new Exception("Expected a bonus of 15 to be both returned and written.");
		}
	}

	// A DEFAULT read of an expression that resolves to unknown fails the whole
	// command, even though the accompanying EVAL_NO_FAIL write is a no-op.
	private void UnknownFailsDefaultRead(Key key)
	{
		client.Put(writePolicy, key, new Bin("score", -1));

		try
		{
			client.Operate(writePolicy, key,
				ExpOperation.Write("bonus", BonusScore, ExpWriteFlags.EVAL_NO_FAIL),
				ExpOperation.Read("strict", BonusScore, ExpReadFlags.DEFAULT));

			throw new Exception("Expected the DEFAULT read to reject the unknown result.");
		}
		catch (AerospikeException ae) when (ae.Result == ResultCode.OP_NOT_APPLICABLE)
		{
			Console.WriteLine("DEFAULT read rejected the unknown result, as expected.");
		}
	}

	// With EVAL_NO_FAIL on every operation, the unknown result is tolerated: the
	// write is skipped and the read returns no value.
	private void UnknownToleratedWithNoFail(Key key)
	{
		client.Put(writePolicy, key, new Bin("score", -1), new Bin("bonus", 15));

		Record result = client.Operate(writePolicy, key,
			ExpOperation.Write("bonus", BonusScore, ExpWriteFlags.EVAL_NO_FAIL),
			ExpOperation.Read("tolerant", BonusScore, ExpReadFlags.EVAL_NO_FAIL));

		Console.WriteLine($"Tolerant read returned: {result.GetValue("tolerant") ?? "no value"}");

		if (client.Get(null, key).GetInt("bonus") != 15)
		{
			throw new Exception("Expected the skipped write to leave the existing bonus bin intact.");
		}
	}
}
