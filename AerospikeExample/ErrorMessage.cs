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

public sealed class ErrorMessage : SyncExample
{
	private const string BinName = "error-message-bin";

	/// <summary>
	/// Demonstrate server-supplied extended error details.
	/// </summary>
	public override void RunExample()
	{
		//RequireMinServerVersion(Node.SERVER_VERSION_8_1_3);

		Key intKey = new(ns, set, "error-message-key");
		WritePolicy errorPolicy = new(writePolicy)
		{
			errorDetailVerbosity = 2
		};

		client.Put(errorPolicy, intKey, new Bin(BinName, 1));
		console.Info("Write succeeded, running error detail examples.");

		AppendToIntegerBin(errorPolicy, intKey);
		DeleteGenerationMismatch(intKey);
		IncrementStringBin(errorPolicy);
		HllAddOnIntegerBin(errorPolicy, intKey);
		HllRefreshCountMissingBin(errorPolicy);

		console.Info("Error message example completed successfully.");
	}

	private void AppendToIntegerBin(WritePolicy errorPolicy, Key key)
	{
		try
		{
			client.Operate(errorPolicy, key, Operation.Append(new Bin(BinName, "bad-append")));
			throw new Exception("Expected error on append to integer bin.");
		}
		catch (AerospikeException ae)
		{
			AssertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, SubCode.NONE, "cannot append");
			console.Info("Append to integer bin failed as expected: {0}: {1}", ae.Result, ae.BaseMessage);
		}
	}

	private void DeleteGenerationMismatch(Key key)
	{
		WritePolicy deletePolicy = new(writePolicy)
		{
			errorDetailVerbosity = 2,
			generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL,
			generation = 777
		};

		try
		{
			client.Delete(deletePolicy, key);
			throw new Exception("Expected error on generation-mismatch delete.");
		}
		catch (AerospikeException ae)
		{
			AssertErrorDetails(ae, ResultCode.GENERATION_ERROR, SubCode.NONE, "generation");
			console.Info("Generation mismatch delete failed as expected: {0}: {1}", ae.Result, ae.BaseMessage);
		}
	}

	private void IncrementStringBin(WritePolicy errorPolicy)
	{
		Key key = new(ns, set, "error-message-key-2");
		client.Put(errorPolicy, key, new Bin(BinName, "hello"));

		try
		{
			client.Operate(errorPolicy, key, Operation.Add(new Bin(BinName, 1)));
			throw new Exception("Expected error on increment of string bin.");
		}
		catch (AerospikeException ae)
		{
			AssertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, SubCode.NONE, "cannot increment");
			console.Info("Increment string bin failed as expected: {0}: {1}", ae.Result, ae.BaseMessage);
		}
	}

	private void HllAddOnIntegerBin(WritePolicy errorPolicy, Key key)
	{
		List<Value> hllList = [Value.Get("element1")];

		try
		{
			client.Operate(errorPolicy, key, HLLOperation.Add(HLLPolicy.Default, BinName, hllList, 8));
			throw new Exception("Expected error on HLL add to integer bin.");
		}
		catch (AerospikeException ae)
		{
			AssertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, SubCode.NONE, "bin is not hll type");
			console.Info("HLL add on integer bin failed as expected: {0}: {1}", ae.Result, ae.BaseMessage);
		}
	}

	private void HllRefreshCountMissingBin(WritePolicy errorPolicy)
	{
		Key key = new(ns, set, "error-message-key-3");
		client.Put(errorPolicy, key, new Bin("other-bin", 1));

		try
		{
			client.Operate(errorPolicy, key, HLLOperation.RefreshCount("no-hll-bin"));
			throw new Exception("Expected error on HLL refresh count of missing bin.");
		}
		catch (AerospikeException ae)
		{
			AssertErrorDetails(
				ae,
				ResultCode.BIN_NOT_FOUND,
				SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP,
				"subcode=" + SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP);
			console.Info("HLL refresh count on missing bin failed as expected: {0}: {1}", ae.Result, ae.BaseMessage);
		}
	}

	private static void AssertErrorDetails(
		AerospikeException ae,
		int expectedResultCode,
		int expectedSubCode,
		params string[] expectedSubstrings)
	{
		if (ae.Result != expectedResultCode)
		{
			throw new Exception($"Expected result code {expectedResultCode}, got {ae.Result}: {ae.BaseMessage}");
		}

		if (ae.SubCode != expectedSubCode)
		{
			throw new Exception($"Expected subcode {expectedSubCode}, got {ae.SubCode}: {ae.BaseMessage}");
		}

		foreach (string expected in expectedSubstrings)
		{
			if (!ae.BaseMessage.Contains(expected))
			{
				throw new Exception($"Expected '{expected}' in error message: {ae.BaseMessage}");
			}
		}
	}
}
