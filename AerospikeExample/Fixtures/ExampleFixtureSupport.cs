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

namespace Aerospike.Example;

/// <summary>
/// Shared helpers for example fixtures: key construction, assertions,
/// bulk data setup/cleanup, index management, UDF management, and CDT-aware equality.
/// </summary>
internal static class ExampleFixtureSupport
{
	public static Key Key(Arguments args, object userKey, string setName = null)
		=> new(args.ns, setName ?? args.set, Value.Get(userKey));

	public static void AssertBin(IAerospikeClient client, Arguments args, object userKey, string binName, object expected, string setName = null)
	{
		Record record = client.Get(args.policy, Key(args, userKey, setName), binName);
		object received = record?.GetValue(binName);

		if (!CdtEquals(received, expected))
		{
			throw new Exception(
				$"Verification failed: expected {userKey}/{binName}={ExampleValueFormatter.Format(expected)}, " +
				$"got {ExampleValueFormatter.Format(received)}.");
		}
	}

	public static void AssertBinExists(IAerospikeClient client, Arguments args, object userKey, string binName, string setName = null)
	{
		Record record = client.Get(args.policy, Key(args, userKey, setName), binName);

		if (record == null || record.GetValue(binName) == null)
		{
			throw new Exception($"Verification failed: expected {userKey}/{binName} to exist.");
		}
	}

	public static void AssertExists(IAerospikeClient client, Arguments args, object userKey, string setName = null)
	{
		if (!client.Exists(args.policy, Key(args, userKey, setName)))
		{
			throw new Exception($"Verification failed: expected record {userKey} to exist.");
		}
	}

	public static void AssertNotExists(IAerospikeClient client, Arguments args, object userKey, string setName = null)
	{
		if (client.Exists(args.policy, Key(args, userKey, setName)))
		{
			throw new Exception($"Verification failed: expected record {userKey} to be deleted.");
		}
	}

	public static void PutBins(IAerospikeClient client, Arguments args, object userKey, params Bin[] bins)
		=> client.Put(args.writePolicy, Key(args, userKey), bins);

	public static void PutBinsInSet(IAerospikeClient client, Arguments args, string setName, object userKey, params Bin[] bins)
		=> client.Put(args.writePolicy, Key(args, userKey, setName), bins);

	public static void DeleteKeys(IAerospikeClient client, Arguments args, IEnumerable<object> userKeys, string setName = null)
	{
		foreach (object userKey in userKeys)
		{
			client.Delete(args.writePolicy, Key(args, userKey, setName));
		}
	}

	public static void DeleteRange(IAerospikeClient client, Arguments args, string keyPrefix, int begin, int end, string setName = null)
	{
		for (int i = begin; i <= end; i++)
		{
			client.Delete(args.writePolicy, Key(args, keyPrefix + i, setName));
		}
	}

	public static void DeleteIntegerRange(IAerospikeClient client, Arguments args, int begin, int end, string setName = null)
	{
		for (int i = begin; i <= end; i++)
		{
			client.Delete(args.writePolicy, Key(args, i, setName));
		}
	}

	public static void CreateIndex(
		IAerospikeClient client,
		Arguments args,
		string setName,
		string indexName,
		string binName,
		IndexType indexType,
		IndexCollectionType collectionType = IndexCollectionType.DEFAULT)
	{
		Policy policy = NoTimeoutPolicy();
		DropIndexQuietly(client, args, setName, indexName, policy);

		IndexTask task = collectionType == IndexCollectionType.DEFAULT
			? client.CreateIndex(policy, args.ns, setName, indexName, binName, indexType)
			: client.CreateIndex(policy, args.ns, setName, indexName, binName, indexType, collectionType);
		task.Wait();
	}

	public static void DropIndexQuietly(IAerospikeClient client, Arguments args, string setName, string indexName, Policy policy = null)
	{
		try
		{
			client.DropIndex(policy ?? args.policy, args.ns, setName, indexName);
		}
		catch (AerospikeException)
		{
			// Index may not exist; intentionally swallow.
		}
	}

	public static void RegisterUdfString(IAerospikeClient client, string contents, string serverPath)
	{
		RegisterTask task = client.RegisterUdfString(null, contents, serverPath, Language.LUA);
		task.Wait();
	}

	public static void RemoveUdfQuietly(IAerospikeClient client, string serverPath)
	{
		try
		{
			client.RemoveUdf(null, serverPath);
		}
		catch (AerospikeException)
		{
			// UDF may not exist; intentionally swallow.
		}
	}

	/// <summary>
	/// Structural equality across CDT shapes. Handles byte arrays, lists,
	/// dictionaries with non-matching key reference identity, and numeric coercion.
	/// </summary>
	public static bool CdtEquals(object left, object right)
	{
		if (left is byte[] leftBytes && right is byte[] rightBytes)
		{
			return leftBytes.SequenceEqual(rightBytes);
		}

		if (left is IList leftList && right is IList rightList)
		{
			if (leftList.Count != rightList.Count)
			{
				return false;
			}

			for (int i = 0; i < leftList.Count; i++)
			{
				if (!CdtEquals(leftList[i], rightList[i]))
				{
					return false;
				}
			}

			return true;
		}

		if (left is IDictionary leftMap && right is IDictionary rightMap)
		{
			if (leftMap.Count != rightMap.Count)
			{
				return false;
			}

			foreach (DictionaryEntry pair in leftMap)
			{
				if (!TryGetMapValue(rightMap, pair.Key, out object value) || !CdtEquals(pair.Value, value))
				{
					return false;
				}
			}

			return true;
		}

		if (left is IConvertible && right is IConvertible &&
			left is not string && right is not string)
		{
			try
			{
				return Convert.ToDecimal(left) == Convert.ToDecimal(right);
			}
			catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
			{
				// Fall through to Equals comparison below.
			}
		}

		return Equals(left, right);
	}

	public static bool TryGetMapValue(IDictionary map, object expectedKey, out object value)
	{
		foreach (DictionaryEntry pair in map)
		{
			if (CdtEquals(pair.Key, expectedKey))
			{
				value = pair.Value;
				return true;
			}
		}

		value = null;
		return false;
	}

	private static Policy NoTimeoutPolicy() => new() { totalTimeout = 0 };
}
