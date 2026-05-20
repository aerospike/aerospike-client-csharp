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

/// <summary>
/// Lifecycle hooks the harness invokes around <see cref="Example.RunExample(Arguments)"/>.
/// All hooks are optional. <see cref="Cleanup"/> always runs, even when the example throws.
/// </summary>
public sealed record ExampleFixture(
	Action<IAerospikeClient, Arguments> Setup = null,
	Action<IAerospikeClient, Arguments> Validate = null,
	Action<IAerospikeClient, Arguments> Cleanup = null);

internal sealed record ExampleDefinition(string Name, Type Type, bool IsAsync, ExampleFixture Fixture = null);

internal static class ExampleRegistry
{
	private static readonly ExampleDefinition[] Examples =
	[
		Sync<Connect>(),
		Sync<ServerInfo>(),
		Sync<PutGet>(ValidateAndCleanup<PutGet>(ExampleStateValidation.PutGet, "putgetkey")),
		Sync<Exists>(ValidateAndCleanup<Exists>(ExampleStateValidation.Exists, "existskey")),
		Sync<Delete>(Fixture<Delete>(
			setup: ExampleStateValidation.SetupDelete,
			validate: ExampleStateValidation.Delete,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, ["deletekey", "durabledeletekey"]))),
		Sync<Replace>(Fixture<Replace>(
			setup: ExampleStateValidation.SetupReplace,
			validate: ExampleStateValidation.Replace,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, ["replacekey", "replaceonlykey"]))),
		Sync<Add>(ValidateAndCleanup<Add>(ExampleStateValidation.Add, "addkey")),
		Sync<Append>(ValidateAndCleanup<Append>(ExampleStateValidation.Append, "appendkey")),
		Sync<Prepend>(ValidateAndCleanup<Prepend>(ExampleStateValidation.Prepend, "prependkey")),
		Sync<Batch>(Fixture<Batch>(
			setup: ExampleStateValidation.SetupBatch,
			validate: ExampleStateValidation.Batch,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteRange(client, args, "batchkey", 1, 8))),
		Sync<BatchOperate>(Fixture<BatchOperate>(
			setup: ExampleStateValidation.SetupBatchOperate,
			validate: ExampleStateValidation.BatchOperate,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteRange(client, args, "bkey", 1, 8))),
		Sync<Generation>(ValidateAndCleanup<Generation>(ExampleStateValidation.Generation, "genkey")),
		Sync<Expire>(ValidateAndCleanup<Expire>(ExampleStateValidation.Expire, "expirekey")),
		Sync<Touch>(ValidateAndCleanup<Touch>(ExampleStateValidation.Touch, "touchkey")),
		Sync<Transaction>(Cleanup<Transaction>(1, 2, 3)),
		Sync<Operate>(Fixture<Operate>(
			setup: ExampleStateValidation.SetupOperate,
			validate: ExampleStateValidation.Operate,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, ["opkey"]))),
		Sync<OperateBit>(Fixture<OperateBit>(
			setup: ExampleStateValidation.SetupOperateBit,
			validate: ExampleStateValidation.OperateBit,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, ["bitkey"]))),
		Sync<OperateList>(ValidateAndCleanup<OperateList>(ExampleStateValidation.OperateList, "listkey", "listkey2")),
		Sync<OperateMap>(ValidateAndCleanup<OperateMap>(
			ExampleStateValidation.OperateMap,
			"mapkey",
			"mapkey_score",
			"mapkey_range",
			"mapkey_nested",
			"mapkey_nested_create",
			"mapkey3")),
		Sync<DeleteBin>(Fixture<DeleteBin>(
			setup: ExampleStateValidation.SetupDeleteBin,
			validate: ExampleStateValidation.DeleteBin,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, ["delbinkey"]))),
		Sync<ScanParallel>(ScanDefaultSetFixture<ScanParallel>()),
		Sync<ScanSeries>(ScanDefaultSetFixture<ScanSeries>()),
		Sync<ScanPage>(Fixture<ScanPage>(
			setup: ExampleStateValidation.SetupScanPage,
			validate: ExampleStateValidation.ScanPage,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteIntegerRange(client, args, 1, 190, "page"))),
		Sync<ScanResume>(Fixture<ScanResume>(
			setup: ExampleStateValidation.SetupScanResume,
			validate: ExampleStateValidation.ScanResume,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteIntegerRange(client, args, 1, 200, "resume"))),
		Sync<ListMap>(ValidateAndCleanup<ListMap>(ExampleStateValidation.ListMap, "listkey1", "listkey2", "mapkey1", "mapkey2", "listmapkey")),
		Sync<UserDefinedFunction>(Fixture<UserDefinedFunction>(
			setup: UserDefinedFunctionFixture.Setup,
			validate: UserDefinedFunctionFixture.Validate,
			cleanup: UserDefinedFunctionFixture.Cleanup)),
		Sync<QueryInteger>(QueryCleanup<QueryInteger>("queryindexint", "querykeyint", 1, 50, setup: QueryExampleFixtures.SetupQueryInteger, validate: ExampleStateValidation.QueryInteger)),
		Sync<QueryString>(QueryCleanup<QueryString>("queryindex", "querykey", 1, 50, setup: QueryExampleFixtures.SetupQueryString, validate: ExampleStateValidation.QueryString)),
		Sync<QueryList>(QueryCleanup<QueryList>("qlindex", "qlkey", 1, 50, setup: QueryExampleFixtures.SetupQueryList, validate: ExampleStateValidation.QueryList)),
		Sync<QueryRegion>(QueryCleanup<QueryRegion>("queryindexloc", "querykeyloc", 0, 20, setup: QueryExampleFixtures.SetupQueryRegion, validate: ExampleStateValidation.QueryRegion)),
		Sync<QueryRegionFilter>(QueryCleanup<QueryRegionFilter>("filterindexloc", "filterkeyloc", 0, 20, setup: QueryExampleFixtures.SetupQueryRegionFilter, validate: ExampleStateValidation.QueryRegionFilter)),
		Sync<QueryFilter>(QueryCleanup<QueryFilter>("profileindex", "profilekey", 1, 3, setup: QueryExampleFixtures.SetupQueryFilter, validate: ExampleStateValidation.QueryFilter)),
		Sync<QueryExp>(QueryCleanupIntegerRange<QueryExp>("predidx", 1, 50, setup: QueryExampleFixtures.SetupQueryExp, validate: ExampleStateValidation.QueryExp)),
		Sync<QueryPage>(Fixture<QueryPage>(
			setup: ExampleStateValidation.SetupQueryPage,
			validate: ExampleStateValidation.QueryPage,
			cleanup: (client, args) =>
			{
				ExampleFixtureSupport.DropIndexQuietly(client, args, "pq", "pqidx");
				ExampleFixtureSupport.DeleteIntegerRange(client, args, 1, 190, "pq");
			})),
		Sync<QueryResume>(Fixture<QueryResume>(
			setup: ExampleStateValidation.SetupQueryResume,
			validate: ExampleStateValidation.QueryResume,
			cleanup: (client, args) =>
			{
				ExampleFixtureSupport.DropIndexQuietly(client, args, "qr", "qridx");
				ExampleFixtureSupport.DeleteIntegerRange(client, args, 1, 200, "qr");
			})),
		Sync<QuerySum>(QueryCleanup<QuerySum>("aggindex", "aggkey", 1, 10, setup: QueryExampleFixtures.SetupQuerySum, udfPackage: "sum_example.lua", validate: ExampleStateValidation.QuerySum)),
		Sync<QueryAverage>(QueryCleanup<QueryAverage>("avgindex", "avgkey", 1, 10, setup: QueryExampleFixtures.SetupQueryAverage, validate: ExampleStateValidation.QueryAverage)),
		Sync<QueryExecute>(QueryCleanup<QueryExecute>("qeindex1", "qekey", 1, 10, setup: QueryExampleFixtures.SetupQueryExecute, validate: ExampleStateValidation.QueryExecute)),
		Sync<QueryGeoCollection>(GeoCollectionCleanup<QueryGeoCollection>(
			setup: QueryExampleFixtures.SetupQueryGeoCollection,
			validate: ExampleStateValidation.QueryGeoCollection)),
		Sync<QueryOpsProjection>(Fixture<QueryOpsProjection>(
			setup: QueryExampleFixtures.SetupQueryOpsProjection,
			validate: ExampleStateValidation.QueryOpsProjection,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteRange(client, args, "qopkey", 1, 10))),
		Sync<PathExpression>(Fixture<PathExpression>(
			setup: (client, args) =>
			{
				ExampleFixtureSupport.DeleteKeys(client, args, PathExpressionKeys);
				PathExpressionFixture.Setup(client, args);
			},
			validate: ExampleStateValidation.PathExpression,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, PathExpressionKeys))),
		Sync<PathExpressionEnhanced>(ValidateAndCleanup<PathExpressionEnhanced>(
			ExampleStateValidation.PathExpressionEnhanced,
			"pathexp1", "pathexp2", "pathexp3", "pathexp4", "pathexp5")),
		Async<AsyncPutGet>(ValidateAndCleanup<AsyncPutGet>(ExampleStateValidation.AsyncPutGet, "putgetkey")),
		Async<AsyncBatch>(Fixture<AsyncBatch>(
			setup: (client, args) =>
			{
				ExampleFixtureSupport.DeleteRange(client, args, "batchkey", 1, 8);
				ExampleStateValidation.SetupBatch(client, args);
			},
			validate: ExampleStateValidation.AsyncBatch,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteRange(client, args, "batchkey", 1, 8))),
		Async<AsyncScan>(ScanDefaultSetFixture<AsyncScan>()),
		Async<AsyncScanPage>(Fixture<AsyncScanPage>(
			setup: ExampleStateValidation.SetupAsyncScanPage,
			validate: ExampleStateValidation.AsyncScanPage,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteIntegerRange(client, args, 1, 200, "apage"))),
		Async<AsyncTransaction>(ValidateAndCleanup<AsyncTransaction>(ExampleStateValidation.AsyncTransaction, 1, 2, 3)),
		Async<AsyncTransactionWithTask>(ValidateAndCleanup<AsyncTransactionWithTask>(ExampleStateValidation.AsyncTransaction, 1, 2, 3)),
		Async<AsyncQuery>(QueryCleanup<AsyncQuery>("asqindex", "asqkey", 1, 50, setup: QueryExampleFixtures.SetupAsyncQuery, validate: ExampleStateValidation.AsyncQuery)),
		Async<AsyncUserDefinedFunction>(Fixture<AsyncUserDefinedFunction>(
			setup: AsyncUserDefinedFunctionFixture.Setup,
			validate: AsyncUserDefinedFunctionFixture.Validate,
			cleanup: AsyncUserDefinedFunctionFixture.Cleanup)),
		Sync<Get>(Fixture<Get>(GetFixture.Setup, GetFixture.Validate, GetFixture.Cleanup))
	];

	private static readonly object[] PathExpressionKeys =
		["pathexp_modify", "pathexp_qs", "pathexp_regex", "pathexp_multi", "pathexp_nofail"];

	public static IReadOnlyList<string> Names { get; } = Examples.Select(example => example.Name).ToArray();

	public static bool TryGetSync(string name, out ExampleDefinition definition)
		=> TryGet(name, isAsync: false, out definition);

	public static bool TryGetAsync(string name, out ExampleDefinition definition)
		=> TryGet(name, isAsync: true, out definition);

	public static bool TryGet(string name, out ExampleDefinition definition)
	{
		definition = Examples.FirstOrDefault(example =>
			string.Equals(example.Name, name, StringComparison.OrdinalIgnoreCase));

		return definition != null;
	}

	private static bool TryGet(string name, bool isAsync, out ExampleDefinition definition)
	{
		definition = Examples.FirstOrDefault(example =>
			example.IsAsync == isAsync &&
			string.Equals(example.Name, name, StringComparison.OrdinalIgnoreCase));

		return definition != null;
	}

	private static ExampleDefinition Sync<T>(ExampleFixture fixture = null) where T : SyncExample
		=> new(typeof(T).Name, typeof(T), IsAsync: false, fixture);

	private static ExampleDefinition Async<T>(ExampleFixture fixture = null) where T : AsyncExample
		=> new(typeof(T).Name, typeof(T), IsAsync: true, fixture);

	private static ExampleFixture Fixture<T>(
		Action<IAerospikeClient, Arguments> setup = null,
		Action<IAerospikeClient, Arguments> validate = null,
		Action<IAerospikeClient, Arguments> cleanup = null)
		=> new(setup, validate, cleanup);

	private static ExampleFixture Cleanup<T>(params object[] userKeys)
		=> Fixture<T>(
			setup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, userKeys),
			cleanup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, userKeys));

	private static ExampleFixture ValidateAndCleanup<T>(Action<IAerospikeClient, Arguments> validate, params object[] userKeys)
		=> Fixture<T>(
			setup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, userKeys),
			validate: validate,
			cleanup: (client, args) => ExampleFixtureSupport.DeleteKeys(client, args, userKeys));

	private static ExampleFixture ScanDefaultSetFixture<T>()
		=> Fixture<T>(
			setup: (client, args) =>
			{
				ExampleStateValidation.CleanupScanDefaultSet(client, args);
				ExampleStateValidation.SetupScanDefaultSet(client, args);
			},
			validate: ExampleStateValidation.ScanDefaultSet,
			cleanup: ExampleStateValidation.CleanupScanDefaultSet);

	private static ExampleFixture QueryCleanup<T>(
		string indexName,
		string keyPrefix,
		int begin,
		int end,
		string setName = null,
		string udfPackage = null,
		Action<IAerospikeClient, Arguments> setup = null,
		Action<IAerospikeClient, Arguments> validate = null)
	{
		void Cleanup(IAerospikeClient client, Arguments args)
		{
			ExampleFixtureSupport.DropIndexQuietly(client, args, setName ?? args.set, indexName);
			ExampleFixtureSupport.DeleteRange(client, args, keyPrefix, begin, end, setName);

			if (udfPackage != null)
			{
				ExampleFixtureSupport.RemoveUdfQuietly(client, udfPackage);
			}
		}

		return Fixture<T>(
			setup: (client, args) =>
			{
				Cleanup(client, args);
				setup?.Invoke(client, args);
			},
			validate: validate,
			cleanup: Cleanup);
	}

	private static ExampleFixture QueryCleanupIntegerRange<T>(
		string indexName,
		int begin,
		int end,
		string setName = null,
		Action<IAerospikeClient, Arguments> setup = null,
		Action<IAerospikeClient, Arguments> validate = null)
	{
		void Cleanup(IAerospikeClient client, Arguments args)
		{
			ExampleFixtureSupport.DropIndexQuietly(client, args, setName ?? args.set, indexName);
			ExampleFixtureSupport.DeleteIntegerRange(client, args, begin, end, setName);
		}

		return Fixture<T>(
			setup: (client, args) =>
			{
				Cleanup(client, args);
				setup?.Invoke(client, args);
			},
			validate: validate,
			cleanup: Cleanup);
	}

	private static ExampleFixture GeoCollectionCleanup<T>(
		Action<IAerospikeClient, Arguments> setup = null,
		Action<IAerospikeClient, Arguments> validate = null)
	{
		static void Cleanup(IAerospikeClient client, Arguments args)
		{
			ExampleFixtureSupport.DropIndexQuietly(client, args, args.set, "geo_map");
			ExampleFixtureSupport.DropIndexQuietly(client, args, args.set, "geo_list");
			ExampleFixtureSupport.DeleteRange(client, args, "map", 0, 999);
			ExampleFixtureSupport.DeleteRange(client, args, "list", 0, 999);
		}

		return Fixture<T>(
			setup: (client, args) =>
			{
				Cleanup(client, args);
				setup?.Invoke(client, args);
			},
			validate: validate,
			cleanup: Cleanup);
	}
}
