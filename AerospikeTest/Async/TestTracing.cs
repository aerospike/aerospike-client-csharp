/*
 * Copyright 2012-2025 Aerospike, Inc.
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
using System.Diagnostics;
using Aerospike.Client;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace Aerospike.Test;

[TestClass]
public class TestTracing : TestSync
{
	private static TracerProvider _tracerProvider;
	private static readonly List<Activity> Spans = [];
	private static readonly IAsyncClient Client = SuiteHelpers.asyncClient;

	[ClassInitialize]
	public static void ClassInitialize(TestContext context)
	{
		_tracerProvider = Sdk.CreateTracerProviderBuilder()
			.AddSource("AerospikeClient")
			.AddInMemoryExporter(Spans)
			.SetSampler(new AlwaysOnSampler())
			.Build();
	}

	[ClassCleanup(ClassCleanupBehavior.EndOfClass)]
	public static void ClassCleanup()
	{
		_tracerProvider.Dispose();
	}

	[TestInitialize]
	public void TestInitialize()
	{
		_tracerProvider.ForceFlush();
		Spans.Clear();
	}

	[TestMethod]
	public async Task GetShouldEmitASpan()
	{
		Key key = new(SuiteHelpers.ns, SuiteHelpers.set, nameof(GetShouldEmitASpan));
		_ = await Client.Get(null, CancellationToken.None, key);

		_tracerProvider.ForceFlush();

		Assert.AreEqual(1, Spans.Count);
		Assert.AreEqual("get test", Spans[0].DisplayName);
		Assert.AreEqual(ActivityStatusCode.Ok, Spans[0].Status);
		Assert.IsNull(Spans[0].StatusDescription);
		CollectionAssert.AreEquivalent(new Dictionary<string, string>(Spans[0].Tags), new Dictionary<string, string>
		{
			["network.peer.address"] = "::1",
			["network.peer.port"] = "3000",
			["db.system.name"] = "aerospike",
			["db.operation.name"] = "get",
			["db.namespace"] = "test",
			["db.collection.name"] = "test",
			["db.query.text"] = "get GetShouldEmitASpan",
		});
	}

	[TestMethod]
	public async Task Timeout()
	{
		Key key = new(SuiteHelpers.ns, SuiteHelpers.set, nameof(Timeout));

		try
		{
			_ = await Client.Get(new Policy
			{
				// TODO: the command can complete before the timeout. How to force a timeout?
				socketTimeout = 1,
				totalTimeout = 1,
			}, CancellationToken.None, key);
		}
		catch (AerospikeException.Timeout)
		{
		}

		// User gets notified of the timeout before the response was received. So we need to wait for the response to
		// get the span.
		int i = 0;
		for (; i < 10 && Spans.Count == 0; i += 1)
		{
			_tracerProvider.ForceFlush();
			await Task.Delay(TimeSpan.FromMilliseconds(500));
		}

		if (i == 10)
		{
			Assert.Fail("No spans were emitted");
		}

		Assert.AreEqual(ActivityStatusCode.Error, Spans[0].Status);
		Assert.AreEqual("timeout", Spans[0].StatusDescription);
	}

	[TestMethod]
	public async Task CommandSpecificError()
	{
		// TODO: I think the span is not completed if AsyncSingleCommand.ParseResult throws. How to force this kind of error?
	}
}
