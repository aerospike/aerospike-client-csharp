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

namespace Aerospike.Test
{
	[TestClass]
	public class TestPolicy
	{
		[TestMethod]
		public void PolicySetTimeoutsClampsSocketTimeoutToTotalTimeout()
		{
			Policy policy = new();
			policy.SetTimeouts(5000, 2000);

			Assert.AreEqual(2000, policy.socketTimeout);
			Assert.AreEqual(2000, policy.totalTimeout);
		}

		[TestMethod]
		public void PolicyCloneCopiesBaseFields()
		{
			Policy original = new()
			{
				readModeAP = ReadModeAP.ALL,
				readModeSC = ReadModeSC.LINEARIZE,
				replica = Replica.MASTER,
				socketTimeout = 1111,
				totalTimeout = 2222,
				maxRetries = 3,
				sendKey = true,
				compress = true,
				failOnFilteredOut = true,
				errorDetailVerbosity = 2
			};

			Policy clone = original.Clone();

			Assert.AreEqual(original.readModeAP, clone.readModeAP);
			Assert.AreEqual(original.readModeSC, clone.readModeSC);
			Assert.AreEqual(original.replica, clone.replica);
			Assert.AreEqual(original.socketTimeout, clone.socketTimeout);
			Assert.AreEqual(original.totalTimeout, clone.totalTimeout);
			Assert.AreEqual(original.maxRetries, clone.maxRetries);
			Assert.AreEqual(original.sendKey, clone.sendKey);
			Assert.AreEqual(original.compress, clone.compress);
			Assert.AreEqual(original.failOnFilteredOut, clone.failOnFilteredOut);
			Assert.AreEqual(original.errorDetailVerbosity, clone.errorDetailVerbosity);
		}

		[TestMethod]
		public void WritePolicyCloneCopiesWriteFields()
		{
			WritePolicy original = new()
			{
				recordExistsAction = RecordExistsAction.CREATE_ONLY,
				generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL,
				commitLevel = CommitLevel.COMMIT_MASTER,
				generation = 7,
				expiration = 3600,
				respondAllOps = true,
				durableDelete = true,
				OnLockingOnly = true,
				maxRetries = 0
			};

			WritePolicy clone = original.Clone();

			Assert.AreEqual(original.recordExistsAction, clone.recordExistsAction);
			Assert.AreEqual(original.generationPolicy, clone.generationPolicy);
			Assert.AreEqual(original.commitLevel, clone.commitLevel);
			Assert.AreEqual(original.generation, clone.generation);
			Assert.AreEqual(original.expiration, clone.expiration);
			Assert.AreEqual(original.respondAllOps, clone.respondAllOps);
			Assert.AreEqual(original.durableDelete, clone.durableDelete);
			Assert.AreEqual(original.OnLockingOnly, clone.OnLockingOnly);
			Assert.AreEqual(original.maxRetries, clone.maxRetries);
		}

		[TestMethod]
		public void QueryPolicyDefaultsAndClone()
		{
			QueryPolicy original = new()
			{
				failOnClusterChange = true,
				maxConcurrentNodes = 2,
				includeBinData = false,
				expectedDuration = QueryDuration.SHORT,
			};

			Assert.AreEqual(0, original.totalTimeout);
			Assert.AreEqual(5, original.maxRetries);

			QueryPolicy clone = original.Clone();
			Assert.AreEqual(original.failOnClusterChange, clone.failOnClusterChange);
			Assert.AreEqual(original.maxConcurrentNodes, clone.maxConcurrentNodes);
			Assert.AreEqual(original.includeBinData, clone.includeBinData);
			Assert.AreEqual(original.expectedDuration, clone.expectedDuration);
		}

		[TestMethod]
		public void BatchPolicyWriteDefaultAndClone()
		{
			BatchPolicy writeDefault = BatchPolicy.WriteDefault();
			Assert.IsTrue(writeDefault.allowInline);
			Assert.IsTrue(writeDefault.respondAllKeys);

			BatchPolicy original = new()
			{
				allowInline = false,
				allowInlineSSD = true,
				maxConcurrentThreads = 4,
				respondAllKeys = false,
				allowProleReads = true
			};

			BatchPolicy clone = original.Clone();
			Assert.AreEqual(original.allowInline, clone.allowInline);
			Assert.AreEqual(original.allowInlineSSD, clone.allowInlineSSD);
			Assert.AreEqual(original.maxConcurrentThreads, clone.maxConcurrentThreads);
			Assert.AreEqual(original.respondAllKeys, clone.respondAllKeys);
			Assert.AreEqual(original.allowProleReads, clone.allowProleReads);
		}

		[TestMethod]
		public void ScanPolicyDefaultsAndClone()
		{
			ScanPolicy original = new()
			{
				maxRecords = 1000,
				maxConcurrentNodes = 1,
				includeBinData = false,
				concurrentNodes = false,
				recordQueueSize = 250
			};

			Assert.AreEqual(0, original.totalTimeout);

			ScanPolicy clone = original.Clone();
			Assert.AreEqual(original.maxRecords, clone.maxRecords);
			Assert.AreEqual(original.maxConcurrentNodes, clone.maxConcurrentNodes);
			Assert.AreEqual(original.includeBinData, clone.includeBinData);
			Assert.AreEqual(original.concurrentNodes, clone.concurrentNodes);
			Assert.AreEqual(original.recordQueueSize, clone.recordQueueSize);
		}

		[TestMethod]
		public void AdminPolicyAndClientPolicyClone()
		{
			AdminPolicy admin = new()
			{
				timeout = 15000
			};
			AdminPolicy adminClone = admin.Clone();
			Assert.AreEqual(admin.timeout, adminClone.timeout);

			ClientPolicy client = new()
			{
				timeout = 5000,
				maxConnsPerNode = 300,
				failIfNotConnected = true,
				user = "admin",
				password = "admin"
			};
			ClientPolicy clientClone = client.Clone();
			Assert.AreEqual(client.timeout, clientClone.timeout);
			Assert.AreEqual(client.maxConnsPerNode, clientClone.maxConnsPerNode);
			Assert.AreEqual(client.failIfNotConnected, clientClone.failIfNotConnected);
			Assert.AreEqual(client.user, clientClone.user);
			Assert.AreEqual(client.password, clientClone.password);
		}

		[TestMethod]
		public void BatchReadPolicyCloneCopiesFields()
		{
			BatchReadPolicy original = new()
			{
				readModeAP = ReadModeAP.ALL,
				readModeSC = ReadModeSC.LINEARIZE,
				readTouchTtlPercent = 80
			};

			BatchReadPolicy clone = original.Clone();
			Assert.AreEqual(original.readModeAP, clone.readModeAP);
			Assert.AreEqual(original.readModeSC, clone.readModeSC);
			Assert.AreEqual(original.readTouchTtlPercent, clone.readTouchTtlPercent);
		}
	}
}
