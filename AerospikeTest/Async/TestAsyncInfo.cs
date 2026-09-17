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
using System.Reflection;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAsyncInfo : TestAsync
	{
		[TestMethod]
		public void AsyncInfoBuildCommand()
		{
			AsyncCluster cluster = GetAsyncCluster();
			AsyncNode node = (AsyncNode)client.Nodes[0];
			BuildInfoHandler handler = new(this);

			AsyncInfo info = new(cluster, null, handler, node, "build");
			info.Execute();
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncQueryValidateBegin()
		{
			AsyncCluster cluster = GetAsyncCluster();
			AsyncNode node = (AsyncNode)client.Nodes[0];
			ValidateBeginHandler handler = new(this);

			AsyncQueryValidate.ValidateBegin(cluster, handler, node, SuiteHelpers.ns);
			WaitTillComplete();
		}

		[TestMethod]
		public void AsyncQueryValidateRoundTrip()
		{
			AsyncCluster cluster = GetAsyncCluster();
			AsyncNode node = (AsyncNode)client.Nodes[0];
			ValidateRoundTripHandler handler = new(this);

			AsyncQueryValidate.ValidateBegin(cluster, handler, node, SuiteHelpers.ns);
			WaitTillComplete();
		}

		private static AsyncCluster GetAsyncCluster()
		{
			FieldInfo field = typeof(AsyncClient).GetField("cluster",
				BindingFlags.Instance | BindingFlags.NonPublic);
			return (AsyncCluster)field.GetValue(client);
		}

		private sealed class BuildInfoHandler(TestAsyncInfo parent) : InfoListener
		{
			public void OnSuccess(Dictionary<string, string> map)
			{
				try
				{
					parent.AssertTrue(map.ContainsKey("build"));
					parent.AssertNotNull(map["build"]);
					parent.AssertTrue(map["build"].Length > 0);
				}
				catch (Exception ex)
				{
					parent.SetError(ex);
				}
				finally
				{
					parent.NotifyCompleted();
				}
			}

			public void OnFailure(AerospikeException e)
			{
				parent.SetError(e);
				parent.NotifyCompleted();
			}
		}

		private sealed class ValidateBeginHandler(TestAsyncInfo parent) : AsyncQueryValidate.BeginListener
		{
			public void OnSuccess(ulong clusterKey)
			{
				try
				{
					parent.AssertGreaterThanZero((long)clusterKey);
				}
				catch (Exception ex)
				{
					parent.SetError(ex);
				}
				finally
				{
					parent.NotifyCompleted();
				}
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		private sealed class ValidateRoundTripHandler(TestAsyncInfo parent) : AsyncQueryValidate.BeginListener
		{
			public void OnSuccess(ulong clusterKey)
			{
				try
				{
					parent.AssertGreaterThanZero((long)clusterKey);
					AsyncCluster cluster = GetAsyncCluster();
					AsyncNode node = (AsyncNode)client.Nodes[0];
					AsyncQueryValidate.Validate(cluster, new ValidateEndHandler(parent), node, SuiteHelpers.ns, clusterKey);
				}
				catch (Exception ex)
				{
					parent.SetError(ex);
					parent.NotifyCompleted();
				}
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}

		private sealed class ValidateEndHandler(TestAsyncInfo parent) : AsyncQueryValidate.Listener
		{
			public void OnSuccess()
			{
				parent.NotifyCompleted();
			}

			public void OnFailure(AerospikeException ae)
			{
				parent.SetError(ae);
				parent.NotifyCompleted();
			}
		}
	}
}
