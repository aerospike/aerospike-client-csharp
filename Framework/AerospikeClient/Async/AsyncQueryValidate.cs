/* 
 * Copyright 2012-2018 Aerospike, Inc.
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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class AsyncQueryValidate
	{
		public interface BeginListener
		{
			void OnSuccess(ulong clusterKey);
			void OnFailure(AerospikeException ae);
		}

		public static void ValidateBegin(AsyncCluster cluster, AsyncQueryValidate.BeginListener listener, AsyncNode node, string ns)
		{
			string command = "cluster-stable:namespace=" + ns;
			AsyncInfo aic = new AsyncInfo(cluster, null, new BeginHandler(listener, command), node, command);
			aic.Execute();
		}

		private class BeginHandler : InfoListener
		{
			internal readonly AsyncQueryValidate.BeginListener listener;
			internal readonly string command;

			internal BeginHandler(AsyncQueryValidate.BeginListener listener, string command)
			{
				this.listener = listener;
				this.command = command;
			}

			public void OnSuccess(Dictionary<string, string> map)
			{
				string result = map[command];
				ulong clusterKey = 0;

				try
				{
					clusterKey = Convert.ToUInt64(result, 16);
				}
				catch (Exception)
				{
					// Yes, even scans return QUERY_ABORTED.
					listener.OnFailure(new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + result));
					return;
				}

				listener.OnSuccess(clusterKey);
			}

			public void OnFailure(AerospikeException ae)
			{
				listener.OnFailure(ae);
			}
		}

		public interface Listener
		{
			void OnSuccess();
			void OnFailure(AerospikeException ae);
		}

		public static void Validate(AsyncCluster cluster, AsyncQueryValidate.Listener listener, AsyncNode node, string ns, ulong expectedKey)
		{
			string command = "cluster-stable:namespace=" + ns;
			AsyncInfo aic = new AsyncInfo(cluster, null, new Handler(listener, command, expectedKey), node, command);
			aic.Execute();
		}

		private class Handler : InfoListener
		{
			internal readonly AsyncQueryValidate.Listener listener;
			internal readonly string command;
			internal readonly ulong expectedKey;

			internal Handler(AsyncQueryValidate.Listener listener, string command, ulong expectedKey)
			{
				this.listener = listener;
				this.command = command;
				this.expectedKey = expectedKey;
			}

			public void OnSuccess(Dictionary<string, string> map)
			{
				string result = map[command];
				ulong clusterKey = 0;

				try
				{
					clusterKey = Convert.ToUInt64(result, 16);
				}
				catch (Exception)
				{
					// Yes, even scans return QUERY_ABORTED.
					listener.OnFailure(new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + result));
					return;
				}

				if (clusterKey != expectedKey)
				{
					listener.OnFailure(new AerospikeException(ResultCode.QUERY_ABORTED, "Cluster is in migration: " + expectedKey + ' ' + clusterKey));
					return;
				}

				listener.OnSuccess();
			}

			public void OnFailure(AerospikeException ae)
			{
				listener.OnFailure(ae);
			}
		}
	}	
}
