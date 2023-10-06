/* 
 * Copyright 2012-2023 Aerospike, Inc.
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
using Aerospike.Client.KVS;
using Grpc.Core;
using static Aerospike.Client.AerospikeException;

namespace Aerospike.Client
{
	/// <summary>
	/// Task used to poll for long running execute job completion.
	/// </summary>
	public sealed class ExecuteTaskProxy : ExecuteTask
	{
		CallInvoker Invoker { get; set; }

		/// <summary>
		/// Initialize task with fields needed to query server nodes.
		/// </summary>
		public ExecuteTaskProxy(CallInvoker callInvoker, Policy policy, Statement statement, ulong taskId)
			: base(null, policy, statement, taskId)
		{
			Invoker = callInvoker;
		}

		/// <summary>
		/// Query all nodes for task completion status.
		/// </summary>
		public override int QueryStatus()
		{
			var statusRequest = new KVS.BackgroundTaskStatusRequest
			{
				TaskId = (long)taskId,
				IsScan = scan
			};

			var request = new KVS.AerospikeRequestPayload
			{
				Id = 0, // ID is only needed in streaming version, can be static for unary
				Iteration = 1,
				BackgroundTaskStatusRequest = statusRequest
			};


			AerospikeResponsePayload response = new();

			try
			{
				var client = new KVS.Query.QueryClient(Invoker);
				var stream = client.BackgroundTaskStatus(request);
				stream.ResponseStream.MoveNext().Wait();
				response = stream.ResponseStream.Current;
				while (true)
				{
					if (!response.HasNext)
					{
						throw new EndOfGRPCStream();
					}

					if (response.Status != 0)
					{
						throw GRPCConversions.GrpcStatusError(response);
					}

					if (response.BackgroundTaskStatus == BackgroundTaskStatus.Complete)
					{
						return BaseTask.COMPLETE;
					}
					stream.ResponseStream.MoveNext().Wait();
					response = stream.ResponseStream.Current;
				}
			}
			catch (EndOfGRPCStream)
			{
				if (response.BackgroundTaskStatus == BackgroundTaskStatus.Complete)
				{
					return BaseTask.COMPLETE;
				}
			}
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, 0, false);
			}

			return BaseTask.COMPLETE;

		}
	}
}
