/* 
 * Copyright 2012-2022 Aerospike, Inc.
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
using Aerospike.Client.Proxy.KVS;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Grpc.Core;
using Microsoft.VisualBasic;
using System;
using System.Collections;
using System.Collections.Generic;

#pragma warning disable 0618

namespace Aerospike.Client.Proxy
{
	/*public abstract class CommandProxy
	{
		readonly Policy policy;
		//private readonly GrpcCallExecutor executor;
		//private readonly MethodDescriptor<KVS.AerospikeRequestPayload, KVS.AerospikeResponsePayload> methodDescriptor;
		private long deadlineNanos;
		private int sendTimeoutMillis;
		private int iteration = 1;
		private readonly int numExpectedResponses;
		bool inDoubt;

		public CommandProxy(
			MethodDescriptor<KVS.AerospikeRequestPayload, KVS.AerospikeResponsePayload> methodDescriptor,
			GrpcCallExecutor executor,
			Policy policy,
			int numExpectedResponses
		)
		{
			this.methodDescriptor = methodDescriptor;
			this.executor = executor;
			this.policy = policy;
			this.numExpectedResponses = numExpectedResponses;
		}

		void Execute()
		{
			if (policy.totalTimeout > 0)
			{
				deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(policy.totalTimeout);
				sendTimeoutMillis = (policy.socketTimeout > 0 && policy.socketTimeout < policy.totalTimeout) ?
									 policy.socketTimeout : policy.totalTimeout;
			}
			else
			{
				deadlineNanos = 0; // No total deadline.
				sendTimeoutMillis = Math.Max(policy.socketTimeout, 0);
			}

			ExecuteCommand();
		}

		private void ExecuteCommand()
		{
			long sendDeadlineNanos =
				(sendTimeoutMillis > 0) ?
					System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sendTimeoutMillis) : 0;

			KVS.AerospikeRequestPayload.Builder builder = getRequestBuilder();

			/*executor.execute(new GrpcStreamingCall(methodDescriptor, builder,
				policy, iteration, deadlineNanos, sendDeadlineNanos, numExpectedResponses,
				new StreamObserver<KVS.AerospikeResponsePayload>() {
			
				public void onNext(KVS.AerospikeResponsePayload response)
				{
					try
					{
						inDoubt |= response.getInDoubt();
						onResponse(response);
					}
					catch (Throwable t)
					{
						OnFailure(t);
						// Re-throw to abort at the proxy/
						throw t;
					}
				}

				public override void OnError(Exception e)
				{
					inDoubt = true;
					OnFailure(t);
				}

				public override void OnCompleted()
				{
				}*/
		/*}

		bool Retry()
		{
			if (iteration > policy.maxRetries)
			{
				return false;
			}

			if (policy.totalTimeout > 0)
			{
				long remaining = deadlineNanos - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);

				if (remaining <= 0)
				{
					return false;
				}
			}

			iteration++;
			executor.getEventLoop().schedule(this::RetryNow, policy.sleepBetweenRetries, TimeUnit.MILLISECONDS);
			return true;
		}

		private void RetryNow()
		{
			try
			{
				ExecuteCommand();
			}
			catch (AerospikeException ae)
			{
				NotifyFailure(ae);
			}
			catch (Exception t)
			{
				NotifyFailure(new AerospikeException(ResultCode.CLIENT_ERROR, t));
			}
		}

		private void OnFailure(Exception e)
		{
			AerospikeException ae;

			try
			{
				if (e.GetType() == typeof(AerospikeException))
				{
					ae = (AerospikeException)e;

					if (ae.Result == ResultCode.TIMEOUT)
					{
						ae = new AerospikeException.Timeout(policy, false);
					}
				}
				else if (e.GetType() == typeof(RpcException)) {
					RpcException rpce = (RpcException)e;
					StatusCode code = rpce.StatusCode;

					if (code == StatusCode.Unavailable)
					{
						if (Retry())
						{
							return;
						}
					}
					ae = GrpcConversions.ToAerospike(rpce, policy, iteration);
				}
				else
				{
					ae = new AerospikeException(ResultCode.CLIENT_ERROR, e);
				}
			}
			catch (AerospikeException ae2)
			{
				ae = ae2;
			}
			catch (Exception t2)
			{
				ae = new AerospikeException(ResultCode.CLIENT_ERROR, t2);
			}

			NotifyFailure(ae);
		}

		void NotifyFailure(AerospikeException ae)
		{
			try
			{
				ae.Policy = policy;
				ae.Iteration = iteration;
				ae.SetInDoubt(inDoubt);
				OnFailure(ae);
			}
			catch (Exception e)
			{
				Log.Error("onFailure() error: " + Util.GetStackTrace(e));
			}
		}

		static void LogOnSuccessError(Exception e)
		{
			Log.Error("onSuccess() error: " + Util.GetStackTrace(e));
		}

		KVS.AerospikeRequestPayload.Builder GetRequestBuilder()
		{
			Command command = new Command(policy.socketTimeout, policy.totalTimeout, policy.maxRetries);
			WriteCommand(command);

			ByteString payload = ByteString.CopyFrom(command.dataBuffer, 0, command.dataOffset);
			return KVS.AerospikeRequestPayload.newBuilder().setPayload(payload);
		}

		abstract void WriteCommand(Command command);
		abstract void OnResponse(KVS.AerospikeResponsePayload response);
		abstract void OnFailure(AerospikeException ae);
	}*/
}
#pragma warning restore 0618
