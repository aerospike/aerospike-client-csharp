/*
 * Copyright 2012-2024 Aerospike, Inc.
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

namespace Aerospike.Client
{
	public sealed class AsyncTouch : AsyncWriteBase
	{
		private readonly WriteListener listener;
		private readonly ExistsListener existsListener;
		private bool touched;

		public AsyncTouch(AsyncCluster cluster, WritePolicy writePolicy, WriteListener listener, Key key)
			: base(cluster, writePolicy, key)
		{
			this.listener = listener;
			this.existsListener = null;
		}

		public AsyncTouch(AsyncCluster cluster, WritePolicy writePolicy, ExistsListener listener, Key key)
			: base(cluster, writePolicy, key)
		{
			this.listener = null;
			this.existsListener = listener;
		}

		public AsyncTouch(AsyncTouch other)
			: base(other)
		{
			this.listener = other.listener;
			this.existsListener = other.existsListener;
		}

		private protected override string CommandName => "touch";

		protected internal override AsyncCommand CloneCommand()
		{
			return new AsyncTouch(this);
		}

		protected internal override void WriteBuffer()
		{
			SetTouch(writePolicy, Key);
		}

		protected internal override bool ParseResult()
		{
			ParseHeader();
			ParseFields(policy.Txn, Key, true);

			if (resultCode == ResultCode.OK)
			{
				touched = true;
				return true;
			}

			touched = false;
			if (resultCode == ResultCode.KEY_NOT_FOUND_ERROR)
			{
				if (existsListener == null)
				{
					throw new AerospikeException(resultCode);
				}
				return true;
			}

			if (resultCode == ResultCode.FILTERED_OUT)
			{
				if (policy.failOnFilteredOut)
				{
					throw new AerospikeException(resultCode);
				}
				return true;
			}

			throw new AerospikeException(resultCode);
		}

		protected internal override void OnSuccess()
		{
			if (listener != null)
			{
				listener.OnSuccess(Key);
			}
			else if (existsListener != null)
			{
				existsListener.OnSuccess(Key, touched);
			}
		}

		protected internal override void OnFailure(AerospikeException e)
		{
			if (listener != null)
			{
				listener.OnFailure(e);
			}
			else if (existsListener != null)
			{
				existsListener.OnFailure(e);
			}
		}
	}
}
