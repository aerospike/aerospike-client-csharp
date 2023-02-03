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
using System;

namespace Aerospike.Client
{
	public sealed class BatchStatus : IBatchStatus
	{
		private Exception exception;
		private bool error;
		private readonly bool hasResultCode;

		public BatchStatus(bool hasResultCode)
		{
			this.hasResultCode = hasResultCode;
		}

		public void BatchKeyError(Cluster cluster, Key key, int index, AerospikeException ae, bool inDoubt, bool hasWrite)
		{
			// Only used in async commands with a sequence listener.
		}

		public void BatchKeyError(AerospikeException e)
		{
			error = true;

			if (!hasResultCode)
			{
				// Legacy batch read commands that do not store a key specific resultCode.
				// Store exception and throw on batch completion.
				if (exception == null)
				{
					exception = e;
				}
			}
		}

		public void SetRowError()
		{
			// Indicate that a key specific error occurred.
			error = true;
		}

		public bool GetStatus()
		{
			return !error;
		}

		public void SetException(Exception e)
		{
			error = true;

			if (exception == null)
			{
				exception = e;
			}
		}

		public void CheckException()
		{
			if (exception != null)
			{
				throw exception;
			}
		}
	}
}
