/* 
 * Copyright 2012-2014 Aerospike, Inc.
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
using System.Threading;

namespace Aerospike.Client
{
	public abstract class AsyncMultiExecutor
	{
		private int completedCount;
		protected internal int completedSize;
		private bool failed;

		protected internal void ChildSuccess()
		{
			int count = Interlocked.Increment(ref completedCount);

			if (!failed && count >= completedSize)
			{
				OnSuccess();
			}
		}

		protected internal void ChildFailure(AerospikeException ae)
		{
			failed = true;
			Interlocked.Increment(ref completedCount);
			OnFailure(ae);
		}

		protected internal abstract void OnSuccess();
		protected internal abstract void OnFailure(AerospikeException ae);
	}
}
