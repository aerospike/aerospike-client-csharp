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
	public sealed class AsyncOperateRead : AsyncRead
	{
		private readonly OperateArgs args;

		public AsyncOperateRead(AsyncCluster cluster, RecordListener listener, Key key, OperateArgs args)
			: base(cluster, args.writePolicy, listener, key, true)
		{
			this.args = args;
		}

		public AsyncOperateRead(AsyncOperateRead other)
			: base(other)
		{
			this.args = other.args;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(args.writePolicy, key, args);
		}
	}
}