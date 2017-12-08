/* 
 * Copyright 2012-2017 Aerospike, Inc.
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
using System.Net.Sockets;

namespace Aerospike.Client
{
	internal abstract class AsyncCommandQueueBase
	{
		// Releases a SocketEventArgs object to the pool.
		public abstract void ReleaseArgs(SocketAsyncEventArgs e);

		// Schedules a command for later execution, and returns a boolean indicating if the command has been executed or is actually going be executed.
		public abstract bool ScheduleCommand(AsyncCommand command);
	}
}
