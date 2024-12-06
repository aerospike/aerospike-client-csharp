﻿/* 
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
using System.Collections.Generic;
using System.Threading;

namespace Aerospike.Client
{
	internal sealed class BatchOperateListListenerAdapter : ListenerAdapter<bool>, BatchOperateListListener
	{
		public BatchOperateListListenerAdapter(CancellationToken token)
			: base(token)
		{
		}

		public void OnSuccess(List<BatchRecord> records, bool status)
		{
			// records is an argument to the async call, so the user already has access to it.
			// Set completion status: true if all batch sub-commands were successful.
			SetResult(status);
		}
	}
}
