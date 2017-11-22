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
using System;
using System.Collections.Generic;

namespace Aerospike.Client
{
	public sealed class OperateCommand : ReadCommand
	{
		private readonly Operation[] operations;
		private WritePolicy writePolicy;
		private OperateArgs args;

		public OperateCommand(Key key, Operation[] operations) 
			: base(key)
		{
			this.operations = operations;
		}

		public void SetArgs(WritePolicy writePolicy, OperateArgs args)
		{
			this.writePolicy = writePolicy;
			this.args = args;
		}

		protected internal override void WriteBuffer()
		{
			SetOperate(writePolicy, key, operations, args);
		}

		protected internal override void HandleNotFound(int resultCode)
		{
			// Only throw not found exception for command with write operations.
			// Read-only command operations return a null record.
			if (args.hasWrite)
			{
				throw new AerospikeException(resultCode);
			}
		}

		protected internal override void AddBin(Dictionary<string, object> bins, string name, object value)
		{
			object prev;

			if (bins.TryGetValue(name, out prev))
			{
				// Multiple values returned for the same bin. 
				if (prev is OpResults)
				{
					// List already exists.  Add to it.
					OpResults list = (OpResults)prev;
					list.Add(value);
				}
				else
				{
					// Make a list to store all values.
					OpResults list = new OpResults();
					list.Add(prev);
					list.Add(value);
					bins[name] = list;
				}
			}
			else
			{
				bins[name] = value;
			}
		}
	}

	public class OpResults : List<object>
	{
		public override string ToString()
		{
			return string.Join(",", base.ToArray());
		}
	}
}
