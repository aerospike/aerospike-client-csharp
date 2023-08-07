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
using System.Net;
using System.Net.Sockets;

namespace Aerospike.Client
{
	/// <summary>
	/// Async connection base class.
	/// </summary>
	public interface IAsyncConnection
	{
		public DateTime LastUsed
		{
			get;
		}

		public IAsyncCommand Command
		{
			get;
			set;
		}

		public abstract void Connect(IPEndPoint address);
		public abstract void Send(byte[] buffer, int offset, int count);
		public abstract void Receive(byte[] buffer, int offset, int count);

		public bool IsValid();

		public void UpdateLastUsed();

		public abstract void Reset();

		public abstract void Close();
	}
}
