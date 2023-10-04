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

namespace Aerospike.Client
{
	/// <summary>
	/// Socket connection wrapper.
	/// </summary>
	public interface IConnection
	{
		public void SetTimeout(int timeoutMillis);

		public abstract void Write(byte[] buffer, int length);

		public abstract void ReadFully(byte[] buffer, int length);

		public abstract Stream GetStream();

		/// <summary>
		/// Is socket closed from client perspective only.
		/// </summary>
		public bool IsClosed();

		public void UpdateLastUsed();

		/// <summary>
		/// Shutdown and close socket.
		/// </summary>
		public void Close();
	}
}
