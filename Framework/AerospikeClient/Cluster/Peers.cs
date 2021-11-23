/* 
 * Copyright 2012-2021 Aerospike, Inc.
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
	public sealed class Peers
	{
		public readonly List<Peer> peers;
		public readonly Dictionary<string, Node> nodes;
		private readonly HashSet<Host> invalidHosts;
		public int refreshCount;
		public bool genChanged;

		public Peers(int peerCapacity)
		{
			peers = new List<Peer>(peerCapacity);
			nodes = new Dictionary<string, Node>();
			invalidHosts = new HashSet<Host>();
		}

		public bool HasFailed(Host host)
		{
			return invalidHosts.Contains(host);
		}

		public void Fail(Host host)
		{
			invalidHosts.Add(host);
		}

		public int InvalidCount
		{
			get { return invalidHosts.Count; }
		}
	}

	public sealed class Peer
	{
		internal String nodeName;
		internal String tlsName;
		internal List<Host> hosts;
	}
}
