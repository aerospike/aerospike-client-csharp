/*
 * Copyright 2012-2026 Aerospike, Inc.
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
	internal sealed partial class MetricsStore
	{
		internal MetricsNodeGeneration RegisterNode(Node node)
		{
			MetricsRuntimeConfig current = config;
			MetricsNodeGeneration generation = nodes.GetOrAdd(
				node,
				key =>
				{
					MetricsNodeGeneration created = new(
						this,
						key.Name,
						current);
					nodeNames.GetOrAdd(
						created.Name,
						name => new MetricsNodeNameState()).Add(created);
					return created;
				});
			generation.Collector.EnsureShape(current.Shape);
			return generation;
		}

		internal void AdmitNode(Node node)
		{
			MetricsNodeGeneration generation = RegisterNode(node);
			nodeNames[generation.Name].Admit(generation);
		}

		internal void InvalidateNode(Node node)
		{
			if (nodes.TryGetValue(node, out MetricsNodeGeneration generation))
			{
				generation.InvalidatePublicReads();
			}
		}

		internal void AddCompatibilityError(
			MetricsNodeGeneration generation,
			string ns)
		{
			if (generation.PublicReadsAllowed)
			{
				generation.AddError(ns);
			}
		}

		internal void AddCompatibilityTimeout(
			MetricsNodeGeneration generation,
			string ns)
		{
			if (generation.PublicReadsAllowed)
			{
				generation.AddTimeout(ns);
			}
		}

		internal void AddCompatibilityKeyBusy(
			MetricsNodeGeneration generation,
			string ns)
		{
			if (generation.PublicReadsAllowed)
			{
				generation.AddKeyBusy(ns);
			}
		}

		internal long GetCompatibilityError(
			MetricsNodeGeneration generation,
			string ns = null)
		{
			return generation.GetErrorCount(ns);
		}

		internal long GetCompatibilityTimeout(
			MetricsNodeGeneration generation,
			string ns = null)
		{
			return generation.GetTimeoutCount(ns);
		}

		internal long GetCompatibilityKeyBusy(
			MetricsNodeGeneration generation,
			string ns = null)
		{
			return generation.GetKeyBusyCount(ns);
		}

		internal long GetCompatibilityError(Node node)
		{
			return RegisterNode(node).GetErrorCount();
		}

		internal MetricsNodeView ReadCompatibilityNode(Node node)
		{
			MetricsNodeGeneration generation = RegisterNode(node);
			return nodeNames[generation.Name].ReadCompatibility(
				generation.Name,
				config);
		}

		internal MetricsNodeView ReadCompatibilityGeneration(Node node)
		{
			return RegisterNode(node).ReadCompatibility(config);
		}

		internal void RecordCompatibilityFailure(
			Node node,
			string ns,
			AerospikeException exception,
			bool clientTimeout)
		{
			if (node == null)
			{
				return;
			}

			MetricsNodeGeneration generation = RegisterNode(node);

			if (exception?.Result == ResultCode.TIMEOUT || clientTimeout)
			{
				generation.AddTimeout(ns);
			}
			else if (exception?.Result == ResultCode.KEY_BUSY)
			{
				generation.AddError(ns);
				generation.AddKeyBusy(ns);
			}
			else
			{
				generation.AddError(ns);
			}
		}

		internal NodeMetrics CreateNodeMetrics(Node node, MetricsPolicy policy)
		{
			RegisterNode(node);
			return new NodeMetrics(policy);
		}

		internal MetricsNodeCollector GetNode(Node node)
		{
			return GetNode(node, config);
		}

		private MetricsNodeCollector GetNode(Node node, MetricsRuntimeConfig expected)
		{
			MetricsNodeGeneration generation = RegisterNode(node);

			if (node.Active && node.MetricsWasAdmitted)
			{
				nodeNames[generation.Name].Admit(generation);
			}
			generation.Collector.EnsureShape(expected.Shape);
			return generation.Collector;
		}
	}
}
