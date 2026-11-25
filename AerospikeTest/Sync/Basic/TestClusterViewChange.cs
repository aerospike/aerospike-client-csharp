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
using System.Reflection;
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// Cluster-view callback wiring: notifyViewChange is true only when the translator
	/// overrides OnClusterViewChanged.
	/// </summary>
	[TestClass]
	public class TestClusterViewChange
	{
		[TestMethod]
		public void ViewChangeTrackedOnlyWhenTranslatorOverrides()
		{
			MethodInfo overrides = typeof(Cluster).GetMethod("OverridesViewChange",
				BindingFlags.NonPublic | BindingFlags.Static);
			Assert.IsNotNull(overrides);

			Assert.IsFalse((bool)overrides.Invoke(null, [PassThroughAddressTranslator.Instance]));
			Assert.IsFalse((bool)overrides.Invoke(null, [new LambdaTranslator(_ => null)]));
			Assert.IsTrue((bool)overrides.Invoke(null, [new ViewChangeTranslator()]));
		}

		private sealed class LambdaTranslator : IAddressTranslator
		{
			private readonly Func<Endpoint, Host> translate;

			internal LambdaTranslator(Func<Endpoint, Host> translate)
			{
				this.translate = translate;
			}

			public Host Translate(Endpoint advertised) => translate(advertised);
		}

		private sealed class ViewChangeTranslator : IAddressTranslator
		{
			public Host Translate(Endpoint advertised) => null;

			public void OnClusterViewChanged(int viewChange)
			{
			}
		}
	}
}
