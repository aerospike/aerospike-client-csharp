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
using Aerospike.Client;

namespace Aerospike.Test
{
	/// <summary>
	/// A translated host with a port outside 1–65535 is a miss.
	/// </summary>
	[TestClass]
	public class TestTranslateInvalidPort
	{
		[TestMethod]
		public void OutOfRangePortIsMissPassThrough()
		{
			Endpoint advertised = new Endpoint("BB1", "tls1", "10.0.0.1", 3000, Endpoint.SourceList.STANDARD);
			IAddressTranslator translator = new FixedPortTranslator(70000);
			Host host = Cluster.Translate(translator, TranslationMissMode.PASS_THROUGH, advertised);
			Assert.AreEqual(advertised.host, host.name);
			Assert.AreEqual(advertised.port, host.port);
			Assert.AreEqual(advertised.tlsName, host.tlsName);
		}

		[TestMethod]
		public void OutOfRangePortIsMissStrict()
		{
			Endpoint advertised = new Endpoint("BB1", null, "10.0.0.1", 3000, Endpoint.SourceList.STANDARD);
			IAddressTranslator translator = new FixedPortTranslator(0);
			AerospikeException.TranslationMiss ex = Assert.Throws<AerospikeException.TranslationMiss>(
				() => Cluster.Translate(translator, TranslationMissMode.STRICT, advertised));
			Assert.AreEqual(ResultCode.TRANSLATION_MISS, ex.Result);
		}

		[TestMethod]
		public void ValidPortPassesThrough()
		{
			Endpoint advertised = new Endpoint("BB1", "tls1", "10.0.0.1", 3000, Endpoint.SourceList.STANDARD);
			IAddressTranslator translator = new FixedPortTranslator(3001);
			Host host = Cluster.Translate(translator, TranslationMissMode.STRICT, advertised);
			Assert.AreEqual("mapped.example", host.name);
			Assert.AreEqual(3001, host.port);
			Assert.AreEqual("tls1", host.tlsName);
		}

		private sealed class FixedPortTranslator : IAddressTranslator
		{
			private readonly int port;

			public FixedPortTranslator(int port)
			{
				this.port = port;
			}

			public Host Translate(Endpoint advertised) => new Host("mapped.example", advertised.tlsName, port);
		}
	}
}
