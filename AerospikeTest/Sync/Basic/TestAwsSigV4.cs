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
using System.Collections.Generic;
using System.Text;
using Aerospike.Client;

namespace Aerospike.Test
{
	[TestClass]
	public class TestAwsSigV4
	{
		private const string Secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
		private const string EmptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

		[TestMethod]
		public void SigningKeyDerivationExample()
		{
			Assert.AreEqual("f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d",
				AwsSigV4.Hex(AwsSigV4.SigningKey(Secret, "20120215", "us-east-1", "iam")));
		}

		[TestMethod]
		public void IamListUsersExample()
		{
			SortedDictionary<string, string> headers = new SortedDictionary<string, string>(StringComparer.Ordinal)
			{
				["content-type"] = "application/x-www-form-urlencoded; charset=utf-8",
				["host"] = "iam.amazonaws.com",
				["x-amz-date"] = "20150830T123600Z"
			};

			string canonical = AwsSigV4.CanonicalRequest("GET", "/", "Action=ListUsers&Version=2010-05-08", headers,
				AwsSigV4.Hex(AwsSigV4.Sha256(Array.Empty<byte>())));
			string expectedCanonical =
				"GET\n" +
				"/\n" +
				"Action=ListUsers&Version=2010-05-08\n" +
				"content-type:application/x-www-form-urlencoded; charset=utf-8\n" +
				"host:iam.amazonaws.com\n" +
				"x-amz-date:20150830T123600Z\n" +
				"\n" +
				"content-type;host;x-amz-date\n" +
				EmptyHash;
			Assert.AreEqual(expectedCanonical, canonical);
			Assert.AreEqual("f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
				AwsSigV4.Hex(AwsSigV4.Sha256(Encoding.UTF8.GetBytes(canonical))));

			string scope = AwsSigV4.Scope("20150830", "us-east-1", "iam");
			string stringToSign = AwsSigV4.StringToSign("20150830T123600Z", scope, canonical);
			Assert.AreEqual(
				"AWS4-HMAC-SHA256\n" +
				"20150830T123600Z\n" +
				"20150830/us-east-1/iam/aws4_request\n" +
				"f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59", stringToSign);

			byte[] key = AwsSigV4.SigningKey(Secret, "20150830", "us-east-1", "iam");
			Assert.AreEqual("c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9", AwsSigV4.Hex(key));

			string signature = AwsSigV4.Signature(key, stringToSign);
			Assert.AreEqual("5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7", signature);
		}

		[TestMethod]
		public void TestSuiteGetVanilla()
		{
			SortedDictionary<string, string> headers = new SortedDictionary<string, string>(StringComparer.Ordinal)
			{
				["host"] = "example.amazonaws.com",
				["x-amz-date"] = "20150830T123600Z"
			};

			string canonical = AwsSigV4.CanonicalRequest("GET", "/", "", headers, EmptyHash);
			Assert.AreEqual(
				"GET\n" +
				"/\n" +
				"\n" +
				"host:example.amazonaws.com\n" +
				"x-amz-date:20150830T123600Z\n" +
				"\n" +
				"host;x-amz-date\n" +
				EmptyHash, canonical);

			string stringToSign = AwsSigV4.StringToSign("20150830T123600Z",
				AwsSigV4.Scope("20150830", "us-east-1", "service"), canonical);
			Assert.AreEqual(
				"AWS4-HMAC-SHA256\n" +
				"20150830T123600Z\n" +
				"20150830/us-east-1/service/aws4_request\n" +
				"bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63", stringToSign);

			string authorization = AwsSigV4.Authorization("AKIDEXAMPLE", Secret, "GET", "/", "", headers,
				Array.Empty<byte>(), "us-east-1", "service", "20150830T123600Z");
			Assert.AreEqual("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, " +
				"SignedHeaders=host;x-amz-date, " +
				"Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31", authorization);
		}

		[TestMethod]
		public void EncodeUnreservedOnly()
		{
			Assert.AreEqual("tag%3Aaerospike-cluster", AwsSigV4.Encode("tag:aerospike-cluster"));
			Assert.AreEqual("a%20b~c_d.e-f%2F%2B%3D", AwsSigV4.Encode("a b~c_d.e-f/+="));
			Assert.AreEqual("%C3%A9", AwsSigV4.Encode("é"));
		}

		[TestMethod]
		public void AmzDate()
		{
			Assert.AreEqual("20150830T123600Z", AwsSigV4.AmzDate(1440938160000L));
		}
	}
}
