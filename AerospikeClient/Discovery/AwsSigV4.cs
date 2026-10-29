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
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Aerospike.Client
{
	/// <summary>
	/// AWS Signature Version 4 signing of one request.
	/// </summary>
	internal static class AwsSigV4
	{
		internal const string Algorithm = "AWS4-HMAC-SHA256";

		private static readonly char[] HexDigits = "0123456789abcdef".ToCharArray();

		internal static string AmzDate(long millis)
		{
			return DateTimeOffset.FromUnixTimeMilliseconds(millis).UtcDateTime.ToString("yyyyMMdd'T'HHmmss'Z'");
		}

		/// <summary>
		/// Return the Authorization header value. Header names must be lower case.
		/// </summary>
		internal static string Authorization(
			string accessKeyId,
			string secretAccessKey,
			string method,
			string path,
			string query,
			SortedDictionary<string, string> headers,
			byte[] payload,
			string region,
			string service,
			string amzDate)
		{
			string scope = Scope(amzDate.Substring(0, 8), region, service);
			string request = CanonicalRequest(method, path, query, headers, Hex(Sha256(payload)));
			string signature = Signature(SigningKey(secretAccessKey, amzDate.Substring(0, 8), region, service),
				StringToSign(amzDate, scope, request));
			return Algorithm + " Credential=" + accessKeyId + '/' + scope + ", SignedHeaders=" +
				SignedHeaders(headers) + ", Signature=" + signature;
		}

		internal static string CanonicalRequest(
			string method,
			string path,
			string query,
			SortedDictionary<string, string> headers,
			string payloadHash)
		{
			StringBuilder sb = new StringBuilder(256);
			sb.Append(method).Append('\n').Append(path).Append('\n').Append(query).Append('\n');

			foreach (KeyValuePair<string, string> entry in headers)
			{
				sb.Append(entry.Key).Append(':').Append(entry.Value.Trim()).Append('\n');
			}

			sb.Append('\n').Append(SignedHeaders(headers)).Append('\n').Append(payloadHash);
			return sb.ToString();
		}

		internal static string SignedHeaders(SortedDictionary<string, string> headers)
		{
			return string.Join(";", headers.Keys);
		}

		internal static string Scope(string date, string region, string service)
		{
			return date + '/' + region + '/' + service + "/aws4_request";
		}

		internal static string StringToSign(string amzDate, string scope, string canonicalRequest)
		{
			return Algorithm + '\n' + amzDate + '\n' + scope + '\n' +
				Hex(Sha256(Encoding.UTF8.GetBytes(canonicalRequest)));
		}

		internal static byte[] SigningKey(string secretAccessKey, string date, string region, string service)
		{
			byte[] key = Hmac(Encoding.UTF8.GetBytes("AWS4" + secretAccessKey), date);
			key = Hmac(key, region);
			key = Hmac(key, service);
			return Hmac(key, "aws4_request");
		}

		internal static string Signature(byte[] signingKey, string stringToSign)
		{
			return Hex(Hmac(signingKey, stringToSign));
		}

		/// <summary>
		/// Percent-encode every byte except the RFC 3986 unreserved characters.
		/// </summary>
		internal static string Encode(string value)
		{
			byte[] bytes = Encoding.UTF8.GetBytes(value);
			StringBuilder sb = new StringBuilder(bytes.Length + 16);

			foreach (byte b in bytes)
			{
				char c = (char)b;

				if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
					c == '-' || c == '_' || c == '.' || c == '~')
				{
					sb.Append(c);
				}
				else
				{
					sb.Append('%').Append(char.ToUpperInvariant(HexDigits[c >> 4]))
						.Append(char.ToUpperInvariant(HexDigits[c & 0xf]));
				}
			}

			return sb.ToString();
		}

		internal static string Hex(byte[] bytes)
		{
			char[] chars = new char[bytes.Length * 2];

			for (int i = 0; i < bytes.Length; i++)
			{
				chars[i * 2] = HexDigits[(bytes[i] >> 4) & 0xf];
				chars[i * 2 + 1] = HexDigits[bytes[i] & 0xf];
			}

			return new string(chars);
		}

		internal static byte[] Sha256(byte[] bytes)
		{
			return SHA256.HashData(bytes);
		}

		private static byte[] Hmac(byte[] key, string data)
		{
			return HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(data));
		}
	}
}
