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
using Aerospike.Client;
using Aerospike.Client.KVS;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Neo.IronLua;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Numerics;
using System.Text.Json;
using System.Threading;
using System.Timers;
using Timer = System.Timers.Timer;

namespace Aerospike.Client
{
	/// <summary>
	/// 
	/// </summary>
	public sealed class AuthTokenInterceptor : Interceptor
	{
		/// <summary>
		/// A conservative estimate of minimum amount of time in millis it takes for
		/// token refresh to complete.Auto refresh should be scheduled at least
		/// this amount before expiry, i.e, if remaining expiry time is less than
		/// this amount refresh should be scheduled immediately.
		/// </summary>
		private static readonly int refreshMinTime = 5000;

		private readonly ClientPolicy clientPolicy;
		private readonly GrpcChannel channel;
		private volatile bool isFetchingToken = false;
		private volatile bool isClosed = false;
		private volatile AccessToken accessToken;
		private Timer refreshTimer;


		public AuthTokenInterceptor(ClientPolicy clientPolicy, GrpcChannel grpcChannel)
		{
			this.clientPolicy = clientPolicy;
			this.channel = grpcChannel;
			
			if (IsTokenRequired())
			{
				this.accessToken = new AccessToken(DateTime.UtcNow.Millisecond, 0, "");
				refreshTimer = new Timer();
				refreshTimer.Enabled = true;
				refreshTimer.Elapsed += (sender, e) => RefreshToken();
				refreshTimer.Start();
			}
		}

		private void RefreshToken()
		{
			try
			{
				FetchToken();
				refreshTimer.Interval = (int)accessToken.expiry - DateTime.Now.Millisecond - 5 * 1000;
			}
			catch (Exception)
			{
				refreshTimer.Interval = 1000;
			}
		}

		/// <summary>
		/// Fetch the new token if expired or scheduled for auto refresh.
		/// </summary>
		private void FetchToken()
		{
			if (isClosed || !IsTokenRequired() || isFetchingToken)
			{
				return;
			}
			try
			{
				if (Log.DebugEnabled())
				{
					Log.Debug("Starting token refresh");
				}
				var authRequest = new Auth.AerospikeAuthRequest
				{
					Username = clientPolicy.user,
					Password = clientPolicy.password,
				};

				isFetchingToken = true;

				var client = new Auth.AuthService.AuthServiceClient(channel);
				var response = client.Get(authRequest);
				accessToken = ParseToken(response.Token);
				isFetchingToken = false;
			}
			catch (Exception e) 
			{
				// TODO
			}
		}

		private bool IsTokenRequired()
		{
			return clientPolicy.user != null;
		}

		private AccessToken ParseToken(string token)
		{
			string claims = token.Split(".")[1];
			byte[] decodedClaims = Convert.FromBase64String(claims);
			Dictionary<object, object> parsedClaims;
			using (StreamReader sr = new StreamReader(new MemoryStream(decodedClaims)))
			{
				parsedClaims = (Dictionary<object, object>)JsonConvert.DeserializeObject(sr.ReadToEnd(), typeof(Dictionary<object, object>));
			}
			//Dictionary<object, object> parsedClaims = (Dictionary<object, object>)System.Text.Json.JsonSerializer.Deserialize(System.Text.Encoding.UTF8.GetString(decodedClaims), typeof(Dictionary<object, object>));
			object expiryToken = parsedClaims.GetValueOrDefault("exp");
			object iat = parsedClaims.GetValueOrDefault("iat");
			if (expiryToken is long expiryTokenLong && iat is long iatLong) 
			{
				long ttl = (expiryTokenLong - iatLong) * 1000;
				if (ttl <= 0) 
				{
					throw new AerospikeException("token 'iat' > 'exp'");
				}
				// Set expiry based on local clock.
				long expiry = DateTime.UtcNow.Millisecond + ttl;
				return new AccessToken(expiry, ttl, token);
			}
			else
			{
				throw new AerospikeException("Unsupported access token format");
			}
		}

		private bool IsTokenValid()
		{
			AccessToken token = accessToken;
			return !IsTokenRequired() || (token != null && !token.HasExpired());
		}

		public override TResponse BlockingUnaryCall<TRequest, TResponse>(
			TRequest request,
			ClientInterceptorContext<TRequest, TResponse> context,
			BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
		{
			if (IsTokenRequired())
			{
				if (IsTokenValid())
				{
					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.BlockingUnaryCall(request, newContext, continuation);
				}
				else if (isFetchingToken)
				{
					while (isFetchingToken)
					{
						Thread.Sleep(500);
					}

					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.BlockingUnaryCall(request, newContext, continuation);
				}
			}

			return continuation(request, context);
		}

		public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
			TRequest request,
			ClientInterceptorContext<TRequest, TResponse> context,
			AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
		{
			if (IsTokenRequired())
			{
				if (IsTokenValid())
				{
					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncUnaryCall(request, newContext, continuation);
				}
				else if (isFetchingToken)
				{
					while (isFetchingToken)
					{
						Thread.Sleep(500);
					}

					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncUnaryCall(request, newContext, continuation);
				}
			}

			var call = continuation(request, context);
			return new AsyncUnaryCall<TResponse>(HandleResponse(call.ResponseAsync), call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);

		}

		private async Task<TResponse> HandleResponse<TResponse>(Task<TResponse> t)
		{
			var response = await t;
			return response;
		}

		public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
			ClientInterceptorContext<TRequest, TResponse> context,
			AsyncClientStreamingCallContinuation<TRequest, TResponse> continuation)
		{
			if (IsTokenRequired())
			{
				if (IsTokenValid())
				{
					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncClientStreamingCall(newContext, continuation);
				}
				else if (isFetchingToken)
				{
					while (isFetchingToken)
					{
						Thread.Sleep(500);
					}

					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncClientStreamingCall(newContext, continuation);
				}
			}

			return continuation(context);
		}

		public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
			TRequest request,
			ClientInterceptorContext<TRequest, TResponse> context,
			AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
		{
			if (IsTokenRequired())
			{
				if (IsTokenValid())
				{
					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncServerStreamingCall(request, newContext, continuation);
				}
				else if (isFetchingToken)
				{
					while (isFetchingToken)
					{
						Thread.Sleep(500);
					}

					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncServerStreamingCall(request, newContext, continuation);
				}
			}

			return continuation(request, context);
		}

		public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
			ClientInterceptorContext<TRequest, TResponse> context,
			AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
		{
			if (IsTokenRequired())
			{
				if (IsTokenValid())
				{
					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncDuplexStreamingCall(newContext, continuation);
				}
				else if (isFetchingToken)
				{
					while (isFetchingToken)
					{
						Thread.Sleep(500);
					}

					var headers = new Metadata();
					headers.Add(new Metadata.Entry("Authorization", $"Bearer {accessToken.token}"));

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncDuplexStreamingCall(newContext, continuation);
				}
			}

			return continuation(context);
		}
	}

	public class TokenStatus
	{
		private readonly Exception error;
		private readonly bool valid;

		public TokenStatus()
		{
			this.valid = true;
			this.error = null;
		}

		public TokenStatus(Exception error)
		{
			this.valid = false;
			this.error = error;
		}

		/// <returns>
		/// true iff the token is valid.
		/// </returns>
		public bool IsValid()
		{
			return valid;
		}

		/// <summary>
		/// Get the token fetch error. Should be used only when {@link #isValid()}
		/// returns false.
		/// </summary>
		/// <returns>
		/// the token fetch error.
		/// </returns>
		public Exception GetError()
		{
			return error;
		}
	}

	internal class AccessToken
	{
		/// <summary>
		/// Local token expiry timestamp in millis.
		/// </summary>
		internal readonly long expiry;
		/// <summary>
		/// Remaining time to live for the token in millis.
		/// </summary>
		internal readonly long ttl;
		/// <summary>
		/// An access token for Aerospike proxy.
		/// </summary>
		internal readonly string token;

		public AccessToken(long expiry, long ttl, string token)
		{
			this.expiry = expiry;
			this.ttl = ttl;
			this.token = token;
		}

		public bool HasExpired()
		{
			return DateTime.UtcNow.Millisecond > expiry;
		}
	}
}

