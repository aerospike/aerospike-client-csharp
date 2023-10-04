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
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using System.Text.Json;
using Timer = System.Timers.Timer;

namespace Aerospike.Client
{
	/// <summary>
	/// Interceptor for fetching auth token and attaching to GRPC calls
	/// </summary>
	public sealed class AuthTokenInterceptor : Interceptor
	{
		private readonly ClientPolicy clientPolicy;
		private readonly GrpcChannel channel;
		private volatile bool isFetchingToken = false;
		private AccessToken accessToken;
		private readonly Timer refreshTimer;


		public AuthTokenInterceptor(ClientPolicy clientPolicy, GrpcChannel grpcChannel)
		{
			this.clientPolicy = clientPolicy;
			this.channel = grpcChannel;

			if (IsTokenRequired())
			{
				this.accessToken = new AccessToken(DateTime.UtcNow.Millisecond, 0, String.Empty);
				refreshTimer = new Timer
				{
					Enabled = true
				};
				refreshTimer.Elapsed += (sender, e) => RefreshToken();
				refreshTimer.Start();
			}
		}

		private void RefreshToken()
		{
			try
			{
				FetchToken();
				var interval = (int)accessToken.expiry - DateTime.Now.Millisecond - 5 * 1000;
				refreshTimer.Interval = interval > 0 ? interval : 1000;
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
			if (!IsTokenRequired() || isFetchingToken)
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
			catch (RpcException e)
			{
				throw GRPCConversions.ToAerospikeException(e, 0, true);
			}
		}

		private bool IsTokenRequired()
		{
			return clientPolicy.user != null;
		}

		private static AccessToken ParseToken(string token)
		{
			string claims = token.Split(".")[1];
			byte[] decodedClaims = Convert.FromBase64String(claims);
			Dictionary<string, object> parsedClaims = (Dictionary<string, object>)System.Text.Json.JsonSerializer.Deserialize(System.Text.Encoding.UTF8.GetString(decodedClaims.ToArray()), typeof(Dictionary<string, object>));
			JsonElement expiryToken = (JsonElement)parsedClaims.GetValueOrDefault("exp");
			JsonElement iat = (JsonElement)parsedClaims.GetValueOrDefault("iat");
			if (expiryToken.ValueKind == JsonValueKind.Number && iat.ValueKind == JsonValueKind.Number)
			{
				long expiryTokenLong = expiryToken.GetInt64();
				long iatLong = iat.GetInt64();
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
			return !IsTokenRequired() || (token.token != String.Empty && !token.HasExpired());
		}

		public override TResponse BlockingUnaryCall<TRequest, TResponse>(
			TRequest request,
			ClientInterceptorContext<TRequest, TResponse> context,
			BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
		{
			if (IsTokenRequired())
			{
				context.Options.CancellationToken.ThrowIfCancellationRequested();
				if (IsTokenValid())
				{
					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.BlockingUnaryCall(request, newContext, continuation);
				}
				else
				{
					while (isFetchingToken || accessToken.token == String.Empty)
					{
						context.Options.CancellationToken.ThrowIfCancellationRequested();
						Task.Delay(500, context.Options.CancellationToken).Wait();
					}

					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

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
				context.Options.CancellationToken.ThrowIfCancellationRequested();
				if (IsTokenValid())
				{
					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncUnaryCall(request, newContext, continuation);
				}
				else
				{
					while (isFetchingToken || accessToken.token == String.Empty)
					{
						context.Options.CancellationToken.ThrowIfCancellationRequested();
						Task.Delay(500, context.Options.CancellationToken).Wait();
					}

					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

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
				context.Options.CancellationToken.ThrowIfCancellationRequested();
				if (IsTokenValid())
				{
					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncClientStreamingCall(newContext, continuation);
				}
				else
				{
					while (isFetchingToken || accessToken.token == String.Empty)
					{
						context.Options.CancellationToken.ThrowIfCancellationRequested();
						Task.Delay(500, context.Options.CancellationToken).Wait();
					}

					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

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
				context.Options.CancellationToken.ThrowIfCancellationRequested();
				if (IsTokenValid())
				{
					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncServerStreamingCall(request, newContext, continuation);
				}
				else
				{
					while (isFetchingToken || accessToken.token == String.Empty)
					{
						context.Options.CancellationToken.ThrowIfCancellationRequested();
						Task.Delay(500, context.Options.CancellationToken).Wait();
					}

					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

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
				context.Options.CancellationToken.ThrowIfCancellationRequested();
				if (IsTokenValid())
				{
					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

					var newOptions = context.Options.WithHeaders(headers);

					var newContext = new ClientInterceptorContext<TRequest, TResponse>(
						context.Method,
						context.Host,
						newOptions);

					return base.AsyncDuplexStreamingCall(newContext, continuation);
				}
				else
				{
					while (isFetchingToken || accessToken.token == String.Empty)
					{
						context.Options.CancellationToken.ThrowIfCancellationRequested();
						Task.Delay(500, context.Options.CancellationToken).Wait();
					}

					var headers = new Metadata
					{
						new Metadata.Entry("Authorization", $"Bearer {accessToken.token}")
					};

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

	internal readonly struct AccessToken
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
