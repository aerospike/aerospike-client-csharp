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
using System;
using System.Collections.Concurrent;
using System.Numerics;
using System.Text.Json;
using System.Threading;
using System.Timers;

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

		/// <summary>
		///  A cap on refresh time in millis to throttle an auto refresh requests in
		///  case of token refresh failure.
		/// </summary>
		private static readonly int maxExponentialBackOff = 15000;

		/// <summary>
		/// Fraction of token expiry time to elapse before scheduling an auto
		/// refresh.
		/// @see AuthTokenManager#refreshMinTime
		/// </summary>
		private static readonly float refreshAfterFraction = 0.95f;


		private readonly ClientPolicy clientPolicy;
		private readonly GrpcChannel channel;
		private volatile bool isFetchingToken = false;
		private volatile bool isClosed = false;
		
		/// <summary>
		/// Count of consecutive errors while refreshing the token.
		/// </summary>
		private volatile int consecutiveRefreshErrors = 0;

		/// <summary>
		/// The error encountered when refreshing the token. It will be null when
		/// {@link #consecutiveRefreshErrors} is zero.
		/// </summary>
		private volatile Exception refreshError = null;

		private volatile AccessToken accessToken;
		private volatile bool fetchScheduled;

		public AuthTokenInterceptor(ClientPolicy clientPolicy, GrpcChannel grpcChannel)
		{
			this.clientPolicy = clientPolicy;
			this.channel = grpcChannel;
			this.accessToken = new AccessToken(DateTime.UtcNow.Millisecond, 0, "");
			FetchToken(true);
		}

		/// <summary>
		/// Fetch the new token if expired or scheduled for auto refresh.
		/// </summary>
		/// <param name="forceRefresh">A boolean flag to refresh token forcefully. This is required for initialization and auto
		/// refresh.Auto refresh will get rejected as token won't be expired at that time, but we need\
		/// to refresh it beforehand.If true, this function will run from the<b> invoking thread</b>,
		/// not from the scheduler.</param>
		private void FetchToken(bool forceRefresh)
		{
			fetchScheduled = false;
			if (isClosed || IsTokenRequired() || isFetchingToken)
			{
				return;
			}
			if (ShouldRefresh(forceRefresh))
			{
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
				}
				catch (Exception e) {
					OnFetchError(e);
				}
			}
		}

		private void ClearRefreshErrors()
		{
			consecutiveRefreshErrors = 0;
			refreshError = null;
		}

		private void UpdateRefreshErrors(Exception e)
		{
			consecutiveRefreshErrors++;
			refreshError = e;
		}

		private void OnFetchError(Exception e)
		{
			UpdateRefreshErrors(e);
			UnsafeScheduleNextRefresh();
			isFetchingToken = false;
		}

		private bool ShouldRefresh(bool forceRefresh)
		{
			return forceRefresh || !IsTokenValid();
		}

		private void UnsafeScheduleNextRefresh()
		{
			long ttl = accessToken.ttl;
			long delay = (long)Math.Floor(ttl * refreshAfterFraction);

			if (ttl - delay < refreshMinTime)
			{
				// We need at least refreshMinTimeMillis to refresh, schedule
				// immediately.
				delay = ttl - refreshMinTime;
			}

			if (!IsTokenValid())
			{
				// Force immediate refresh.
				delay = 0;
			}

			if (delay == 0 && consecutiveRefreshErrors > 0)
			{
				// If we continue to fail then schedule will be too aggressive on fetching new token. Avoid that by increasing
				// fetch delay.

				delay = (long)(Math.Pow(2, consecutiveRefreshErrors) * 1000);
				if (delay > maxExponentialBackOff)
				{
					delay = maxExponentialBackOff;
				}

				// Handle wrap around.
				if (delay < 0)
				{
					delay = 0;
				}
			}
			UnsafeScheduleRefresh(delay, true);
		}

		private void UnsafeScheduleRefresh(long delay, bool forceRefresh)
		{
			if (isClosed || !forceRefresh || fetchScheduled)
			{
				return;
			}
		}

		private bool IsTokenRequired()
		{
			return clientPolicy.user != null;
		}

		private AccessToken ParseToken(string token) //throws IOException
		{
			string claims = token.Split("\\.")[1];
			byte[] decodedClaims = Convert.FromBase64String(claims);
			Dictionary<object, object> parsedClaims = (Dictionary<object, object>)JsonSerializer.Deserialize(decodedClaims, typeof(Dictionary<object, object>));
			object expiryToken = parsedClaims.GetValueOrDefault("exp");
			object iat = parsedClaims.GetValueOrDefault("iat");
			if (expiryToken is int && iat is int) 
			{
				int ttl = ((int)expiryToken - (int)iat) * 1000;
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

		public CallOptions SetCallCredentials(CallOptions callOptions)
		{
			if (IsTokenRequired())
			{
				if (!IsTokenValid())
				{
					if (Log.WarnEnabled())
					{
						// TODO: This warns for evey call, spamming the output.
						//  Should be rate limited. Possibly once in a few seconds.
						// This alerts that auto refresh didn't finish correctly. In normal scenario, this should never
						// happen.
						Log.Warn("Trying to refresh token before setting into call");
					}
					UnsafeScheduleRefresh(0, false);
				}
				if (!IsTokenValid())
				{
					throw new AerospikeException("Access token has expired");
				}
				//return callOptions.Credentials.(new BearerTokenCallCredentials(accessToken.token));
			}
			return callOptions;
		}

		/// <returns>the minimum amount of time it takes for the token to refresh.</returns>
		public int GetRefreshMinTime()
		{
			return refreshMinTime;
		}

		private bool IsTokenValid()
		{
			AccessToken token = accessToken;
			return !IsTokenRequired() || (token != null && !token.HasExpired());
		}

		public TokenStatus GetTokenStatus()
		{
			if (IsTokenValid())
			{
				return new TokenStatus();
			}

			Exception error = refreshError;
			if (error != null)
			{
				return new TokenStatus(error);
			}

			AccessToken token = accessToken;
			if (token != null && token.HasExpired())
			{
				return new TokenStatus(new AerospikeException(ResultCode.NOT_AUTHENTICATED,
					"token has expired"));
			}

			return new TokenStatus(new AerospikeException(ResultCode.NOT_AUTHENTICATED));
		}

		public override TResponse BlockingUnaryCall<TRequest, TResponse>(
			TRequest request,
			ClientInterceptorContext<TRequest, TResponse> context,
			BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
		{
			var credentials = CallCredentials.FromInterceptor(async (context, metadata) =>
			{
				var token = accessToken.token;
				metadata.Add("Authorization", $"Bearer {token}");
			});
			context.Options.WithCredentials(credentials);

			return continuation(request, context);
		}

		public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
			TRequest request,
			ClientInterceptorContext<TRequest, TResponse> context,
			AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
		{
			var credentials = CallCredentials.FromInterceptor(async (context, metadata) =>
			{
				var token = accessToken.token;
				metadata.Add("Authorization", $"Bearer {token}");
			});
			context.Options.WithCredentials(credentials);
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
			var credentials = CallCredentials.FromInterceptor(async (context, metadata) =>
			{
				var token = accessToken.token;
				metadata.Add("Authorization", $"Bearer {token}");
			});
			context.Options.WithCredentials(credentials);

			return continuation(context);
		}

		public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
			TRequest request,
			ClientInterceptorContext<TRequest, TResponse> context,
			AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
		{
			var credentials = CallCredentials.FromInterceptor(async (context, metadata) =>
			{
				var token = accessToken.token;
				metadata.Add("Authorization", $"Bearer {token}");
			});
			context.Options.WithCredentials(credentials);
			return continuation(request, context);
		}

		public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
			ClientInterceptorContext<TRequest, TResponse> context,
			AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
		{
			var credentials = CallCredentials.FromInterceptor(async (context, metadata) =>
			{
				var token = accessToken.token;
				metadata.Add("Authorization", $"Bearer {token}");
			});
			context.Options.WithCredentials(credentials);

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

