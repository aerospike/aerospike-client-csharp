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

using System.Diagnostics;
using System.Text.Json;
using static Aerospike.Client.Log;
using Timer = System.Timers.Timer;

namespace Aerospike.Client
{
    /// <summary>
    /// Interceptor for fetching auth token and attaching to GRPC calls
    /// </summary>
    public sealed class AuthTokenInterceptor : Interceptor
    {
        private ClientPolicy ClientPolicy { get; }
        private GrpcChannel Channel { get; }
        
        private AccessToken AccessToken { get; set; }
        private Metadata MetaData { get; set; }

        private readonly ManualResetEventSlim UpdatingTokenEvent = new ManualResetEventSlim(false);
        private Timer RefreshTimer { get; }
        

        public AuthTokenInterceptor(ClientPolicy clientPolicy, GrpcChannel grpcChannel)
        {
            this.ClientPolicy = clientPolicy;
            this.Channel = grpcChannel;

            if (IsTokenRequired())
            {
                if (Log.DebugEnabled())
                    Log.Debug("Grpc Token Required");

                RefreshTimer = new Timer
                {
                    Enabled = false,
                    AutoReset = false,
                };
                RefreshTimer.Elapsed += (sender, e) => RefreshToken();
                RefreshToken();
            }
        }

        private void RefreshToken()
        {
            RefreshTimer.Stop();

            try
            {
               if (Log.DebugEnabled())
                    Log.Debug($"Refresh Token Timer: Enter: {AccessToken}: '{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}'");

                FetchToken();
                RefreshTimer.Interval = AccessToken.RefreshTime;
                RefreshTimer.Start();

                if (Log.DebugEnabled())
                    Log.Debug($"Refresh Token Timer: Exit: {AccessToken}: '{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}'");                
            }
            catch (Exception ex)
            {                
                Log.Error($"Refresh Token Timer Error {AccessToken} '{DateTime.UtcNow}' Exception: '{ex}'");
                System.Diagnostics.Debug.WriteLine($"Refresh Token Timer Error {AccessToken} '{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}' '{ex}'");

                //Make sure Timer is stopped so that we force the API calls to reauthorize the token. 
                //In this manner, if there are any issues the exception is properly thrown upstream 
                //instead of in the timer thread.
                RefreshTimer.Stop();

                throw;
            }
        }

        /// <summary>
        /// Fetch the new token if expired or scheduled for auto refresh.
        /// </summary>
        private void FetchToken()
        {
            if (this.Channel is null 
                    || !this.IsTokenRequired()) { return; }
            
            this.UpdatingTokenEvent.Reset();
            try
            {
                if (Log.DebugEnabled())
                    Log.Debug($"FetchToken: Enter: {AccessToken}");

                var authRequest = new Auth.AerospikeAuthRequest
                {
                    Username = ClientPolicy.user,
                    Password = ClientPolicy.password
                };

                var client = new Auth.AuthService.AuthServiceClient(Channel);
                var response = client.Get(authRequest,
                                            deadline: DateTime.UtcNow.AddMilliseconds(this.ClientPolicy.loginTimeout));

                this.AccessToken = ParseToken(response.Token);
                this.MetaData = new Metadata
                                    {
                                        new Metadata.Entry("Authorization", $"Bearer {response.Token}")
                                    };

                if (Log.DebugEnabled())
                    Log.Debug($"FetchToken: Exchanged New Token: {AccessToken}");
            }
            catch (RpcException e)
            {
                Log.Error($"FetchToken: Error: {AccessToken}: '{DateTime.UtcNow}': Exception: '{e}'");
                System.Diagnostics.Debug.WriteLine($"FetchToken: Error: {AccessToken}: '{DateTime.UtcNow}': '{e}'");
                
                throw GRPCConversions.ToAerospikeException(e, this.ClientPolicy.loginTimeout, false);
            }
            finally
            {
                this.UpdatingTokenEvent.Set();
            }
        }

        private bool IsTokenRequired()
        {
            return ClientPolicy.user != null;
        }

        private static AccessToken ParseToken(string token)
        {
            string claims = token.Split(".")[1];
            claims = claims.Replace('_', '/').Replace('-', '+');
            int extraEquals = claims.Length % 4;
            if (extraEquals != 0)
            {
                for (int i = 0; i < 4 - extraEquals; i++)
                {
                    claims += "=";
                }
            }
            byte[] decodedClaims = Convert.FromBase64String(claims);
            var strClaims = System.Text.Encoding.UTF8.GetString(decodedClaims.ToArray());
            Dictionary<string, object> parsedClaims = (Dictionary<string, object>)System.Text.Json.JsonSerializer.Deserialize(strClaims, typeof(Dictionary<string, object>));
            JsonElement expiryToken = (JsonElement)parsedClaims.GetValueOrDefault("exp");
            JsonElement iat = (JsonElement)parsedClaims.GetValueOrDefault("iat");
            if (expiryToken.ValueKind == JsonValueKind.Number && iat.ValueKind == JsonValueKind.Number)
            {
                long expiryTokenLong = expiryToken.GetInt64();
                long iatLong = iat.GetInt64();
                long ttl = (expiryTokenLong - iatLong) * 1000;
                if (ttl <= 0)
                {
                    Log.Error($"ParseToken Error 'iat' > 'exp' token: '{strClaims}'");
                    System.Diagnostics.Debug.WriteLine($"ParseToken 'iat' > 'exp' Error token: '{strClaims}'");

                    throw new AerospikeException("token 'iat' > 'exp'");
                }

                return new AccessToken(ttl, token);
            }
            else
            {
                Log.Error($"ParseToken Error token: '{strClaims}'");
                System.Diagnostics.Debug.WriteLine($"ParseToken Error token: '{strClaims}'");
                
                throw new AerospikeException("Unsupported access token format");
            }
        }
        
        /// <summary>
        /// Checks to ensure that the token is still valid or if it is in the process of being updated.
        /// If the token is being updated, the call is blocked waiting for the token to be reauthorized.
        /// If the token is expired and not being updated, call <seealso cref="RefreshToken"/> to reschedule the timer and get a valid token.
        /// </summary>
        /// <returns>returns true if new token</returns>
        private bool GetTokenIfNeeded()
        {
            if (IsTokenRequired())
            {
                if (AccessToken.HasExpired && this.UpdatingTokenEvent.IsSet)
                {
                    if (Log.DebugEnabled())
                        Log.Debug($"GetTokenIfNeeded: Expired: Token: {AccessToken}");

                    this.UpdatingTokenEvent.Reset();
                    this.RefreshToken();

                    if (Log.DebugEnabled())
                        Log.Debug($"GetTokenIfNeeded: New Token: {AccessToken}");
                }
                this.UpdatingTokenEvent.Wait(this.ClientPolicy.timeout);
                return true;
            }

            return false;
        }
        
        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            BlockingUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            
            context.Options.CancellationToken.ThrowIfCancellationRequested();
            if (GetTokenIfNeeded())
            {
                if (Log.DebugEnabled())
                    Log.Debug($"BlockingUnaryCall<TRequest, TResponse>: New Token: {AccessToken}");

                var newOptions = context.Options.WithHeaders(this.MetaData);

                var newContext = new ClientInterceptorContext<TRequest, TResponse>(
                    context.Method,
                    context.Host,
                    newOptions);

                return base.BlockingUnaryCall(request, newContext, continuation);
            }

            if (Log.DebugEnabled())
                Log.Debug($"BlockingUnaryCall<TRequest, TResponse>: Enter: {AccessToken}");

            return continuation(request, context);            
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        {
           context.Options.CancellationToken.ThrowIfCancellationRequested();
            if (GetTokenIfNeeded())
            {
                if (Log.DebugEnabled())
                    Log.Debug($"AsyncUnaryCall<TRequest, TResponse>: New token: {AccessToken}");

                var newOptions = context.Options.WithHeaders(this.MetaData);

                var newContext = new ClientInterceptorContext<TRequest, TResponse>(
                    context.Method,
                    context.Host,
                    newOptions);

                var r = base.AsyncUnaryCall(request, newContext, continuation);

                return r;
            }

            if (Log.DebugEnabled())
                Log.Debug($"AsyncUnaryCall<TRequest, TResponse>: Enter: {AccessToken}");

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
           context.Options.CancellationToken.ThrowIfCancellationRequested();
                if (GetTokenIfNeeded())
                {
                    if (Log.DebugEnabled())
                        Log.Debug($"AsyncClientStreamingCall<TRequest, TResponse>: New token: {AccessToken}");

                    var newOptions = context.Options.WithHeaders(this.MetaData);

                    var newContext = new ClientInterceptorContext<TRequest, TResponse>(
                        context.Method,
                        context.Host,
                        newOptions);

                    return base.AsyncClientStreamingCall(newContext, continuation);
                }

                if (Log.DebugEnabled())
                    Log.Debug($"AsyncClientStreamingCall<TRequest, TResponse>: Enter: {AccessToken}");

                return continuation(context);            
        }

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncServerStreamingCallContinuation<TRequest, TResponse> continuation)
        {
            context.Options.CancellationToken.ThrowIfCancellationRequested();
            if (GetTokenIfNeeded())
            {
                if (Log.DebugEnabled())
                    Log.Debug($"AsyncServerStreamingCall<TRequest, TResponse>: New token: {AccessToken}");

                var newOptions = context.Options.WithHeaders(this.MetaData);

                var newContext = new ClientInterceptorContext<TRequest, TResponse>(
                    context.Method,
                    context.Host,
                    newOptions);

                return base.AsyncServerStreamingCall(request, newContext, continuation);
            }
            if (Log.DebugEnabled())
                Log.Debug($"AsyncServerStreamingCall<TRequest, TResponse>: Enter: {AccessToken}");

            return continuation(request, context);            
        }

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
        {
            context.Options.CancellationToken.ThrowIfCancellationRequested();
            if (GetTokenIfNeeded())
            {
                if (Log.DebugEnabled())
                    Log.Debug($"AsyncDuplexStreamingCall<TRequest, TResponse>: New token: {AccessToken}");

                var newOptions = context.Options.WithHeaders(this.MetaData);

                var newContext = new ClientInterceptorContext<TRequest, TResponse>(
                    context.Method,
                    context.Host,
                    newOptions);

                return base.AsyncDuplexStreamingCall(newContext, continuation);
            }

            if (Log.DebugEnabled())
                Log.Debug($"AsyncDuplexStreamingCall<TRequest, TResponse>: Enter: {AccessToken}");

            return continuation(context);            
        }
    }

    internal class AccessToken
    {
        private const float refreshAfterFraction = 0.75f;

        /// <summary>
        /// Local token expiry timestamp in mills.
        /// </summary>
        private readonly Stopwatch expiry;
        /// <summary>
        /// Remaining time to live for the token in mills.
        /// </summary>
        private readonly long ttl;

        /// <summary>
        /// The time before <see cref="ttl"/> to obtain the new token by getting one from the server.
        /// </summary>
        public readonly long RefreshTime;

        /// <summary>
        /// An access token for Aerospike proxy.
        /// </summary>
        public readonly string Token;

        public AccessToken(long ttl, string token)
        {
            this.expiry = Stopwatch.StartNew();
            this.ttl = ttl;
            this.RefreshTime = (long)Math.Floor(ttl * refreshAfterFraction);
            this.Token = token;
            System.Diagnostics.Debug.WriteLine(this);
        }

        /// <summary>
        /// Token Has Expired
        /// </summary>
        public bool HasExpired => expiry.ElapsedMilliseconds >= ttl;

        /// <summary>
        /// Token should be refreshed before hitting expiration
        /// </summary>
        public bool ShouldRefreshToken => expiry.ElapsedMilliseconds >= RefreshTime;

        public override string ToString()
        {
            return $"AccessToken{{TokenHash:{Token?.GetHashCode()}, TimeRemain:{expiry.ElapsedMilliseconds}, TTL:{ttl}, RefreshOn:{RefreshTime}}}";
        }
    }
}

