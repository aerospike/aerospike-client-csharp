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
using Microsoft.AspNetCore.Razor.Runtime.TagHelpers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using static Aerospike.Client.Log;
using Timer = System.Timers.Timer;

namespace Aerospike.Client
{
    /// <summary>
    /// Manager for custom user authorization token
    /// </summary>
    /// <remarks>
    /// For token reauthorization to work properly the driver code would need to be re-factored to allow for Unauthorized exceptions to be retried. 
    /// Also support for OAuth2 tokens or JWT would greatly help instead of this custom token being provided.
    /// </remarks>
    public sealed class AuthTokenManager : IDisposable
    {
        private ClientPolicy ClientPolicy { get; }
        private GrpcChannel Channel { get; set; }

        private AccessToken AccessToken;
        private readonly ManualResetEventSlim UpdatingToken = new ManualResetEventSlim(false);
        
        private Timer RefreshTokenTimer { get; set; }
        public AuthTokenManager(ClientPolicy clientPolicy)
        {
            this.ClientPolicy = clientPolicy;            
        }

        public void SetChannel(GrpcChannel grpcChannel)
        {
            this.Channel = grpcChannel;

            if (Log.DebugEnabled())
            {
                Log.Debug($"SetChannel: Enter: {grpcChannel.Target} ");
            }

            if (IsTokenRequired())
            {
                if (Log.DebugEnabled())
                {
                    Log.Debug($"SetChannel: Token Required");
                }

				RefreshTokenTimer = new Timer
                {
                    Enabled = false,
                    AutoReset = false,
                };
                RefreshTokenTimer.Elapsed += (sender, e) => RefreshTokenEvent();


                var cancellationSrc = new CancellationTokenSource();
                var cancellationToken = cancellationSrc.Token;
                var timeOut = Math.Max(this.ClientPolicy.timeout, this.ClientPolicy.loginTimeout);

                Task refreshTokenTask = RefreshToken(cancellationToken, timeout: timeOut, forceRefresh: true);

                if (Task.WaitAny(new[] { refreshTokenTask }, 
                                        timeOut + 500, //Wait a little longer...
                                        cancellationToken) < 0)
                {
                    cancellationSrc.Cancel();
                    Log.Error($"SetChannel: Wait for Completion Timed Out: {timeOut + 500}");
                    System.Diagnostics.Debug.WriteLine($"SetChannel: Wait for Completion Timed Out: {timeOut + 500}");
                    throw new AerospikeException.Timeout(timeOut, false, refreshTokenTask.Exception);
                }

                if (refreshTokenTask.IsFaulted)
                {
                    Log.Error($"SetChannel: Refresh Token Task Faulted Exception: '{refreshTokenTask.Exception}'");
                    System.Diagnostics.Debug.WriteLine($"SetChannel: Refresh Token Task Faulted Exception: '{refreshTokenTask.Exception}'");
                    throw refreshTokenTask.Exception;
                }
                if (refreshTokenTask.IsCanceled)
                {
                    Log.Error($"SetChannel: Refresh Token Task Canceled: Time Out: {timeOut}: Exception: '{refreshTokenTask.Exception}'");
                    System.Diagnostics.Debug.WriteLine($"SetChannel: Refresh Token Task Canceled: Time Out: {timeOut}: Exception: '{refreshTokenTask.Exception}'");
                    throw new OperationCanceledException("Initial Token Fetch was Canceled", refreshTokenTask.Exception);
                }
            }

            if (Log.DebugEnabled())
            {
                Log.Debug($"SetChannel: Exit: {grpcChannel.Target} ");
            }
        }

        /// <summary>
        /// Called by the Token Refresh Timer <seealso cref="RefreshTokenTimer"/>
        /// Note: <see cref="RefreshToken(CancellationToken, int, bool)"/> method must be called to activate the timer properly!
        /// </summary>
        /// <remarks>
        /// These are not precise timers and can fire later than the defined interval. 
        /// </remarks>
        private void RefreshTokenEvent()
        {
            //If the Token is not being updated and the token needs to be refreshed, than get a new token... 
            if (this.UpdatingToken.IsSet && this.AccessToken.ShouldRefreshToken)
            {
                if (Log.DebugEnabled())
                {
                    Log.Debug($"Refresh Token Timer Event: Enter: {AccessToken}: '{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}'");
                }

                if (IsTokenRequired())
                {
                    try
                    {
                        this.RefreshToken(CancellationToken.None).Wait(this.ClientPolicy.timeout);
                    }
                    catch
                    {
                        if (Log.DebugEnabled())
                        {
                            Log.Debug($"Refresh Token Timer Event: Exception: {AccessToken}: '{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}'");
                        }
                        throw;
                    }
                }

                if (Log.DebugEnabled())
                {
                    Log.Debug($"Refresh Token Timer Event: Exit: {AccessToken}: '{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}'");
                }
            }
            else
            {
                if (Log.DebugEnabled())
                {
                    Log.Debug($"Refresh Token Timer Event: Skipped: {AccessToken}: '{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}'");
                }
            }
        }

        /// <summary>
        /// Performs the act of fetching a new token. 
        /// This will cause all new requests to block until a new token is obtain. 
        /// Request that are already obtained their token and in queue are not effective. 
        /// 
        /// This method also sets/configs the <see cref="RefreshTokenTimer"/> and this must be called after the timer is initially configured.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="timeout"></param>
        /// <param name="forceRefresh">
        /// If true the token is refreshed regardless of state
        /// </param>
        /// <returns></returns>
        private async Task RefreshToken(CancellationToken cancellationToken, int timeout = -1, bool forceRefresh = false)
        {
            //Block requests until a new token is obtained when it is very close to expire...
            if (this.AccessToken?.WillExpire ?? true && !forceRefresh)
            {
                var isSet = this.UpdatingToken.IsSet;
                this.UpdatingToken.Reset();
                if(!isSet) //Try to prevent double entry...
                {
                    return;
                }
                if (Log.DebugEnabled())
                {
                    Log.Debug($"Refresh Token: Reset: {AccessToken}");
                }
            }
            
            //Stop Timer
            RefreshTokenTimer.Stop();
            var prevTimerInterval = this.RefreshTokenTimer.Interval;

            try
            {
                if (Log.DebugEnabled())
                {
                    Log.Debug($"Refresh Token: Enter: {AccessToken}");
                }
                
                var prevToken = Interlocked.Exchange(ref this.AccessToken,
                                                        await FetchToken(this.Channel,
                                                                            this.ClientPolicy.user,
                                                                            this.ClientPolicy.password,
                                                                            timeout > 0
                                                                                ? timeout
                                                                                :this.ClientPolicy.timeout,
                                                                            this.AccessToken,
                                                                            cancellationToken));
                RefreshTokenTimer.Interval = AccessToken.RefreshTime;

                //Restart timer
                RefreshTokenTimer.Start();
                prevToken?.Dispose();

                if (Log.DebugEnabled())
                    Log.Debug($"Refresh Token: Exit: {AccessToken}");                
            }
            catch(AerospikeException)
            {
                throw;
            }
            catch (ArgumentException argEx) //thrown if timer interval is bad...
            {
                Log.Error($"Refresh Token Error {AccessToken} Exception: '{argEx}'");
                System.Diagnostics.Debug.WriteLine($"Refresh Token Error {AccessToken} '{argEx}'");

                this.RefreshTokenTimer.Interval = prevTimerInterval;
                //Restart timer
                RefreshTokenTimer.Start();
            }
            catch (OperationCanceledException)
            {
                Log.Error($"Refresh Token: Cancellation: {AccessToken}: '{DateTime.UtcNow}'");
                throw;
            }            
            catch (Exception ex)
            {                
                Log.Error($"Refresh Token Error {AccessToken} Exception: '{ex}'");
                System.Diagnostics.Debug.WriteLine($"Refresh Token Error {AccessToken} '{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")}' '{ex}'");

                throw;
            }
            finally
            {                
                //Always Unblock requests
                this.UpdatingToken.Set();
            }
        }

        /// <summary>
        /// Fetch the new token regardless of TTL...
        /// Should not be called directly...
        /// </summary>
        /// <param name="channel"></param>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        /// <param name="timeout"></param>
        /// <param name="currentToken"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private static async Task<AccessToken> FetchToken(GrpcChannel channel,
                                                            string userName,
                                                            string password,
                                                            long timeout,
                                                            AccessToken currentToken,
                                                            CancellationToken cancellationToken)
        {           
            try
            {
                if (Log.DebugEnabled())
                    Log.Debug($"FetchToken: Enter: {currentToken}");

                //This will not be the true Get latency since it may include scheduling costs
                var trackLatency = Stopwatch.StartNew();
                var authRequest = new Auth.AerospikeAuthRequest
                {
                    Username = userName,
                    Password = password
                };

                var client = new Auth.AuthService.AuthServiceClient(channel);                
                var response = await client.GetAsync(authRequest,
                                                        cancellationToken: cancellationToken,
                                                        deadline: DateTime.UtcNow.AddMilliseconds(timeout));                                    
                trackLatency.Stop();

                if (Log.DebugEnabled())
                {
                    Log.Debug($"FetchToken: Server Responded: Latency: {trackLatency.ElapsedMilliseconds}");
                }

                var newToken = ParseToken(response.Token, 
                                            trackLatency.ElapsedMilliseconds,
                                            timeout);

                if (Log.DebugEnabled())
                {
                    Log.Debug($"FetchToken: Exchanged New Token: {newToken}");
                }

                return newToken;
            }
            catch (OperationCanceledException)
            {
                Log.Error($"FetchToken: Cancellation: {currentToken}: '{DateTime.UtcNow}'");
                throw;
            }
            catch (RpcException e)
            {
                Log.Error($"FetchToken: Error: {currentToken}: '{DateTime.UtcNow}': Exception: '{e}'");
                System.Diagnostics.Debug.WriteLine($"FetchToken: Error: {currentToken}: '{DateTime.UtcNow}': '{e}'");
                
                throw GRPCConversions.ToAerospikeException(e, (int) timeout, false);
            }            
        }
        
        private static AccessToken ParseToken(string token, long tokenFetchLatency, long timeout)
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

                return new AccessToken(ttl, token, tokenFetchLatency, timeout);
            }
            else
            {
                Log.Error($"ParseToken Error token: '{strClaims}'");
                System.Diagnostics.Debug.WriteLine($"ParseToken Error token: '{strClaims}'");
                
                throw new AerospikeException("Unsupported access token format");
            }
        }

        /// <summary>
        /// Determines if a Token is required
        /// </summary>
        /// <returns>True id token is required</returns>
        public bool IsTokenRequired()
        {
            return ClientPolicy.user != null;
        }
        
        /// <summary>
        /// Returns a token if one is required. 
        /// If the token is being updated, the call is blocked waiting for the token to be reauthorized.
        /// If the token is current, it is returned.
        /// </summary>
        /// <returns>returns current token or null indicating a token is not required</returns>
        /// <param name="cancellationToken"></param>
        public async Task<AccessToken> GetToken(CancellationToken cancellationToken)
        {            
            if (IsTokenRequired())
            {
                if (AccessToken.WillExpire && this.UpdatingToken.IsSet)
                {
                    //Need to block at this point so that any new requests won't get an unauthorized exception
                    this.UpdatingToken.Reset();
                    if (Log.DebugEnabled())
                    {
                        Log.Debug($"GetTokenIfNeeded: Expired: Token: {AccessToken}");
                    }

                    await this.RefreshToken(cancellationToken, forceRefresh: true);

                    if (Log.DebugEnabled())
                    {
                        Log.Debug($"GetTokenIfNeeded: New Token: {AccessToken}");
                    }
                }

                //If token is close to being expired, the request is blocked until a new token obtained.
                if(!this.UpdatingToken.Wait(-1, cancellationToken))
                {
                    if (Log.DebugEnabled())
                    {
                        Log.Debug($"GetTokenIfNeeded: Slim Token Wait Failed: IsCancled: {cancellationToken.IsCancellationRequested}");
                    }
                }

                return this.AccessToken;
            }

            return null;
        }

        public bool Disposed { get; private set; }
        private void Dispose(bool disposing)
        {
            if (!Disposed)
            {
                if (disposing)
                {
                    this.RefreshTokenTimer?.Dispose();
                    this.Channel = null;
                }

                Disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }

    [DebuggerDisplay("{ToString(),nq}")]
    public sealed class AccessToken : IDisposable
    {
        /// <summary>
        /// This factor is used when the <see cref="RefreshTime"/> calculation is less or equal to zero
        /// </summary>
        private const float refreshZeroFraction = 0.10f;
        /// <summary>
        /// This factor is used when the <see cref="RefreshTime"/> calculation is greater or equal to <see cref="ttl"/>
        /// </summary>
        private const float refreshAfterFraction = 0.85f;

        /// <summary>
        /// Local token expiry timestamp in mills.
        /// </summary>
        private readonly Stopwatch expiry;
        /// <summary>
        /// Remaining time to live for the token in mills.
        /// </summary>
        private readonly long ttl;

        /// <summary>
        /// The time before <see cref="ttl"/> to obtain the new token by getting a new one from the proxy server.
        /// This field is calculated from <see cref="ttl"/> taking into consideration network latency and other factors like timer delay, etc.
        /// </summary>
        public readonly long RefreshTime;

        /// <summary>
        /// Time close to <see cref="ttl"/> that the token will expire based on <see cref="TokenFetchLatency"/>
        /// </summary>
        public readonly long WillExpireSoon;

        /// <summary>
        /// An access token for Aerospike proxy.
        /// </summary>
        public readonly string Token;

        /// <summary>
        /// The latency involved when fetching the token. 
        /// </summary>
        public readonly long TokenFetchLatency;
        
        public AccessToken(long ttl, string token, long tokenFetchLatency, long timeout)
        {
            this.expiry = Stopwatch.StartNew();
            this.ttl = ttl;

            var useFetchLatency = tokenFetchLatency * 2 >= timeout
                                        ? timeout * 0.5
                                        : tokenFetchLatency;
            var possibleRefreshTime = (ttl * refreshAfterFraction) - useFetchLatency;

            if (possibleRefreshTime > 0 && possibleRefreshTime < ttl)
            {
                this.RefreshTime = (long) possibleRefreshTime;
            }
            else
            {
                this.RefreshTime = (long)Math.Floor(ttl * refreshZeroFraction);
            }
            
            var willExpireSoon = this.RefreshTime + tokenFetchLatency;

            this.TokenFetchLatency = tokenFetchLatency;
            this.WillExpireSoon = willExpireSoon >= ttl ? this.RefreshTime : willExpireSoon;
            this.Token = token;

            if (Log.DebugEnabled())
            {
                System.Diagnostics.Debug.WriteLine(this);
            }
        }

        /// <summary>
        /// Token Has Expired
        /// </summary>
        public bool HasExpired => expiry.ElapsedMilliseconds >= ttl;

        /// <summary>
        /// True to indicate that the token is close to expiring based on latency
        /// </summary>
        public bool WillExpire => expiry.ElapsedMilliseconds >= WillExpireSoon;
        /// <summary>
        /// Token should be refreshed before hitting expiration
        /// </summary>
        public bool ShouldRefreshToken => expiry.ElapsedMilliseconds >= RefreshTime;

        public override string ToString()
        {
            var expired = this.HasExpired ? ", Expired:true" : (this.ShouldRefreshToken ? ", NeedRefresh:true" : string.Empty);
            var disposed = this.Disposed ? ", Disposed:true" : string.Empty;

            return $"AccessToken{{TokenHash:{Token?.GetHashCode()}, RunningTime:{expiry.ElapsedMilliseconds}, TTL:{ttl}, RefreshOn:{RefreshTime}, Latency:{TokenFetchLatency}{expired}{disposed}}}";
        }

        public bool Disposed { get; private set; }

        private void Dispose(bool disposing)
        {
            if (!Disposed)
            {
                if (disposing)
                {
                    this.expiry.Stop();                    
                }

                Disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

