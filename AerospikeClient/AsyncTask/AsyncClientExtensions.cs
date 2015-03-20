using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Aerospike.Client;

namespace Aerospike.Client
{
    /// <summary>
    /// Async/await extensions for AsyncClient.
    /// </summary>
    public static class AsyncClientExtensions
    {
        /// <summary>
        /// Asynchronously write record bin(s). 
        /// <para>
        /// The policy specifies the transaction timeout, record expiration and how the transaction is
        /// handled when the record already exists.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">write configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <param name="bins">array of bin name/value pairs</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task PutAsync(this AsyncClient client, CancellationToken token, WritePolicy policy, Key key, params Bin[] bins)
        {
            var listener = new WriteListenerAdapter(token);
            client.Put(policy, listener, key, bins);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously append bin string values to existing record bin values.
        /// <para>
        /// The policy specifies the transaction timeout, record expiration and how the transaction is
        /// handled when the record already exists.
        /// This call only works for string values. 
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">write configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <param name="bins">array of bin name/value pairs</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task AppendAsync(this AsyncClient client, CancellationToken token, WritePolicy policy, Key key, params Bin[] bins)
        {
            var listener = new WriteListenerAdapter(token);
            client.Append(policy, listener, key, bins);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously prepend bin string values to existing record bin values.
        /// <para>
        /// The policy specifies the transaction timeout, record expiration and how the transaction is
        /// handled when the record already exists.
        /// This call works only for string values. 
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">write configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <param name="bins">array of bin name/value pairs</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task Prepend(this AsyncClient client, CancellationToken token, WritePolicy policy, Key key, params Bin[] bins)
        {
            var listener = new WriteListenerAdapter(token);
            client.Prepend(policy, listener, key, bins);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously add integer bin values to existing record bin values.
        /// <para>
        /// The policy specifies the transaction timeout, record expiration and how the transaction is
        /// handled when the record already exists.
        /// This call only works for integer values. 
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">write configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <param name="bins">array of bin name/value pairs</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task AddAsync(this AsyncClient client, CancellationToken token, WritePolicy policy, Key key, params Bin[] bins)
        {
            var listener = new WriteListenerAdapter(token);
            client.Add(policy, listener, key, bins);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously delete record for specified key.
        /// <para>
        /// The policy specifies the transaction timeout.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">delete configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<bool> DeleteAsync(this AsyncClient client, CancellationToken token, WritePolicy policy, Key key)
        {
            var listener = new DeleteListenerAdapter(token);
            client.Delete(policy, listener, key);
            return listener.Task;
        }


        /// <summary>
        /// Asynchronously create record if it does not already exist.  If the record exists, the record's 
        /// time to expiration will be reset to the policy's expiration.
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">write configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task TouchAsync(this AsyncClient client, CancellationToken token, WritePolicy policy, Key key)
        {
            var listener = new WriteListenerAdapter(token);
            client.Touch(policy, listener, key);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously determine if a record key exists.
        /// <para>
        /// The policy can be used to specify timeouts.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">generic configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<bool> ExistsAsync(this AsyncClient client, CancellationToken token, Policy policy, Key key)
        {
            var listener = new ExistsListenerAdapter(token);
            client.Exists(policy, listener, key);
            return listener.Task;
        }


        /// <summary>
        /// Asynchronously check if multiple record keys exist in one batch call.
        /// <para>
        /// The policy can be used to specify timeouts.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">generic configuration parameters, pass in null for defaults</param>
        /// <param name="keys">array of unique record identifiers</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<bool[]> ExistsAsync(this AsyncClient client, CancellationToken token, Policy policy, Key[] keys)
        {
            var listener = new ExistsArrayListenerAdapter(token);
            client.Exists(policy, listener, keys);
            return listener.Task;
        }


        /// <summary>
        /// Asynchronously read entire record for specified key.
        /// <para>
        /// The policy can be used to specify timeouts.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">generic configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<Record> GetAsync(this AsyncClient client, CancellationToken token, Policy policy, Key key)
        {
            var listener = new RecordListenerAdapter(token);
            client.Get(policy, listener, key);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously read record generation and expiration only for specified key.  Bins are not read.
        /// <para>
        /// The policy can be used to specify timeouts.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">generic configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<Record> GetHeaderAsync(this AsyncClient client, CancellationToken token, Policy policy, Key key)
        {
            var listener = new RecordListenerAdapter(token);
            client.GetHeader(policy, listener, key);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously read record header and bins for specified key.
        /// <para>
        /// The policy can be used to specify timeouts.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">generic configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <param name="binNames">bins to retrieve</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<Record> GetAsync(this AsyncClient client, CancellationToken token, Policy policy, Key key, params string[] binNames)
        {
            var listener = new RecordListenerAdapter(token);
            client.Get(policy, listener, key, binNames);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously read multiple records for specified keys in one batch call.
        /// <para>
        /// If a key is not found, the record will be null.
        /// The policy can be used to specify timeouts.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">generic configuration parameters, pass in null for defaults</param>
        /// <param name="keys">array of unique record identifiers</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<Record[]> GetAsync(this AsyncClient client, CancellationToken token, Policy policy, Key[] keys)
        {
            var listener = new RecordArrayListenerAdapter(token);
            client.Get(policy, listener, keys);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously read multiple record header data for specified keys in one batch call.
        /// <para>
        /// If a key is not found, the record will be null.
        /// The policy can be used to specify timeouts.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">generic configuration parameters, pass in null for defaults</param>
        /// <param name="keys">array of unique record identifiers</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<Record[]> GetHeaderAsync(this AsyncClient client, CancellationToken token, Policy policy, Key[] keys)
        {
            var listener = new RecordArrayListenerAdapter(token);
            client.GetHeader(policy, listener, keys);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously read multiple record headers and bins for specified keys in one batch call.
        /// <para>
        /// If a key is not found, the record will be null.
        /// The policy can be used to specify timeouts.
        /// </para>
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">generic configuration parameters, pass in null for defaults</param>
        /// <param name="keys">array of unique record identifiers</param>
        /// <param name="binNames">array of bins to retrieve</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<Record[]> GetAsync(this AsyncClient client, CancellationToken token, Policy policy, Key[] keys, params string[] binNames)
        {
            var listener = new RecordArrayListenerAdapter(token);
            client.Get(policy, listener, keys, binNames);
            return listener.Task;
        }

        /// <summary>
        /// Asynchronously perform multiple read/write operations on a single key in one batch call.
        /// An example would be to add an integer value to an existing record and then
        /// read the result, all in one database call.
        /// </summary>
        /// <param name="client">AsyncClient istance</param>
        /// <param name="token">cancellation token</param>
        /// <param name="policy">write configuration parameters, pass in null for defaults</param>
        /// <param name="key">unique record identifier</param>
        /// <param name="operations">database operations to perform</param>
        /// <exception cref="AerospikeException">if queue is full</exception>
        public static System.Threading.Tasks.Task<Record> OperateAsync(this AsyncClient client, CancellationToken token, WritePolicy policy, Key key, params Operation[] operations)
        {
            var listener = new RecordListenerAdapter(token);
            client.Operate(policy, listener, key, operations);
            return listener.Task;
        }
    }
}
