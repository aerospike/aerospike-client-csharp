using System.Threading;

namespace Aerospike.Client.Query
{
    public interface IResultSet
    {
        /// <summary>
        /// Retrieve next result. Returns true if result exists and false if no more 
        /// results are available.
        /// This method will block until a result is retrieved or the query is cancelled.
        /// </summary>
        bool Next();

        /// <summary>
        /// Close query.
        /// </summary>
        void Dispose();

        /// <summary>
        /// Close query.
        /// </summary>
        void Close();

        /// <summary>
        /// Get result.
        /// </summary>
        object Object { get; }

        /// <summary>
        /// Get CancellationToken associated with this query.
        /// </summary>
        CancellationToken CancelToken { get; }

        /// <summary>
        /// Put object on the queue.
        /// </summary>
        bool Put(object obj);
    }
}