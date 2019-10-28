using System.Threading;

namespace Aerospike.Client.Query
{
    public interface IRecordSet
    {
        /// <summary>
        /// Retrieve next record. Returns true if record exists and false if no more 
        /// records are available.
        /// This method will block until a record is retrieved or the query is cancelled.
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
        /// Get record's unique identifier.
        /// </summary>
        Key Key { get; }

        /// <summary>
        /// Get record's header and bin data.
        /// </summary>
        Record Record { get; }

        /// <summary>
        /// Get CancellationToken associated with this query.
        /// </summary>
        CancellationToken CancelToken { get; }

        /// <summary>
        /// Put a record on the queue.
        /// </summary>
        bool Put(KeyRecord record);
    }
}