using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Aerospike.Benchmarks
{
    public struct PrefStat
    {
        /// <summary>
        /// The Sequence Id of when this was captured
        /// </summary>
        public long SequenceNbr;
        /// <summary>
        /// The elapsed time since the application was loaded
        /// </summary>
        [JsonConverter(typeof(TimespanConverter))]
        public TimeSpan AppElapsedTime;
        /// <summary>
        /// Timing of the measured event
        /// </summary>
        //[JsonConverter(typeof(TimespanConverterMS))]
        public double Timing;
        /// <summary>
        /// Event (e.g., Put, Get)
        /// </summary>
        public string Event;
        /// <summary>
        /// Name of the calling function where the event occurred
        /// </summary>
        public string FuncName;
        /// <summary>
        /// Associated Primary Key, if there is one...
        /// </summary>
        public string PK;

    }

    public static class PrefStats
    {

        public static bool EnableTimings { get; set; } = true;
        public static Stopwatch RunningStopwatch { get; } = Stopwatch.StartNew();
        private static long SequenceNbr = 0;
        public static ConcurrentQueue<PrefStat> ConcurrentCollection { get; } = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void StopRecording(this Stopwatch stopWatch, string eventType, string funcName, Aerospike.Client.Key pk)
        {
            stopWatch.Stop();
            if (EnableTimings)
                ConcurrentCollection.Enqueue(new PrefStat
                {
                    SequenceNbr = Interlocked.Increment(ref SequenceNbr),
                    AppElapsedTime = RunningStopwatch.Elapsed,
                    Timing = stopWatch.Elapsed.TotalMilliseconds,
                    Event = eventType,
                    FuncName = funcName,
                    PK = pk?.ToString()
                });
        }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void StopRecording(this Stopwatch stopWatch, string eventType, string funcName, Aerospike.Client.Key[] keys)
		{
			stopWatch.Stop();
			if (EnableTimings)
				ConcurrentCollection.Enqueue(new PrefStat
				{
					SequenceNbr = Interlocked.Increment(ref SequenceNbr),
					AppElapsedTime = RunningStopwatch.Elapsed,
					Timing = stopWatch.Elapsed.TotalMilliseconds,
					Event = eventType,
					FuncName = funcName,
					PK = keys?.ToString()
				});
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void RecordEvent(this double eventTimeSpan, string eventType, string funcName, Aerospike.Client.Key pk)
        {
            if (EnableTimings)
                ConcurrentCollection.Enqueue(new PrefStat
                {
                    SequenceNbr = Interlocked.Increment(ref SequenceNbr),
                    AppElapsedTime = RunningStopwatch.Elapsed,
                    Timing = eventTimeSpan,
                    Event = eventType,
                    FuncName = funcName,
                    PK = pk?.ToString()
                });
        }

        /// <summary>
        /// Format: Days.Hours:Minutes:Seconds:Milliseconds
        /// </summary>
        public const string TimeSpanFormatString = @"hh\:mm\:ss\.fffffff";

        public static void ToCSV(string csvFile)
        {
            if (string.IsNullOrEmpty(csvFile)) return;

            var csvString = new StringBuilder();

            using var sw = new StreamWriter(csvFile);

            csvString.Append(nameof(PrefStat.AppElapsedTime))
                    .Append(',')
                    .Append(nameof(PrefStat.SequenceNbr))
                    .Append(',')
                    .Append(nameof(PrefStat.Timing))
                    .Append("(ms)")
                    .Append(',')
                    .Append(nameof(PrefStat.Event))
                    .Append(',')
                    .Append(nameof(PrefStat.FuncName))
                    .Append(',')
                    .Append(nameof(PrefStat.PK));

            sw.WriteLine(csvString);
            csvString.Clear();

            foreach (var stat in ConcurrentCollection)
            {
                csvString.Append(stat.AppElapsedTime.ToString(TimeSpanFormatString))
                            .Append(',')
                            .Append(stat.SequenceNbr)
                            .Append(',')
                            .Append(stat.Timing)
                            .Append(',')
                            .Append(stat.Event)
                            .Append(',')
                            .Append(stat.FuncName)
                            .Append(',')
                            .Append(stat.PK);
                sw.WriteLine(csvString);
                csvString.Clear();
            }

        }

        public static void ToJson(string jsonTimingFile)
        {
            if (string.IsNullOrEmpty(jsonTimingFile)) return;
            File.WriteAllText(jsonTimingFile,
                                JsonSerializer.Serialize(ConcurrentCollection,
                                                            new JsonSerializerOptions()
                                                            {
                                                                WriteIndented = true,
                                                                IncludeFields = true,
                                                                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
                                                            }));
        }
    }

    public class TimespanConverterMS : JsonConverter<TimeSpan>
    {
        public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => TimeSpan.FromMilliseconds((double)reader.GetDouble());

        public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options)
            => writer.WriteNumberValue(value.TotalMilliseconds);
    }

    public class TimespanConverter : JsonConverter<TimeSpan>
    {
        public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => TimeSpan.ParseExact(reader.GetString(), PrefStats.TimeSpanFormatString, null);

        public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options)
            => writer.WriteStringValue(value.ToString(PrefStats.TimeSpanFormatString));
    }
}
