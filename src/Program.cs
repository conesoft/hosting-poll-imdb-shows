using Conesoft.Files;
using Conesoft.Hosting;
using Conesoft.Notifications;
using Conesoft.Services.PollImdbShows;
using Humanizer;
using Serilog;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

var builder = Host.CreateApplicationBuilder(args);

builder
    .AddHostConfigurationFiles()
    .AddHostEnvironmentInfo()
    .AddLoggingService()
    .AddNotificationService()
    ;

var host = builder.Build();

using var lifetime = await host.StartConsoleAsync();

var configuration = builder.Configuration;
var environment = host.Services.GetRequiredService<HostEnvironment>();
var notifier = host.Services.GetRequiredService<Notifier>();

var stopwatch = new Stopwatch();
var stringbuilder = new StringBuilder();

var timer = new PeriodicTimer(TimeSpan.FromHours(6));


var storage = environment.Global.Storage / "FromSources" / "IMDb";
Log.Information("IMDb storage: {storage}", storage);

do
{
    {

        TimeStamp("beginning downloads");

        Dictionary<int, Show> shows = [];
        Dictionary<int, string> entriesById = [];
        List<EpisodeEntry> entries = [];

        await Task.WhenAll(Task.Run(async () =>
        {
            var client = new HttpClient();
            var bytes = await client.GetByteArrayAsync("https://datasets.imdbws.com/title.basics.tsv.gz");
            using var zipped = new MemoryStream(bytes);
            using var stream = new GZipStream(zipped, CompressionMode.Decompress);
            var reader = new StreamReader(stream);

            string? line = reader.ReadLine(); // skip
            while ((line = reader.ReadLine()) != null)
            {
                Process(line!);
            }

            void Process(ReadOnlySpan<char> line)
            {
                var to1 = line.IndexOf('\t');
                var to2 = line[(to1 + 1)..].IndexOf('\t') + 1 + to1;
                var to3 = line[(to2 + 1)..].IndexOf('\t') + 1 + to2;
                //var to4 = line[(to3 + 1)..].IndexOf('\t') + 1 + to3;
                //var to5 = line[(to4 + 1)..].IndexOf('\t') + 1 + to4;
                //var to6 = line[(to5 + 1)..].IndexOf('\t') + 1 + to5;
                //var to7 = line[(to6 + 1)..].IndexOf('\t') + 1 + to6;
                //var to8 = line[(to7 + 1)..].IndexOf('\t') + 1 + to7;
                var s0 = line[..to1];
                var s1 = line[(to1 + 1)..to2];
                var s2 = line[(to2 + 1)..to3];
                //var s3 = line[(to3 + 1)..to4];
                //var s4 = line[(to4 + 1)..to5];
                //var s5 = line[(to5 + 1)..to6];
                //var s6 = line[(to6 + 1)..to7];
                //var s7 = line[(to7 + 1)..to8];
                //var s8 = line[(to8 + 1)..];
                if (s1.SequenceEqual("tvEpisode") || s1.SequenceEqual("tvSeries"))
                {
                    entriesById.Add(int.Parse(s0[2..]), s2.ToString());
                }
            }
        }), Task.Run(async () =>
        {
            var client = new HttpClient();
            var bytes = await client.GetByteArrayAsync("https://datasets.imdbws.com/title.episode.tsv.gz");
            using var zipped = new MemoryStream(bytes);
            using var stream = new GZipStream(zipped, CompressionMode.Decompress);
            var reader = new StreamReader(stream);
            string? line = reader.ReadLine(); // skip
            while ((line = reader.ReadLine()) != null)
            {
                Process(line!);
            }

            void Process(ReadOnlySpan<char> line)
            {
                var to1 = line.IndexOf('\t');
                var to2 = line[(to1 + 1)..].IndexOf('\t') + 1 + to1;
                var to3 = line[(to2 + 1)..].IndexOf('\t') + 1 + to2;
                var s0 = line[..to1];
                var s1 = line[(to1 + 1)..to2];
                var s2 = line[(to2 + 1)..to3];
                var s3 = line[(to3 + 1)..];
                entries.Add(new EpisodeEntry(
                    EpisodeId: int.Parse(s0[2..]),
                    SeriesId: int.Parse(s1[2..]),
                    Season: int.TryParse(s2, out var season) ? season : int.MaxValue,
                    Episode: int.TryParse(s3, out var episode) ? episode : int.MaxValue
                ));
            }
        }));
        TimeStamp("db built");

        var showsDirectory = storage / "shows";
        showsDirectory.Create();

        var newshows = entries.AsParallel().GroupBy(m => m.SeriesId).Where(g => entriesById.ContainsKey(g.Key)).Select(g => new Show(
            Id: g.Key,
            Name: entriesById[g.Key],
            Episodes: g
                .GroupBy(g => g.Season)
                .OrderBy(g => g.Key)
                .Select(s => s
                    .Where(e => entriesById.ContainsKey(e.EpisodeId))
                    .OrderBy(e => e.Episode)
                    .Select(e => entriesById[e.EpisodeId])
                    .ToArray()
                )
                .ToArray()
        )).Where(s => s.Episodes.Length != 0 && s.Episodes[0].Length > 1).ToArray();

        TimeStamp($"{newshows.Length} shows found");

        int saved = 0;
        int skipped = 0;

        Parallel.ForEach(newshows, parallelOptions: new() { MaxDegreeOfParallelism = 1024 }, show =>
        {
            if (shows.TryGetValue(show.Id, out Show? oldshow))
            {
                if (oldshow.Name != show.Name)
                {
                    goto notequal;
                }
                if (oldshow.Episodes.Length != show.Episodes.Length)
                {
                    goto notequal;
                }
                for (var i = 0; i < oldshow.Episodes.Length; i++)
                {
                    var oldseason = oldshow.Episodes[i];
                    var season = show.Episodes[i];

                    if (oldseason.Length != season.Length)
                    {
                        goto notequal;
                    }

                    for (var ii = 0; ii < oldseason.Length; ii++)
                    {
                        var oldepisode = oldseason[ii];
                        var episode = season[ii];

                        if (oldepisode != episode)
                        {
                            goto notequal;
                        }
                    }
                }

                Interlocked.Increment(ref skipped);
                return;
            }

        notequal:

            var invalidcharacters = Path.GetInvalidFileNameChars().Append('-').Append(' ').ToArray();
            var safename = string.Join(' ', show.Name.Split(invalidcharacters, StringSplitOptions.RemoveEmptyEntries));

            var file = showsDirectory / Filename.From($"{safename} - {show.Id}", "json");

            var output = $"[{string.Join(", ", show.Episodes.Select(season =>
            {
                return $"{Environment.NewLine}\t[{Environment.NewLine}{string.Join(", " + Environment.NewLine, season.Select(episode => $"\t\t\"{episode.Replace("\"", "\\\"")}\""))}{Environment.NewLine}\t]";
            }))}{Environment.NewLine}]";

            Interlocked.Increment(ref saved);

            file.WriteText(output);
        });

        shows = newshows.ToDictionary(s => s.Id);

        TimeStamp($"{saved} updated shows stored");

        await notifier.Notify(title: "Poll IMDb Shows", stringbuilder.ToString());

        TimeStamp("notified");

        // cleanup
        stringbuilder.Clear();
        stringbuilder = new();
        entries = [];
        entriesById = [];

        TimeStamp("cleaned up");

        stopwatch.Stop();
    }
}
while (await timer.WaitForNextTickAsync(lifetime.CancellationToken).ReturnFalseWhenCancelled());

void TimeStamp(string description)
{
    if (stopwatch.IsRunning == false)
    {
        Log.Information(description);
    }
    else
    {
        var line = $"{description} in {stopwatch.Elapsed.Humanize(precision: 2)}";
        Log.Information(line);
        stringbuilder.AppendLine(line);
    }
    stopwatch.Restart();
}
