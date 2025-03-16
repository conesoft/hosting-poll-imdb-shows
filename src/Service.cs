using Conesoft.Files;
using Conesoft.Hosting;
using Conesoft.Notifications;
using Humanizer;
using Serilog;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace Conesoft.Services.PollImdbShows;

public class Service(HostEnvironment environment, Notifier notifier, IHttpClientFactory factory) : PeriodicTask(TimeSpan.FromHours(6))
{
    readonly Files.Directory storage = environment.Global.Storage / "content" / "tv shows";
    Dictionary<int, Show> shows = [];

    protected override async Task Process()
    {
        var stopwatch = new Stopwatch();
        var stringbuilder = new StringBuilder();
        stopwatch.Start();
        storage.Create();

        await DelayIfRecentlyRun(storage, TimeSpan.FromHours(1));

        Log.Information("beginning downloads");

        var loadShows = Task.Run(LoadShows);
        var loadEpisodes = Task.Run(LoadEpisodes);
        var entriesById = await loadShows;
        var entries = await loadEpisodes;
        TimeStamp("db built");

        var newshows = CreateShowsFromEntries(entries, entriesById);
        TimeStamp($"shows found", newshows.Length);

        var saved = SaveNewShows(newshows);
        shows = newshows.ToDictionary(s => s.Id);
        TimeStamp($"updated shows stored", saved);

        await notifier.Notify(title: "Poll IMDb Shows", stringbuilder.ToString());

        void TimeStamp(string description, int? count = null)
        {
            Log.Information("'{count}{space}{description}' done in {time}", count.HasValue ? count : "", count.HasValue ? " " : "", description, stopwatch.Elapsed.Humanize(precision: 2));
            stringbuilder.AppendLine($"{(count.HasValue ? count + " " : "")}{description} in {stopwatch.Elapsed.Humanize(precision: 1)}");
            stopwatch.Restart();
        }
    }

    Task DelayIfRecentlyRun(Files.Directory storage, TimeSpan delay) => storage.Info.LastWriteTimeUtc + delay > DateTime.UtcNow && environment.IsInHostedEnvironment ? Task.Delay(TimeSpan.FromHours(1)) : Task.CompletedTask;

    async Task<Dictionary<int, string>> LoadShows()
    {
        Dictionary<int, string> entriesById = [];
        var client = factory.CreateClient();

        using var stream = new GZipStream(await client.GetStreamAsync("https://datasets.imdbws.com/title.basics.tsv.gz"), CompressionMode.Decompress);
        var reader = new StreamReader(stream);
        reader.ReadLine(); // skip header line

        while (reader.ReadLine() is string line)
        {
            var to1 = line.IndexOf('\t');
            var to2 = line[(to1 + 1)..].IndexOf('\t') + 1 + to1;
            var to3 = line[(to2 + 1)..].IndexOf('\t') + 1 + to2;
            var s0 = line[..to1];
            var s1 = line[(to1 + 1)..to2];
            var s2 = line[(to2 + 1)..to3];
            if (s1.SequenceEqual("tvEpisode") || s1.SequenceEqual("tvSeries"))
            {
                entriesById.Add(int.Parse(s0[2..]), s2.ToString());
            }
        }
        return entriesById;
    }

    async Task<List<EpisodeEntry>> LoadEpisodes()
    {
        List<EpisodeEntry> entries = [];
        var client = factory.CreateClient();

        using var stream = new GZipStream(await client.GetStreamAsync("https://datasets.imdbws.com/title.episode.tsv.gz"), CompressionMode.Decompress);
        var reader = new StreamReader(stream);
        reader.ReadLine(); // skip header line

        while (reader.ReadLine() is string line)
        {
            var to1 = line.IndexOf('\t');
            var to2 = line[(to1 + 1)..].IndexOf('\t') + 1 + to1;
            var to3 = line[(to2 + 1)..].IndexOf('\t') + 1 + to2;
            var s0 = line[..to1];
            var s1 = line[(to1 + 1)..to2];
            var s2 = line[(to2 + 1)..to3];
            var s3 = line[(to3 + 1)..];
            entries.Add(new(
                EpisodeId: int.Parse(s0[2..]),
                SeriesId: int.Parse(s1[2..]),
                Season: int.TryParse(s2, out var season) ? season : int.MaxValue,
                Episode: int.TryParse(s3, out var episode) ? episode : int.MaxValue
            ));
        }
        return entries;
    }

    static Show[] CreateShowsFromEntries(List<EpisodeEntry> entries, Dictionary<int, string> entriesById) => entries.AsParallel()
        .GroupBy(m => m.SeriesId)
        .Where(g => entriesById.ContainsKey(g.Key))
        .Select(g => new Show(
            Id: g.Key,
            Name: entriesById[g.Key],
            Episodes: [.. g
                .GroupBy(g => g.Season)
                .OrderBy(g => g.Key)
                .Select(s => s
                    .Where(e => entriesById.ContainsKey(e.EpisodeId))
                    .OrderBy(e => e.Episode)
                    .Select(e => entriesById[e.EpisodeId])
                    .ToArray()
                )
            ]
        ))
        .Where(s => s.Episodes.Length != 0 && s.Episodes[0].Length > 1)
        .ToArray();

    bool ShowIsNew(Show show)
    {
        if (shows.TryGetValue(show.Id, out Show? old) == false) return true;
        if (old.Name != show.Name) return true;
        if (old.Episodes.Length != show.Episodes.Length) return true;
        for (var i = 0; i < old.Episodes.Length; i++)
        {
            var oldseason = old.Episodes[i];
            var season = show.Episodes[i];
            if (oldseason.Length != season.Length) return true;
            for (var ii = 0; ii < oldseason.Length; ii++)
            {
                var oldepisode = oldseason[ii];
                var episode = season[ii];
                if (oldepisode != episode) return true;
            }
        }
        return false;
    }

    int SaveNewShows(Show[] newshows)
    {
        int saved = 0;
        Parallel.ForEach(newshows, new ParallelOptions { MaxDegreeOfParallelism = 1024 }, show =>
        {
            if (ShowIsNew(show))
            {
                SaveShow(show, storage);
                Interlocked.Increment(ref saved);
            }
        });
        return saved;
    }

    static void SaveShow(Show show, Files.Directory storage)
    {
        var safename = string.Join(' ', show.Name.Split(Path.GetInvalidFileNameChars().Append('-').Append(' ').ToArray(), StringSplitOptions.RemoveEmptyEntries));
        var file = storage / Filename.From($"{safename} - {show.Id}", "json");
        file.Now.WriteText($"[{string.Join(", ", show.Episodes.Select(s => $"\n\t[\n{string.Join(", \n", s.Select(e => $"\t\t\"{e.Replace("\"", "\\\"")}\""))}\n\t]"))}\n]");
    }
}
