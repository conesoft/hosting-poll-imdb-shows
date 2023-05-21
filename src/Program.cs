using Conesoft.Files;
using Humanizer;
using Microsoft.AspNetCore.Http.Extensions;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

var configuration = new ConfigurationBuilder().AddJsonFile(Conesoft.Hosting.Host.GlobalSettings.Path).Build();
var conesoftSecret = configuration["conesoft:secret"] ?? throw new Exception("Conesoft Secret not found in Configuration");

var client = new HttpClient();
var stopwatch = new Stopwatch();
var stringbuilder = new StringBuilder();

var timer = new PeriodicTimer(TimeSpan.FromHours(6));

var settings = Conesoft.Hosting.Host.LocalSettings;
var storage = Conesoft.Hosting.Host.GlobalStorage / "FromSources" / "IMDb";

var webSources = new[]
{
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.episode.tsv.gz"
};

do
{
    TimeStamp("beginning downloads");
    var sources = webSources.Select(s => new
    {
        Web = s,
        LocalZipped = storage / Filename.FromExtended(new Uri(s).AbsolutePath),
        Local = storage / Filename.FromExtended(new Uri(s).AbsolutePath.Replace(".gz", ""))
    });

    await Task.WhenAll(sources.Select(async s =>
    {
        s.Local.Parent.Create();

        s.Local.Delete();
        s.LocalZipped.Delete();

        await s.LocalZipped.WriteBytes(await client.GetByteArrayAsync(s.Web));

        using var zipped = s.LocalZipped.OpenRead();
        using var unzipped = System.IO.File.Create(s.Local.Path);
        using var unzip = new GZipStream(zipped, CompressionMode.Decompress);
        unzip.CopyTo(unzipped);
    }));
    TimeStamp("downloaded");

    Dictionary<int, string> entriesById = new();
    EpisodeEntry[] entries = Array.Empty<EpisodeEntry>();

    Parallel.Invoke(() =>
    {
        var basics = sources.First(s => s.Web.Contains("title.basics.tsv"));
        foreach (var line in System.IO.File.ReadLines(basics.Local.Path).Skip(1))
        {
            var _line = line.AsSpan();
            var to1 = _line.IndexOf('\t');
            var to2 = _line[(to1 + 1)..].IndexOf('\t') + 1 + to1;
            var to3 = _line[(to2 + 1)..].IndexOf('\t') + 1 + to2;
            //var to4 = _line[(to3 + 1)..].IndexOf('\t') + 1 + to3;
            //var to5 = _line[(to4 + 1)..].IndexOf('\t') + 1 + to4;
            //var to6 = _line[(to5 + 1)..].IndexOf('\t') + 1 + to5;
            //var to7 = _line[(to6 + 1)..].IndexOf('\t') + 1 + to6;
            //var to8 = _line[(to7 + 1)..].IndexOf('\t') + 1 + to7;
            var s0 = _line[..to1];
            var s1 = _line[(to1 + 1)..to2];
            var s2 = _line[(to2 + 1)..to3];
            //var s3 = _line[(to3 + 1)..to4];
            //var s4 = _line[(to4 + 1)..to5];
            //var s5 = _line[(to5 + 1)..to6];
            //var s6 = _line[(to6 + 1)..to7];
            //var s7 = _line[(to7 + 1)..to8];
            //var s8 = _line[(to8 + 1)..];
            if (s1.SequenceEqual("tvEpisode") || s1.SequenceEqual("tvSeries"))
            {
                entriesById.Add(int.Parse(s0[2..]), s2.ToString());
            }
        }
    }, () =>
    {
        var episodes = sources.First(s => s.Web.Contains("title.episode.tsv"));
        entries = System.IO.File.ReadLines(episodes.Local.Path).Skip(1).Select(line =>
        {
            var _line = line.AsSpan();
            var to1 = _line.IndexOf('\t');
            var to2 = _line[(to1 + 1)..].IndexOf('\t') + 1 + to1;
            var to3 = _line[(to2 + 1)..].IndexOf('\t') + 1 + to2;
            var s0 = _line[..to1];
            var s1 = _line[(to1 + 1)..to2];
            var s2 = _line[(to2 + 1)..to3];
            var s3 = _line[(to3 + 1)..];
            return new EpisodeEntry(
                EpisodeId: int.Parse(s0[2..]),
                SeriesId: int.Parse(s1[2..]),
                Season: int.TryParse(s2, out var season) ? season : int.MaxValue,
                Episode: int.TryParse(s3, out var episode) ? episode : int.MaxValue
            );
        }).ToArray();
    });
    TimeStamp("db built");

    var showsDirectory = storage / "shows";
    showsDirectory.Create();

    var shows = entries.AsParallel().GroupBy(m => m.SeriesId).Where(g => entriesById.ContainsKey(g.Key)).Select(g => new Show(
        Id: g.Key,
        Name: entriesById[g.Key],
        Episodes: g
            .GroupBy(g => g.Season)
            .Select(s => s
                .Where(e => entriesById.ContainsKey(e.EpisodeId))
                .Select(e => entriesById[e.EpisodeId])
                .ToArray()
            )
            .ToArray()
    )).Where(s => s.Episodes.Any() && s.Episodes[0].Length > 1).ToArray();

    TimeStamp("shows generated");

    Parallel.ForEach(shows, new ParallelOptions { MaxDegreeOfParallelism = 64 }, show =>
    {
        var characters = Path.GetInvalidFileNameChars().Append('-').Append(' ').ToArray();
        var safefilename = string.Join(' ', show.Name.Split(characters, StringSplitOptions.RemoveEmptyEntries));
        var file = showsDirectory / Filename.From(safefilename, "json");
        file.WriteAsJson(show.Episodes);
    });

    TimeStamp("shows stored");

    await Notify(stringbuilder.ToString());
    stopwatch.Stop();
    stringbuilder = new StringBuilder();
}
while (await timer.WaitForNextTickAsync());

void TimeStamp(string description)
{
    if (stopwatch.IsRunning == false)
    {
        Console.WriteLine(description);
    }
    else
    {
        var line = $"{description} in {stopwatch.Elapsed.Humanize()}";
        Console.WriteLine(line);
        stringbuilder.AppendLine(line);
    }
    stopwatch.Restart();
}

async Task Notify(string message)
{
    var title = "Poll IMDb Shows";

    var query = new QueryBuilder
    {
        { "token", conesoftSecret! },
        { "title", title },
        { "message", message }
    };

    await new HttpClient().GetAsync($@"https://conesoft.net/notify" + query.ToQueryString());
}

record Show(int Id, string Name, string[][] Episodes);

internal record EpisodeEntry(int EpisodeId, int SeriesId, int Season, int Episode);
