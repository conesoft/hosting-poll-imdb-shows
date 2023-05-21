using Conesoft.Files;
using Humanizer;
using System.Diagnostics;
using System.IO.Compression;

var configuration = new ConfigurationBuilder().AddJsonFile(Conesoft.Hosting.Host.GlobalSettings.Path).Build();
var conesoftSecret = configuration["conesoft:secret"] ?? throw new Exception("Conesoft Secret not found in Configuration");

var client = new HttpClient();
Stopwatch stopwatch = new Stopwatch();

var timer = new PeriodicTimer(TimeSpan.FromHours(1));

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

        if (DateTime.UtcNow - s.Local.Info.CreationTimeUtc > TimeSpan.FromDays(1))
        {
            s.Local.Delete();
            s.LocalZipped.Delete();
        }
        if (s.Local.Exists == false && s.LocalZipped.Exists == false)
        {
            await s.LocalZipped.WriteBytes(await client.GetByteArrayAsync(s.Web));
        }
        if (s.Local.Exists == false)
        {
            using var zipped = s.LocalZipped.OpenRead();
            using var unzipped = System.IO.File.Create(s.Local.Path);
            using var unzip = new GZipStream(zipped, CompressionMode.Decompress);
            unzip.CopyTo(unzipped);
        }
    }));
    TimeStamp("downloaded and unzipped");

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

    stopwatch.Stop();
}
while (await timer.WaitForNextTickAsync());

void TimeStamp(string description)
{
    if(stopwatch.IsRunning == false)
    {
        Console.WriteLine(description);
    }
    else
    {
        Console.WriteLine($"{description} took {stopwatch.Elapsed.Humanize()}");
    }
    stopwatch.Restart();
}

//async Task Notify(Entry entry, Conesoft.Files.File? image)
//{
//    var title = entry.Name;
//    var message = $"from: {entry.Feed}";
//    var url = entry.Url;
//    var imageUrl = image != null ? $"https://kontrol.conesoft.net/content/feeds/thumbnail/{image.Name}" : "";

//    var query = new QueryBuilder
//    {
//        { "token", conesoftSecret },
//        { "title", title },
//        { "message", message },
//        { "url", url }
//    };
//    if (image != null)
//    {
//        query.Add("imageUrl", imageUrl);
//    }

//    await new HttpClient().GetAsync($@"https://conesoft.net/notify" + query.ToQueryString());
//}

internal record EpisodeEntry(int EpisodeId, int SeriesId, int Season, int Episode);
