using Microsoft.AspNetCore.Http.Extensions;

namespace Conesoft.Services.PollImdbShows.Helpers;

public class Notification(string conesoftSecret)
{
    public async Task Notify(string message)
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
}
