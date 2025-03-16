using Conesoft.Hosting;
using Conesoft.Notifications;
using Conesoft.Services.PollImdbShows;

var builder = Host.CreateApplicationBuilder(args);

builder
    .AddHostConfigurationFiles()
    .AddHostEnvironmentInfo()
    .AddLoggingService()
    .AddNotificationService()
    ;

builder.Services
    .AddHttpClient()
    .AddHostedService<Service>()
    ;

var host = builder.Build();
await host.RunAsync();