# Dotnet global tool for monitoring event hubs

Very simple dotnet global tool that dumps messages coming from [Azure event hubs](https://azure.microsoft.com/en-gb/services/event-hubs/)

# Installation

```
dotnet install tool -g dotnet-watch-event-hubs
```

# Usage

```
Dump messages from event hubs

Usage: watch-event-hubs [arguments] [options]

Arguments:
  ConnectionString  The connection string for the event hub

Options:
  -f|--from-start   Get events from the start
  -?|-h|--help      Show help information
  ```

  Events from event hubs are then just dumped out as standard strings. Probably not a good idea for high volume event hubs, but a great development tool to see what is going on.
  

