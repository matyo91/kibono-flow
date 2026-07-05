# Kibono Flow — Profile Demo

Observable **product sync consumer** using [darkwood/flow](https://flow.darkwood.com) as a container for a [php-etl/pipeline](https://php-etl.github.io/documentation/) (Kiboko) ETL pipeline.

## Run the consumer

```bash
# Process one message and exit (quick test)
bin/console app:flow:profile-demo --limit=1 -vv

# Long-running worker (default)
bin/console app:flow:profile-demo -vv
```

## Pipeline steps

Each queue message triggers a Flow with four visible stages:

1. **extract** — read product rows from the message
2. **transform** — normalize SKU, uppercase, `str_rot13` (CPU hotspot)
3. **load** — accumulate and write `var/output/products-*.json`
4. **walk** — iterate pipeline results and log summary

## Profiling with Blackfire

This demo follows the [JoliCode consumer profiling pattern](https://jolicode.com/blog/profiler-un-consumer-avec-blackfire): use POSIX signals on a long-running Symfony command.

### Prerequisites

```bash
composer require --dev blackfire/php-sdk
```

Set credentials in `.env.local`:

```
BLACKFIRE_CLIENT_ID=your-client-id
BLACKFIRE_CLIENT_TOKEN=your-client-token
```

### Profile a running worker

```bash
bin/console app:flow:profile-demo -vv
```

In another terminal (or after `Ctrl+Z` → `bg`):

```bash
ps aux | grep profile-demo
kill -USR2 <pid>   # start profiling
# let a few iterations run…
kill -USR2 <pid>   # stop profiling → URL appears in logs
```

### What to look for in Blackfire

- Time split across **extract** / **transform** / **load** generator yields
- `str_rot13` and `array_map` hotspots in the transform step
- File I/O in the load/walk steps (`file_put_contents`)
- Memory growth across batch sizes
