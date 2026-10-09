# Postmortem: building FlowSentinel Lakehouse

I built this to show what I can do as a data engineer, on top of FlowSentinel, my Rust network
sensor. The plan sounded simple: take the flows the sensor produces, put them through a proper
data platform, and end up with detections you can trust. Then I wanted other people to be able
to run it on their own networks, which turned out to be a second project of its own.

This is the honest list of what broke along the way, why, and what I changed. Some of it is
embarrassing. All of it taught me something.

## Part 1: the lakehouse

### The incremental model that silently loaded nothing

`fct_flows` is incremental: each run only reads the bronze rows loaded after the newest row it
already has, minus a lookback. If the very first run happens on an empty lake, dbt creates an
empty table. From then on every run is incremental, `max(loaded_at)` over that empty table is
`NULL`, and `loaded_at > NULL - interval 30 minutes` is never true. So every later run loaded
nothing, forever. Nothing errored either, because an empty table breaks no uniqueness test.

Fix: `coalesce` the watermark, so an empty table means "read everything". Then a test that
builds an empty lake first and checks that real data flows through afterwards, and a check
that the incremental-equals-full-refresh test really fails when I put the bug back. That
"break it on purpose" habit stayed with me for the rest of the project.

**Lesson:** a pipeline that runs green on an empty table is not proven. Assert on counts.

### dbt and my own code fighting over one DuckDB file

I first ran dbt in-process, from the same Python process that had the warehouse open. DuckDB
allows one writer per file, so dbt and my own connection got in each other's way. I moved dbt
into a child process (`python -c "from dbt.cli.main import cli; cli()"`), so it always owns
the file alone while it runs.

### dbt-duckdb and Hive partitions

Writing partitioned Parquet with `external_location` failed with a `KeyError` that pointed
nowhere useful. The cause was an empty `hive_types {}` option in the generated SQL. Dropping it
fixed it. Small thing, took me far longer than it should have.

### Failures that were remembered forever

The ledger remembers every batch, so a re-run skips what is done. The question is what it
should remember when something goes wrong. A broken capture should be recorded once and left
alone. A missing FlowSentinel binary or a timeout should not: the capture is fine, and the
next run should try again.

So I split it in two. **Rejected** means the input itself is bad (FlowSentinel refuses the
capture, the JSON is not a flow document). It is recorded and never retried. **Failed** means
something around it went wrong (binary missing, timeout, disk). It is not recorded, so the next
run tries again. I came back to this distinction several more times while packaging, which
tells me it was the right one.

### One NTP packet moved my whole week

The generator produces a week of office traffic with four injected attacks and a ground truth
file, so CI can score precision and recall. Part of that traffic is a domain controller syncing
its clock every 1024 seconds, with a few seconds of random jitter to look real. The jitter went
both ways, so the very first sync could land a few seconds *before* the week started, and the
dashboard said the data began a day early. One packet, wrong headline. The jitter now only
goes forward.

In the same review I found that the dashboard's uploaded-bytes figure double-counted flow
direction. Both bugs were in code I had "finished". Reading your own output with fresh eyes
finds things tests don't.

### Dagster and `from __future__ import annotations`

Almost every module in the project starts with `from __future__ import annotations`. In the
Dagster module that broke things, because Dagster reads type hints at runtime to wire up
resources and config, and the import turns them into strings. That module now goes without
it, with a comment saying why.

Two more Dagster surprises: dagster-dbt prefixes asset keys with the schema (`gold/fct_alerts`,
not `fct_alerts`), so my dependencies pointed at assets that did not exist until I used
`get_asset_key_for_model`. And `dagster dev` could not find the dbt executable or a manifest
to load, so the code now looks for dbt next to the running Python, and parses the project
itself into a temporary directory before swapping the manifest in atomically.

### The bugs in my tests

When I added pcapng support, I wrote a test helper that turns a pcap into pcapng. The helper
read the snapshot length and the link type in the wrong order, so my converter "passed" against
a broken fixture. The test that runs the real FlowSentinel CLI on the converted file caught
it. Then I found that my converter read block types as little-endian even in
big-endian sections, and that my "corrupt length" test was corrupting the wrong bytes.

**Lesson:** test helpers are code too, and the most valuable test is the one that runs the real
consumer of your output.

### A small one that bit me twice

`FLOWLAKE_KAFKA_BOOTSTRAP=` (set, but empty) counted as "a broker is configured", so the Kafka
tests tried to connect to an empty address instead of skipping. Empty now means unset.

And one I'm a bit ashamed of: while cleaning up stray processes I ran `pkill -f` with a pattern
that also matched the shell running the command. It killed itself. Now I select processes by
their command name.

## Part 2: making it something people can actually run

Once the lakehouse worked, I wanted anyone to be able to run it: pick a machine, run one
command, drop captures in a folder. That is where I learned the most.

### Real captures are not like my test data

* Most capture tools write **pcapng** by default, and FlowSentinel's CLI reads classic pcap. I
  did not want to make every user install Wireshark's `editcap`, so I wrote a pure-Python
  converter: both byte orders, any timestamp resolution and offset, one output per interface.
* FlowSentinel's CLI stops at **a million packets** per run, which a busy network fills in
  minutes. Big captures are now split into parts and ingested as one source.
* When FlowSentinel hits a limit, its output is **cut short**. The lakehouse now records that,
  warns about it in a data test, and shows it on the dashboard instead of pretending the
  capture was complete.

### Every network is different

My first version had the demo's office subnets hardcoded as seeds. Useless for anyone else. Now
zones, an allowlist and thresholds come from a config directory, validated before anything
runs. If you write `10.0.0.1/8` instead of `10.0.0.0/8`, you get the file, the line and the
reason, not a dashboard where all your internal traffic is "outbound".

That broke my dbt unit tests the first time a config changed a threshold, because they used
the project's thresholds. They now pin their own, so tuning a deployment can't break them.

### Building the images

* Docker Hub **rate-limited** my image pulls halfway through, so I pulled the base images
  through a mirror.
* My build machine sat behind a proxy that blocked Debian's package mirrors, so `apt-get`
  failed. Instead of fighting it I removed `apt` from the Dockerfile completely. BuildKit can
  fetch a git repository by itself (`ADD https://...git#<commit>`), so the build needs neither
  git nor a package manager. Fewer moving parts, so the restriction did me a favor.
* One build "passed" that had actually failed, because I was reading the exit status of a
  timing wrapper instead of the build. Now I check the real exit code every time.
* Changing one line of Python reinstalled every dependency, because the code was copied before
  the install. The Dockerfile now installs the locked dependencies first and the project last,
  so a code change rebuilds in seconds.

### The dashboard that said "Welcome to nginx!"

First full run of the suite: the dashboard showed nginx's welcome page, and the pipeline could
not write the real one. When Docker mounts an empty named volume, it copies in whatever the
image has at that path. nginx started first, so its root-owned welcome page landed in the
shared volume, and my pipeline, running as an unprivileged user, could not replace it.

Fix: the dashboard waits for the pipeline, the nginx mount never copies anything in, and the
pipeline's entrypoint checks that its directories are writable and says clearly what to do if
they are not.

### The run that blocked everything forever

This one could have been really bad in production. Runs execute inside the Dagster daemon, one at
a time, because DuckDB has one writer. I restarted the daemon in the middle of a run, as anyone
would after changing the config, and the run stayed "started" forever. The next run sat in the
queue behind it, forever. Nothing errored and nothing logged. The pipeline just stopped.

Dagster's own run monitoring can't detect a dead worker with the default launcher. But in this
setup every run is a child of the daemon, so when the daemon starts, nothing can still be
running. It now marks those runs as failed on startup. I reproduced the hang, deployed the fix,
and watched the queued run go through.

### One unreadable file stopped all ingestion

A capture copied in with permissions `600` made ingestion crash with a traceback, and every run
after that crashed on the same file. One bad file held up every good one. Now an unreadable
file fails on its own, the rest is ingested, and it is tried again on the next run once the
permissions are fixed. The same review caught a sneakier case: an I/O error while preparing a
capture (a full disk, say) was being recorded as "rejected" forever. It is now a retry.

### The sensor that missed rsync'd files

The inbox sensor started a run when it saw a file newer than the newest one it had seen. But
`rsync -a` and `cp -p` keep the original timestamps, so a capture from yesterday copied in
today was invisible to it. The sensor now fingerprints names, sizes and times instead.

### My own secrets almost went into the package

This is the one I'm most glad I caught. Building the source distribution to test the release, I
listed its contents and found `deploy/.env` with the database password and the admin password
file. They were in `deploy/.gitignore`, so git never saw them, but the build tool only reads the
root `.gitignore`. Anyone who built a release on a machine where they had run the suite would
have shipped their own passwords.

The release workflow builds from a clean checkout, so no published release ever had them. But I
don't want to rely on that. The sdist now uses an explicit list of what goes in, and the root
`.gitignore` covers the deploy secrets too.

**Lesson:** look inside your artifacts. Every time.

### Smaller things

* RAM-backed `/tmp` would have held whole pcapng conversions in memory. Scratch files now go on
  the data volume.
* The incremental `fct_flows` keeps the direction each flow had when it was loaded, so changing
  zones needs a full refresh. That is now a documented one-line command, and the docs say when
  you need it.
* I wrote the backup and restore steps, then tested them: back up, delete every volume,
  restore, sign in, check the alerts. The first draft forgot that the dashboard is not in the
  backup, so the restored suite showed "Waiting for data" until the next run.

## What went well

* **The contract at the door.** Validating every record before it touches the lake made
  everything downstream easier to reason about.
* **Testing against the real producer.** CI builds FlowSentinel at a pinned commit and checks
  its output field for field. It catches what my own fixtures can't.
* **Breaking things on purpose.** Mutation-checking the incremental test, restarting the daemon
  mid-run, deleting the volumes before a restore. Every one of those found something.
* **One end-to-end smoke test** that does what a user does: start the suite, drop files in,
  wait for alerts. If it passes, the product works.

## What I'd do differently

* Write the deployment docs earlier. Several of the packaging bugs above surfaced because
  writing "how to do X" made me actually do X.
* Treat "failed" and "rejected" as different things from day one.
* Run the whole suite from scratch sooner. A setup you have been using for weeks hides the
  problems a fresh one shows in minutes.

## Still open

* No automatic retention yet; old data has to be deleted by hand.
* The Dagster UI has no sign-in. The docs say how to put it behind a tunnel or a proxy, but it
  should be in the box.
* The DNS tunneling rule uses the last two labels as the parent domain, which is wrong for
  suffixes like `co.uk`. It needs the Public Suffix List.

If you run into something I haven't, open an issue. I'd like to hear about it.
