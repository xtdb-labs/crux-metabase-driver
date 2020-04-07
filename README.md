# Sample Metabase Driver: Dremio

:warning: [Some things doesn't work !](TODO.md)

![screenshot](screenshots/dremio-driver.png)

All you need you do is drop the driver in your `plugins/` directory. You can grab it [here](https://github.com/metabase/dremio-driver/releases/download/1.0.0/dremio.metabase-driver.jar) or build it yourself:

## Building the driver 

### Prereq: Install Metabase as a local maven dependency, compiled for building drivers

Clone the [Metabase repo](https://github.com/metabase/metabase) first if you haven't already done so.

```bash
cd /path/to/metabase_source
lein install-for-building-drivers
```

### Prereq: Install dremio driver as a local maven dependency

Grab [lein-localrepo](https://github.com/kumarshantanu/lein-localrepo) first if you haven't already done so.

```bash
lein localrepo install lib/dremio-jdbc-driver-4.1.7.jar com.dremio/dremio 4.1.7
```

### Build the Dremio driver

```bash
# (In the Dremio driver directory)
lein clean
DEBUG=1 LEIN_SNAPSHOTS_IN_RELEASE=true lein uberjar
```

### Copy it to your plugins dir and restart Metabase
```bash
mkdir -p /path/to/metabase/plugins/
cp target/uberjar/dremio.metabase-driver.jar /path/to/metabase/plugins/
jar -jar /path/to/metabase/metabase.jar
```

*or:*

```bash
mkdir -p /path/to/metabase_source/plugins
cp target/uberjar/dremio.metabase-driver.jar /path/to/metabase_source/plugins/
cd /path/to/metabase_source
lein run
```
