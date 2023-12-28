# stream-loader errors

## senzing-50010404W

1. Warning
    1. SENZING_DATA_SOURCE not set.
1. Background
    1. `stream-loader.py` uses `SENZING_DATA_SOURCE` value to add a `DATA_SOURCE` JSON key/value to JSON lines  without the `DATA_SOURCE` key.
1. Documentation
    1. [Configuration](../README.md#configuration)

## senzing-50010406E

1. Error
    1. Cannot find G2Project.ini.
1. Problem
    1. `stream-loader.py` needs the contents of `G2Project.ini`, but the file cannot be found.
1. Solution
    1. Place the `G2Project.ini` file in a location that can be found by [stream-loader.py](../stream-loader.py)'s `get_g2project_ini_filename(...)` function.
    Example: /opt/senzing/g2/python/G2Project.ini

## senzing-50010407E

1. Error
    1. G2Engine licensing error.  Error: {0}
1. Problem
    1. The Senzing license is not valid for the workload.  It may be expired. The license file is `${SENZING_DIR}/g2/data/g2.lic`.  If `g2.lic` is not at that location, the free, limited license is being used.
1. Solution
    1. If using a free, limited license, download and extract a new copy of [Senzing_API.tgz](https://s3.amazonaws.com/public-read-access/SenzingComDownloads/Senzing_API.tgz).
    1. If using a production license, contact [Senzing](http://senzing.com) to obtain a new license to be placed at `${SENZING_DIR}/g2/data/g2.lic`.

## senzing-50010414E

1. Error
    1. LD_LIBRARY_PATH environment variable not set.
1. Problem
    1. `stream-loader.py` needs `LD_LIBRARY_PATH` environment variable to find shared libraries.
1. Solution
    1. Debian: See [Set Environment variables](debian-based-installation.md#set-environment-variables)
    1. RPM: See [Set Environment variables](rpm-based-installation.md#set-environment-variables)

## senzing-50010415E

1. Error
    1. PYTHONPATH environment variable not set.
1. Problem
    1. `stream-loader.py` needs `PYTHONPATH` environment variable to find shared python libraries.
1. Solution
    1. Debian: See [Set Environment variables](debian-based-installation.md#set-environment-variables)
    1. RPM: See [Set Environment variables](rpm-based-installation.md#set-environment-variables)

## senzing-50010567E

1. Error
    1. When using a PostgreSQL database, `senzing_governor.py` is not found anywhere in `PYTHONPATH`.
1. Problem
    1. The `senzing_governor.py` script was not downloaded from [Senzing/governor-postgresql-transaction-id](https://github.com/senzing-garage/governor-postgresql-transaction-id) and placed somewhere in the `PYTHONPATH`.
    1. This is done automatically by [init-container.py](https://github.com/senzing-garage/docker-init-container).
    1. In an air-gapped environment, the downloading of `senzing_governor.py` is blocked.
1. Solution
    1. Download `senzing_governor.py` locally.
       Example:

        ```console
        curl -X GET \
            --output /path/on/local/system/senzing_governor.py \
             https://raw.githubusercontent.com/Senzing/governor-postgresql-transaction-id/main/senzing_governor.py
        ```

    1. Copy the downloaded `senzing_governor.py` file to a directory listed in the `PYTHONPATH` environment variable on the machine running Senzing.
