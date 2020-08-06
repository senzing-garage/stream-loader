#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# stream-loader.py Loader for streaming input.
# -----------------------------------------------------------------------------

from glob import glob
from urllib.parse import urlparse, urlunparse
from urllib.request import urlopen
import argparse
import boto3
import configparser
import confluent_kafka
import datetime
import json
import linecache
import logging
import math
import multiprocessing
import os
import pika
import queue
import signal
import string
import sys
import threading
import time

# Import Senzing libraries.

try:
    from G2Config import G2Config
    from G2ConfigMgr import G2ConfigMgr
    from G2Diagnostic import G2Diagnostic
    from G2Engine import G2Engine
    from G2Product import G2Product
    import G2Exception
except ImportError:
    pass

__all__ = []
__version__ = "1.5.6"  # See https://www.python.org/dev/peps/pep-0396/
__date__ = '2018-10-29'
__updated__ = '2020-08-06'

SENZING_PRODUCT_ID = "5001"  # See https://github.com/Senzing/knowledge-base/blob/master/lists/senzing-product-ids.md
log_format = '%(asctime)s %(message)s'

# Working with bytes.

KILOBYTES = 1024
MEGABYTES = 1024 * KILOBYTES
GIGABYTES = 1024 * MEGABYTES

MINIMUM_TOTAL_MEMORY_IN_GIGABYTES = 8
MINIMUM_AVAILABLE_MEMORY_IN_GIGABYTES = 6

# Lists from https://www.ietf.org/rfc/rfc1738.txt

safe_character_list = ['$', '-', '_', '.', '+', '!', '*', '(', ')', ',', '"' ] + list(string.ascii_letters)
unsafe_character_list = [ '"', '<', '>', '#', '%', '{', '}', '|', '\\', '^', '~', '[', ']', '`']
reserved_character_list = [ ';', ',', '/', '?', ':', '@', '=', '&']

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

configuration_locator = {
    "config_path": {
        "default": "/etc/opt/senzing",
        "env": "SENZING_CONFIG_PATH",
        "cli": "config-path"
    },
    "configuration_check_frequency_in_seconds": {
        "default": 60,
        "env": "SENZING_CONFIGURATION_CHECK_FREQUENCY",
        "cli": "configuration-check-frequency"
    },
    "data_source": {
        "default": None,
        "env": "SENZING_DATA_SOURCE",
        "cli": "data-source"
    },
    "debug": {
        "default": False,
        "env": "SENZING_DEBUG",
        "cli": "debug"
    },
    "delay_in_seconds": {
        "default": 0,
        "env": "SENZING_DELAY_IN_SECONDS",
        "cli": "delay-in-seconds"
    },
    "engine_configuration_json": {
        "default": None,
        "env": "SENZING_ENGINE_CONFIGURATION_JSON",
        "cli": "engine-configuration-json"
    },
    "entity_type": {
        "default": None,
        "env": "SENZING_ENTITY_TYPE",
        "cli": "entity-type"
    },
    "expiration_warning_in_days": {
        "default": 30,
        "env": "SENZING_EXPIRATION_WARNING_IN_DAYS",
        "cli": "expiration-warning-in-days"
    },
    "g2_configuration_file": {
        "default": "/opt/senzing/g2/python/g2config.json",
        "env": "SENZING_G2_CONFIGURATION_FILE",
        "cli": "g2-configuration-file"
    },
    "g2_database_url_generic": {
        "default": "sqlite3://na:na@/var/opt/senzing/sqlite/G2C.db",
        "env": "SENZING_DATABASE_URL",
        "cli": "database-url"
    },
    "input_url": {
        "default": None,
        "env": "SENZING_INPUT_URL",
        "cli": "input-url"
    },
    "kafka_bootstrap_server": {
        "default": "localhost:9092",
        "env": "SENZING_KAFKA_BOOTSTRAP_SERVER",
        "cli": "kafka-bootstrap-server",
    },
    "kafka_failure_bootstrap_server": {
        "default": None,
        "env": "SENZING_KAFKA_FAILURE_BOOTSTRAP_SERVER",
        "cli": "kafka-failure-bootstrap-server",
    },
    "kafka_failure_topic": {
        "default": "senzing-kafka-failure-topic",
        "env": "SENZING_KAFKA_FAILURE_TOPIC",
        "cli": "kafka-failure-topic"
    },
    "kafka_group": {
        "default": "senzing-kafka-group",
        "env": "SENZING_KAFKA_GROUP",
        "cli": "kafka-group"
    },
    "kafka_info_bootstrap_server": {
        "default": None,
        "env": "SENZING_KAFKA_INFO_BOOTSTRAP_SERVER",
        "cli": "kafka-info-bootstrap-server",
    },
    "kafka_info_topic": {
        "default": "senzing-kafka-info-topic",
        "env": "SENZING_KAFKA_INFO_TOPIC",
        "cli": "kafka-info-topic"
    },
    "kafka_topic": {
        "default": "senzing-kafka-topic",
        "env": "SENZING_KAFKA_TOPIC",
        "cli": "kafka-topic"
    },
    "ld_library_path": {
        "env": "LD_LIBRARY_PATH"
    },
    "log_license_period_in_seconds": {
        "default": 60 * 60 * 24,
        "env": "SENZING_LOG_LICENSE_PERIOD_IN_SECONDS",
        "cli": "log-license-period-in-seconds"
    },
    "monitoring_period_in_seconds": {
        "default": 60 * 10,
        "env": "SENZING_MONITORING_PERIOD_IN_SECONDS",
        "cli": "monitoring-period-in-seconds",
    },
    "processes": {
        "default": 1,
        "env": "SENZING_PROCESSES",
        "cli": "processes",
    },
    "python_path": {
        "env": "PYTHONPATH"
    },
    "queue_maxsize": {
        "default": 10,
        "env": "SENZING_QUEUE_MAX",
    },
    "rabbitmq_failure_host": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_HOST",
        "cli": "rabbitmq-failure-host",
    },
    "rabbitmq_failure_password": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_PASSWORD",
        "cli": "rabbitmq-failure-password",
    },
    "rabbitmq_failure_queue": {
        "default": "senzing-rabbitmq-failure-queue",
        "env": "SENZING_RABBITMQ_FAILURE_QUEUE",
        "cli": "rabbitmq-failure-queue",
    },
    "rabbitmq_failure_username": {
        "default": None,
        "env": "SENZING_RABBITMQ_FAILURE_USERNAME",
        "cli": "rabbitmq-failure-username",
    },
    "rabbitmq_host": {
        "default": "localhost:5672",
        "env": "SENZING_RABBITMQ_HOST",
        "cli": "rabbitmq-host",
    },
    "rabbitmq_info_host": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_HOST",
        "cli": "rabbitmq-info-host",
    },
    "rabbitmq_info_password": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_PASSWORD",
        "cli": "rabbitmq-info-password",
    },
    "rabbitmq_info_queue": {
        "default": "senzing-rabbitmq-info-queue",
        "env": "SENZING_RABBITMQ_INFO_QUEUE",
        "cli": "rabbitmq-info-queue",
    },
    "rabbitmq_info_username": {
        "default": None,
        "env": "SENZING_RABBITMQ_INFO_USERNAME",
        "cli": "rabbitmq-info-username",
    },
    "rabbitmq_password": {
        "default": "bitnami",
        "env": "SENZING_RABBITMQ_PASSWORD",
        "cli": "rabbitmq-password",
    },
    "rabbitmq_prefetch_count": {
        "default": 50,
        "env": "SENZING_RABBITMQ_PREFETCH_COUNT",
        "cli": "rabbitmq-prefetch-count",
    },
    "rabbitmq_queue": {
        "default": "senzing-rabbitmq-queue",
        "env": "SENZING_RABBITMQ_QUEUE",
        "cli": "rabbitmq-queue",
    },
    "rabbitmq_username": {
        "default": "user",
        "env": "SENZING_RABBITMQ_USERNAME",
        "cli": "rabbitmq-username",
    },
    "resource_path": {
        "default": "/opt/senzing/g2/resources",
        "env": "SENZING_RESOURCE_PATH",
        "cli": "resource-path"
    },
    "skip_database_performance_test": {
        "default": False,
        "env": "SENZING_SKIP_DATABASE_PERFORMANCE_TEST",
        "cli": "skip-database-performance-test"
    },
    "sleep_time_in_seconds": {
        "default": 0,
        "env": "SENZING_SLEEP_TIME_IN_SECONDS",
        "cli": "sleep-time-in-seconds"
    },
    "sqs_failure_queue_url": {
        "default": None,
        "env": "SENZING_SQS_FAILURE_QUEUE_URL",
        "cli": "sqs-failure-queue-url"
    },
    "sqs_info_queue_url": {
        "default": None,
        "env": "SENZING_SQS_INFO_QUEUE_URL",
        "cli": "sqs-info-queue-url"
    },
    "sqs_queue_url": {
        "default": None,
        "env": "SENZING_SQS_QUEUE_URL",
        "cli": "sqs-queue-url"
    },
    "subcommand": {
        "default": None,
        "env": "SENZING_SUBCOMMAND",
    },
    "support_path": {
        "default": "/opt/senzing/data",
        "env": "SENZING_SUPPORT_PATH",
        "cli": "support-path"
    },
    "threads_per_process": {
        "default": 4,
        "env": "SENZING_THREADS_PER_PROCESS",
        "cli": "threads-per-process",
    }
}

# Enumerate keys in 'configuration_locator' that should not be printed to the log.

keys_to_redact = [
    "counter_bad_records",
    "counter_processed_records",
    "counter_queued_records",
    "g2_database_url_generic",
    "g2_database_url_specific",
    "kafka_ack_elapsed",
    "kafka_poll_elapsed",
    "rabbitmq_ack_elapsed",
    "rabbitmq_failure_password",
    "rabbitmq_info_password",
    "rabbitmq_password",
    "rabbitmq_poll_elapsed",
]

# -----------------------------------------------------------------------------
# Define argument parser
# -----------------------------------------------------------------------------


def get_parser():
    ''' Parse commandline arguments. '''

    subcommands = {
        'kafka': {
            "help": 'Read JSON Lines from Apache Kafka topic.',
            "argument_aspects": ["common", "kafka_base"],
        },
        'kafka-withinfo': {
            "help": 'Read JSON Lines from Apache Kafka topic. Return info to a queue.',
            "argument_aspects": ["common", "kafka_base"],
            "arguments": {
                "--kafka-failure-bootstrap-server": {
                    "dest": "kafka_failure_bootstrap_server",
                    "metavar": "SENZING_KAFKA_FAILURE_BOOTSTRAP_SERVER",
                    "help": "Kafka bootstrap server. Default: SENZING_KAFKA_BOOTSTRAP_SERVER"
                },
                "--kafka-failure-topic": {
                    "dest": "kafka_failure_topic",
                    "metavar": "SENZING_KAFKA_FAILURE_TOPIC",
                    "help": "Kafka topic for failures. Default: senzing-kafka-failure-topic"
                },
                "--kafka-info-bootstrap-server": {
                    "dest": "kafka_info_bootstrap_server",
                    "metavar": "SENZING_KAFKA_INFO_BOOTSTRAP_SERVER",
                    "help": "Kafka bootstrap server. Default: SENZING_KAFKA_BOOTSTRAP_SERVER"
                },
                "--kafka-info-topic": {
                    "dest": "kafka_info_topic",
                    "metavar": "SENZING_KAFKA_INFO_TOPIC",
                    "help": "Kafka topic for info. Default: senzing-kafka-info-topic"
                },
            },
        },
        'rabbitmq': {
            "help": 'Read JSON Lines from RabbitMQ queue.',
            "argument_aspects": ["common", "rabbitmq_base"],
        },
        'rabbitmq-withinfo': {
            "help": 'Read JSON Lines from RabbitMQ queue. Return info to a queue.',
            "argument_aspects": ["common", "rabbitmq_base"],
            "arguments": {
                "--rabbitmq-info-host": {
                    "dest": "rabbitmq_info_host",
                    "metavar": "SENZING_RABBITMQ_INFO_HOST",
                    "help": "RabbitMQ host. Default: SENZING_RABBITMQ_HOST"
                },
                "--rabbitmq-info-password": {
                    "dest": "rabbitmq_info_password",
                    "metavar": "SENZING_RABBITMQ_INFO_PASSWORD",
                    "help": "RabbitMQ password. Default: SENZING_RABBITMQ_PASSWORD"
                },
                "--rabbitmq-info-queue": {
                    "dest": "rabbitmq_info_queue",
                    "metavar": "SENZING_RABBITMQ_INFO_QUEUE",
                    "help": "RabbitMQ queue for info. Default: senzing-rabbitmq-info-queue"
                },
                "--rabbitmq-info-username": {
                    "dest": "rabbitmq_info_username",
                    "metavar": "SENZING_RABBITMQ_INFO_USERNAME",
                    "help": "RabbitMQ username. Default: SENZING_RABBITMQ_USERNAME"
                },
                "--rabbitmq-failure-host": {
                    "dest": "rabbitmq_failure_host",
                    "metavar": "SENZING_RABBITMQ_FAILURE_HOST",
                    "help": "RabbitMQ host. Default: SENZING_RABBITMQ_HOST"
                },
                "--rabbitmq-failure-password": {
                    "dest": "rabbitmq_failure_password",
                    "metavar": "SENZING_RABBITMQ_FAILURE_PASSWORD",
                    "help": "RabbitMQ password. Default: SENZING_RABBITMQ_PASSWORD"
                },
                "--rabbitmq-failure-queue": {
                    "dest": "rabbitmq_failure_queue",
                    "metavar": "SENZING_RABBITMQ_FAILURE_QUEUE",
                    "help": "RabbitMQ queue for failures. Default: senzing-rabbitmq-failure-queue"
                },
                "--rabbitmq-failure-username": {
                    "dest": "rabbitmq_failure_username",
                    "metavar": "SENZING_RABBITMQ_FAILURE_USERNAME",
                    "help": "RabbitMQ username. Default: SENZING_RABBITMQ_USERNAME"
                },
            },
        },
        'sleep': {
            "help": 'Do nothing but sleep. For Docker testing.',
            "arguments": {
                "--sleep-time-in-seconds": {
                    "dest": "sleep_time_in_seconds",
                    "metavar": "SENZING_SLEEP_TIME_IN_SECONDS",
                    "help": "Sleep time in seconds. DEFAULT: 0 (infinite)"
                },
            },
        },
        'sqs': {
            "help": 'Read JSON Lines from AWS SQS queue.',
            "argument_aspects": ["common", "sqs_base"],
        },
        'sqs-withinfo': {
            "help": 'Read JSON Lines from AWS SQS queue.  Return info to a queue.',
            "argument_aspects": ["common", "sqs_base"],
            "arguments": {
                "--sqs-failure-queue-url": {
                    "dest": "sqs_failure_queue_url",
                    "metavar": "SENZING_SQS_FAILURE_QUEUE_URL",
                    "help": "AWS SQS URL for failures. Default: none"
                },
                "--sqs-info-queue-url": {
                    "dest": "sqs_info_queue_url",
                    "metavar": "SENZING_SQS_INFO_QUEUE_URL",
                    "help": "AWS SQS URL for info. Default: none"
                },
            },
        },
        'url': {
            "help": 'Read JSON Lines from URL-addressable file.',
            "argument_aspects": ["common"],
            "arguments": {
                "-input-url": {
                    "dest": "input_url",
                    "metavar": "SENZING_INPUT_URL",
                    "help": "URL to file of JSON lines."
                },
            },
        },
        'version': {
            "help": 'Print version of program.',
        },
        'docker-acceptance-test': {
            "help": 'For Docker acceptance testing.',
        },
    }

    # Define argument_aspects.

    argument_aspects = {
        "common": {
            "--data-source": {
                "dest": "data_source",
                "metavar": "SENZING_DATA_SOURCE",
                "help": "Data Source."
            },
            "--delay-in-seconds": {
                "dest": "delay_in_seconds",
                "metavar": "SENZING_DELAY_IN_SECONDS",
                "help": "Delay before processing in seconds. DEFAULT: 0"
            },
            "--debug": {
                "dest": "debug",
                "action": "store_true",
                "help": "Enable debugging. (SENZING_DEBUG) Default: False"
            },
            "--engine-configuration-json": {
                "dest": "engine_configuration_json",
                "metavar": "SENZING_ENGINE_CONFIGURATION_JSON",
                "help": "Advanced Senzing engine configuration. Default: none"
            },
            "--entity-type": {
                "dest": "entity_type",
                "metavar": "SENZING_ENTITY_TYPE",
                "help": "Entity type."
            },
            "--monitoring-period-in-seconds": {
                "dest": "monitoring_period_in_seconds",
                "metavar": "SENZING_MONITORING_PERIOD_IN_SECONDS",
                "help": "Period, in seconds, between monitoring reports. Default: 600"
            },
            "--threads-per-process": {
                "dest": "threads_per_process",
                "metavar": "SENZING_THREADS_PER_PROCESS",
                "help": "Number of threads per process. Default: 4"
            },
        },
        "kafka_base": {
          "--kafka-bootstrap-server": {
                "dest": "kafka_bootstrap_server",
                "metavar": "SENZING_KAFKA_BOOTSTRAP_SERVER",
                "help": "Kafka bootstrap server. Default: localhost:9092"
            },
            "--kafka-group": {
                "dest": "kafka_group",
                "metavar": "SENZING_KAFKA_GROUP",
                "help": "Kafka group. Default: senzing-kafka-group"
            },
            "--kafka-topic": {
                "dest": "kafka_topic",
                "metavar": "SENZING_KAFKA_TOPIC",
                "help": "Kafka topic. Default: senzing-kafka-topic"
            },
        },
        "rabbitmq_base": {
            "--rabbitmq-host": {
                "dest": "rabbitmq_host",
                "metavar": "SENZING_RABBITMQ_HOST",
                "help": "RabbitMQ host. Default: localhost:5672"
            },
            "--rabbitmq-password": {
                "dest": "rabbitmq_password",
                "metavar": "SENZING_RABBITMQ_PASSWORD",
                "help": "RabbitMQ password. Default: bitnami"
            },
            "--rabbitmq-queue": {
                "dest": "rabbitmq_queue",
                "metavar": "SENZING_RABBITMQ_QUEUE",
                "help": "RabbitMQ queue. Default: senzing-rabbitmq-queue"
            },
            "--rabbitmq-username": {
                "dest": "rabbitmq_username",
                "metavar": "SENZING_RABBITMQ_USERNAME",
                "help": "RabbitMQ username. Default: user"
            },
            "--rabbitmq-prefetch-count": {
                "dest": "rabbitmq_prefetch_count",
                "metavar": "SENZING_RABBITMQ_PREFETCH_COUNT",
                "help": "RabbitMQ prefetch-count. Default: 50"
            }
        },
        "sqs_base": {
            "--sqs-queue-url": {
                "dest": "sqs_queue_url",
                "metavar": "SENZING_SQS_QUEUE_URL",
                "help": "AWS SQS URL. Default: none"
            },
        },
    }

    # Augment "subcommands" variable with arguments specified by aspects.

    for subcommand, subcommand_value in subcommands.items():
        if 'argument_aspects' in subcommand_value:
            for aspect in subcommand_value['argument_aspects']:
                if 'arguments' not in subcommands[subcommand]:
                    subcommands[subcommand]['arguments'] = {}
                arguments = argument_aspects.get(aspect, {})
                for argument, argument_value in arguments.items():
                    subcommands[subcommand]['arguments'][argument] = argument_value

    parser = argparse.ArgumentParser(prog="init-container.py", description="Initialize Senzing installation. For more information, see https://github.com/Senzing/docker-init-container")
    subparsers = parser.add_subparsers(dest='subcommand', help='Subcommands (SENZING_SUBCOMMAND):')

    for subcommand_key, subcommand_values in subcommands.items():
        subcommand_help = subcommand_values.get('help', "")
        subcommand_arguments = subcommand_values.get('arguments', {})
        subparser = subparsers.add_parser(subcommand_key, help=subcommand_help)
        for argument_key, argument_values in subcommand_arguments.items():
            subparser.add_argument(argument_key, **argument_values)

    return parser

# -----------------------------------------------------------------------------
# Message handling
# -----------------------------------------------------------------------------

# 1xx Informational (i.e. logging.info())
# 3xx Warning (i.e. logging.warning())
# 5xx User configuration issues (either logging.warning() or logging.err() for Client errors)
# 7xx Internal error (i.e. logging.error for Server errors)
# 9xx Debugging (i.e. logging.debug())


MESSAGE_INFO = 100
MESSAGE_WARN = 300
MESSAGE_ERROR = 700
MESSAGE_DEBUG = 900

message_dictionary = {
    "100": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}I",
    "103": "Kafka topic: {0}; message: {1}; error: {2}; error: {3}",
    "120": "Sleeping for requested delay of {0} seconds.",
    "121": "Adding JSON to failure queue: {0}",
    "122": "Quitting time!",
    "123": "Total     memory: {0:>15} bytes",
    "124": "Available memory: {0:>15} bytes",
    "125": "G2 engine statistics: {0}",
    "126": "G2 project statistics: {0}",
    "127": "Monitor: {0}",
    "128": "Adding JSON to info queue: {0}",
    "129": "{0} is running.",
    "130": "RabbitMQ channel closed by the broker. Shutting down thread {0}.",
    "140": "System Resources:",
    "141": "    Physical cores: {0}",
    "142": "     Logical cores: {0}",
    "143": "      Total Memory: {0:.1f} GB",
    "144": "  Available Memory: {0:.1f} GB",
    "145": "Resource requested:",
    "146": "                    Processes: {0}",
    "147": "          Threads per process: {0}",
    "148": "    Minimum recommended cores: {0}",
    "149": "   Minimum recommended memory: {0:.1f} GB",
    "150": "Insertion test: {0} records inserted in {1}ms with an average of {2:.2f}ms per insert.",
    "151": "For database tuning help, see: https://senzing.zendesk.com/hc/en-us/sections/360000386433-Technical-Database",
    "152": "Sleeping {0} seconds before deploying administrative threads.",
    "153": "Created datasource {0}. Return code: {1}",
    "160": "{0} LICENSE {0}",
    "161": "          Version: {0} ({1})",
    "162": "         Customer: {0}",
    "163": "             Type: {0}",
    "164": "  Expiration date: {0}",
    "165": "  Expiration time: {0} days until expiration",
    "166": "          Records: {0}",
    "167": "         Contract: {0}",
    "168": "  Expiration time: EXPIRED {0} days ago",
    "180": "User-supplied Governor loaded from {0}.",
    "181": "User-supplied InfoFilter loaded from {0}.",
    "190": "AWS SQS Long-polling: No messages from {0}",
    "201": "Python 'psutil' not installed. Could not report memory.",
    "202": "Non-fatal exception on Line {0}: {1} Error: {2}",
    "203": "          WARNING: License will expire soon. Only {0} days left.",
    "292": "Configuration change detected.  Old: {0} New: {1}",
    "293": "For information on warnings and errors, see https://github.com/Senzing/stream-loader#errors",
    "294": "Version: {0}  Updated: {1}",
    "295": "Sleeping infinitely.",
    "296": "Sleeping {0} seconds.",
    "297": "Enter {0}",
    "298": "Exit {0}",
    "299": "{0}",
    "300": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "401": "Failure queue not open.  Could not add: {0}",
    "402": "Info queue not open.  Could not add: {0}",
    "403": "Bad file protocol in --input-file-name: {0}.",
    "404": "Kafka topic: {0} BufferError: {1} Message: {2}",
    "405": "Kafka topic: {0} KafkaException: {1} Message: {2}",
    "406": "Kafka topic: {0} NotImplemented: {1} Message: {2}",
    "407": "Kafka topic: {0} Unknown error: {1} Message: {2}",
    "408": "Kafka topic: {0}; message: {1}; error: {2}; error: {3}",
    "410": "RabbitMQ queue: {0} Unknown RabbitMQ error when connecting: {1}.",
    "411": "RabbitMQ queue: {0} Unknown RabbitMQ error: {1} Message: {2}",
    "412": "RabbitMQ queue: {0} AMQPConnectionError: {1} Could not connect to RabbitMQ host at {2}. The host name maybe wrong, it may not be ready, or your credentials are incorrect. See the RabbitMQ log for more details.",
    "499": "{0}",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "551": "Missing G2 database URL.",
    "552": "SENZING_DATA_SOURCE not set.",
    "553": "SENZING_ENTITY_TYPE not set.",
    "554": "Running with less than the recommended total memory of {0} GiB.",
    "555": "Running with less than the recommended available memory of {0} GiB.",
    "556": "SENZING_KAFKA_BOOTSTRAP_SERVER not set. See ./stream-loader.py kafka --help.",
    "557": "Invalid JSON received: {0}",
    "558": "LD_LIBRARY_PATH environment variable not set.",
    "559": "PYTHONPATH environment variable not set.",
    "560": "SENZING_PROCESSES for 'url' subcommand must be 1. Currently set to {0}.",
    "561": "Unknown RabbitMQ error when connecting: {0}.",
    "562": "Could not connect to RabbitMQ host at {1}. The host name maybe wrong, it may not be ready, or your credentials are incorrect. See the RabbitMQ log for more details. Error: {0}",
    "563": "Could not perform database performance test.",
    "564": "Database performance of {0:.2f}ms per insert is slower than the recommended minimum performance of {1:.2f}ms per insert",
    "565": "System has {0} cores which is less than the recommended minimum of {1} cores for this configuration.",
    "566": "System has {0:.1f} GB memory which is less than the recommended minimum of {1:.1f} GB memory",
    "695": "Unknown database scheme '{0}' in database url '{1}'",
    "696": "Bad SENZING_SUBCOMMAND: {0}.",
    "697": "No processing done.",
    "698": "Program terminated with error.",
    "699": "{0}",
    "700": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "721": "Running low on workers.  May need to restart",
    "722": "Kafka commit failed for {0}",
    "726": "Could not do performance test. G2 Translation error. Error: {0}",
    "727": "Could not do performance test. G2 module initialization error. Error: {0}",
    "728": "Could not do performance test. G2 generic exception. Error: {0}",
    "730": "There are not enough safe characters to do the translation. Unsafe Characters: {0}; Safe Characters: {1}",
    "885": "License has expired.",
    "886": "G2Engine.addRecord() bad return code: {0}; JSON: {1}",
    "888": "G2Engine.addRecord() G2ModuleNotInitialized: {0}; JSON: {1}",
    "889": "G2Engine.addRecord() G2ModuleGenericException: {0}; JSON: {1}",
    "890": "G2Engine.addRecord() Exception: {0}; JSON: {1}",
    "891": "Original and new database URLs do not match. Original URL: {0}; Reconstructed URL: {1}",
    "892": "Could not initialize G2Product with '{0}'. Error: {1}",
    "893": "Could not initialize G2Hasher with '{0}'. Error: {1}",
    "894": "Could not initialize G2Diagnostic with '{0}'. Error: {1}",
    "895": "Could not initialize G2Audit with '{0}'. Error: {1}",
    "896": "Could not initialize G2ConfigMgr with '{0}'. Error: {1}",
    "897": "Could not initialize G2Config with '{0}'. Error: {1}",
    "898": "Could not initialize G2Engine with '{0}'. Error: {1}",
    "899": "{0}",
    "900": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}D",
    "901": "Queued: {0}",
    "902": "Processed: {0}",
    "903": "{0} queued: {1}",
    "904": "{0} processed: {1}",
    "905": "{0} Kafka read: {1} Kafka commit: {2}",
    "906": "{0} RabbitMQ read: {1} RabbitMQ ack: {2}",
    "910": "Adding JSON to info queue: {0}",
    "911": "Adding JSON to failure queue: {0}",
    "999": "{0}",
}


def message(index, *args):
    index_string = str(index)
    template = message_dictionary.get(index_string, "No message for index {0}.".format(index_string))
    return template.format(*args)


def message_generic(generic_index, index, *args):
    index_string = str(index)
    return "{0} {1}".format(message(generic_index, index), message(index, *args))


def message_info(index, *args):
    return message_generic(MESSAGE_INFO, index, *args)


def message_warning(index, *args):
    return message_generic(MESSAGE_WARN, index, *args)


def message_error(index, *args):
    return message_generic(MESSAGE_ERROR, index, *args)


def message_debug(index, *args):
    return message_generic(MESSAGE_DEBUG, index, *args)


def get_exception():
    ''' Get details about an exception. '''
    exception_type, exception_object, traceback = sys.exc_info()
    frame = traceback.tb_frame
    line_number = traceback.tb_lineno
    filename = frame.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, line_number, frame.f_globals)
    return {
        "filename": filename,
        "line_number": line_number,
        "line": line.strip(),
        "exception": exception_object,
        "type": exception_type,
        "traceback": traceback,
    }

# -----------------------------------------------------------------------------
# Database URL parsing
# -----------------------------------------------------------------------------


def translate(map, astring):
    new_string = str(astring)
    for key, value in map.items():
        new_string = new_string.replace(key, value)
    return new_string


def get_unsafe_characters(astring):
    result = []
    for unsafe_character in unsafe_character_list:
        if unsafe_character in astring:
            result.append(unsafe_character)
    return result


def get_safe_characters(astring):
    result = []
    for safe_character in safe_character_list:
        if safe_character not in astring:
            result.append(safe_character)
    return result


def parse_database_url(original_senzing_database_url):
    ''' Given a canonical database URL, decompose into URL components. '''

    result = {}

    # Get the value of SENZING_DATABASE_URL environment variable.

    senzing_database_url = original_senzing_database_url

    # Create lists of safe and unsafe characters.

    unsafe_characters = get_unsafe_characters(senzing_database_url)
    safe_characters = get_safe_characters(senzing_database_url)

    # Detect an error condition where there are not enough safe characters.

    if len(unsafe_characters) > len(safe_characters):
        logging.error(message_error(730, unsafe_characters, safe_characters))
        return result

    # Perform translation.
    # This makes a map of safe character mapping to unsafe characters.
    # "senzing_database_url" is modified to have only safe characters.

    translation_map = {}
    safe_characters_index = 0
    for unsafe_character in unsafe_characters:
        safe_character = safe_characters[safe_characters_index]
        safe_characters_index += 1
        translation_map[safe_character] = unsafe_character
        senzing_database_url = senzing_database_url.replace(unsafe_character, safe_character)

    # Parse "translated" URL.

    parsed = urlparse(senzing_database_url)
    schema = parsed.path.strip('/')

    # Construct result.

    result = {
        'scheme': translate(translation_map, parsed.scheme),
        'netloc': translate(translation_map, parsed.netloc),
        'path': translate(translation_map, parsed.path),
        'params': translate(translation_map, parsed.params),
        'query': translate(translation_map, parsed.query),
        'fragment': translate(translation_map, parsed.fragment),
        'username': translate(translation_map, parsed.username),
        'password': translate(translation_map, parsed.password),
        'hostname': translate(translation_map, parsed.hostname),
        'port': translate(translation_map, parsed.port),
        'schema': translate(translation_map, schema),
    }

    # For safety, compare original URL with reconstructed URL.

    url_parts = [
        result.get('scheme'),
        result.get('netloc'),
        result.get('path'),
        result.get('params'),
        result.get('query'),
        result.get('fragment'),
    ]
    test_senzing_database_url = urlunparse(url_parts)
    if test_senzing_database_url != original_senzing_database_url:
        logging.warning(message_warning(891, original_senzing_database_url, test_senzing_database_url))

    # Return result.

    return result

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------


def get_g2_database_url_specific(generic_database_url):
    ''' Given a canonical database URL, transform to the specific URL. '''

    result = ""
    parsed_database_url = parse_database_url(generic_database_url)
    scheme = parsed_database_url.get('scheme')

    # Format database URL for a particular database.

    if scheme in ['mysql']:
        result = "{scheme}://{username}:{password}@{hostname}:{port}/?schema={schema}".format(**parsed_database_url)
    elif scheme in ['postgresql']:
        result = "{scheme}://{username}:{password}@{hostname}:{port}:{schema}/".format(**parsed_database_url)
    elif scheme in ['db2']:
        result = "{scheme}://{username}:{password}@{schema}".format(**parsed_database_url)
    elif scheme in ['sqlite3']:
        result = "{scheme}://{netloc}{path}".format(**parsed_database_url)
    elif scheme in ['mssql']:
        result = "{scheme}://{username}:{password}@{schema}".format(**parsed_database_url)
    else:
        logging.error(message_error(695, scheme, generic_database_url))

    return result


def get_configuration(args):
    ''' Order of precedence: CLI, OS environment variables, INI file, default. '''
    result = {}

    # Copy default values into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        result[key] = value.get('default', None)

    # "Prime the pump" with command line args. This will be done again as the last step.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Copy OS environment variables into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        os_env_var = value.get('env', None)
        if os_env_var:
            os_env_value = os.getenv(os_env_var, None)
            if os_env_value:
                result[key] = os_env_value

    # Copy 'args' into configuration dictionary.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Special case: subcommand from command-line

    if args.subcommand:
        result['subcommand'] = args.subcommand

    # Special case: Change boolean strings to booleans.

    booleans = [
        'debug',
        'skip_database_performance_test'
    ]
    for boolean in booleans:
        boolean_value = result.get(boolean)
        if isinstance(boolean_value, str):
            boolean_value_lower_case = boolean_value.lower()
            if boolean_value_lower_case in ['true', '1', 't', 'y', 'yes']:
                result[boolean] = True
            else:
                result[boolean] = False

    # Special case: Change integer strings to integers.

    integers = [
        'configuration_check_frequency_in_seconds',
        'delay_in_seconds',
        'expiration_warning_in_days',
        'log_license_period_in_seconds',
        'monitoring_period_in_seconds',
        'processes',
        'queue_maxsize',
        'sleep_time_in_seconds',
        'threads_per_process',
        'rabbitmq_prefetch_count',
    ]
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    # Special case:  Tailored database URL

    result['g2_database_url_specific'] = get_g2_database_url_specific(result.get("g2_database_url_generic"))

    # Initialize counters.

    result['counter_processed_records'] = 0
    result['counter_queued_records'] = 0
    result['counter_bad_records'] = 0
    result['kafka_ack_elapsed'] = 0
    result['kafka_poll_elapsed'] = 0
    result['rabbitmq_ack_elapsed'] = 0
    result['rabbitmq_poll_elapsed'] = 0

    return result


def validate_configuration(config):
    ''' Check aggregate configuration from commandline options, environment variables, config files, and defaults. '''

    user_warning_messages = []
    user_error_messages = []

    if not config.get('g2_database_url_generic'):
        user_error_messages.append(message_error(551))

    # Perform subcommand specific checking.

    subcommand = config.get('subcommand')

    if subcommand in ['kafka', 'stdin', 'url']:

        if not config.get('ld_library_path'):
            user_error_messages.append(message_error(558))

        if not config.get('python_path'):
            user_error_messages.append(message_error(559))

    if subcommand in ['stdin', 'url']:

        if config.get('processes') > 1:
            user_error_messages.append(message_error(560, config.get('processes')))

    if subcommand in ['stdin']:

        if not config.get('data_source'):
            user_warning_messages.append(message_warning(552))

        if not config.get('entity_type'):
            user_warning_messages.append(message_warning(553))

    if subcommand in ['kafka']:

        if not config.get('kafka_bootstrap_server'):
            user_error_messages.append(message_error(556))

    # Log warning messages.

    for user_warning_message in user_warning_messages:
        logging.warning(user_warning_message)

    # Log error messages.

    for user_error_message in user_error_messages:
        logging.error(user_error_message)

    # Log where to go for help.

    if len(user_warning_messages) > 0 or len(user_error_messages) > 0:
        logging.info(message_info(293))

    # If there are error messages, exit.

    if len(user_error_messages) > 0:
        exit_error(697)


def redact_configuration(config):
    ''' Return a shallow copy of config with certain keys removed. '''
    result = config.copy()
    for key in keys_to_redact:
        try:
            result.pop(key)
        except:
            pass
    return result

# -----------------------------------------------------------------------------
# Class: Governor
# -----------------------------------------------------------------------------


class Governor:

    def __init__(self, g2_engine=None, hint=None, *args, **kwargs):
        self.g2_engine = g2_engine

    def govern(self, *args, **kwargs):
        return

# -----------------------------------------------------------------------------
# Class: InfoFilter
# -----------------------------------------------------------------------------


class InfoFilter:

    def __init__(self, g2_engine=None, *args, **kwargs):
        self.g2_engine = g2_engine

    def filter(self, message=None, *args, **kwargs):
        return message

# -----------------------------------------------------------------------------
# Class: KafkaProcess
# -----------------------------------------------------------------------------


class KafkaProcess(multiprocessing.Process):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        multiprocessing.Process.__init__(self)

        # Create kafka reader threads.

        self.threads = []
        threads_per_process = config.get('threads_per_process')
        for i in range(0, threads_per_process):
            thread = ReadKafkaWriteG2Thread(config, g2_engine, g2_configuration_manager)
            thread.name = "{0}-thread-{1}".format(self.name, i)
            self.threads.append(thread)

        # Create administrative threads for this process.

        self.adminThreads = []
        thread = MonitorThread(config, g2_engine, self.threads)
        thread.name = "{0}-thread-monitor".format(self.name)
        self.adminThreads.append(thread)

    def run(self):

        # Start threads.

        for thread in self.threads:
            thread.start()

        for thread in self.adminThreads:
            thread.start()

        # Collect inactive threads.

        for thread in self.threads:
            thread.join()

# -----------------------------------------------------------------------------
# Class: RabbitMQProcess
# -----------------------------------------------------------------------------


class RabbitMQProcess(multiprocessing.Process):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        multiprocessing.Process.__init__(self)

        # Create RabbitMQ reader threads.

        self.threads = []
        threads_per_process = config.get('threads_per_process')
        for i in range(0, threads_per_process):
            thread = ReadRabbitMQWriteG2Thread(config, g2_engine, g2_configuration_manager)
            thread.name = "{0}-thread-{1}".format(self.name, i)
            self.threads.append(thread)

        # Create administrative threads for this process.

        self.adminThreads = []
        thread = MonitorThread(config, g2_engine, self.threads)
        thread.name = "{0}-thread-monitor".format(self.name)
        self.adminThreads.append(thread)

    def run(self):

        # Start threads.

        for thread in self.threads:
            thread.start()

        for thread in self.adminThreads:
            thread.start()

        # Collect inactive threads.

        for thread in self.threads:
            thread.join()

# -----------------------------------------------------------------------------
# Class: WriteG2Thread
# -----------------------------------------------------------------------------


class WriteG2Thread(threading.Thread):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        threading.Thread.__init__(self)
        self.config = config
        self.g2_engine = g2_engine
        self.g2_configuration_manager = g2_configuration_manager
        self.governor = Governor(g2_engine=g2_engine)
        self.info_filter = InfoFilter(g2_engine=g2_engine)

    def add_to_failure_queue(self, jsonline):
        '''Default behavior. This may be implemented in the subclass.'''
        assert type(jsonline) == str
        logging.info(message_info(121, jsonline))

    def add_to_info_queue(self, jsonline):
        '''Default behavior. This may be implemented in the subclass.'''
        assert type(jsonline) == str
        logging.info(message_info(128, jsonline))

    def filter_info_message(self, message=None):
        assert type(message) == str
        return self.info_filter.filter(message=message)

    def govern(self):
        return self.governor.govern()

    def is_time_to_check_g2_configuration(self):
        now = time.time()
        next_check_time = self.config.get('last_configuration_check', time.time()) + self.config.get('configuration_check_frequency_in_seconds')
        return now > next_check_time

    def is_g2_default_configuration_changed(self):

        # Update early to avoid "thundering heard problem".

        self.config['last_configuration_check'] = time.time()

        # Get active Configuration ID being used by g2_engine.

        active_config_id = bytearray()
        self.g2_engine.getActiveConfigID(active_config_id)

        # Get most current Configuration ID from G2 database.

        default_config_id = bytearray()
        self.g2_configuration_manager.getDefaultConfigID(default_config_id)

        # Determine if configuration has changed.

        result = active_config_id != default_config_id
        if result:
            logging.info(message_info(292, active_config_id.decode(), default_config_id.decode()))

        return result

    def update_active_g2_configuration(self):

        # Get most current Configuration ID from G2 database.

        default_config_id = bytearray()
        self.g2_configuration_manager.getDefaultConfigID(default_config_id)

        # Apply new configuration to g2_engine.

        self.g2_engine.reinitV2(default_config_id)

    def add_record(self, jsonline):
        ''' Send a record to Senzing. '''
        assert type(jsonline) == str
        json_dictionary = json.loads(jsonline)
        data_source = str(json_dictionary.get('DATA_SOURCE', self.config.get("data_source")))
        record_id = json_dictionary.get('RECORD_ID')
        if record_id is not None:
            record_id = str(record_id)

        try:
            self.g2_engine.addRecord(data_source, record_id, jsonline)
        except Exception as err:
            if self.is_g2_default_configuration_changed():
                self.update_active_g2_configuration()
                self.g2_engine.addRecord(data_source, record_id, jsonline)
            else:
                raise err
        return

    def add_record_withinfo(self, jsonline):
        ''' Send a record to Senzing and return the "info" returned by Senzing. '''
        assert type(jsonline) == str
        json_dictionary = json.loads(jsonline)
        data_source = str(json_dictionary.get('DATA_SOURCE', self.config.get("data_source")))
        record_id = json_dictionary.get('RECORD_ID')
        if record_id is not None:
            record_id = str(record_id)
        response_bytearray = bytearray()
        try:
            self.g2_engine.addRecordWithInfo(data_source, record_id, jsonline, response_bytearray)
        except Exception as err:
            if self.is_g2_default_configuration_changed():
                self.update_active_g2_configuration()
                self.g2_engine.addRecordWithInfo(data_source, record_id, jsonline, response_bytearray)
            else:
                raise err
        return response_bytearray.decode()

    def send_jsonline_to_g2_engine(self, jsonline):
        '''Send the JSONline to G2 engine.'''
        assert type(jsonline) == str

        # Periodically, check for configuration update.

        if self.is_time_to_check_g2_configuration():
            if self.is_g2_default_configuration_changed():
                self.update_active_g2_configuration()

        # Add Record to Senzing G2.

        try:
            self.add_record(jsonline)
        except G2Exception.G2ModuleNotInitialized as err:
            exit_error(888, err, jsonline)
        except G2Exception.G2ModuleGenericException as err:
            logging.error(message_error(889, err, jsonline))
            self.add_to_failure_queue(jsonline)
        except Exception as err:
            logging.error(message_error(890, err, jsonline))
            self.add_to_failure_queue(jsonline)

        logging.debug(message_debug(904, threading.current_thread().name, jsonline))

    def send_jsonline_to_g2_engine_withinfo(self, jsonline):
        '''Send the JSONline to G2 engine.'''
        assert type(jsonline) == str

        # Periodically, check for configuration update.

        if self.is_time_to_check_g2_configuration():
            if self.is_g2_default_configuration_changed():
                self.update_active_g2_configuration()

        # Add Record to Senzing G2.

        info_json = None
        try:
            info_json = self.add_record_withinfo(jsonline)
        except G2Exception.G2ModuleNotInitialized as err:
            self.add_to_failure_queue(jsonline)
            exit_error(888, err, jsonline)
        except G2Exception.G2ModuleGenericException as err:
            self.add_to_failure_queue(jsonline)
            logging.error(message_error(889, err, jsonline))
            return
        except Exception as err:
            self.add_to_failure_queue(jsonline)
            logging.error(message_error(890, err, jsonline))
            return

        # Allow user to manipulate the message.

        filtered_info_json = self.filter_info_message(message=info_json)

#         # Put "info" on info queue.

        if filtered_info_json:
            self.add_to_info_queue(filtered_info_json)
            logging.debug(message_debug(904, threading.current_thread().name, filtered_info_json))

# -----------------------------------------------------------------------------
# Class: ReadKafkaWriteG2Thread
# -----------------------------------------------------------------------------


class ReadKafkaWriteG2Thread(WriteG2Thread):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        super().__init__(config, g2_engine, g2_configuration_manager)

    def run(self):
        '''Process for reading lines from Kafka and feeding them to a process_function() function'''

        logging.info(message_info(129, threading.current_thread().name))

        # Create Kafka client.

        consumer_configuration = {
            'bootstrap.servers': self.config.get('kafka_bootstrap_server'),
            'group.id': self.config.get("kafka_group"),
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
            }
        consumer = confluent_kafka.Consumer(consumer_configuration)
        consumer.subscribe([self.config.get("kafka_topic")])

        # Data to be inserted into messages.

        data_source = self.config.get('data_source')
        entity_type = self.config.get('entity_type')

        # In a loop, get messages from Kafka.

        while True:

            # Invoke Governor.

            self.govern()

            # Get message from Kafka queue.
            # Timeout quickly to allow other co-routines to process.

            kafka_message = consumer.poll(1.0)

            # Handle non-standard Kafka output.

            if kafka_message is None:
                continue
            if kafka_message.error():
                if kafka_message.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(message_error(722, kafka_message.error()))
                    continue

            # Construct and verify Kafka message.

            kafka_message_string = kafka_message.value().strip()
            if not kafka_message_string:
                continue
            logging.debug(message_debug(903, threading.current_thread().name, kafka_message_string))
            self.config['counter_queued_records'] += 1

            # Verify that message is valid JSON.

            try:
                kafka_message_dictionary = json.loads(kafka_message_string)
            except:
                logging.info(message_debug(557, kafka_message_string))
                if not consumer.commit():
                    logging.error(message_error(722, kafka_message_string))
                continue

            # If needed, modify JSON message.

            if 'DATA_SOURCE' not in kafka_message_dictionary:
                kafka_message_dictionary['DATA_SOURCE'] = data_source
            if 'ENTITY_TYPE' not in kafka_message_dictionary:
                kafka_message_dictionary['ENTITY_TYPE'] = entity_type
            kafka_message_string = json.dumps(kafka_message_dictionary, sort_keys=True)

            # Send valid JSON to Senzing.

            self.send_jsonline_to_g2_engine(kafka_message_string)

            # Record successful transfer to Senzing.

            self.config['counter_processed_records'] += 1

            # After successful import into Senzing, tell Kafka we're done with message.

            consumer.commit()

        consumer.close()

# -----------------------------------------------------------------------------
# Class: ReadKafkaWriteG2WithInfoThread
# -----------------------------------------------------------------------------


class ReadKafkaWriteG2WithInfoThread(WriteG2Thread):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        super().__init__(config, g2_engine, g2_configuration_manager)
        self.info_producer = None
        self.info_topic = config.get("kafka_info_topic")
        self.failure_producer = None
        self.failure_topic = config.get("kafka_failure_topic")

    def on_kafka_delivery(error, message):
        message_topic = message.topic()
        message_value = message.value()
        message_error = message.error()
        logging.debug(message_debug(103, message_topic, message_value, message_error, error))
        if error is not None:
            logging.warn(message_warning(408, message_topic, message_value, message_error, error))

    def add_to_failure_queue(self, jsonline):
        '''Overwrite superclass method.'''
        assert type(jsonline) == str

        try:
            self.failure_producer.produce(self.failure_topic, jsonline, on_delivery=self.on_kafka_delivery)
            logging.info(message_info(911, jsonline))
        except BufferError as err:
            logging.warn(message_warning(404, self.failure_topic, err, jsonline))
        except KafkaException as err:
            logging.warn(message_warning(405, self.failure_topic, err, jsonline))
        except NotImplemented as err:
            logging.warn(message_warning(406, self.failure_topic, err, jsonline))
        except:
            logging.warn(message_warning(407, self.failure_topic, err, jsonline))

    def add_to_info_queue(self, jsonline):
        '''Overwrite superclass method.'''
        assert type(jsonline) == str

        try:
            self.info_producer.produce(self.info_topic, jsonline, on_delivery=self.on_kafka_delivery)
            logging.debug(message_debug(910, jsonline))
        except BufferError as err:
            logging.warn(message_warning(404, self.info_topic, err, jsonline))
        except KafkaException as err:
            logging.warn(message_warning(405, self.info_topic, err, jsonline))
        except NotImplemented as err:
            logging.warn(message_warning(406, self.info_topic, err, jsonline))
        except:
            logging.warn(message_warning(407, self.info_topic, err, jsonline))

    def run(self):
        '''Process for reading lines from Kafka and feeding them to a process_function() function'''

        logging.info(message_info(129, threading.current_thread().name))

        # Create Kafka client.

        consumer_configuration = {
            'bootstrap.servers': self.config.get('kafka_bootstrap_server'),
            'group.id': self.config.get("kafka_group"),
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
            }
        consumer = confluent_kafka.Consumer(consumer_configuration)
        consumer.subscribe([self.config.get("kafka_topic")])

        # Create Kafka Producer for "info".

        kafka_info_producer_configuration = {
            'bootstrap.servers': self.config.get('kafka_info_bootstrap_server')
        }
        self.info_producer = confluent_kafka.Producer(kafka_info_producer_configuration)

        # Create Kafka Producer for "failure".

        kafka_failure_producer_configuration = {
            'bootstrap.servers': self.config.get('kafka_failure_bootstrap_server')
        }
        self.failure_producer = confluent_kafka.Producer(kafka_failure_producer_configuration)

        # Data to be inserted into messages.

        data_source = self.config.get('data_source')
        entity_type = self.config.get('entity_type')

        # In a loop, get messages from Kafka.

        while True:

            # Invoke Governor.

            self.govern()

            # Get message from Kafka queue.
            # Timeout quickly to allow other co-routines to process.

            kafka_message = consumer.poll(1.0)

            # Handle non-standard Kafka output.

            if kafka_message is None:
                continue
            if kafka_message.error():
                if kafka_message.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(message_error(722, kafka_message.error()))
                    continue

            # Construct and verify Kafka message.

            kafka_message_string = kafka_message.value().strip()
            if not kafka_message_string:
                continue
            logging.debug(message_debug(903, threading.current_thread().name, kafka_message_string))
            self.config['counter_queued_records'] += 1

            # Verify that message is valid JSON.

            try:
                kafka_message_dictionary = json.loads(kafka_message_string)
            except:
                logging.info(message_debug(557, kafka_message_string))
                if not consumer.commit():
                    logging.error(message_error(722, kafka_message_string))
                continue

            # If needed, modify JSON message.

            if 'DATA_SOURCE' not in kafka_message_dictionary:
                kafka_message_dictionary['DATA_SOURCE'] = data_source
            if 'ENTITY_TYPE' not in kafka_message_dictionary:
                kafka_message_dictionary['ENTITY_TYPE'] = entity_type
            kafka_message_string = json.dumps(kafka_message_dictionary, sort_keys=True)

            # Send valid JSON to Senzing.

            self.send_jsonline_to_g2_engine_withinfo(kafka_message_string)

            # Record successful transfer to Senzing.

            self.config['counter_processed_records'] += 1

            # After successful import into Senzing, tell Kafka we're done with message.

            consumer.commit()

        consumer.close()

# -----------------------------------------------------------------------------
# Class: ReadRabbitMQWriteG2Thread
# -----------------------------------------------------------------------------


class ReadRabbitMQWriteG2Thread(WriteG2Thread):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        super().__init__(config, g2_engine, g2_configuration_manager)

    def callback(self, ch, method, header, body):
        logging.debug(message_debug(903, threading.current_thread().name, body))
        self.config['counter_queued_records'] += 1

        # Invoke Governor.

        self.govern()

        # Verify that message is valid JSON.

        try:
            rabbitmq_message_dictionary = json.loads(body)
        except:
            logging.info(message_debug(557, body))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # If needed, modify JSON message.

        if 'DATA_SOURCE' not in rabbitmq_message_dictionary:
            rabbitmq_message_dictionary['DATA_SOURCE'] = self.data_source
        if 'ENTITY_TYPE' not in rabbitmq_message_dictionary:
            rabbitmq_message_dictionary['ENTITY_TYPE'] = self.entity_type
        rabbitmq_message_string = json.dumps(rabbitmq_message_dictionary, sort_keys=True)

        # Send valid JSON to Senzing.

        self.send_jsonline_to_g2_engine(rabbitmq_message_string)

        # Record successful transfer to Senzing.

        self.config['counter_processed_records'] += 1

        # After successful import into Senzing, tell RabbitMQ we're done with message.

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        '''Process for reading lines from RabbitMQ and feeding them to a process_function() function'''

        logging.info(message_info(129, threading.current_thread().name))

        # Get config parameters.

        rabbitmq_queue = self.config.get("rabbitmq_queue")
        rabbitmq_username = self.config.get("rabbitmq_username")
        rabbitmq_password = self.config.get("rabbitmq_password")
        rabbitmq_host = self.config.get("rabbitmq_host")
        rabbitmq_prefetch_count = self.config.get("rabbitmq_prefetch_count")
        self.data_source = self.config.get("data_source")
        self.entitiy_type = self.config.get("entity_type")

        # Connect to RabbitMQ queue.

        try:
            credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()
            channel.queue_declare(queue=rabbitmq_queue)
            channel.basic_qos(prefetch_count=rabbitmq_prefetch_count)
            channel.basic_consume(on_message_callback=self.callback, queue=rabbitmq_queue)
        except pika.exceptions.AMQPConnectionError as err:
            exit_error(562, err, rabbitmq_host)
        except BaseException as err:
            exit_error(561, err)

        # Start consuming.

        try:
            channel.start_consuming()
        except pika.exceptions.ChannelClosed:
            logging.info(message_info(130, threading.current_thread().name))

# -----------------------------------------------------------------------------
# Class: ReadRabbitMQWriteG2WithInfoThread
# -----------------------------------------------------------------------------


class ReadRabbitMQWriteG2WithInfoThread(WriteG2Thread):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        super().__init__(config, g2_engine, g2_configuration_manager)
        self.data_source = self.config.get("data_source")
        self.entitiy_type = self.config.get("entity_type")
        self.rabbitmq_info_queue = self.config.get("rabbitmq_info_queue")
        self.info_channel = None
        self.failure_channel = None

    def add_to_failure_queue(self, jsonline):
        '''Overwrite superclass method.'''
        assert type(jsonline) == str
        jsonline_bytes = jsonline.encode()
        try:
            self.failure_channel.basic_publish(
                exchange='',
                routing_key=self.rabbitmq_failure_queue,
                body=jsonline_bytes,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )  # make message persistent
            logging.debug(message_debug(911, jsonline))

        except BaseException as err:
            logging.warn(message_warning(411, self.rabbitmq_failure_queue, err, jsonline))

    def add_to_info_queue(self, jsonline):
        '''Overwrite superclass method.'''
        assert type(jsonline) == str
        jsonline_bytes = jsonline.encode()
        try:
            self.info_channel.basic_publish(
                exchange='',
                routing_key=self.rabbitmq_info_queue,
                body=jsonline_bytes,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )  # make message persistent
            logging.debug(message_debug(910, jsonline))

        except BaseException as err:
            logging.warn(message_warning(411, self.rabbitmq_info_queue, err, jsonline))

    def callback(self, channel, method, header, body):
        logging.debug(message_debug(903, threading.current_thread().name, body))
        self.config['counter_queued_records'] += 1

        # Invoke Governor.

        self.govern()

        # Verify that message is valid JSON.

        try:
            rabbitmq_message_dictionary = json.loads(body)
        except:
            self.add_to_failure_queue(body)
            logging.info(message_debug(557, body))
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # If needed, modify JSON message.

        if 'DATA_SOURCE' not in rabbitmq_message_dictionary:
            rabbitmq_message_dictionary['DATA_SOURCE'] = self.data_source
        if 'ENTITY_TYPE' not in rabbitmq_message_dictionary:
            rabbitmq_message_dictionary['ENTITY_TYPE'] = self.entity_type
        rabbitmq_message_string = json.dumps(rabbitmq_message_dictionary, sort_keys=True)

        # Send valid JSON to Senzing.

        self.send_jsonline_to_g2_engine_withinfo(rabbitmq_message_string)

        # Record successful transfer to Senzing.

        self.config['counter_processed_records'] += 1

        # After successful import into Senzing, tell RabbitMQ we're done with message.

        channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        '''Process for reading lines from RabbitMQ and feeding them to a process_function() function'''

        logging.info(message_info(129, threading.current_thread().name))

        # Get config parameters.

        rabbitmq_host = self.config.get("rabbitmq_host")
        rabbitmq_password = self.config.get("rabbitmq_password")
        rabbitmq_queue = self.config.get("rabbitmq_queue")
        rabbitmq_username = self.config.get("rabbitmq_username")

        rabbitmq_info_host = self.config.get("rabbitmq_info_host")
        rabbitmq_info_password = self.config.get("rabbitmq_info_password")
        rabbitmq_info_queue = self.config.get("rabbitmq_info_queue")
        rabbitmq_info_username = self.config.get("rabbitmq_info_username")

        rabbitmq_failure_host = self.config.get("rabbitmq_failure_host")
        rabbitmq_failure_password = self.config.get("rabbitmq_failure_password")
        rabbitmq_failure_queue = self.config.get("rabbitmq_failure_queue")
        rabbitmq_failure_username = self.config.get("rabbitmq_failure_username")

        rabbitmq_prefetch_count = self.config.get("rabbitmq_prefetch_count")

        # Create RabbitMQ channel to publish "info".

        try:
            info_credentials = pika.PlainCredentials(rabbitmq_info_username, rabbitmq_info_password)
            info_connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_info_host, credentials=info_credentials))
            self.info_channel = info_connection.channel()
            self.info_channel.queue_declare(queue=rabbitmq_info_queue)
        except (pika.exceptions.AMQPConnectionError) as err:
            exit_error(412, rabbitmq_info_queue, err, rabbitmq_info_host)
        except BaseException as err:
            exit_error(410, rabbitmq_info_queue, err)

        # Create RabbitMQ channel to publish "failure".

        try:
            failure_credentials = pika.PlainCredentials(rabbitmq_failure_username, rabbitmq_failure_password)
            failure_connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_failure_host, credentials=failure_credentials))
            self.failure_channel = failure_connection.channel()
            self.failure_channel.queue_declare(queue=rabbitmq_failure_queue)
        except (pika.exceptions.AMQPConnectionError) as err:
            exit_error(412, rabbitmq_failure_queue, err, rabbitmq_failure_host)
        except BaseException as err:
            exit_error(410, rabbitmq_failure_queue, err)

        # Create RabbitMQ channel to subscribe to records.

        try:
            credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()
            channel.queue_declare(queue=rabbitmq_queue)
            channel.basic_qos(prefetch_count=rabbitmq_prefetch_count)
            channel.basic_consume(on_message_callback=self.callback, queue=rabbitmq_queue)
        except pika.exceptions.AMQPConnectionError as err:
            exit_error(562, err, rabbitmq_host)
        except BaseException as err:
            exit_error(561, err)

        # Start consuming.

        try:
            channel.start_consuming()
        except pika.exceptions.ChannelClosed:
            logging.info(message_info(130, threading.current_thread().name))

# -----------------------------------------------------------------------------
# Class: ReadSqsWriteG2Thread
# -----------------------------------------------------------------------------


class ReadSqsWriteG2Thread(WriteG2Thread):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        super().__init__(config, g2_engine, g2_configuration_manager)
        self.queue_url = config.get("sqs_queue_url")
        self.sqs = boto3.client("sqs")

    def run(self):
        '''Process for reading lines from AWS SQS and feeding them to a process_function() function'''

        logging.info(message_info(129, threading.current_thread().name))

        # Data to be inserted into messages.

        data_source = self.config.get('data_source')
        entity_type = self.config.get('entity_type')

        # In a loop, get messages from AWS SQS.

        while True:

            # Invoke Governor.

            self.govern()

            # Get message from AWS SQS queue.

            sqs_response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=[],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[],
                VisibilityTimeout=30,
                WaitTimeSeconds=20
            )

            # If non-standard SQS output or empty messages, just loop.

            if sqs_response is None:
                continue
            sqs_messages = sqs_response.get("Messages", [])
            if not sqs_messages:
                logging.info(message_info(190, self.queue_url))
                continue

            # Construct and verify SQS message.

            sqs_message = sqs_messages[0]
            sqs_message_body = sqs_message.get("Body")
            sqs_message_receipt_handle = sqs_message.get("ReceiptHandle")
            logging.debug(message_debug(903, threading.current_thread().name, sqs_message_body))
            self.config['counter_queued_records'] += 1

            # Verify that message is valid JSON.

            try:
                sqs_message_dictionary = json.loads(sqs_message_body)
            except:
                logging.info(message_debug(557, sqs_message_body))
                continue

            # If needed, modify JSON message.

            if 'DATA_SOURCE' not in sqs_message_dictionary:
                sqs_message_dictionary['DATA_SOURCE'] = data_source
            if 'ENTITY_TYPE' not in sqs_message_dictionary:
                sqs_message_dictionary['ENTITY_TYPE'] = entity_type
            sqs_message_string = json.dumps(sqs_message_dictionary, sort_keys=True)

            # Send valid JSON to Senzing.

            self.send_jsonline_to_g2_engine(sqs_message_string)

            # Record successful transfer to Senzing.

            self.config['counter_processed_records'] += 1

            # After successful import into Senzing, tell AWS SQS we're done with message.

            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=sqs_message_receipt_handle
            )

# -----------------------------------------------------------------------------
# Class: ReadSqsWriteG2WithInfoThread
# -----------------------------------------------------------------------------


class ReadSqsWriteG2WithInfoThread(WriteG2Thread):

    def __init__(self, config, g2_engine, g2_configuration_manager):
        super().__init__(config, g2_engine, g2_configuration_manager)
        self.failure_queue_url = config.get("sqs_failure_queue_url")
        self.info_queue_url = config.get("sqs_info_queue_url")
        self.queue_url = config.get("sqs_queue_url")
        self.sqs = boto3.client("sqs")

    def add_to_failure_queue(self, jsonline):
        '''Overwrite superclass method.'''
        assert type(jsonline) == str
        try:
            response = self.sqs.send_message(
                QueueUrl=self.failure_queue_url,
                DelaySeconds=10,
                MessageAttributes={},
                MessageBody=(jsonline),
            )
            logging.info(message_info(911, jsonline))
        except:
            logging.warn(message_warning(407, self.failure_topic, err, jsonline))

    def add_to_info_queue(self, jsonline):
        '''Overwrite superclass method.'''
        assert type(jsonline) == str
        try:
            response = self.sqs.send_message(
                QueueUrl=self.info_queue_url,
                DelaySeconds=10,
                MessageAttributes={},
                MessageBody=(jsonline),
            )
            logging.debug(message_debug(910, jsonline))
        except:
            logging.warn(message_warning(407, self.info_topic, err, jsonline))

    def run(self):
        '''Process for reading lines from Kafka and feeding them to a process_function() function'''

        logging.info(message_info(129, threading.current_thread().name))

        # Data to be inserted into messages.

        data_source = self.config.get('data_source')
        entity_type = self.config.get('entity_type')

        # In a loop, get messages from Kafka.

        while True:

            # Invoke Governor.

            self.govern()

            # Get message from AWS SQS queue.

            sqs_response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=[],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[],
                VisibilityTimeout=30,
                WaitTimeSeconds=20
            )

            # If non-standard SQS output or empty messages, just loop.

            if sqs_response is None:
                continue
            sqs_messages = sqs_response.get("Messages", [])
            if not sqs_messages:
                logging.info(message_info(190, self.queue_url))
                continue

            # Construct and verify SQS message.

            sqs_message = sqs_messages[0]
            sqs_message_body = sqs_message.get("Body")
            sqs_message_receipt_handle = sqs_message.get("ReceiptHandle")
            logging.debug(message_debug(903, threading.current_thread().name, sqs_message_body))
            self.config['counter_queued_records'] += 1

            # Verify that message is valid JSON.

            try:
                sqs_message_dictionary = json.loads(sqs_message_body)
            except:
                logging.info(message_debug(557, sqs_message_body))
                continue

            # If needed, modify JSON message.

            if 'DATA_SOURCE' not in sqs_message_dictionary:
                sqs_message_dictionary['DATA_SOURCE'] = data_source
            if 'ENTITY_TYPE' not in sqs_message_dictionary:
                sqs_message_dictionary['ENTITY_TYPE'] = entity_type
            sqs_message_string = json.dumps(sqs_message_dictionary, sort_keys=True)

            # Send valid JSON to Senzing.

            self.send_jsonline_to_g2_engine_withinfo(sqs_message_string)

            # Record successful transfer to Senzing.

            self.config['counter_processed_records'] += 1

            # After successful import into Senzing, tell AWS SQS we're done with message.

            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=sqs_message_receipt_handle
            )

# -----------------------------------------------------------------------------
# Class: UrlProcess
# -----------------------------------------------------------------------------


class UrlProcess(multiprocessing.Process):

    def __init__(self, config, work_queue):
        multiprocessing.Process.__init__(self)

        # Get the G2Engine resource.

        engine_name = "loader-G2-engine-{0}".format(self.name)
        self.g2_engine = get_g2_engine(config, engine_name)

        # List of all threads.

        self.threads = []

        # Create URL reader thread.

        thread = ReadUrlWriteQueueThread(config, work_queue)
        thread.name = "{0}-reader".format(self.name)
        self.threads.append(thread)

        # Create URL writer threads.

        threads_per_process = config.get('threads_per_process')
        for i in range(0, threads_per_process):
            thread = ReadQueueWriteG2Thread(config, self.g2_engine, self.g2_configuration_manager, work_queue)
            thread.name = "{0}-writer-{1}".format(self.name, i)
            self.threads.append(thread)

        # Create monitor thread.

        thread = MonitorThread(config, self.g2_engine, self.threads)
        thread.name = "{0}-monitor".format(self.name)
        self.threads.append(thread)

    def run(self):

        # Start threads.

        for thread in self.threads:
            thread.start()

        # Collect inactive threads.

        for thread in self.threads:
            thread.join()

        # Cleanup.

        self.g2_engine.destroy()

# -----------------------------------------------------------------------------
# Class: ReadUrlWriteQueueThread
# -----------------------------------------------------------------------------


class ReadUrlWriteQueueThread(threading.Thread):

    def __init__(self, config, queue):
        threading.Thread.__init__(self)
        self.config = config
        self.queue = queue

    def create_input_lines_function_factory(self):
        '''Choose which input_lines_from_* function should be used.'''

        result = None
        input_url = self.config.get('input_url')

        def input_lines_from_stdin(self, output_line_function):
            '''Process for reading lines from STDIN and feeding them to a output_line_function() function'''

            # Note: The alternative, 'for line in sys.stdin:',  suffers from a 4K buffering issue.

            reading = True
            while reading:
                line = sys.stdin.readline()
                self.config['counter_queued_records'] += 1
                logging.debug(message_debug(901, line))
                if line:
                    output_line_function(self, line)
                else:
                    reading = False  # FIXME: Not sure if this is the best method of exiting.

        def input_lines_from_file(self, output_line_function):
            '''Process for reading lines from a file and feeding them to a output_line_function() function'''
            input_url = self.config.get('input_url')
            with open(input_url, 'r') as input_file:
                line = input_file.readline()
                while line:
                    self.config['counter_queued_records'] += 1
                    logging.debug(message_debug(901, line))
                    output_line_function(self, line)
                    line = input_file.readline()

        def input_lines_from_url(self, output_line_function):
            '''Process for reading lines from a URL and feeding them to a output_line_function() function'''
            input_url = self.config.get('input_url')
            data = urllib.request.urlopen(input_url)
            for line in data:
                self.config['counter_queued_records'] += 1
                logging.debug(message_debug(901, line))
                output_line_function(self, line)

        # If no file, input comes from STDIN.

        if not input_url:
            return input_lines_from_stdin

        # Return a function based on URI protocol.

        parsed_file_name = urlparse(input_url)
        if parsed_file_name.scheme in ['http', 'https']:
            result = input_lines_from_url
        elif parsed_file_name.scheme in ['file', '']:
            result = input_lines_from_file
        return result

    def create_output_line_function_factory(self):
        '''Tricky code.  Uses currying and factory techniques. Create a function for output_line_function(line).'''

        # Indicators of which function to return from factory.

        data_source = self.config.get('data_source')
        entity_type = self.config.get('entity_type')

        # Candidate functions to return from factory.

        def result_function_1(self, line):
            '''Simply put line into the queue.'''
            self.queue.put(line.strip())

        def result_function_2(self, line):
            line_dictionary = json.loads(line)
            if 'DATA_SOURCE' not in line_dictionary:
                line_dictionary['DATA_SOURCE'] = data_source
            self.queue.put(json.dumps(line_dictionary, sort_keys=True))

        def result_function_3(self, line):
            line_dictionary = json.loads(line)
            if 'ENTITY_TYPE' not in line_dictionary:
                line_dictionary['ENTITY_TYPE'] = entity_type
            self.queue.put(json.dumps(line_dictionary, sort_keys=True))

        def result_function_4(self, line):
            line_dictionary = json.loads(line)
            if 'DATA_SOURCE' not in line_dictionary:
                line_dictionary['DATA_SOURCE'] = data_source
            if 'ENTITY_TYPE' not in line_dictionary:
                line_dictionary['ENTITY_TYPE'] = entity_type
            self.queue.put(json.dumps(line_dictionary, sort_keys=True))

        # Determine which function to return.

        result_function = None
        if data_source is not None and entity_type is not None:
            result_function = result_function_4
        elif entity_type is not None:
            result_function = result_function_3
        elif data_source is not None:
            result_function = result_function_2
        else:
            result_function = result_function_1

        return result_function

    def run(self):
        input_lines_function = self.create_input_lines_function_factory()
        output_line_function = self.create_output_line_function_factory()
        input_lines_function(self, output_line_function)

# -----------------------------------------------------------------------------
# Class: ReadQueueWriteG2Thread
# -----------------------------------------------------------------------------


class ReadQueueWriteG2Thread(WriteG2Thread):
    '''Thread for writing ...'''

    def __init__(self, config, g2_engine, g2_configuration_manager, queue):
        super().__init__(config, g2_engine, g2_configuration_manager)
        self.queue = queue

    def run(self):
        while True:

            # Invoke Governor.

            self.govern()

            # Process queued message.

            try:
                jsonline = self.queue.get()
                self.send_jsonline_to_g2_engine(jsonline)
                self.config['counter_processed_records'] += 1
            except queue.Empty:
                logging.info(message_info(122))

# -----------------------------------------------------------------------------
# Class: MonitorThread
# -----------------------------------------------------------------------------


class MonitorThread(threading.Thread):

    def __init__(self, config, g2_engine, workers):
        threading.Thread.__init__(self)
        self.config = config
        self.g2_engine = g2_engine
        self.workers = workers
        # FIXME: self.last_daily = datetime.

    def run(self):
        '''Periodically monitor what is happening.'''

        last_processed_records = 0
        last_queued_records = 0
        last_time = time.time()
        last_log_license = time.time()
        log_license_period_in_seconds = self.config.get("log_license_period_in_seconds")

        # Define monitoring report interval.

        sleep_time_in_seconds = self.config.get('monitoring_period_in_seconds')

        # Sleep-monitor loop.

        active_workers = len(self.workers)
        for worker in self.workers:
            if not worker.is_alive():
                active_workers -= 1

        while active_workers > 0:

            time.sleep(sleep_time_in_seconds)

            # Calculate active Threads.

            active_workers = len(self.workers)
            for worker in self.workers:
                if not worker.is_alive():
                    active_workers -= 1

            # Determine if we're running out of workers.

            if (active_workers / float(len(self.workers))) < 0.5:
                logging.warning(message_warning(721))

            # Calculate times.

            now = time.time()
            uptime = now - self.config.get('start_time', now)
            elapsed_time = now - last_time
            elapsed_log_license = now - last_log_license

            # Log license periodically to show days left in license.

            if elapsed_log_license > log_license_period_in_seconds:
                log_license(self.config)
                last_log_license = now

            # Calculate rates.

            processed_records_total = self.config['counter_processed_records']
            processed_records_interval = processed_records_total - last_processed_records
            rate_processed_total = int(processed_records_total / uptime)
            rate_processed_interval = int(processed_records_interval / elapsed_time)

            queued_records_total = self.config['counter_queued_records']
            queued_records_interval = queued_records_total - last_queued_records
            rate_queued_total = int(queued_records_total / uptime)
            rate_queued_interval = int(queued_records_interval / elapsed_time)

            # Construct and log monitor statistics.

            stats = {
                "processed_records_interval": processed_records_interval,
                "processed_records_total": processed_records_total,
                "queued_records_interval": queued_records_interval,
                "queued_records_total": queued_records_total,
                "rate_processed_interval": rate_processed_interval,
                "rate_processed_total": rate_processed_total,
                "rate_queued_interval": rate_queued_interval,
                "rate_queued_total": rate_queued_total,
                "uptime": int(uptime),
                "workers_total": len(self.workers),
                "workers_active": active_workers,
            }
            logging.info(message_info(127, json.dumps(stats, sort_keys=True)))

            # Log engine statistics with sorted JSON keys.

            g2_engine_stats_response = bytearray()
            self.g2_engine.stats(g2_engine_stats_response)
            g2_engine_stats_dictionary = json.loads(g2_engine_stats_response.decode())
            logging.info(message_info(125, json.dumps(g2_engine_stats_dictionary, sort_keys=True)))

            # Store values for next iteration of loop.

            last_processed_records = processed_records_total
            last_queued_records = queued_records_total
            last_time = now

# -----------------------------------------------------------------------------
# Class: MonitorThread
# -----------------------------------------------------------------------------


class MonitorTestThread(threading.Thread):

    def __init__(self, config, workers):
        threading.Thread.__init__(self)
        self.config = config
        self.workers = workers

    def run(self):
        '''Periodically monitor what is happening.'''

        last_queued_records = 0
        last_kafka_poll = 0
        last_kafka_commit = 0
        last_time = time.time()

        # Define monitoring report interval.

        sleep_time_in_seconds = self.config.get('monitoring_period_in_seconds')

        # Sleep-monitor loop.

        active_workers = len(self.workers)
        for worker in self.workers:
            if not worker.is_alive():
                active_workers -= 1

        while active_workers > 0:

            time.sleep(sleep_time_in_seconds)

            # Calculate active Threads.

            active_workers = len(self.workers)
            for worker in self.workers:
                if not worker.is_alive():
                    active_workers -= 1

            # Determine if we're running out of workers.

            if (active_workers / float(len(self.workers))) < 0.5:
                logging.warning(message_warning(721))

            # Calculate rates.

            now = time.time()
            uptime = now - self.config.get('start_time', now)
            elapsed_time = now - last_time

            queued_records_total = self.config['counter_queued_records']
            queued_records_interval = queued_records_total - last_queued_records
            rate_queued_total = queued_records_total / uptime
            rate_queued_interval = queued_records_interval / elapsed_time

            kafka_poll_total = self.config['kafka_poll_elapsed']
            kafka_poll_interval = kafka_poll_total - last_kafka_poll
            rate_kafka_poll_total = kafka_poll_total / uptime
            rate_kafka_poll_interval = kafka_poll_interval / elapsed_time

            kafka_commit_total = self.config['kafka_commit_elapsed']
            kafka_commit_interval = kafka_commit_total - last_kafka_commit
            rate_kafka_commit_total = kafka_commit_total / uptime
            rate_kafka_commit_interval = kafka_commit_interval / elapsed_time

            # Calculate averages.

            average_rate_kafka_commit_interval = 0
            average_rate_kafka_poll_interval = 0
            if queued_records_interval > 0:
                average_rate_kafka_commit_interval = rate_kafka_commit_interval / queued_records_interval
                average_rate_kafka_poll_interval = rate_kafka_poll_interval / queued_records_interval

            # Construct and log monitor statistics.

            stats = {
                "average_rate_kafka_commit_interval": average_rate_kafka_commit_interval,
                "average_rate_kafka_poll_interval": average_rate_kafka_poll_interval,
                "kafka_commit_interval": kafka_commit_interval,
                "kafka_commit_total": kafka_commit_total,
                "kafka_poll_interval": kafka_poll_interval,
                "kafka_poll_total": kafka_poll_total,
                "queued_records_interval": queued_records_interval,
                "queued_records_total": queued_records_total,
                "rate_kafka_commit_interval": rate_kafka_commit_interval,
                "rate_kafka_commit_total": rate_kafka_commit_total,
                "rate_kafka_poll_interval": rate_kafka_poll_interval,
                "rate_kafka_poll_total": rate_kafka_poll_total,
                "rate_queued_interval": rate_queued_interval,
                "rate_queued_total": rate_queued_total,
                "uptime": int(uptime),
                "workers_total": len(self.workers),
                "workers_active": active_workers,
            }
            logging.info(message_info(127, json.dumps(stats, sort_keys=True)))

            # Store values for next iteration of loop.

            last_queued_records = queued_records_total
            last_kafka_poll = kafka_poll_total
            last_kafka_commit = kafka_commit_total
            last_time = now

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------


def bootstrap_signal_handler(signal, frame):
    sys.exit(0)


def create_signal_handler_function(args):
    ''' Tricky code.  Uses currying technique. Create a function for signal handling.
        that knows about "args".
    '''

    def result_function(signal_number, frame):
        logging.info(message_info(298, args))
        sys.exit(0)

    return result_function


def delay(config):
    delay_in_seconds = config.get('delay_in_seconds')
    if delay_in_seconds > 0:
        logging.info(message_info(120, delay_in_seconds))
        time.sleep(delay_in_seconds)


def entry_template(config):
    ''' Format of entry message. '''
    debug = config.get("debug", False)
    config['start_time'] = time.time()
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(297, config_json)


def exit_template(config):
    ''' Format of exit message. '''
    debug = config.get("debug", False)
    stop_time = time.time()
    config['stop_time'] = stop_time
    config['elapsed_time'] = stop_time - config.get('start_time', stop_time)
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(298, config_json)


def exit_error(index, *args):
    ''' Log error message and exit program. '''
    logging.error(message_error(index, *args))
    logging.error(message_error(698))
    sys.exit(1)


def exit_silently():
    ''' Exit program. '''
    sys.exit(0)

# -----------------------------------------------------------------------------
# Senzing configuration.
# -----------------------------------------------------------------------------


def get_g2_configuration_dictionary(config):
    ''' Construct a dictionary in the form of the old ini files. '''
    result = {
        "PIPELINE": {
            "CONFIGPATH": config.get("config_path"),
            "RESOURCEPATH": config.get("resource_path"),
            "SUPPORTPATH": config.get("support_path"),
        },
        "SQL": {
            "CONNECTION": config.get("g2_database_url_specific"),
        }
    }
    return result


def get_g2_configuration_json(config):
    result = ""
    if config.get('engine_configuration_json'):
        result = config.get('engine_configuration_json')
    else:
        result = json.dumps(get_g2_configuration_dictionary(config))
    return result

# -----------------------------------------------------------------------------
# Senzing services.
# -----------------------------------------------------------------------------


def get_g2_config(config, g2_config_name="loader-G2-config"):
    '''Get the G2Config resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Config()
        result.initV2(g2_config_name, g2_configuration_json, config.get('debug', False))
    except G2Exception.G2ModuleException as err:
        exit_error(897, g2_configuration_json, err)
    return result


def get_g2_configuration_manager(config, g2_configuration_manager_name="loader-G2-configuration-manager"):
    '''Get the G2Config resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2ConfigMgr()
        result.initV2(g2_configuration_manager_name, g2_configuration_json, config.get('debug', False))
    except G2Exception.G2ModuleException as err:
        exit_error(896, g2_configuration_json, err)
    return result


def get_g2_diagnostic(config, g2_diagnostic_name="loader-G2-diagnostic"):
    '''Get the G2Diagnostic resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Diagnostic()
        result.initV2(g2_diagnostic_name, g2_configuration_json, config.get('debug', False))
    except G2Exception.G2ModuleException as err:
        exit_error(894, g2_configuration_json, err)
    return result


def get_g2_engine(config, g2_engine_name="loader-G2-engine"):
    '''Get the G2Engine resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Engine()
        result.initV2(g2_engine_name, g2_configuration_json, config.get('debug', False))
        config['last_configuration_check'] = time.time()
    except G2Exception.G2ModuleException as err:
        exit_error(898, g2_configuration_json, err)
    return result


def get_g2_product(config, g2_product_name="loader-G2-product"):
    '''Get the G2Product resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Product()
        result.initV2(g2_product_name, g2_configuration_json, config.get('debug'))
    except G2Exception.G2ModuleException as err:
        exit_error(892, config.get('g2project_ini'), err)
    return result

# -----------------------------------------------------------------------------
# Log information.
# -----------------------------------------------------------------------------


def log_license(config):
    '''Capture the license and version info in the log.'''

    g2_product = get_g2_product(config)
    license = json.loads(g2_product.license())
    version = json.loads(g2_product.version())

    logging.info(message_info(160, '-' * 20))
    if 'VERSION' in version:
        logging.info(message_info(161, version['VERSION'], version['BUILD_DATE']))
    if 'customer' in license:
        logging.info(message_info(162, license['customer']))
    if 'licenseType' in license:
        logging.info(message_info(163, license['licenseType']))
    if 'expireDate' in license:
        logging.info(message_info(164, license['expireDate']))

        # Calculate days remaining.

        expire_date = datetime.datetime.strptime(license['expireDate'], '%Y-%m-%d')
        today = datetime.datetime.today()
        remaining_time = expire_date - today
        if remaining_time.days > 0:
            logging.info(message_info(165, remaining_time.days))
            expiration_warning_in_days = config.get('expiration_warning_in_days')
            if remaining_time.days < expiration_warning_in_days:
                logging.warning(message_warning(203, remaining_time.days))
        else:
            logging.info(message_info(168, abs(remaining_time.days)))

        # Issue warning if license is about to expire.

    if 'recordLimit' in license:
        logging.info(message_info(166, license['recordLimit']))
    if 'contract' in license:
        logging.info(message_info(167, license['contract']))
    logging.info(message_info(299, '-' * 49))

    # Garbage collect g2_product.

    g2_product.destroy()

    # If license has expired, exit with error.

    if remaining_time.days < 0:
        exit_error(885)


def log_performance(config):
    '''Log performance estimates.'''

    try:

        # Initialized G2Diagnostic object.

        g2_diagnostic = get_g2_diagnostic(config)

        # Calculations for memory.

        total_system_memory = g2_diagnostic.getTotalSystemMemory() / float(GIGABYTES)
        total_available_memory = g2_diagnostic.getAvailableMemory() / float(GIGABYTES)

        # Log messages for system.

        logging.info(message_info(140))
        logging.info(message_info(141, g2_diagnostic.getPhysicalCores()))
        if g2_diagnostic.getPhysicalCores() != g2_diagnostic.getLogicalCores():
            logging.info(message_info(142, g2_diagnostic.getLogicalCores()))
        logging.info(message_info(143, total_system_memory))
        logging.info(message_info(144, total_available_memory))

        # Calculations for processes, threads, and cores.

        processes = 1
        threads_per_process = config.get('threads_per_process')
        memory_per_process = 2.5
        memory_per_thread = 0.5
        threads_per_core = float(4 + 1)
        minimum_recommended_cores = int(math.ceil((processes * threads_per_process) / threads_per_core))
        minimum_recommended_memory = (processes * memory_per_process) + (threads_per_process * memory_per_thread)

        # Log messages for resource request.

        logging.info(message_info(145))
        logging.info(message_info(146, processes))
        logging.info(message_info(147, threads_per_process))
        logging.info(message_info(148, minimum_recommended_cores))
        logging.info(message_info(149, minimum_recommended_memory))

        # Database performance testing.

        db_perf_response = bytearray()
        g2_diagnostic.checkDBPerf(3, db_perf_response)
        performance_information = json.loads(db_perf_response.decode())
        number_of_records_inserted = performance_information.get('numRecordsInserted', 0)
        time_to_insert = performance_information.get('insertTime', 0)
        time_per_insert = None
        if number_of_records_inserted and time_to_insert:
            time_per_insert = time_to_insert / float(number_of_records_inserted)
            logging.info(message_info(150, number_of_records_inserted, time_to_insert, time_per_insert))
        else:
            logging.warning(message_warning(563))

        # Analysis.

        maximum_time_allowed_per_insert_in_ms = 4
        if time_per_insert and (time_per_insert > maximum_time_allowed_per_insert_in_ms):
            logging.warning(message_warning(564, time_per_insert, maximum_time_allowed_per_insert_in_ms))
            logging.info(message_info(151))

        if g2_diagnostic.getPhysicalCores() < minimum_recommended_cores:
            logging.warning(message_warning(565, g2_diagnostic.getPhysicalCores(), minimum_recommended_cores))

        if total_available_memory < minimum_recommended_memory:
            logging.warning(message_warning(566, total_available_memory, minimum_recommended_memory))

    except G2Exception.G2ModuleNotInitialized as err:
        logging.warning(message_warning(727, err))
    except G2Exception.G2ModuleGenericException as err:
        logging.warning(message_warning(728, err))


def log_memory():
    '''Write total and available memory to log.  Check if it meets minimums.'''
    try:
        import psutil

        total_memory = psutil.virtual_memory().total
        available_memory = psutil.virtual_memory().available

        # Log actual memory.

        logging.info(message_info(123, total_memory))
        logging.info(message_info(124, available_memory))

        # Check total memory.

        minimum_total_memory = MINIMUM_TOTAL_MEMORY_IN_GIGABYTES * GIGABYTES
        if total_memory < minimum_total_memory:
            logging.warning(message_warning(554, MINIMUM_TOTAL_MEMORY_IN_GIGABYTES))

        # Check available memory.

        minimum_available_memory = MINIMUM_AVAILABLE_MEMORY_IN_GIGABYTES * GIGABYTES
        if available_memory < minimum_available_memory:
            logging.warning(message_warning(555, MINIMUM_AVAILABLE_MEMORY_IN_GIGABYTES))

    except:
        logging.warning(message_warning(201))

# -----------------------------------------------------------------------------
# Worker functions
# -----------------------------------------------------------------------------


def common_prolog(config):

    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # If requested, delay start.

    delay(config)

    # Write license information to log.

    log_license(config)

    # Write memory statistics to log.

    log_memory()

    # Test performance.

    if not config.get('skip_database_performance_test', False):
        log_performance(config)

# -----------------------------------------------------------------------------
# dohelper_* functions
# -----------------------------------------------------------------------------


def dohelper_thread_runner(args, threadClass, options_to_defaults_map):
    ''' Performs threadClass. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # If configuration values not specified, use defaults.

    for key, value in options_to_defaults_map.items():
        if not config.get(key):
            config[key] = config.get(value)

    # Perform common initialization tasks.

    common_prolog(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    # Create RabbitMQ reader threads for master process.

    threads = []
    for i in range(0, threads_per_process):
        thread = threadClass(config, g2_engine, g2_configuration_manager)
        thread.name = "{0}-0-thread-{1}".format(threadClass.__name__, i)
        threads.append(thread)

    # Create monitor thread for master process.

    adminThreads = []
    thread = MonitorThread(config, g2_engine, threads)
    thread.name = "{0}-0-thread-monitor".format(threadClass.__name__)
    adminThreads.append(thread)

    # Sleep, if requested.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')
    if sleep_time_in_seconds > 0:
        logging.info(message_info(152, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    # Start threads for master process.

    for thread in threads:
        thread.start()

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))

# -----------------------------------------------------------------------------
# do_* functions
#   Common function signature: do_XXX(args)
# -----------------------------------------------------------------------------


def do_docker_acceptance_test(args):
    ''' For use with Docker acceptance testing. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Epilog.

    logging.info(exit_template(config))


def do_kafka(args):
    ''' Read from Kafka. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Perform common initialization tasks.

    common_prolog(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    # Create kafka reader threads for master process.

    threads = []
    for i in range(0, threads_per_process):
        thread = ReadKafkaWriteG2Thread(config, g2_engine, g2_configuration_manager)
        thread.name = "KafkaProcess-0-thread-{0}".format(i)
        threads.append(thread)

    # Create monitor thread for master process.

    adminThreads = []
    thread = MonitorThread(config, g2_engine, threads)
    thread.name = "KafkaProcess-0-thread-monitor"
    adminThreads.append(thread)

    # Start threads for master process.

    for thread in threads:
        thread.start()

    # Sleep, if requested.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')
    if sleep_time_in_seconds > 0:
        logging.info(message_info(152, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))


def do_kafka_withinfo(args):
    ''' Read from Kafka. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # If configuration values not specified, use defaults.

    options_to_defaults_map = {
        "kafka_failure_bootstrap_server": "kafka_bootstrap_server",
        "kafka_info_bootstrap_server": "kafka_bootstrap_server",
    }

    for key, value in options_to_defaults_map.items():
        if not config.get(key):
            config[key] = config.get(value)

    # Perform common initialization tasks.

    common_prolog(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    # Create kafka reader threads for master process.

    threads = []
    for i in range(0, threads_per_process):
        thread = ReadKafkaWriteG2WithInfoThread(config, g2_engine, g2_configuration_manager)
        thread.name = "KafkaProcess-0-thread-{0}".format(i)
        threads.append(thread)

    # Create monitor thread for master process.

    adminThreads = []
    thread = MonitorThread(config, g2_engine, threads)
    thread.name = "KafkaProcess-0-thread-monitor"
    adminThreads.append(thread)

    # Start threads for master process.

    for thread in threads:
        thread.start()

    # Sleep, if requested.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')
    if sleep_time_in_seconds > 0:
        logging.info(message_info(152, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))


def do_rabbitmq(args):
    ''' Read from rabbitmq. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Perform common initialization tasks.

    common_prolog(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    # Create RabbitMQ reader threads for master process.

    threads = []
    for i in range(0, threads_per_process):
        thread = ReadRabbitMQWriteG2Thread(config, g2_engine, g2_configuration_manager)
        thread.name = "RabbitMQProcess-0-thread-{0}".format(i)
        threads.append(thread)

    # Create monitor thread for master process.

    adminThreads = []
    thread = MonitorThread(config, g2_engine, threads)
    thread.name = "RabbitMQProcess-0-thread-monitor"
    adminThreads.append(thread)

    # Start threads for master process.

    for thread in threads:
        thread.start()

    # Sleep, if requested.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')
    if sleep_time_in_seconds > 0:
        logging.info(message_info(152, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))


def do_rabbitmq_withinfo(args):
    ''' Read from rabbitmq. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # If configuration values not specified, use defaults.

    options_to_defaults_map = {
        "rabbitmq_failure_host": "rabbitmq_host",
        "rabbitmq_failure_password": "rabbitmq_password",
        "rabbitmq_failure_username": "rabbitmq_username",
        "rabbitmq_info_host": "rabbitmq_host",
        "rabbitmq_info_password": "rabbitmq_password",
        "rabbitmq_info_username": "rabbitmq_username",
    }

    for key, value in options_to_defaults_map.items():
        if not config.get(key):
            config[key] = config.get(value)

    # Perform common initialization tasks.

    common_prolog(config)

    # Pull values from configuration.

    threads_per_process = config.get('threads_per_process')

    # Get the Senzing G2 resources.

    g2_engine = get_g2_engine(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    # Create RabbitMQ reader threads for master process.

    threads = []
    for i in range(0, threads_per_process):
        thread = ReadRabbitMQWriteG2WithInfoThread(config, g2_engine, g2_configuration_manager)
        thread.name = "RabbitMQProcess-0-thread-{0}".format(i)
        threads.append(thread)

    # Create monitor thread for master process.

    adminThreads = []
    thread = MonitorThread(config, g2_engine, threads)
    thread.name = "RabbitMQProcess-0-thread-monitor"
    adminThreads.append(thread)

    # Start threads for master process.

    for thread in threads:
        thread.start()

    # Sleep, if requested.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')
    if sleep_time_in_seconds > 0:
        logging.info(message_info(152, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    # Start administrative threads for master process.

    for thread in adminThreads:
        thread.start()

    # Collect inactive threads from master process.

    for thread in threads:
        thread.join()

    # Cleanup.

    g2_engine.destroy()

    # Epilog.

    logging.info(exit_template(config))


def do_sleep(args):
    ''' Sleep.  Used for debugging. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')

    # Sleep

    if sleep_time_in_seconds > 0:
        logging.info(message_info(296, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    else:
        sleep_time_in_seconds = 3600
        while True:
            logging.info(message_info(295))
            time.sleep(sleep_time_in_seconds)

    # Epilog.

    logging.info(exit_template(config))


def do_sqs(args):
    ''' Read from SQS. '''

    dohelper_thread_runner(args, ReadSqsWriteG2Thread, {})


def do_sqs_withinfo(args):
    ''' Read from SQS. '''

    dohelper_thread_runner(args, ReadSqsWriteG2WithInfoThread, {})


def do_url(args):
    '''Read from URL-addressable file.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Perform common initialization tasks.

    common_prolog(config)

    # Pull values from configuration.

    number_of_processes = config.get('processes')
    queue_maxsize = config.get('queue_maxsize')

    # Create Queue.

    work_queue = multiprocessing.Queue(queue_maxsize)

    # Start processes.

    processes = []
    for i in range(0, number_of_processes):
        process = UrlProcess(config, work_queue)
        process.start()

    # Collect inactive processes.

    for process in processes:
        process.join()

    # Epilog.

    logging.info(exit_template(config))


def do_version(args):
    ''' Log version information. '''

    logging.info(message_info(294, __version__, __updated__))

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


if __name__ == "__main__":

    # Configure logging. See https://docs.python.org/2/library/logging.html#levels

    log_level_map = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "fatal": logging.FATAL,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
    log_level = log_level_map.get(log_level_parameter, logging.INFO)
    logging.basicConfig(format=log_format, level=log_level)

    # Trap signals temporarily until args are parsed.

    signal.signal(signal.SIGTERM, bootstrap_signal_handler)
    signal.signal(signal.SIGINT, bootstrap_signal_handler)

    # Import plugins

    try:
        import senzing_governor
        from senzing_governor import Governor
        logging.info(message_info(180, senzing_governor.__file__))
    except ImportError:
        pass

    try:
        import senzing_info_filter
        from senzing_info_filter import InfoFilter
        logging.info(message_info(181, senzing_info_filter.__file__))
    except ImportError:
        pass

    # Parse the command line arguments.

    subcommand = os.getenv("SENZING_SUBCOMMAND", None)
    parser = get_parser()
    if len(sys.argv) > 1:
        args = parser.parse_args()
        subcommand = args.subcommand
    elif subcommand:
        args = argparse.Namespace(subcommand=subcommand)
    else:
        parser.print_help()
        if len(os.getenv("SENZING_DOCKER_LAUNCHED", "")):
            subcommand = "sleep"
            args = argparse.Namespace(subcommand=subcommand)
            do_sleep(args)
        exit_silently()

    # Catch interrupts. Tricky code: Uses currying.

    signal_handler = create_signal_handler_function(args)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Transform subcommand from CLI parameter to function name string.

    subcommand_function_name = "do_{0}".format(subcommand.replace('-', '_'))

    # Test to see if function exists in the code.

    if subcommand_function_name not in globals():
        logging.warning(message_warning(696, subcommand))
        parser.print_help()
        exit_silently()

    # Tricky code for calling function based on string.

    globals()[subcommand_function_name](args)
