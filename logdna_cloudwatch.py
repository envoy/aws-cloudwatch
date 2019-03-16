import boto3
from botocore.vendored import requests
import os
import json
import gzip
# uncomment to enable debug logging
# import logging
from StringIO import StringIO
from datetime import datetime, timedelta

# Constants:
MAX_LINE_LENGTH = 32000
MAX_REQUEST_TIMEOUT = 30


# ECS Service Event Handler:
def lambda_handler(event, context):
    key, hostname, ecs_cluster, services, event_cutoff, tags, baseurl = setup()
    # If we find awslogs, parse them.  Otherwise get ECS service events
    if 'awslogs' in event:
        log_lines = get_cloudwatch_log_lines(event)
        messages, options = prepare_cloudwatch_log_messages(log_lines, hostname, tags)
    else:
        ecs_events = get_ecs_events(ecs_cluster, services, event_cutoff)
        messages, options = prepare_ecs_event_messages(ecs_events, hostname, tags)
    send_log(messages=messages, options=options, key=key, baseurl=baseurl)


# Getting Parameters from Environment Variables:
def setup():
    key = os.environ.get('LOGDNA_KEY', None)
    hostname = os.environ.get('LOGDNA_HOSTNAME', None)
    ecs_cluster = os.environ.get('ECS_CLUSTER', None)
    services = os.environ.get('SERVICES').split(",")
    event_cutoff_seconds = os.environ.get('EVENT_CUTOFF_SECONDS', 70)
    event_cutoff = datetime.now() - timedelta(seconds=event_cutoff_seconds)
    tags = os.environ.get('LOGDNA_TAGS', None)
    baseurl = build_url(os.environ.get('LOGDNA_URL', None))
    return key, hostname, ecs_cluster, services, event_cutoff, tags, baseurl


# Building URL using baseurl parameter:
def build_url(baseurl):
    if baseurl is None:
        return 'https://logs.logdna.com/logs/ingest'
    return 'https://' + baseurl


def get_cloudwatch_log_lines(event):
    cw_data = str(event['awslogs']['data'])
    cw_logs = gzip.GzipFile(fileobj=StringIO(cw_data.decode('base64', 'strict'))).read()
    return json.loads(cw_logs)


def get_ecs_events(ecs_cluster, services, event_cutoff):
    ecs = boto3.client('ecs')
    events = {}
    for service in services:
        events[service] = []
        response = ecs.describe_services(cluster=ecs_cluster, services=[service])
        all_service_events = response['services'][0]['events']
        time_cutoff = datetime.now() - timedelta(minutes=1)
        for event in all_service_events:
            if event['createdAt'].replace(tzinfo=None) > event_cutoff:
                events[service].append(event)
    return events


# Preparing the ECS Event Payload:
def prepare_ecs_event_messages(events, hostname=None, tags=None):
    messages = []
    options = {}
    if hostname is not None:
        options['hostname'] = hostname
    if tags is not None:
        options['tags'] = tags
    for service_name, service_events in events.items():
        app = 'ecs[' + service_name + ']'
        for event in service_events:
            message = {
                'line': event['message'],
                'timestamp': event['createdAt'].isoformat(),
                'file': app
            }
            messages.append(sanitizeMessage(message))
    return messages, options


# Preparing the Payload:
def prepare_cloudwatch_log_messages(cw_log_lines, hostname=None, tags=None):
    messages = list()
    options = dict()
    app = 'CloudWatch'
    meta = {'type': app}
    if 'logGroup' in cw_log_lines:
        app_name = cw_log_lines['logStream'].split('/')[0]
        app = "app[" + app_name + "]"
        meta['group'] = cw_log_lines['logGroup'];
    if 'logStream' in cw_log_lines:
        options['hostname'] = cw_log_lines['logStream'].split('/')[-1].split(']')[-1]
        meta['stream'] = cw_log_lines['logStream']
    if hostname is not None:
        options['hostname'] = hostname
    if tags is not None:
        options['tags'] = tags
    for cw_log_line in cw_log_lines['logEvents']:
        message = {
            'line': cw_log_line['message'],
            'timestamp': cw_log_line['timestamp'],
            'file': app,
            'meta': meta}
        messages.append(sanitizeMessage(message))
    return messages, options


# Polishing the Message:
def sanitizeMessage(message):
    if message and message['line']:
        if len(message['line']) > MAX_LINE_LENGTH:
            message['line'] = message['line'][:MAX_LINE_LENGTH] + ' (cut off, too long...)'
    return message


# Submitting the Log Payload into LogDNA:
def send_log(messages, options, baseurl, key=None):
    # Uncomment the below to debug message sending
    # try:
    #     import http.client as http_client
    # except ImportError:
    #     # Python 2
    #     import httplib as http_client
    # http_client.HTTPConnection.debuglevel = 1
    # logging.basicConfig()
    # logging.getLogger().setLevel(logging.DEBUG)
    # requests_log = logging.getLogger("requests.packages.urllib3")
    # requests_log.setLevel(logging.DEBUG)
    # requests_log.propagate = True
    if key is not None:
        data = {'e': 'ls', 'ls': messages}
        requests.post(
            url=baseurl,
            json=data,
            auth=('user', key),
            params={
                'hostname': options['hostname'],
                'tags': options['tags'] if 'tags' in options else None},
            stream=True,
            timeout=MAX_REQUEST_TIMEOUT)