import csv
import datetime
import os.path
import re

import pytz
from dateutil.parser import parse
from flask import Flask, jsonify, request

from log_fields import field_names
from loki import LokiClient

loki_url = os.getenv('LOKI_URL')

loki_client = LokiClient(url=loki_url)
loki_ready = loki_client.ready()
if not loki_ready:
    print('Loki is not ready.')
    exit(1)

log_tz = pytz.timezone(os.getenv('TIME_ZONE', 'Europe/Oslo'))
app = Flask(__name__)
now = datetime.datetime.now(log_tz)
log_tz = now.tzinfo


def get_hosts(query):
    hosts_temp = []
    logs = loki_client.query_range(query)
    for log in logs['data']['result']:
        if log['stream']['HOST'] not in hosts_temp:
            hosts_temp.append(log['stream']['HOST'])
    return hosts_temp


@app.route('/hosts', methods=['GET'])
def find_hosts():
    query = '{job="syslog"} | logfmt'
    host_list = []
    while True:
        for host in host_list:
            query += ' != "%s"' % host
        hosts = get_hosts(query)
        if not hosts:
            break
        host_list += hosts
    return jsonify(host_list)


# @app.route('/', defaults={'logtype_arg': 'TRAFFIC'})
@app.route('/<string:logtype_arg>', methods=['GET'])  # , defaults={'logtype_arg': 'TRAFFIC'})
def read_log(logtype_arg):
    if logtype_arg == 'favicon.ico':
        return ''
    print('Logtype %s' % logtype_arg)
    time_from = request.values.get('from')
    time_to = request.values.get('to')
    limit = request.values.get('limit')
    host = request.values.get('host')

    try:
        limit = int(limit or 200)
    except ValueError:
        limit = 200

    if time_to:
        time_to_obj = parse(time_to)
    else:
        time_to_obj = datetime.datetime.now()

    if not time_from:
        time_from_obj = time_to_obj - datetime.timedelta(hours=2)
    else:
        time_from_obj = parse(time_from)

    if logtype_arg:
        query = '{job="syslog"} |= `%s` | logfmt' % logtype_arg
    else:
        query = '{job="syslog"} | logfmt'
    if host:
        query += ' | HOST=`%s`' % host
    logs = loki_client.query_range(query, limit, time_from_obj, time_to_obj)
    print('Got %d logs from Loki' % len(logs['data']['result']))

    lines = []
    for line in logs['data']['result']:
        line = line['values'][0][1]
        message = re.sub(r'.*MESSAGE="(.+)".*', r'\1', line)
        message = message.replace('\\"', '"')
        fields = list(csv.reader([message], delimiter=',', escapechar='\\', quotechar='"'))[0]
        log_type = fields[3]
        if logtype_arg != log_type:
            continue

        fields_named = {}
        pos = 0
        for field in fields:
            if logtype_arg in field_names and pos in field_names[logtype_arg]:
                name = field_names[logtype_arg][pos]
            else:
                name = '%d' % pos

            if re.match(r'([\d/]+ [\d:]+)', field):
                field = parse(field)
                field = field.replace(tzinfo=log_tz)
                field = field.strftime(os.getenv('DATE_FORMAT', '%Y-%m-%d %H:%M:%S'))

            fields_named[name] = field
            pos += 1

            lines.append(fields_named)

    return jsonify(lines)


if __name__ == '__main__':
    app.run(port=int(os.getenv('LISTEN_PORT', 80)), host='0.0.0.0')
