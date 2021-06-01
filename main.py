# Qlik Sense Data Retention Controller
# Created by: Jesper Bagge, 2021
# This application is licensed under MIT.

import datetime
import os
import json
import sys
from datetime import datetime as dt
from datetime import timedelta
import argparse
import csv

from websocket import create_connection

base_path = os.path.dirname(os.path.abspath(__file__))
cert_path = os.path.join(base_path, 'certs')

ssl = {
    'certfile': os.path.join(cert_path, 'client.pem'),
    'keyfile': os.path.join(cert_path, 'client_key.pem'),
    'ca_certs': os.path.join(cert_path, 'root.pem')
}


def _connect(hostname):
    """Helper function to manage websocket connection"""

    url = f"wss://{hostname}:4747/app/"

    ws = create_connection(
        url=url,
        header={'X-Qlik-User': 'UserDirectory=internal; UserId=sa_engine'},
        sslopt={**ssl}
    )

    # consume connection info
    _ = ws.recv()

    return ws


def _communicate(ws, message: dict) -> dict:
    """Sends a JSON-formatted python dict on the supplied websocket connection"""
    ws.send(json.dumps(message))
    return json.loads(ws.recv())


def get_app_list(host: str):

    # get full document list
    websocket = _connect(host)
    msg = {
        "method": "GetDocList",
        "handle": -1,  # global context
        "params": []
    }
    response = _communicate(websocket, msg)
    websocket.close()

    # parse and filter application list
    return response['result']['qDocList']


def stale_apps(app_list: list, days_stale=180, min_mb=1, include_published=False) -> list:
    # set reload time threshold
    threshold = dt.utcnow() - timedelta(days=days_stale)

    apps = []
    for doc in app_list:

        # filter on include_published option
        if doc['qMeta']['published'] in (False, include_published):

            # parse reload time
            qLastReloadTime = doc.get('qLastReloadTime', None)
            if qLastReloadTime:
                reload_time = dt.strptime(qLastReloadTime, '%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                reload_time = dt.fromtimestamp(0)

            # match threshold
            if reload_time < threshold:

                # parse size
                size = round(doc['qFileSize'] / 1024 / 1024, 2)

                # skip files that are below minimum size. probably already empty.
                if min_mb < size:
                    # add stale doc to list
                    apps.append({
                        'name': doc['qDocName'],
                        'id': doc['qDocId'],
                        'size_mb': round(doc['qFileSize'] / 1024 / 1024, 2),
                        'last_reload': reload_time
                    })

    return apps


def drop_data_from_app(host: str, doc_id: str):
    """Creates a websocket, opens an app without data, saves app to disk and closes websocket."""

    websocket = _connect(host)

    # open document without data
    msg = {
        'handle': -1,
        'method': 'OpenDoc',
        "params": {
            'qDocName': doc_id,
            'qNoData': True
        }
    }

    response = _communicate(websocket, msg)

    # save document
    if response['result']['qReturn']['qType'] == 'Doc':
        msg = {
            'handle': response['result']['qReturn']['qHandle'],
            'method': 'DoSave',
            'params': {
                'qFileName': ''
            }
        }

        # consume status from save
        response = _communicate(websocket, msg)

    # close the socket
    websocket.close()

    return response


def write_stale_apps_to_csv(apps: list) -> None:
    """Creates a CSV file from list of stale apps"""
    ts = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
    filename = f"stale_apps_{ts}.csv"

    data = [
        {
            'name': a['name'],
            'id': a['id'],
            'size_mb': a['size_mb'],
            'last_reload': a['last_reload'].strftime('%Y-%m-%d %H:%M:%S')  # can't dump datetime object to disk
        } for a in apps
    ]

    with open(os.path.join(base_path, filename), 'w+', encoding='utf-8') as f:
        writer = csv.DictWriter(f, ['name', 'id', 'size_mb', 'last_reload'], quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        writer.writerows(data)

    return None


if __name__ == '__main__':

    # create parser
    parser = argparse.ArgumentParser(
        prog='Qlik Sense Data Retention Controller',
        usage='%(prog)s [options]',
        description='Drops stale data from unpublished Qlik Sense apps.',
        epilog='Happy dumping!'
    )

    # parse args
    parser.add_argument('-host',
                        action='store',
                        required=True,
                        type=str,
                        help='URL to the Qlik Sense server.')

    parser.add_argument('-d',
                        '--days',
                        action='store',
                        type=int,
                        default=180,
                        help='Days since reload threshold to consider an app stale')

    parser.add_argument('-mb',
                        '--min',
                        action='store',
                        type=float,
                        default=1.0,
                        help='Minimum filesize (mb) to be considered. Default is 1.')

    parser.add_argument('-ip',
                        '--include-published',
                        action='store_true',
                        help='Include published applications. Default is False.')

    parser.add_argument('-r',
                        '--report',
                        action='store_true',
                        help='Reports list of found apps to disk. Default is False.')

    parser.add_argument('-t',
                        '--truncate',
                        action='store_true',
                        help='Truncates data from found apps. Default is False.')

    args = parser.parse_args()

    servername = args.host

    # call server for complete app list
    applications = get_app_list(servername)

    # parse list and filter for stale applications
    print(f"Searching for apps that are more than {args.days} days old.")
    stale_applications = stale_apps(applications,
                                    days_stale=args.days,
                                    min_mb=args.min,
                                    include_published=args.include_published)
    tot_size = round(sum([i['size_mb'] for i in stale_applications]), 1)

    print(f"Found {len(stale_applications)} applications with a total of {tot_size} MB's of data.")
    print(f"Execute with --report to write list to CSV and execute with --truncate to clear the data from them.")

    # voluntarily dump a report of applications to disk
    if args.report:
        print(f"Writing apps to CSV.")
        write_stale_apps_to_csv(stale_applications)

    # if the truncate argument isn't supplied, the script will exit gracefully here
    if not args.truncate:
        sys.exit(0)
    else:

        # walk through list of stale apps, open them without data and save them
        doc_ids = [doc['id'] for doc in stale_applications]
        for i, _id in enumerate(doc_ids):

            print(f"Truncating data from app: {_id}. {i}/{len(doc_ids)}.")
            _ = drop_data_from_app(servername, _id)

    print("Done!")
