# qs-data-retention
CLI app to truncate data from unused Qlik Sense applications.
Note that this app has only been tested on the server version for Windows.

It uses Qlik Sense's Engine JSON API (websockets) to communicate with the server.

## Installation
### Certificates
Clone this repository and add exported certificates from your Qlik Sense server
to the folder `certs`. Password protected certs are not supported.

The following files are required:
* client.pem
* client_key.pem
* root.pem

### Python modules
Install the required websocket library

    pyton3 -m pip install -r requirements.txt

## Usage

    python main.py -host name.of.your.qs.server