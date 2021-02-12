import requests
import os
import hashlib
import urllib
import json
from loguru import logger

from flask import Blueprint, redirect, request
urls_blueprint = Blueprint('urls', __name__,)

from .db_handler import DataBaseHandler
from .shelve_handler import ShelveHandler

def get_user():
    db_temp = ShelveHandler()
    db_dict = db_temp.read_shelve_by_keys(["db_name",
                                           "db_type",
                                           "db_path",
                                           "db_user",
                                           "db_hash"])

    dbh = DataBaseHandler(db_type=db_dict["db_type"])
    dbh.set_db_path(db_path=db_dict["db_path"])
    dbh.set_db_name(db_name=db_dict["db_name"])

    db_entry = dbh.list_user_by_hash(db_dict["db_hash"])

    del db_temp
    return db_entry

def token_to_shelf(stoken):

    db_temp = ShelveHandler()
    db_dict = db_temp.read_shelve_by_keys(["db_name",
                                           "db_type",
                                           "db_path",
                                           "db_user",
                                           "db_hash"])

    db_dict["db_strava"] = {
        "access_token": stoken["access_token"],
        "refresh_token": stoken["refresh_token"],
        "expires_at": stoken["expires_at"],
        "expires_in": stoken["expires_in"],
        "token_type": stoken["token_type"],
        "athlete_id": stoken["athlete"]["id"]
    }

    db_temp.write_shelve(db_dict)

def authorize_url():
    """Generate authorization uri"""

    db_entry = get_user()
    latest_strava = sorted(db_entry["strava"], key=lambda k: k['datetime'])[-1]
    latest_strava_client_id = latest_strava["client_id"]

    app_url = os.getenv('APP_URL', 'http://localhost')
    logger.debug(f"APP_URL={app_url}")
    params = {
        "client_id": latest_strava_client_id,
        "response_type": "code",
        "redirect_uri": f"{app_url}:5000/authorization_successful",
        "scope": "read_all,profile:read_all,activity:read_all",
        #"state": 'https://github.com/sladkovm/strava-oauth',
        "approval_prompt": "force"
    }
    values_url = urllib.parse.urlencode(params)
    base_url = 'https://www.strava.com/oauth/authorize'
    rv = base_url + '?' + values_url
    logger.debug(rv)
    return rv

@urls_blueprint.route('/')
def home():
    return "Welcome to strava-oauth"

@urls_blueprint.route('/client')
def client():
    resp.text = os.getenv('STRAVA_CLIENT_ID')

@urls_blueprint.route('/authorize')
def authorize():
    """Redirect user to the Strava Authorization page"""
    return redirect(location=authorize_url())


@urls_blueprint.route('/authorization_successful')
def authorization_successful():
    """Exchange code for a user token"""

    db_entry = get_user()
    latest_strava = sorted(db_entry["strava"], key=lambda k: k['datetime'])[-1]
    latest_strava_client_id = latest_strava["client_id"]
    latest_strava_client_secret = latest_strava["client_secret"]


    params = {
        "client_id": latest_strava_client_id,
        "client_secret": latest_strava_client_secret,
        "code": request.args.get("code"),
        "grant_type": "authorization_code"
    }
    r = requests.post("https://www.strava.com/oauth/token", params)
    logger.debug(r.text)

    client_file = "Notebooks/%s.json" % hashlib.md5(latest_strava_client_id.encode('utf-8')).hexdigest()
    print(client_file)

    if r.json().get("message") == "Bad Request":
        print("Return message")
        print(r.json())
        exit()
    else:
        print("Safe token")
        token_to_shelf(stoken=r.json())


        with open(client_file, 'w') as outfile:
            json.dump(r.json(), outfile)

    return r.json()





