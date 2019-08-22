import os
import sys

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'packages'))

import re
import json
import urllib.request
from time import sleep
from datetime import date, datetime
import boto3
import bs4
from requests_oauthlib import OAuth1Session


class Event:

    def __init__(self, **kwargs):
        self.__dict__ = kwargs

    def as_dynamodb_item(self):
        return {key: value2dynamo(self.__dict__[key]) for key in self.__dict__}
        
    def as_slack_attachment(self):
        attachment = {
            'fallback': self.title,
            'title': self.title,
            'title_link': self.url,
            'text': self.place,
            'ts': int(self.dt_start.timestamp()),
        }
        if self.thumbnail:
            attachment['thumb_url'] = self.thumbnail
        if self.community:
            attachment['author_name'] = self.community
        return attachment


def crawl_pref_events(pref, page_begin=1, from_date=date.today().isoformat()):
    """connpass のページから指定した都道府県のイベント情報をクロールする"""
    page = page_begin
    next_page = True
    events = []
    while next_page:
        url = f'https://connpass.com/search/?selectItem={pref}&prefectures={pref}&start_from={from_date}&page={page}'
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as res:
            body = res.read()
        soup = bs4.BeautifulSoup(body, 'html.parser')
        for event in soup.select('.event_list'):
            try:
                event = parse_event(event)
            except Exception as e:
                print(url)
                print(e)
            else:
                events.append(event)
        next_page = bool(soup.select_one('.to_next'))
        if next_page:
            page += 1
            sleep(1)
    return events


def parse_event(event: bs4.element.Tag):
    """イベントひとつ分の要素から情報を抜き出す"""
    url = event.select_one('.event_title a').get('href')
    community = event.select_one('.series_title')
    community = community.text if community else None
    thumbnail = event.select_one('.event_thumbnail img').get('src')
    if re.search(r'/no_image_', thumbnail):
        thumbnail = None
    return Event(id = int(re.match(r'.+/(\d+)/?', url)[1]),
                 title = event.select_one('.event_title a').text,
                 url = url,
                 dt_start = datetime.strptime(event.select_one('.dtstart .value-title').get('title'), '%Y-%m-%dT%H:%M:%S%z'),
                 dt_end = datetime.strptime(event.select_one('.dtend .value-title').get('title'), '%Y-%m-%dT%H:%M:%S%z'),
                 amount = event.select_one('.amount').text,
                 thumbnail = thumbnail,
                 community = community,
                 owner = event.select_one('.event_owner img').get('title'),
                 place = event.select_one('.event_place').text.strip())


def value2dynamo(v):
    """入力データを DynamoDB の Item フォーマットに変換する"""
    t = type(v)
    if v is None:     return {'NULL': True}
    if t == bool:     return {'BOOL': v}
    if t == str:      return {'S': v}
    if t == bytes:    return {'B': v}
    if t == int:      return {'N': str(v)}
    if t == dict:     return {'M': {k: value2dynamo(v) for k in v}}
    if t == list:     return {'L': [value2dynamo(v2) for v2 in v]}
    if t == datetime: return {'N': str(int(v.timestamp()))}
    raise NotImplementedError(t)

def post_slack(message):
    """Slack に投稿する"""
    url = os.environ.get('SLACK_INCOMING_WEBHOOK_URL')
    body = json.dumps(message).encode('ascii')
    req = urllib.request.Request(url, body)
    urllib.request.urlopen(req)

def handler(event, context):
    # Crawl
    events = crawl_pref_events('okinawa')
    # Store
    db = boto3.client('dynamodb')
    results = [db.put_item(TableName='connpass', ReturnValues='ALL_OLD', Item=event.as_dynamodb_item()) for event in events]
    # Filter
    updated = [int(r['Attributes']['id']['N']) if 'Attributes' in r else None for r in results]
    new_events = list(filter(lambda event: event.id not in updated, events))
    # Tweet
    twitter = OAuth1Session(
        client_key=os.environ.get('TWITTER_API_KEY'),
        client_secret=os.environ.get('TWITTER_API_SECRET_KEY'),
        resource_owner_key=os.environ.get('TWITTER_ACCESS_TOKEN'),
        resource_owner_secret=os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')
    )
    for event in new_events:
        twitter.post('https://api.twitter.com/1.1/statuses/update.json', params={'status': event.url})
        post_slack({'attachments': [event.as_slack_attachment()]})
        sleep(1)
