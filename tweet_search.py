from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import sys
import time
from urllib.parse import quote_plus, unquote_plus

from dateutil import parser
import plac
import requests
from requests_oauthlib import OAuth1


SEARCH_API = "https://api.twitter.com/1.1/search/tweets.json"


@plac.annotations(
    search_keywords=("search keywords", "positional"),
    accept_regexp_text=("accept regexp for text (default='.+')", "option", "t", str),
    reject_regexp_text=("reject regexp for text (default='^$')", "option", "r", str),
    accept_regexp_user=("accept regexp for user (default='.+')", "option", "u", str),
    reject_regexp_user=("reject regexp for user (default='^$')", "option", "v", str),
    auth_json_path=("auth json path (default=./auth.json)", "option", "a", str),
    max_pages=("max pages (default=0)", "option", "p", int),
    output_directory=("output directory (default=./)", "option", "o", Path),
)
def main(
        search_keywords=None,
        accept_regexp_text=None,
        reject_regexp_text=None,
        accept_regexp_user=None,
        reject_regexp_user=None,
        auth_json_path=None,
        max_pages=0,
        output_directory=Path('./'),
):
    config_path = output_directory / 'config.json'

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            if search_keywords is None or config['search_keywords'] == search_keywords:
                latest_tweet_id = config['latest_tweet_id']
            else:
                if search_keywords:
                    print('Search keywords changed from "{}" to "{}". Will fetch all search result.'.format(
                        config['search_condition'],
                        search_keywords,
                    ))
                latest_tweet_id = None
            if search_keywords is None:
                search_keywords = config['search_keywords']
            if accept_regexp_text is None:
                accept_regexp_text = config['accept_regexp_text']
            if reject_regexp_text is None:
                reject_regexp_text = config['reject_regexp_text']
            if accept_regexp_user is None:
                accept_regexp_user = config['accept_regexp_user']
            if reject_regexp_user is None:
                reject_regexp_user = config['reject_regexp_user']
            if auth_json_path is None:
                auth_json_path = config['auth_json_path']
    except FileNotFoundError:
        if search_keywords is None:
            raise Exception('search keywords required')
        if accept_regexp_text is None:
            accept_regexp_text = r'.+'
        if reject_regexp_text is None:
            reject_regexp_text = r'^$'
        if accept_regexp_user is None:
            accept_regexp_user = r'.+'
        if reject_regexp_user is None:
            reject_regexp_user = r'^$'
        if auth_json_path is None:
            auth_json_path = './auth.json'
        latest_tweet_id = None
    config = {
        "search_keywords": search_keywords,
        "accept_regexp_text": accept_regexp_text,
        "reject_regexp_text": reject_regexp_text,
        "accept_regexp_user": accept_regexp_user,
        "reject_regexp_user": reject_regexp_user,
        "auth_json_path": auth_json_path,
        "latest_tweet_id": latest_tweet_id,
    }
    print('search configuration:')
    for k, v in config.items():
        print('  ', k, '=', v)

    with open(auth_json_path, 'r') as f:
        auth = OAuth1(**json.load(f))

    tweets = search_tweets(
        auth,
        latest_tweet_id,
        search_keywords,
        max_pages,
    )
    if not tweets:
        return
    tweets = sorted(tweets, key=lambda t: t['id'])
    latest_tweet_id = tweets[-1]['id']

    print('apply filters')
    filtered_tweets = filter_tweets(
        tweets,
        accept_regexp_text,
        reject_regexp_text,
        accept_regexp_user,
        reject_regexp_user,
    )
    with open('tweets.txt', 'a') as f:
        for tweet in filtered_tweets:
            print_tweet(tweet, file=f)
            print_tweet(tweet, file=sys.stdout)

    config["latest_tweet_id"] = latest_tweet_id
    with open(config_path, 'w') as f:
        json.dump(
            config,
            f,
            ensure_ascii=False,
            indent=2,
        )


def request_url(parameters):
    return SEARCH_API + '?' + '&'.join([
        '{}={}'.format(k, quote_plus(str(v))) for k, v in parameters.items() if v
    ])


def search_tweets(
        auth,
        latest_tweet_id,
        search_keywords,
        max_pages,
):
    count = 0
    retry = 0
    tweets = []
    max_id = None
    while True:
        url = request_url({
            'q': search_keywords,
            'count': 100,
            'include_entities': 'false',
            'max_id': max_id,
            'since_id': latest_tweet_id,
        })
        print(count + 1, url, end='', flush=True)
        try:
            response = requests.get(url, auth=auth)
            statuses = response.json()['statuses']
            print(' ->', len(statuses), 'tweets')
        except:
            start = datetime.now() + timedelta(minutes=15)
            print('wait 15 min', start.strftime('%Y-%m-%d %H:%M:%S'))
            time.sleep(900)
            statuses = []
        if not statuses:
            retry += 1
            if retry >= 3:
                break
            continue
        retry = 0
        for tweet in statuses:
            tweet['text'] = resolve_redirects(tweet['text'])
        tweets += statuses
        count += 1
        if max_pages != 0 and count >= max_pages:
            break
        max_id = min([t['id'] for t in tweets]) - 1
    return tweets


URL_PATTERN = re.compile(r"https://t.co/[0-9a-zA-Z]+")


def resolve_redirects(text):
    result = ''
    prev = 0
    for m in URL_PATTERN.finditer(text):
        try:
            url = unquote_plus(requests.get(m.group(0), timeout=3.0).url)
            print('resolved', m.group(0), '->', url)
            result += text[prev:m.start()] + url
            prev = m.end()
        except:
            print('not resolved', m.group(0))
    result += text[prev:]
    return result


def filter_tweets(
        tweets,
        accept_regexp_text,
        reject_regexp_text,
        accept_regexp_user,
        reject_regexp_user,
):
    accept_regexp_text = re.compile(accept_regexp_text)
    reject_regexp_text = re.compile(reject_regexp_text)
    accept_regexp_user = re.compile(accept_regexp_user)
    reject_regexp_user = re.compile(reject_regexp_user)
    return [
        t for t in tweets if (
            accept_regexp_text.match(t['text']) and
            not reject_regexp_text.match(t['text']) and
            accept_regexp_user.match(t['user']['name']) and
            not reject_regexp_user.match(t['user']['name'])
        )
    ]


def print_tweet(t, file):
    print(' '.join([
        'https://twitter.com/i/web/status/' + str(t['id']),
        (parser.parse(t['created_at']) + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S'),
        t['user']['name'],
        t['text'].replace('\t', ' ').replace('\n', ' '),
    ]), file=file)


if __name__ == '__main__':
    plac.call(main)
