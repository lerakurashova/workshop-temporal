#!/usr/bin/env python
# coding: utf-8

import os
from urllib.parse import quote
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig
import requests
import yaml
from tqdm.auto import tqdm
from elasticsearch import Elasticsearch

def create_proxy_config():
    PROXY_USER = os.environ["WEBSHARE_PROXY_USER"]
    PROXY_PASS = quote(os.environ["WEBSHARE_PROXY_PASS"])
    return WebshareProxyConfig(
        proxy_username=PROXY_USER,
        proxy_password=PROXY_PASS,
        filter_ip_locations=["de", "us"],
    )

def find_podcast_videos(commit_id):
    url = f'https://raw.githubusercontent.com/DataTalksClub/datatalksclub.github.io/{commit_id}/_data/events.yaml'

    content = requests.get(url).content
    events_data = yaml.load(content, yaml.SafeLoader)

    podcasts = [d for d in events_data if (d.get('type') == 'podcast') and (d.get('youtube'))]

    print(f"Found {len(podcasts)} podcast videos")

    videos = []

    for podcast in podcasts:
        _, video_id = podcast['youtube'].split('watch?v=')
   
        # Skip problematic videos
        if video_id in ['FRi0SUtxdMw', 's8kyzy8V5b8']:
            continue

        videos.append({
            'title': podcast['title'],
            'video_id': video_id
        })

    print(f"Will process {len(videos)} videos")
    return videos


def format_timestamp(seconds: float) -> str:
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours == 0:
        return f"{minutes}:{secs:02}"
    return f"{hours}:{minutes:02}:{secs:02}"

def make_subtitles(transcript) -> str:
    lines = []

    for entry in transcript:
        ts = format_timestamp(entry.start)
        text = entry.text.replace('\n', ' ')
        lines.append(ts + ' ' + text)

    return '\n'.join(lines)

def workflow (commit_id: str):

    # network 
    videos = find_podcast_videos(commit_id)

    # many things can go wrong here 
    ytt_api = YouTubeTranscriptApi(proxy_config=create_proxy_config())

    es = Elasticsearch("http://localhost:9200")

    for video in tqdm(videos):
        video_id = video['video_id']
        video_title = video['title']

        # network 
        if es.exists(index='podcasts', id=video_id):
            print(f'already processed {video_id}')
            continue

        transcript = ytt_api.fetch(video_id)
        subtitles = make_subtitles(transcript)

        doc = {
            "video_id": video_id,
            "title": video_title,
            "subtitles": subtitles
        }

        # network 
        es.index(index="podcasts", id=video_id, document=doc)

