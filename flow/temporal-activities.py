import requests
import yaml
import logging
from youtube_transcript_api import YouTubeTranscriptApi
import os
from urllib.parse import quote
from youtube_transcript_api.proxies import WebshareProxyConfig
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)

def list_podcast_videos(commit_id: str):
    url = f"https://raw.githubusercontent.com/DataTalksClub/datatalksclub.github.io/{commit_id}/_data/events.yaml"

    r = requests.get(url, timeout=30)
    r.raise_for_status()

    events_data = yaml.safe_load(r.text) #YAML parsing - YAML is text, not bytes.

    podcasts = [d for d in events_data if (d.get("type") == "podcast") and (d.get("youtube"))]
    logger.info(f"Found {len(podcasts)} podcast videos")

    videos = []
    for podcast in podcasts:
        if "watch?v=" not in podcast["youtube"]:
            continue
        video_id = podcast["youtube"].split("watch?v=", 1)[1].split("&", 1)[0]
       
        # Skip problematic videos
        if video_id in ["FRi0SUtxdMw", "s8kyzy8V5b8"]:
            continue

        videos.append({"title": podcast["title"], "video_id": video_id})

    logger.info(f"Will process {len(videos)} videos")
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
        ts = format_timestamp(entry["start"])
        text = entry["text"].replace("\n", " ")
        lines.append(f"{ts} {text}")
    return "\n".join(lines)

def create_proxy_config():
    PROXY_USER = os.environ["WEBSHARE_PROXY_USER"]
    PROXY_PASS = quote(os.environ["WEBSHARE_PROXY_PASS"])
    return WebshareProxyConfig(
        proxy_username=PROXY_USER,
        proxy_password=PROXY_PASS,
        filter_ip_locations=["de", "us"],
    )

def process_video(video_id: str, title: str) -> str:
    es = Elasticsearch("http://localhost:9200")

    if es.exists(index="podcasts", id=video_id):
        return "skipped"

    ytt_api = YouTubeTranscriptApi(proxy_config=create_proxy_config())
    transcript = ytt_api.fetch(video_id)
    subtitles = make_subtitles(transcript)

    doc = {"video_id": video_id, "title": title, "subtitles": subtitles}
    es.index(index="podcasts", id=video_id, document=doc)
    return "processed"