import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from temporal-workflow import PodcastWorkflow
from temporal-activities import list_podcast_videos, process_video

TASK_QUEUE = "podcast_transcript_task_queue"

async def run_worker():
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[PodcastWorkflow],
        activities=[list_podcast_videos, process_video],
    )

    await worker.run()

if __name__ == "__main__":
    asyncio.run(run_worker())
