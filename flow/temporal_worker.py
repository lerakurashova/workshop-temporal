import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_workflow import PodcastWorkflow
from temporal_activities import list_podcast_videos, process_video

TASK_QUEUE = "podcast_transcript_task_queue"

async def main():
    client = await Client.connect("localhost:7233")

    # Thread pool for sync activities
    executor = ThreadPoolExecutor(max_workers=20)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[PodcastWorkflow],
        activities=[list_podcast_videos, process_video],
        activity_executor=executor,
    )

    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
