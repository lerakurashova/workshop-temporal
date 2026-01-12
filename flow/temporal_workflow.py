from datetime import timedelta
from temporalio import workflow
from temporalio.client import Client
import asyncio
from uuid import uuid4


with workflow.unsafe.imports_passed_through():
    from temporal_activities import list_podcast_videos, process_video

@workflow.defn
class PodcastWorkflow:
    @workflow.run
    async def run(self, commit_id: str) -> dict:
        workflow.logger.info(f"Listing podcast videos from commit_id={commit_id}...")
        
        videos = await workflow.execute_activity(
            list_podcast_videos,
            commit_id,
            start_to_close_timeout=timedelta(minutes=2),
        )

        total = len(videos)
        processed = skipped = failed = 0

        workflow.logger.info(f"Processing videos transcripts to ElasticSearch...")

        for i, video in enumerate(videos, start=1):
            video_id = video["video_id"]
            title = video["title"]

            # progress log every 10, plus first/last
            if i == 1 or i == total or i % 10 == 0:
                workflow.logger.info("Progress %s/%s (last=%s)", i, total, video_id)

            try:
                status = await workflow.execute_activity(
                    process_video,
                    args=(video_id, title),
                    start_to_close_timeout=timedelta(minutes=3),
                )

                if status == "processed":
                    processed += 1
                elif status == "skipped":
                    skipped += 1

            except Exception:
                failed += 1
                workflow.logger.exception("Failed video_id=%s", video_id)
                raise

        return {
            "status": "completed",
            "total": total,
            "processed": processed,
            "skipped": skipped,
            "failed": failed,
        }

TASK_QUEUE = "podcast_transcript_task_queue"

async def start_workflow():
    client = await Client.connect("localhost:7233")

    #commit_id = "187b7d056a36d5af6ac33e4c8096c52d13a078a7"
    commit_id = "main"

    result = await client.execute_workflow(
        PodcastWorkflow.run,
        args=(commit_id, ),
        id=f"podcast-workflow-{uuid4().hex[:8]}",
        task_queue=TASK_QUEUE,
    )

if __name__ == "__main__":
    asyncio.run(start_workflow())