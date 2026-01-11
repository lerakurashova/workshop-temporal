from datetime import timedelta
from temporalio import workflow

# import your activity functions (registered in your worker)
from temporal_activities import list_podcast_videos, process_video
@workflow.defn
class PodcastWorkflow:
    @workflow.run
    async def run(self, commit_id: str) -> dict:
        videos = await workflow.execute_activity(
            list_podcast_videos,
            commit_id,
            start_to_close_timeout=timedelta(minutes=2),
        )

        total = len(videos)
        processed = skipped = failed = 0

        for i, video in enumerate(videos, start=1):
            video_id = video["video_id"]
            title = video["title"]

            # progress log every 10, plus first/last
            if i == 1 or i == total or i % 10 == 0:
                workflow.logger.info("Progress %s/%s (last=%s)", i, total, video_id)

            try:
                status = await workflow.execute_activity(
                    process_video,
                    video_id,
                    title,
                    start_to_close_timeout=timedelta(minutes=3),
                )
                if status == "processed":
                    processed += 1
                elif status == "skipped":
                    skipped += 1

            except Exception:
                failed += 1
                workflow.logger.exception("Failed video_id=%s", video_id)

        return {
            "total": total,
            "processed": processed,
            "skipped": skipped,
            "failed": failed,
        }