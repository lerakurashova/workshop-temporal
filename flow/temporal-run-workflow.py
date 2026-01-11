import asyncio
from temporalio.client import Client
from temporal-workflow import PodcastWorkflow

TASK_QUEUE = "podcast_transcript_task_queue"

async def run_workflow():
    client = await Client.connect("localhost:7233")

    commit_id = "187b7d056a36d5af6ac33e4c8096c52d13a078a7"

    result = await client.execute_workflow(
        PodcastWorkflow.run,
        args=(commit_id, ),
        id="podcast-workflow-1",
        task_queue=TASK_QUEUE,
    )
    print(result)

if __name__ == "__main__":
    asyncio.run(run_workflow())