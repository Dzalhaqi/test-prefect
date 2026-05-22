import os
import asyncio

os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"

from prefect.workers.process import ProcessWorker

async def main():
    async with ProcessWorker(work_pool_name="Perpu") as worker_perpu, \
               ProcessWorker(work_pool_name="UU") as worker_uu:
        
        await asyncio.gather(
            worker_perpu.start(),
            worker_uu.start()
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
