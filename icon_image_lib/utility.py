#utility.py
import asyncio
import httpx
import time,os
from httpx import ConnectTimeout
import logging
#from dotenv import load_dotenv

#load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
async def create_image_task(dataset_split):
    try:
        logger.info("Attempting to create an image task")
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.post(f"{str(os.environ.get('PRODUCTAPIENDPOINT'))}/api/v1/image/create", json={"dataset_split": dataset_split})
            result = response.json()
            logger.info(f"Image task created successfully with response: {result}")
            return result
    except Exception as e:
        logger.exception(f"Failed to create image task with exception: {e}")
        raise


async def poll_task_status(task_id, timeout=1000):
    try:
        logger.info(f"Starting to poll task status for task_id: {task_id}")
        async with httpx.AsyncClient(timeout=None) as client:
            start_time = asyncio.get_event_loop().time()
            while True:
                current_time = asyncio.get_event_loop().time()
                if current_time - start_time > timeout:
                    logger.warning(f"Timeout reached for task {task_id}. Abandoning task.")
                    return {'error': 'Polling timeout reached. Task abandoned.'}
                response = await client.get(f"{os.environ.get('PRODUCTAPIENDPOINT')}/api/v1/image/poll/{task_id}")
                data = response.json()
                if data['status'] == 'Completed':
                    logger.info(f"Task {task_id} completed successfully with data: {data}")
                    return data
                elif data['status'] in ['Failed', 'Error']:
                    logger.error(f"Task {task_id} failed or encountered an error with data: {data}")
                    return {'error': 'Task failed or encountered an error'}
                else:
                    await asyncio.sleep(int(os.environ.get('POLL_INTERVAL'))) # Poll every 60 seconds
    except Exception as e:
        logger.exception(f"Exception occurred while polling task {task_id} Exception: {e}")
        raise

async def process_row(row):
    try:
        logger.info(f"Processing row: {row}")
        dataset_split = [str(row.get('brandValue')), str(row.get('searchValue'))]
        absolute_row_index = row.get('absoluteRowIndex')
        original_search_value = row.get('searchValue')

        create_response = await create_image_task(dataset_split)
        task_id = create_response.get('task_id')

        if task_id:
            logger.info(f"Task ID {task_id} received, starting to poll for completion...")
            await asyncio.sleep(int(os.environ.get('POLL_AFTER'))) # Wait for 3 minutes before polling, asynchronously

            result = await asyncio.wait_for(poll_task_status(task_id), timeout=1000)  # Example timeout

            if result:
                logger.info(f"Task {task_id} completed with result: {result}")
                return {
                    "result": result,
                    "absoluteRowIndex": absolute_row_index,
                    "originalSearchValue": original_search_value
                }
        else:
            logger.warning("Failed to create task. No task ID received.")
            return {
                "error": "Failed to start task.",
                "absoluteRowIndex": absolute_row_index,
                "originalSearchValue": original_search_value
            }
    except Exception as e:
        logger.exception("An error occurred while processing row")
        return {
            "error": str(e),
            "absoluteRowIndex": absolute_row_index,
            "originalSearchValue": original_search_value
        }