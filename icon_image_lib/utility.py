# utility.py

import asyncio
import httpx
import time
from httpx import ConnectTimeout
import asyncio

async def create_image_task(dataset_split):
    async with httpx.AsyncClient() as client:
        response = await client.post("https://supreme-space-waddle-q777pqp5gpwwc6645-8080.app.github.dev/api/v1/image/create", json={"dataset_split": dataset_split})
        return response.json()


async def poll_task_status(task_id, timeout=99999999):
    async with httpx.AsyncClient() as client:
        start_time = asyncio.get_event_loop().time()
        while True:
            current_time = asyncio.get_event_loop().time()
            if current_time - start_time > timeout:
                print(f"Timeout reached for task {task_id}. Abandoning task.")
                return {'error': 'Polling timeout reached. Task abandoned.'}
            response = await client.get(f"https://supreme-space-waddle-q777pqp5gpwwc6645-8080.app.github.dev/api/v1/image/poll/{task_id}")
            data = response.json()
            if data['status'] == 'Completed':
                if isinstance(data.get('result'), list):
                    for result in data['result']:
                        if result['status'] == 'Completed' and result.get('result'):
                            return result['result']
                elif data.get('result') and data['result']['status'] == 'Completed':
                    return data['result']
            elif data['status'] in ['Failed', 'Error']:
                return {'error': 'Task failed or encountered an error'}
            else:
                await asyncio.sleep(60)  # Poll every 1 second



# async def process_row(row):
#     try:
#         print(f"Processing row: {row}")
        
#         dataset_split = [row.get('brandValue'), row.get('searchValue')]
#         absolute_row_index = row.get('absoluteRowIndex')
#         original_search_value = row.get('searchValue')
#         create_response = await create_image_task(dataset_split)
#         task_id = create_response.get('task_id')
#         if task_id:
#             print(f"Task ID received: {task_id}. Starting to poll for completion...")
#             await asyncio.sleep(5)  # Wait for 5 seconds before polling, asynchronously
#             try:
#                 result = await asyncio.wait_for(poll_task_status(task_id), timeout=120)  # Wait for a maximum of 2 minutes
#                 if result:
#                     print(f"Task {task_id} completed with result: {result}")
#                     return result
                
#             except asyncio.TimeoutError:
#                 print(f"Polling timeout for task {task_id}. Task abandoned.")
#                 return {"error": "Polling timeout. Task abandoned."}
#         else:
#             print("Failed to create task. No task ID received.")
#             return {"error": "Failed to start task."}
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         return {"error": str(e)}
async def process_row(row):
    try:
        print(f"Processing row: {row}")
        
        # Extracting the necessary information from the row
        dataset_split = [row.get('brandValue'), row.get('searchValue')]
        absolute_row_index = row.get('absoluteRowIndex')
        original_search_value = row.get('searchValue')

        # Attempting to create an image task
        create_response = await create_image_task(dataset_split)
        task_id = create_response.get('task_id')

        if task_id:
            print(f"Task ID received: {task_id}. Starting to poll for completion...")
            await asyncio.sleep(180)  # Wait for 5 seconds before polling, asynchronously

            # try:
                # Polling the task status with a timeout
            result = await asyncio.wait_for(poll_task_status(task_id), timeout=99999999)  # Timeout after 2 minutes
                
            if result:
                print(f"Task {task_id} completed with result: {result}")
                    # Including absolute row index and original search value in the result
                return {
                        "result": result,
                        "absoluteRowIndex": absolute_row_index,
                        "originalSearchValue": original_search_value
                    }
                
            # except asyncio.TimeoutError:
            #     print(f"Polling timeout for task {task_id}. Task abandoned.")
            #     return {
            #         "error": "Polling timeout. Task abandoned.",
            #         "absoluteRowIndex": absolute_row_index,
            #         "originalSearchValue": original_search_value
            #     }
        else:
            print("Failed to create task. No task ID received.")
            return {
                "error": "Failed to start task.",
                "absoluteRowIndex": absolute_row_index,
                "originalSearchValue": original_search_value
            }
    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            "error": str(e),
            "absoluteRowIndex": absolute_row_index,
            "originalSearchValue": original_search_value
        }
