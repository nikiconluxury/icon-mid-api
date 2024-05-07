from fastapi import FastAPI, BackgroundTasks
import asyncio, os,threading,uuid,requests,openpyxl,uvicorn,shutil,mimetypes,time
from icon_image_lib.utility import process_row  # Assuming this is correctly implemented
from openpyxl import load_workbook
from PIL import Image as IMG2
from PIL import UnidentifiedImageError
from openpyxl.drawing.image import Image
from openpyxl.styles import PatternFill
import datetime
import boto3
import logging
from io import BytesIO
from openpyxl.utils import get_column_letter
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition,Personalization,Cc,To
from base64 import b64encode
import aiohttp
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient, ExponentialRetry
#logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# Example usage
logger.info("Informational message")
logger.error("Error message")

#from dotenv import load_dotenv
#load_dotenv()

def get_spaces_client():
    logger.info("Creating spaces client")
    session = boto3.session.Session()
    client = session.client('s3',
                            region_name='nyc3',
                            endpoint_url=os.getenv('SPACES_ENDPOINT'),
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    logger.info("Spaces client created successfully")
    return client

def upload_file_to_space(file_src, save_as, is_public, content_type, meta=None):
    spaces_client = get_spaces_client()
    space_name = 'iconluxurygroup-s3'  # Your space name
    print('Content Type')
    print(content_type)
    if not content_type:
        content_type_guess = mimetypes.guess_type(file_src)[0]
        if not content_type_guess:
            raise Exception("Content type could not be guessed. Please specify it directly.")
        content_type = content_type_guess

    extra_args = {'ACL': 'public-read' if is_public else 'private', 'ContentType': content_type}
    if meta:
        extra_args['Metadata'] = meta

    spaces_client.upload_file(
        Filename=file_src,
        Bucket=space_name,
        Key=save_as,
        ExtraArgs=extra_args
    )
    print(f"File uploaded successfully to {space_name}/{save_as}")
    # Generate and return the public URL if the file is public
    if is_public:
        upload_url = f"{str(os.getenv('SPACES_ENDPOINT'))}/{space_name}/{save_as}"
        print(f"Public URL: {upload_url}")
        return upload_url



def send_email(to_emails, subject, download_url, excel_file_path,execution_time,message="Total Rows:\nFilename:\nBatch ID:\nLocation:\nUploaded File:"):
    # Encode the URL if necessary (example shown, adjust as needed)
    # from urllib.parse import quote
    # download_url = quote(download_url, safe='')
    execution_time_timedelta = datetime.timedelta(seconds=execution_time)



    message_with_breaks = message.replace("\n", "<br>")

    html_content = f"""
<html>
<body>
<div class="container">
    <p>Your file is ready for download.</p>
    <p>Total Elapsed Time: {str(execution_time_timedelta)}</p>
    <p>Message details:<br>{message_with_breaks}</p>
    <a href="{download_url}" class="download-button">Download File</a>
    <p>API Live View: <a href="http://159.223.162.238:5555/workers">Live Tasks</a></p>
    <p>Beta:v2.4</p>
</div>
</body>
</html>
"""




#     html_content = f"""
# <html>
# <body>
# <div class="container">
#     <p>Your file is ready for download.</p>
#     <p>Total Elapsed Time: {str(execution_time_timedelta)}</p>
#     <a href="{download_url}" class="download-button">Download File</a>
# </div>
# </body>
# </html>
# """
    message = Mail(
        from_email='distrotool@iconluxurygroup.com',
        subject=subject,
        html_content=html_content
    )
    # Read and encode the Excel file
    with open(excel_file_path, 'rb') as f:
        excel_data = f.read()
    encoded_excel_data = b64encode(excel_data).decode()

    attachment = Attachment(
        FileContent(encoded_excel_data),
        FileName(excel_file_path.split('/')[-1]),
        FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        Disposition('attachment')
    )
    message.attachment = attachment
    
    cc_recipient = 'notifications@popovtech.com'
    personalization = Personalization()
    personalization.add_cc(Cc(cc_recipient))
    personalization.add_to(To(to_emails))
    message.add_personalization(personalization)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e)
        #raise
        
def send_message_email(to_emails, subject,message):
    message_with_breaks = message.replace("\n", "<br>")

    html_content = f"""
<html>
<body>
<div class="container">
    <!-- Use the modified message with <br> for line breaks -->
    <p>Message details:<br>{message_with_breaks}</p>
    <p>API Live View: <a href="http://159.223.162.238:5555/workers">Live Tasks</a></p>
    <p>Beta:v2.4</p>
</div>
</body>
</html>
"""
    message = Mail(
        from_email='distrotool@iconluxurygroup.com',
        subject=subject,
        html_content=html_content
    )
    
    cc_recipient = 'notifications@popovtech.com'
    personalization = Personalization()
    personalization.add_cc(Cc(cc_recipient))
    personalization.add_to(To(to_emails))
    message.add_personalization(personalization)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e)
        #raise
        
        
async def create_temp_dirs(unique_id):
    loop = asyncio.get_running_loop()  # Get the current loop directly
    base_dir = os.path.join(os.getcwd(), 'temp_files')
    temp_images_dir = os.path.join(base_dir, 'images', unique_id)
    temp_excel_dir = os.path.join(base_dir, 'excel', unique_id)

    await loop.run_in_executor(None, lambda: os.makedirs(temp_images_dir, exist_ok=True))
    await loop.run_in_executor(None, lambda: os.makedirs(temp_excel_dir, exist_ok=True))

    return temp_images_dir, temp_excel_dir

async def cleanup_temp_dirs(directories):
    loop = asyncio.get_running_loop()  # Get the current loop directly
    for dir_path in directories:
        await loop.run_in_executor(None, lambda dp=dir_path: shutil.rmtree(dp, ignore_errors=True))



app = FastAPI()



async def process_image_batch(payload: dict):
    start_time = time.time()
    # Your existing logic here
    # Include all steps from processing start to finish,
    # such as downloading images, writing images to Excel, etc.

    logger.info(f"Processing started for payload: {payload}")
    rows = payload.get('rowData', [])
    provided_file_path = payload.get('filePath')
    logger.info("Received request to process image batch")
    file_name = provided_file_path.split('/')[-1]
    send_to_email = payload.get('sendToEmail', 'nik@iconluxurygroup.com')
    preferred_image_method = payload.get('preferredImageMethod', 'append')
    semaphore = asyncio.Semaphore(int(os.environ.get('MAX_THREAD')))  # Limit concurrent tasks to avoid overloading
    loop = asyncio.get_running_loop()
    try:

        # Create a temporary directory to save downloaded images
        unique_id = str(uuid.uuid4())[:8]
        temp_images_dir, temp_excel_dir = await create_temp_dirs(unique_id)
        local_filename = os.path.join(temp_excel_dir, file_name)
        

        await loop.run_in_executor(ThreadPoolExecutor(), send_message_email, send_to_email, f'Started {file_name}', f'Total Rows: {len(rows)}\nFilename: {file_name}\nBatch ID: {unique_id}\nLocation: {local_filename}\nUploaded File: {provided_file_path}') 

        tasks = [process_with_semaphore(row, semaphore,unique_id) for row in rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        if any(isinstance(result, Exception) for result in results):
            logger.error("Error occurred during image processing.")
            return {"error": "An error occurred during processing."}
        print(results)
        logger.info("Downloading images")     
             
        #clean_results = await loop.run_in_executor(ThreadPoolExecutor(), prepare_images_for_downloadV2, results,send_to_email)
        clean_results = await loop.run_in_executor(ThreadPoolExecutor(), prepare_images_for_downloadV2, results)
        if clean_results == []:
            send_message_email(send_to_email,f'Started {file_name}','No images found\nPlease make sure correct column values are provided\nIf api is disabled this reponse will be sent')
        print(clean_results)
        logger.info("clean_results: {}".format(clean_results))
        
        #d_complete_ = await loop.run_in_executor(ThreadPoolExecutor(), download_all_images, clean_results, temp_images_dir)
        failed_img_urls = await download_all_images(clean_results, temp_images_dir)
        
        contenttype = os.path.splitext(local_filename)[1]
        logger.info("Downloading Excel from web")
        response = await loop.run_in_executor(None, requests.get, provided_file_path, {'allow_redirects': True, 'timeout': 60})
        if response.status_code != 200:
            logger.error(f"Failed to download file: {response.status_code}")
            return {"error": "Failed to download the provided file."}
        with open(local_filename, "wb") as file:
            file.write(response.content)
        
        logger.info("Writing images to Excel")
        failed_rows = await loop.run_in_executor(ThreadPoolExecutor(), write_excel_image, local_filename, temp_images_dir, preferred_image_method)
        print(f"failed rows: {failed_rows}")
        #if failed_rows != []:
        if failed_img_urls:
            print(failed_img_urls)
            #fail_rows_written = await loop.run_in_executor(ThreadPoolExecutor(), write_failed_img_urls, local_filename, clean_results,failed_rows)
            fail_rows_written = await loop.run_in_executor(ThreadPoolExecutor(), write_failed_downloads_to_excel, failed_img_urls,local_filename)
            logger.error(f"Failed to write images for rows: {failed_rows}")
            logger.error(f"Failed rows added to excel: {fail_rows_written})")
            
        logger.info("Uploading file to space")
        #public_url = upload_file_to_space(local_filename, local_filename, is_public=True)
        is_public = True
        public_url = await loop.run_in_executor(ThreadPoolExecutor(), upload_file_to_space, local_filename, local_filename,is_public,contenttype)  
        end_time = time.time()
        execution_time = end_time - start_time
        #await loop.run_in_executor(ThreadPoolExecutor(), send_email, send_to_email, 'Your File Is Ready', public_url, local_filename)
        if os.listdir(temp_images_dir) !=[]:
            logger.info("Sending email")
            await loop.run_in_executor(ThreadPoolExecutor(), send_email, send_to_email, f'Started {file_name}', public_url, local_filename,execution_time,'')
        #await send_email(send_to_email, 'Your File Is Ready', public_url, local_filename)
        logger.info("Cleaning up temporary directories")
        await cleanup_temp_dirs([temp_images_dir, temp_excel_dir])
        
        logger.info("Processing completed successfully")
        
        return {"message": "Processing completed successfully.", "results": results, "public_url": public_url}

    except Exception as e:
        logger.exception("An unexpected error occurred during processing: %s", e)
        await loop.run_in_executor(ThreadPoolExecutor(), send_message_email, send_to_email, f'Started {file_name}', f"An unexpected error occurred during processing.\nError: {str(e)}")
        return {"error": f"An unexpected error occurred during processing. Error: {e}"}
    
    
@app.post("/process-image-batch/")
def process_payload(background_tasks: BackgroundTasks, payload: dict):
    logger.info("Received request to process image batch")
    background_tasks.add_task(process_image_batch, payload)
    return {"message": "Processing started successfully. You will be notified upon completion."}

async def process_with_semaphore(row, semaphore,fileid):
    async with semaphore:
        return await process_row(row,fileid)  # Assuming process_row is an async function you've defined elsewhere
def highlight_cell(excel_file, cell_reference):
    workbook = openpyxl.load_workbook(excel_file)
    sheet = workbook.active
    sheet[cell_reference].fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    workbook.save(excel_file)
def write_failed_downloads_to_excel(failed_downloads, excel_file):
    if failed_downloads:
        workbook = openpyxl.load_workbook(excel_file)

        # Select the active worksheet or specify by name
        worksheet = workbook.active  # or workbook.get_sheet_by_name('SheetName')
        # Iterate over the failed rows
        for row in failed_downloads:
            url = row[0]
            row_id = row[1]
            if url:
                if url != 'None found in this filter':
                    # Write the URL to column A of the failed row
                    # Adjust the cell reference as needed (row index might need +1 depending on header row)
                    cell_reference = f"{get_column_letter(1)}{row_id}"  # Column A, row number
                    worksheet[cell_reference] = str(url)
                    highlight_cell(excel_file, cell_reference)
        workbook.save(excel_file)
        logger.info(f"Failed downloads written to Excel file: {excel_file}")
    else:
        logger.info("No failed downloads to write to Excel.")
def write_failed_img_urls(excel_file_path, clean_results, failed_rows):
    # Load the workbook
    added_rows = [] 
    workbook = openpyxl.load_workbook(excel_file_path)
    
    # Select the active worksheet or specify by name
    worksheet = workbook.active  # or workbook.get_sheet_by_name('SheetName')
    
    # Convert clean_results to a dictionary for easier lookup
    clean_results_dict = {row: url for row, url in clean_results}
    
    # Iterate over the failed rows
    for row in failed_rows:
        # Look up the URL in the clean_results_dict using the row as a key
        url = clean_results_dict.get(row)
        
        if url:
            # Write the URL to column A of the failed row
            # Adjust the cell reference as needed (row index might need +1 depending on header row)
            cell_reference = f"{get_column_letter(1)}{row}"  # Column A, row number
            worksheet[cell_reference] = str(url)
            highlight_cell(excel_file_path,cell_reference)
            added_rows.append(row)
            
        # Save the workbook
        workbook.save(excel_file_path)
    return added_rows
def prepare_images_for_downloadV2(results):
    images_to_download = []

    for package in results:
        print(package)
        if package.get('result', {}).get('url'):
            url = package.get('result').get('url')
            thumb = package.get('result').get('thumbnail')
            
            print(url)
            # Ensure the URL is not None or empty.
            if url:
                if url != 'None found in this filter':
                    print(package.get('absoluteRowIndex'))
                    images_to_download.append((package.get('absoluteRowIndex'), url,thumb))
    print(images_to_download)

    return images_to_download

def prepare_images_for_download(results,send_to_email):
    images_to_download = []

    for package in results:
        # Ensure the 'result' key is available and its 'status' is 'Completed'.
        if package.get('result', {}).get('url') == 'Completed':
            # Iterate over each 'result' entry if it exists and is a list.
            for result_entry in package.get('result', {}).get('result', []):
                # Check if the entry is 'Completed' and contains a 'result' key with a URL.
                if result_entry.get('status') == 'Completed' and isinstance(result_entry.get('result'), dict):
                    result_data = result_entry.get('result')
                    url = result_data.get('url')
                    if url:  # Ensure the URL is not None or empty.
                        images_to_download.append((package.get('absoluteRowIndex'), url))

    #if not images_to_download:
        #send_message_email(send_to_email,f'Started {file_name}','No images found in the results')
        #raise Exception("No valid image URLs found in the results")


    return images_to_download
import tldextract
from collections import Counter
def extract_domains_and_counts(data):
    """Extract domains from URLs and count their occurrences."""
    domains = [tldextract.extract(url).registered_domain for _, url,thumb in data]
    domain_counts = Counter(domains)
    return domain_counts

def analyze_data(data):
    domain_counts = extract_domains_and_counts(data)
    logger.info("Domain counts: %s", domain_counts)
    unique_domains = len(domain_counts)
    print(f"Unique Domain Len: {unique_domains}")
    pool_size = min(500, max(10, unique_domains * 2))  # Adjust as needed
    print(f"Pool size: {pool_size}")
    return pool_size

# async def download_all_images(data, save_path):
#     pool_size = analyze_data(data)  # Determine your pool size based on data analysis
#     logger.info(f"Initializing session with pool size: {pool_size}")
#     session = requests.Session()
#     retries = Retry(total=1, backoff_factor=1, status_forcelist=[502, 503, 504])
#     adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retries)
#     session.mount('http://', adapter)
#     session.mount('https://', adapter)
#     logger.info(f"Creating ThreadPoolExecutor with max_workers: {pool_size*2}")
#     with ThreadPoolExecutor(max_workers=(pool_size*2)) as executor:
#         loop = asyncio.get_running_loop()  # Updated to use get_running_loop for current loop
#         logger.info(f"Scheduling download for item: {item[1].strip("[]'\"")}")
#         futures = [
#             loop.run_in_executor(executor, imageDownload, item[1].strip("[]'\""), str(item[0]), save_path, session)
#             for item in data
#         ]
#         # Wrap each future in asyncio.wait_for to enforce a timeout
#         timeout_futures = [asyncio.wait_for(future, timeout=180) for future in futures]  # 180 seconds = 3 minutes

#         for future in asyncio.as_completed(timeout_futures):
#             try:
#                 logger.info("Awaiting future completion...")
#                 await future
#             except asyncio.TimeoutError:
#                 future.cancel()  # Attempt to cancel the future
#                 logger.error('Download task exceeded the 3-minute timeout and was aborted.')

#             except Exception as exc:
#                 logger.error(f'Task generated an exception: {exc}')
# async def download_all_images(data, save_path):
#     # Assuming analyze_data is a function that returns an integer
#     pool_size = analyze_data(data)  # Placeholder for your actual data analysis function
#     logger.info(f"Initializing session with pool size: {pool_size}")
    
#     session = requests.Session()
#     retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
#     adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retries)
#     session.mount('http://', adapter)
#     session.mount('https://', adapter)

#     logger.info(f"Session initialized with retries and backoff strategy. Pool size: {pool_size}")

#     logger.info(f"Creating ThreadPoolExecutor with max_workers: {pool_size * 2}")
#     with ThreadPoolExecutor(max_workers=(pool_size * 2)) as executor:
#         loop = asyncio.get_running_loop()
        
#         futures = []
#         for item in data:
#             item_url = item[1].strip("[]'\"")
#             logger.info(f"Scheduling download for item URL: {item_url}, ID: {item[0]}")
#             future = loop.run_in_executor(executor, imageDownload, item_url, str(item[0]), save_path, session)
#             futures.append(future)
        
#         logger.info("All download tasks have been scheduled.")
        
#         # Wrap each future in asyncio.wait_for to enforce a timeout
#         timeout_futures = [asyncio.wait_for(future, timeout=180) for future in futures]  # 180 seconds = 3 minutes
        
#         for future in asyncio.as_completed(timeout_futures):
#             try:
#                 result = await future
#                 logger.info(f"Download and processing completed with result: {result}")
#             except asyncio.TimeoutError:
#                 future.cancel()  # Attempt to cancel the future
#                 logger.error('Download task exceeded the 3-minute timeout and was aborted.')
#             except Exception as exc:
#                 logger.error(f'Task generated an exception: {exc}', exc_info=True)

def build_headers(url):
    domain_info = tldextract.extract(url)
    domain = f"{domain_info.domain}.{domain_info.suffix}"
    
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        # "Referer": "Set this if needed based on your logic"
    }
    #if domain:
        #headers["Referer"] = f"https://{domain}/"
        #print(f"Headers: {headers['Referer']}")
    # Additional dynamic header settings can go here.
    # Example for Referer (if applicable):
    # headers["Referer"] = f"https://{domain}/"
    
    return headers
def try_convert_to_png(image_path, new_path, image_name):
    logger.info(f"Attempting to convert image to PNG: {image_path}")
    try:
        with IMG2.open(image_path) as img:
            final_image_path = os.path.join(new_path, f"{image_name}.png")
            img.convert("RGB").save(final_image_path, 'PNG')
            os.remove(image_path)  # Cleanup original/temp file
            logger.info(f"Image successfully converted to PNG: {final_image_path}")
            return True
    except IOError as e:
        logger.error(f"Failed to convert image to PNG: {e}")
        return False
async def image_download(semaphore, url, thumbnail ,image_name, save_path, session, fallback_formats=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with semaphore:
        if fallback_formats is None:
            fallback_formats = ['png', 'jpeg', 'gif', 'bmp', 'webp', 'avif', 'tiff', 'ico']
        
        logger.info(f"Initiating download for URL: {url} Img: {image_name}")
        try:
            async with session.get(url,headers=headers) as response:
                logger.info(f"Requesting URL: {url} with stream=True")
                #response = session.get(url, stream=True)
                logger.info(f"Received response: {response.status } for URL: {url}")

                if response.status == 200:
                    logger.info(f"Processing content from URL: {url}")
                    data = await response.read()
                    image_data = BytesIO(data)
                    try:
                        logger.info(f"Attempting to open image stream and save as PNG for {image_name}")
                        with IMG2.open(image_data) as img:
                            final_image_path = os.path.join(save_path, f"{image_name}.png")
                            img.save(final_image_path)
                            logger.info(f"Successfully saved: {final_image_path}")
                            return True
                    except UnidentifiedImageError as e:
                        logger.error(f"Image file type unidentified, trying fallback formats for {image_name}: {e}")
                        for fmt in fallback_formats:
                            image_data.seek(0)  # Reset stream position
                            try:
                                logger.info(f"Trying to save image with fallback format {fmt} for {image_name}")
                                with IMG2.open(image_data) as img:
                                    final_image_path = os.path.join(save_path, f"{image_name}.{fmt}")
                                    img.save(final_image_path)
                                    logger.info(f"Successfully saved with fallback format {fmt}: {final_image_path}")
                                    return True
                            except Exception as fallback_exc:
                                logger.error(f"Failed with fallback format {fmt} for {image_name}: {fallback_exc}")
                else:
                    logger.error(f"Download failed with status code {response.status} for URL: {url}")
                    await thumbnail_download(semaphore, thumbnail ,image_name, save_path, session, fallback_formats=None)
                    
        except TimeoutError as exc:
            # Handle the timeout specifically
            logger.error(f"Timeout occurred while downloading {url} Image: {image_name}")
            print('timeout error inside the downlaod function')
            #await thumbnail_download(semaphore, thumbnail ,image_name, save_path, session, fallback_formats=None)
            #return False
            return exc
        except Exception as exc:
            logger.error(f"Exception occurred during download or processing for URL: {url}: {exc}", exc_info=True)
            #await thumbnail_download(semaphore, thumbnail ,image_name, save_path, session, fallback_formats=None)
            #return False
            return exc
    
async def thumbnail_download(semaphore, url,image_name, save_path, session, fallback_formats=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with semaphore:
        if fallback_formats is None:
            fallback_formats = ['png', 'jpeg', 'gif', 'bmp', 'webp', 'avif', 'tiff', 'ico']
        
        logger.info(f"Initiating download for URL: {url} Img: {image_name}")
        try:
            async with session.get(url,headers=headers) as response:
                logger.info(f"Requesting URL: {url} with stream=True")
                #response = session.get(url, stream=True)
                logger.info(f"Received response: {response.status } for URL: {url}")

                if response.status == 200:
                    logger.info(f"Processing content from URL: {url}")
                    data = await response.read()
                    image_data = BytesIO(data)
                    try:
                        logger.info(f"Attempting to open image stream and save as PNG for {image_name}")
                        with IMG2.open(image_data) as img:
                            final_image_path = os.path.join(save_path, f"{image_name}.png")
                            img.save(final_image_path)
                            logger.info(f"Successfully saved: {final_image_path}")
                            return True
                    except UnidentifiedImageError as e:
                        logger.error(f"Image file type unidentified, trying fallback formats for {image_name}: {e}")
                        for fmt in fallback_formats:
                            image_data.seek(0)  # Reset stream position
                            try:
                                logger.info(f"Trying to save image with fallback format {fmt} for {image_name}")
                                with IMG2.open(image_data) as img:
                                    final_image_path = os.path.join(save_path, f"{image_name}.{fmt}")
                                    img.save(final_image_path)
                                    logger.info(f"Successfully saved with fallback format {fmt}: {final_image_path}")
                                    return True
                            except Exception as fallback_exc:
                                logger.error(f"Failed with fallback format {fmt} for {image_name}: {fallback_exc}")
                else:
                    logger.error(f"Download failed with status code {response.status} for URL: {url}")
                    
        except TimeoutError:
            # Handle the timeout specifically
            logger.error(f"Timeout occurred while downloading {url} Image: {image_name}")
            return False
        except Exception as exc:
            logger.error(f"Exception occurred during download or processing for URL: {url}: {exc}", exc_info=True)
        return False
    
    
    
# async def download_all_images(data, save_path):
#     failed_downloads = []
#     pool_size = analyze_data(data)  # Placeholder for your actual data analysis function
#     logger.info(f"Setting up session with pool size: {pool_size}")
#
#     # Setup async session with retry policy
#     timeout = ClientTimeout(total=60)
#     retry_options = ExponentialRetry(attempts=3, start_timeout=3)
#     connector = aiohttp.TCPConnector(ssl=False)
#     async with RetryClient(raise_for_status=False, retry_options=retry_options, timeout=timeout,connector=connector) as session:
#         semaphore = asyncio.Semaphore(pool_size)
#
#         logger.info("Scheduling image downloads")
#         tasks = [
#             image_download(semaphore, str(item[1]), str(item[0]), save_path, session)
#             for item in data
#         ]
#
#         results = await asyncio.gather(*tasks, return_exceptions=True)
#
#         logger.info("Processing download results")
#         for result in results:
#             if isinstance(result, Exception):
#                 logger.error(f"Download task generated an exception: {result}")
#             else:
#                 logger.info(f"Download task completed with result: {result}")



async def download_all_images(data, save_path):
    failed_downloads = []
    pool_size = analyze_data(data)  # Placeholder for your actual data analysis function

    logger.info(f"Setting up session with pool size: {pool_size}")

    # Setup async session with retry policy
    timeout = ClientTimeout(total=60)
    retry_options = ExponentialRetry(attempts=3, start_timeout=3)
    connector = aiohttp.TCPConnector(ssl=False)

    async with RetryClient(raise_for_status=False, retry_options=retry_options, timeout=timeout, connector=connector) as session:
        semaphore = asyncio.Semaphore(pool_size)

        logger.info("Scheduling image downloads")
        tasks = [
            image_download(semaphore, str(item[1]),str(item[2]), str(item[0]), save_path, session, index)
            for index, item in enumerate(data, start=1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("Processing download results")
        for index, result in enumerate(results):
            if isinstance(result, Exception):
                #THUMBNAIL DOWNLOAD ON FAIL
                print(data[index])
                logger.error(f"Download task generated an exception: {result}")
                logger.error(f"Trying again with :{str(data[index][2])}")
                print(f"Download task generated an exception: {result}")
                print(f"Trying again with :{str(data[index][2])}")
                thumbnail_download(semaphore, str(data[index][2]),str(data[index][0]), save_path, session, fallback_formats=None)
                #THUMBNAIL DOWNLOAD ON FAIL
                failed_downloads.append((data[index][1], data[index][0]))  # Append the image URL and row ID
            else:
                logger.info(f"Download task completed with result: {result}")
                if result is False:
                    failed_downloads.append((data[index][1], data[index][0]))  # Append the image URL and row ID

    return failed_downloads
# def imageDownload(url, image_name, new_path, session, fallback_formats=['png', 'jpeg', 'gif', 'bmp', 'webp', 'avif', 'tiff', 'ico']):
#     logger.info(f"Starting download for: {url}")
#     headers = {"User-Agent": "Mozilla/5.0"}
#     logger.info(f"Sending request to download image: {url}")
#     response = session.get(url, headers=headers, stream=True)
#     logger.info(f"Response received, status code: {response.status_code}")
#     if response.status_code == 200:
#         temp_image_path = os.path.join(new_path, f"{image_name}.temp")  # Temporary file
#         logger.info(f"Writing to temporary image path: {temp_image_path}")
#         with open(temp_image_path, 'wb') as handle:
#             for block in response.iter_content(1024):
#                 if not block:
#                     break
#                 handle.write(block)
#         logger.info(f"Image downloaded: {temp_image_path}")

#         if try_convert_to_png(temp_image_path, new_path, image_name):
#             logger.info(f"Direct conversion to PNG successful for: {image_name}")
#             return True

#         logger.info(f"Direct conversion to PNG failed, attempting fallback formats for: {image_name}")
#         for fmt in fallback_formats:
#             formatted_temp_path = f"{temp_image_path}.{fmt}"
#             logger.info(f"Attempting conversion with fallback format {fmt} for: {image_name}")
#             try:
#                 os.rename(temp_image_path, formatted_temp_path)
#                 if try_convert_to_png(formatted_temp_path, new_path, image_name):
#                     logger.info(f"Conversion to PNG successful with fallback format {fmt} for: {image_name}")
#                     return True
#                 else:
#                     os.rename(formatted_temp_path, temp_image_path)
#             except Exception as e:
#                 logger.error(f"Failed during conversion attempt for format {fmt}: {e}")
#                 if os.path.exists(formatted_temp_path):
#                     logger.info(f"Cleanup temporary files for: {image_name}")
#                     os.remove(formatted_temp_path)  # Cleanup if format-specific file was created

#         if os.path.exists(temp_image_path):
#             os.remove(temp_image_path)
#         logger.error(f"All format conversion attempts failed for image: {url}")
#     else:
#         logger.error(f"Failed to download image. Response: {response.status_code}, URL: {url}")
#     return False

def verify_png_image_single(image_path):
    try:
        img = IMG2.open(image_path)
        img.verify()  # I'm using verify() method to check if it's a valid image
        logging.info(f"Image verified successfully: {image_path}")
    except Exception as e:
        logging.error(f"IMAGE verify ERROR: {e}, for image: {image_path}")
        return False

    imageSize = os.path.getsize(image_path)
    logging.debug(f"Image size: {imageSize} bytes")

    if imageSize < 3000:
        logging.warning(f"File may be corrupted or too small: {image_path}")
        return False

    try:
        resize_image(image_path)
    except Exception as e:
        logging.error(f"Error resizing image: {e}, for image: {image_path}")
        return False
    return True

def resize_image(image_path):
    try:
        img = IMG2.open(image_path)
        MAXSIZE = 145
        if img:
            h, w = img.height, img.width  # original size
            logging.debug(f"Original size: height={h}, width={w}")
            if h > MAXSIZE or w > MAXSIZE:
                if h > w:
                    w = int(w * MAXSIZE / h)
                    h = MAXSIZE
                else:
                    h = int(h * MAXSIZE / w)
                    w = MAXSIZE
            logging.debug(f"Resized to: height={h}, width={w}")
            newImg = img.resize((w, h))
            newImg.save(image_path)
            logging.info(f"Image resized and saved: {image_path}")
            return True
    except Exception as e:
        logging.error(f"Error resizing image: {e}, for image: {image_path}")
        return False               
def write_excel_image(local_filename, temp_dir,preferred_image_method):
    failed_rows = []
    # Load the workbook and select the active worksheet
    wb = load_workbook(local_filename)
    ws = wb.active
    print(os.listdir(temp_dir))
    
    # Iterate through each file in the temporary directory
    for image_file in os.listdir(temp_dir):
        image_path = os.path.join(temp_dir, image_file)
        # Extract row number or other identifier from the image file name
        try:
            # Assuming the file name can be directly converted to an integer row number
            row_number = int(image_file.split('.')[0])
            logging.info(f"Processing row {row_number}, image path: {image_path}")
        except ValueError:
            logging.warning(f"Skipping file {image_file}: does not match expected naming convention")
            continue  # Skip files that do not match the expected naming convention
        verify_image = verify_png_image_single(image_path)    
        # Check if the image meets the criteria to be added
        if verify_image:
            logging.info('Inserting image')
            img = openpyxl.drawing.image.Image(image_path)
            # Determine the anchor point based on the preferred image method
            if preferred_image_method in ["overwrite", "append"]:
                anchor = "A" + str(row_number)
                logging.info('Anchor assigned')
            elif preferred_image_method == "NewColumn":
                anchor = "B" + str(row_number)  # Example adjustment for a different method
            else:
                logging.error(f'Unrecognized preferred image method: {preferred_image_method}')
                continue  # Skip if the method is not recognized
                
            img.anchor = anchor
            ws.add_image(img)
            #wb.save(local_filename)
            logging.info(f'Image saved at {anchor}')
        else:
            failed_rows.append(row_number)
            logging.warning('Inserting image skipped due to verify_png_image_single failure.')   
    # Finalize changes to the workbook
    logging.info('Finished processing all images.')
    wb.save(local_filename)
    return failed_rows 


if __name__ == "__main__":
    logger.info("Starting Uvicorn server")
    print(os.environ)
    #uvicorn.run("main:app", port=8000, host='0.0.0.0', reload=True)
    uvicorn.run("main:app", port=8080, host='0.0.0.0')
