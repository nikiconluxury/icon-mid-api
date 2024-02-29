from fastapi import FastAPI, BackgroundTasks
import asyncio, os, requests, threading,uuid,requests,openpyxl,uvicorn,shutil,mimetypes
from icon_image_lib.utility import process_row  # Assuming this is correctly implemented
from openpyxl import load_workbook
from PIL import Image as IMG
from openpyxl.drawing.image import Image
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Example usage
logger.info("Informational message")
logger.error("Error message")

load_dotenv()

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

def upload_file_to_space(file_src, save_as, is_public=True, content_type=None, meta=None):
    spaces_client = get_spaces_client()
    space_name = 'iconluxurygroup-s3'  # Your space name
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
        upload_url = f"{os.getenv('SPACES_ENDPOINT')}/{space_name}/{save_as}"
        print(f"Public URL: {upload_url}")
        return upload_url


from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition,Personalization,Cc,To
from base64 import b64encode
def send_email(to_emails, subject, download_url, excel_file_path):
    # Encode the URL if necessary (example shown, adjust as needed)
    # from urllib.parse import quote
    # download_url = quote(download_url, safe='')
    html_content = f"""
<html>
<body>
<div class="container">
    <p>Your file is ready for download.</p>
    <a href="{download_url}" class="download-button">Download File</a>
</div>
</body>
</html>
"""
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
@app.post("/process-image-batch/")
async def process_payload(payload: dict):
    logger.info("Received request to process image batch")
    try:
        logger.info(f"Processing started for payload: {payload}")
        rows = payload.get('rowData', [])
        provided_file_path = payload.get('filePath')
        send_to_email = payload.get('sendToEmail')
        preferred_image_method = payload.get('preferredImageMethod')
        semaphore = asyncio.Semaphore(os.environ.get('MAX_THREAD'))  # Limit concurrent tasks to avoid overloading
        loop = asyncio.get_running_loop()
        # Create a temporary directory to save downloaded images
        unique_id = str(uuid.uuid4())[:8]
        temp_images_dir, temp_excel_dir = await create_temp_dirs(unique_id)
            
        tasks = [process_with_semaphore(row, semaphore) for row in rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        if any(isinstance(result, Exception) for result in results):
            logger.error("Error occurred during image processing.")
            return {"error": "An error occurred during processing."}
        print(results)
        logger.info("Downloading images")

        
        
        
        
        clean_results = await loop.run_in_executor(ThreadPoolExecutor(), prepare_images_for_download, results)
        print(clean_results)
        logger.info("clean_results: {}".format(clean_results))
        await loop.run_in_executor(ThreadPoolExecutor(), download_all_images, clean_results, temp_images_dir)

        local_filename = os.path.join(temp_excel_dir, provided_file_path.split('/')[-1])

        logger.info("Downloading Excel from web")
        response = await loop.run_in_executor(None, requests.get, provided_file_path, {'allow_redirects': True, 'timeout': 60})
        if response.status_code != 200:
            logger.error(f"Failed to download file: {response.status_code}")
            return {"error": "Failed to download the provided file."}
        with open(local_filename, "wb") as file:
            file.write(response.content)
        
        logger.info("Writing images to Excel")
        await loop.run_in_executor(ThreadPoolExecutor(), write_excel_image, local_filename, temp_images_dir, preferred_image_method)
        
        logger.info("Uploading file to space")
        #public_url = upload_file_to_space(local_filename, local_filename, is_public=True)
        public_url = await loop.run_in_executor(ThreadPoolExecutor(), upload_file_to_space, local_filename, local_filename)
        logger.info("Sending email")
        send_email(send_to_email, 'Your File Is Ready', public_url, local_filename)
        
        logger.info("Cleaning up temporary directories")
        await cleanup_temp_dirs([temp_images_dir, temp_excel_dir])
        
        logger.info("Processing completed successfully")
        return {"message": "Processing completed successfully.", "results": results, "public_url": public_url}

    except Exception as e:
        logger.exception("An unexpected error occurred during processing.")
        return {"error": "An unexpected error occurred during processing."}

async def process_with_semaphore(row, semaphore):
    async with semaphore:
        return await process_row(row)  # Assuming process_row is an async function you've defined elsewhere

def prepare_images_for_download(results):
    images_to_download = []

    for package in results:
        # Ensure the 'result' key is available and its 'status' is 'Completed'.
        if package.get('result', {}).get('status') == 'Completed':
            # Iterate over each 'result' entry if it exists and is a list.
            for result_entry in package.get('result', {}).get('result', []):
                # Check if the entry is 'Completed' and contains a 'result' key with a URL.
                if result_entry.get('status') == 'Completed' and isinstance(result_entry.get('result'), dict):
                    result_data = result_entry.get('result')
                    url = result_data.get('url')
                    if url:  # Ensure the URL is not None or empty.
                        images_to_download.append((package.get('absoluteRowIndex'), url))

    if not images_to_download:
        raise Exception("No valid image URLs found in the results")

    return images_to_download


def download_all_images(data, save_path):
    s = requests.Session()
    #s.mount('https://', HTTPAdapter(pool_connections=1, pool_maxsize=2))
    threads = []
    for item in data:
        logger.info(f"Downloading image: {item[1]}")
        image_url = item[1].strip("[]'\"")  # Remove the brackets and quotes
        input_sku = item[0]  # Grab the input SKU
        thread = threading.Thread(target=imageDownload, args=(str(image_url), str(input_sku), save_path, s))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
def imageDownload(url, image_name, new_path, session):
    timeout = 120
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0"
    }
    
    if ('/' in image_name) or ('\\' in image_name):
        print("Invalid image name.")
        return False
    
    try:
        response = session.get(url, headers=headers, timeout=timeout, stream=True)
        if not response.ok:
            print(f"Failed to download image. Response: {response}")
            return False
        
        content_type = response.headers['content-type']
        image_format = content_type.split('/')[-1]  # Extracts format from content-type
        
        # Determine the file extension (default to .png for unrecognized formats)
        file_extension = 'png' if image_format not in ['jpeg', 'png', 'gif', 'bmp', 'webp'] else image_format
        
        temp_image_path = os.path.join(new_path, f"{image_name}.{file_extension}")
        
        # Save the image temporarily in its original format
        with open(temp_image_path, 'wb') as handle:
            for block in response.iter_content(1024):
                if not block:
                    break
                handle.write(block)
        
        # Convert and save the image as PNG
        final_image_path = os.path.join(new_path, f"{image_name}.png")
        with IMG.open(temp_image_path) as img:
            img.convert("RGB").save(final_image_path, 'PNG')
        
        # Clean up the temporary file if it's different from the final format
        if file_extension != 'png':
            os.remove(temp_image_path)
        
        print(f"Image downloaded and converted to PNG: {final_image_path}")
        return True
    except Exception as exc:
        print(f"Error downloading or converting image {image_name} from {url}: {exc}")
        return False
# def imageDownload(url, imageName, newpath, s):
#     timeout = 120
#     headers = {
#         "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
#         "accept-encoding": "gzip, deflate, br",
#         "accept-language": "en-US,en;q=0.9",
#         "upgrade-insecure-requests": "1",
#         "user-agent": "Mozilla/5.0"
#     }
#     if ('/' in imageName) or ('\\' in imageName):
#         return False
#     try:
#         response = s.get(url, headers=headers, timeout=timeout, stream=True)
#         if not response.ok:
#             print(response)
#             return
#         image_format = 'png' if ".webp" not in url else 'webp'
#         image_path = os.path.join(newpath, f"{imageName}.{image_format}")
#         with open(image_path, 'wb') as handle:
#             for block in response.iter_content(1024):
#                 if not block:
#                     break
#                 handle.write(block)
#         if image_format == 'webp':
#             im = Image.open(image_path).convert("RGB")
#             im.save(os.path.join(newpath, f"{imageName}.png"), 'png')
#     except Exception as exc:
#         print(f"Error downloading image {imageName} from {url}: {exc}")

def verify_png_image_single(image_path):
    try:
        img = IMG.open(image_path)
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

# def verify_png_image_single(image_path):
#         try:
#             img = IMG.open(image_path)
#             img.getdata()[0]
#         except OSError as osexfc:
#             # ! logging.error("IMAGE verify ERROR: %s " % osexfc)
#             print(osexfc)
#         imageSize = os.path.getsize(image_path)
#         print("IMAGE SIZE " + str(imageSize))

#         if imageSize < 3000:
#             print("File Corrrupted")
#             return False

#         # try:
#         # crop_extra_background_space(image_path)
#         # except:
#         # print("could not be cropped")
#         # ! logging.error("could not be cropped")
#         # ! logging.error(image_path)
#         try:
#             resize_image(image_path)
#         except:
#             print("could not be resized")
#             # !  logging.error(image_path)
#         return True
# 
def resize_image(image_path):
    try:
        img = IMG.open(image_path)
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
def write_excel_image(local_filename, temp_dir, preferred_image_method):
    # Load the workbook and select the active worksheet
    wb = load_workbook(local_filename)
    ws = wb.active

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
            wb.save(local_filename)
            logging.info(f'Image saved at {anchor}')
        logging.warning('Inserting image skipped due to verify_png_image_single failure.')    
    # Finalize changes to the workbook
    logging.info('Finished processing all images.')

if __name__ == "__main__":
    logger.info("Starting Uvicorn server")
    uvicorn.run("main:app", port=8000, host='0.0.0.0', reload=True)
    #uvicorn.run("main:app", port=8000, host='0.0.0.0')
