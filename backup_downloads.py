# def imageDownload(url, image_name, new_path, session, retry_count=1, timeout=300):
#     headers = build_headers(url)
#     start_time = time.time()

#     if '/' in image_name or '\\' in image_name:
#         logger.error("Invalid image name.")
#         return False

#     while retry_count > 0 and (time.time() - start_time) < timeout:
#         try:
#             response = session.get(url, headers=headers, stream=True, timeout=30) # Setting a timeout for each request
#             if response.status_code == 200:
#                 temp_image_path = os.path.join(new_path, f"{image_name}.temp")
#                 with open(temp_image_path, 'wb') as handle:
#                     for block in response.iter_content(1024):
#                         if not block:
#                             break
#                         handle.write(block)
                
#                 final_image_path = os.path.join(new_path, f"{image_name}.png")
#                 try:
#                     with IMG2.open(temp_image_path) as img:
#                         img.convert("RGB").save(final_image_path, 'PNG')
#                     os.remove(temp_image_path) # Cleanup temp file
#                     logger.info(f"Image downloaded and converted to PNG: {final_image_path}")
#                     return True
#                 except IOError as e:
#                     logger.error(f"Failed to convert image: {e}")
#                     os.remove(temp_image_path) # Ensure cleanup if conversion fails
#                 except Exception as e:
#                     logger.error(f"Unexpected error during image handling: {e}")
#                     if os.path.exists(temp_image_path):
#                         os.remove(temp_image_path) # Cleanup temp file for any other exceptions
#             else:
#                 logger.error(f"Failed to download image. Response: {response.status_code}, URL: {url}")
#         except Exception as exc:
#             logger.error(f"Error downloading or converting image {image_name} from {url}: {exc}")

#         retry_count -= 1

#     # In case of timeout or all retries exhausted
#     if (time.time() - start_time) >= timeout:
#         logger.error(f"Operation timed out for image {image_name} from {url}")

#     return False
# def imageDownload(url, image_name, new_path, session, retry_count=1):
#     headers = build_headers(url)
    
#     if '/' in image_name or '\\' in image_name:
#         logger.error("Invalid image name.")
#         return False

#     fallback_formats = ['png', 'jpeg', 'gif', 'bmp', 'webp', 'avif', 'tiff', 'ico']  # Expanded list of image formats

#     while retry_count > 0:
#         try:
#             response = session.get(url, headers=headers, stream=True)
#             if response.status_code == 200:
#                 content_type = response.headers.get('content-type', '')
#                 file_extension = content_type.split('/')[-1] if 'image' in content_type else fallback_formats[0]

#                 saved = False
#                 for fmt in ([file_extension] + fallback_formats):
#                     temp_image_path = os.path.join(new_path, f"{image_name}.{fmt}")
#                     with open(temp_image_path, 'wb') as handle:
#                         for block in response.iter_content(1024):
#                             if not block:
#                                 break
#                             handle.write(block)

#                     try:
#                         final_image_path = os.path.join(new_path, f"{image_name}.png")
#                         with IMG.open(temp_image_path) as img:
#                             img.convert("RGB").save(final_image_path, 'PNG')
#                         saved = True
#                         logger.info(f"Image downloaded and converted to PNG: {final_image_path}")
#                         break
#                     except IOError:
#                         print('remove image?')
#                         #os.remove(temp_image_path)

#                 if not saved:
#                     logger.error(f"Failed to identify and convert image from URL: {url}")
#                     return False
                
#                 return True
#             else:
#                 logger.error(f"Failed to download image. Response: {response.status_code}, URL: {url}")
#         except Exception as exc:
#             logger.error(f"Error downloading or converting image {image_name} from {url}: {exc}")

#         retry_count -= 1

#     return False
# def imageDownload(url, image_name, new_path, session, retry_count=3):
#     headers = build_headers(url)
    
#     if '/' in image_name or '\\' in image_name:
#         logger.error("Invalid image name.")
#         return False

#     while retry_count > 0:
#         try:
#             response = session.get(url, headers=headers, stream=True)
#             if response.status_code == 200:
#                 content_type = response.headers.get('content-type', '')
#                 # Determine the image format based on the content-type or default to 'png'
#                 image_format = content_type.split('/')[-1] if 'image' in content_type else 'png'
#                 file_extension = image_format if image_format in ['jpeg', 'png', 'gif', 'bmp', 'webp', 'avif'] else 'png'

#                 temp_image_path = os.path.join(new_path, f"{image_name}.{file_extension}")

#                 with open(temp_image_path, 'wb') as handle:
#                     for block in response.iter_content(1024):
#                         if not block:
#                             break
#                         handle.write(block)

#                 final_image_path = os.path.join(new_path, f"{image_name}.png")
#                 # Try to convert any image to PNG, regardless of its initial format
#                 try:
#                     with IMG.open(temp_image_path) as img:
#                         img.convert("RGB").save(final_image_path, 'PNG')
#                 except Exception as e:
#                     logger.error(f"Could not process image {temp_image_path} as an image: {e}")
#                     os.remove(temp_image_path)  # Remove the temp file if it's not a valid image
#                     return False

#                 if file_extension != 'png':
#                     os.remove(temp_image_path)

#                 logger.info(f"Image downloaded and converted to PNG: {final_image_path}")
#                 return True
#             else:
#                 logger.error(f"Failed to download image. Response: {response.status_code} Url: {url}")
#         except Exception as exc:
#             logger.error(f"Error downloading or converting image {image_name} from {url}: {exc}")

#         retry_count -= 1

#     return False
            
# def imageDownload(url, image_name, new_path, session):
#     timeout = 180
#     headers = {
#         "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
#         "accept-encoding": "gzip, deflate, br",
#         "accept-language": "en-US,en;q=0.9",
#         "upgrade-insecure-requests": "1",
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
#     }
    
#     if ('/' in image_name) or ('\\' in image_name):
#         print("Invalid image name.")
#         return False
    
#     try:
#         response = session.get(url, headers=headers, timeout=timeout, stream=True)
#         if not response.ok:
#             print(f"Failed to download image. Response: {response}")
#             return False
        
#         content_type = response.headers['content-type']
#         image_format = content_type.split('/')[-1]  # Extracts format from content-type
        
#         # Determine the file extension (default to .png for unrecognized formats)
#         file_extension = 'png' if image_format not in ['jpeg', 'png', 'gif', 'bmp', 'webp'] else image_format
        
#         temp_image_path = os.path.join(new_path, f"{image_name}.{file_extension}")
        
#         # Save the image temporarily in its original format
#         with open(temp_image_path, 'wb') as handle:
#             for block in response.iter_content(1024):
#                 if not block:
#                     break
#                 handle.write(block)
        
#         # Convert and save the image as PNG
#         final_image_path = os.path.join(new_path, f"{image_name}.png")
#         with IMG.open(temp_image_path) as img:
#             img.convert("RGB").save(final_image_path, 'PNG')
        
#         # Clean up the temporary file if it's different from the final format
#         if file_extension != 'png':
#             os.remove(temp_image_path)
        
#         print(f"Image downloaded and converted to PNG: {final_image_path}")
#         return True
#     except Exception as exc:
#         print(f"Error downloading or converting image {image_name} from {url}: {exc}")
#         return False
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
