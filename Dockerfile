# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the specified directories and files into the container at /app
COPY icon_image_lib/ icon_image_lib/
COPY temp_files/ temp_files/
COPY main.py main.py
#COPY .env .env
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run main.py when the container launches
CMD ["python", "main.py"]
