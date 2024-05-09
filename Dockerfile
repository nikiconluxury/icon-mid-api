# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
# Doing this before copying the entire application allows us to cache the installed dependencies layer
# and not reinstall them on every build unless requirements.txt changes
RUN pip install -r requirements.txt
#pip install --upgrade pip && \
    #pip install --no-cache-dir -r requirements.txt
#RUN apt-get install unixodbc

# Now copy the rest of the application into the container
COPY icon_image_lib/ icon_image_lib/
COPY main.py .
COPY install_sql_server.sh .
RUN apt-get -y update; apt-get -y install curl
RUN apt-get update && apt-get install -y lsb-release && apt-get clean all
RUN yes | apt-get install unixodbc
RUN apt-get update



RUN chmod +x install_sql_server.sh
RUN bash install_sql_server.sh
RUN apt-get update
RUN odbcinst -j
RUN DD_API_KEY="228af60e51d9413d70070b1162bee174" DD_SITE="datadoghq.com" bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script_agent7.sh)"
RUN datadog-agent status
# Make port 8000 available to the world outside this container
EXPOSE 8080

# Run main.py when the container launches
CMD ["python", "main.py"]
