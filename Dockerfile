# Use a specific, slim version of Python for a smaller and more secure image.
FROM python:3.10-slim-buster

# Set environment variables for best practices in Docker.
# 1. Prevents Python from writing .pyc files to the container.
# 2. Ensures that print() and logging output is sent directly to the terminal
#    without being buffered, which is crucial for viewing logs in real-time.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container.
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc kafkacat && rm -rf /var/lib/apt/lists/*

# The most important optimization: Install dependencies first.
# Copy only the requirements file to leverage Docker's layer caching.
# This layer will only be rebuilt if requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

# Now copy the rest of the application source code into the container.
# This layer will be rebuilt on any code change.
COPY . .

ENTRYPOINT ["/app/docker-entrypoint.sh"]