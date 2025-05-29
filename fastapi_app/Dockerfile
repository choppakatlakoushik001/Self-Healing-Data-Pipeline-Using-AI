# Use a minimal base image with Python pre-installed
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all contents of fastapi_app into the container's /app directory
COPY . .

# Expose the port Uvicorn will run on
EXPOSE 8000

# Start FastAPI using Uvicorn in debug mode
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload", "--log-level", "debug"]
