# Use official Python base image
FROM python:3.12-slim

# Set work directory
WORKDIR /app

# Install system dependencies for SQL Server ODBC
RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy project files
COPY . .

# Expose port 9000
EXPOSE 9000

# Use production env file
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]
