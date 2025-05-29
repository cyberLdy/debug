# Base Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY backend/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend files
COPY backend/ .

# Expose port
EXPOSE 7002

# Set environment variables
ENV PORT=7002
ENV HOSTNAME=0.0.0.0
ENV NODE_ENV=production

# Start the FastAPI server with auto-reload disabled in production
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7002", "--workers", "4"]