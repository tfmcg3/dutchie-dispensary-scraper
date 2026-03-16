FROM apify/actor-python:3.11

# Copy requirements first for Docker layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . ./

# Run the actor
CMD ["python", "-m", "src.main"]
