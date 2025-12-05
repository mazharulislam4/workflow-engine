FROM python:3.12-slim

WORKDIR /app

COPY requiremnts.txt /app/requiremnts.txt

# Install Python dependencies (psycopg2-binary doesn't need compilation)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requiremnts.txt

# COPY the rest of the application
COPY . /app

# EXPOSE the port
EXPOSE 5002

# RUN the application
CMD ["python", "manage.py", "runserver", "0.0.0.0:5002"]