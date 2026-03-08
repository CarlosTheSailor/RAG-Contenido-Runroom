FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8000

# Coolify can override this command to run CLI jobs with the same image.
CMD ["sh", "-c", "uvicorn src.interfaces.http.app:app --host ${HOST:-0.0.0.0} --port ${PORT:-8000}"]
