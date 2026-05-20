FROM python:3.12-alpine

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN apk add --no-cache \
    curl \
    tzdata

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN chmod +x /app/healthcheck.sh

RUN addgroup -S app && adduser -S app -G app

RUN mkdir -p /app/data && chown -R app:app /app

USER app

CMD ["python3", "/app/app.py"]
