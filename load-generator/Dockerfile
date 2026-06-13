FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/

EXPOSE 8000
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "src/generator.py"]
CMD ["--rate", "100", "--fraud-pct", "5"]
