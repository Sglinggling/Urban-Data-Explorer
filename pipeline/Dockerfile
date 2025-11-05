FROM python:3.11-slim
WORKDIR /app
COPY pipeline/requirements.txt ./pipeline/requirements.txt
RUN pip install --no-cache-dir -r pipeline/requirements.txt
COPY main.py ./main.py
COPY pipeline/ ./pipeline/
CMD ["python", "main.py"]
