FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bridge.py .
EXPOSE 8000
CMD ["uvicorn","bridge:app","--host","0.0.0.0","--port","8000"]
