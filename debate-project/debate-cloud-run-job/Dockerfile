FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt # --no-cache-dir giúp giảm kích thước image

COPY . .

# Sử dụng shell form để biến môi trường PORT được mở rộng
ENV PORT 8080
CMD flask run --host=0.0.0.0 --port=$PORT
