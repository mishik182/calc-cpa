FROM python:3.10-slim-buster

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN pytest --cov=app

CMD ["/bin/bash", "-c", "alembic upgrade head && python run.py"]