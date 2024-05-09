FROM python:3.12-slim-bookworm

RUN pip install --upgrade pip
COPY ./requirements.txt .
RUN pip install --no-cache -r requirements.txt

COPY ./src .

CMD ["python", "./logparser.py"]
EXPOSE 5000