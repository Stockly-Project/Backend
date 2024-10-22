FROM python:3.12

WORKDIR /code

COPY ./src/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
RUN pip uninstall websocket
RUN pip install websocket-client

COPY . /code

COPY ./init.sql /docker-entrypoint-initdb.d/

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]