FROM python:3.12

WORKDIR /code

COPY ./src/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . /code

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]