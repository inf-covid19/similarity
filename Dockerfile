FROM python:3.8

ENV PORT=8080
ARG DATA=inf-covid19-data
ARG SIMILARITY_DATA=inf-covid19-similarity-data

WORKDIR /opt/services/percy

RUN pip install pipenv

COPY Pipfile* ./

RUN pipenv install --deploy --system

RUN mkdir $DATA && git -C $DATA init && git -C $DATA remote add origin https://github.com/inf-covid19/data.git && \
    mkdir $SIMILARITY_DATA && git -C $SIMILARITY_DATA init && git -C $SIMILARITY_DATA remote add origin https://github.com/inf-covid19/similarity-data.git

COPY . .

COPY docker-entrypoint.sh /

RUN useradd -m -U percy && chown percy:percy -R .

USER percy

EXPOSE $PORT

CMD gunicorn -t 300 --log-level=info --bind 0.0.0.0:$PORT percy.server:app
