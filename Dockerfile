FROM python:3.8

WORKDIR /opt/services/percy

RUN pip install pipenv

COPY Pipfile* ./

RUN pipenv install --deploy --system && \
    git clone https://github.com/inf-covid19/data.git inf-covid19-data && \
    git clone https://github.com/inf-covid19/similarity-data.git inf-covid19-similarity-data

COPY . .

COPY docker-entrypoint.sh /

RUN useradd -m -U percy && chown percy:percy -R .

USER percy

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD gunicorn -t 300 --bind 0.0.0.0:$PORT server:app
