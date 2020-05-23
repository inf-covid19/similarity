FROM python:3.8

WORKDIR /opt/services/percy

RUN pip install pipenv

COPY Pipfile* ./

RUN python -m pipenv install --deploy --system

RUN ls -la .

RUN git clone https://github.com/inf-covid19/data.git inf-covid19-data

RUN git clone https://github.com/inf-covid19/similarity.git inf-covid19-similarity

COPY . .

RUN useradd -m -U percy
RUN chown percy:percy -R .
USER percy

ENTRYPOINT docker-entrypoint.sh