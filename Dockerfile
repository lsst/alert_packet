FROM python:3.9.7-buster AS base-image

# Create a Python virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Put the latest pip and setuptools in the virtualenv
RUN pip install --upgrade --no-cache-dir pip setuptools wheel

COPY . /app
WORKDIR /app

# Install package
RUN pip install --no-cache-dir .

ENTRYPOINT ["sh", "-c"]
CMD "syncLatestSchemaToRegistry.py --help"
