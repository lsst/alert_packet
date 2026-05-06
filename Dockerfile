FROM python:3.12-slim-bookworm as base-image

# Create a Python virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Put the latest pip and setuptools in the virtualenv
RUN pip install --upgrade --no-cache-dir pip setuptools wheel

COPY dist/ /dist/

# Install pre-built wheel (avoids needing git at build time)
RUN pip install --no-cache-dir /dist/*.whl

ENTRYPOINT ["sh", "-c"]
CMD "syncAllSchemasToRegistry.py --help"
