FROM python:3.9

WORKDIR /app
VOLUME /project
ENV DQP_PROJECT_PATH=/project

EXPOSE 8000

#Install environment
COPY dist/*_lock*.whl /app/

RUN pip install --no-cache-dir *.whl uvicorn \
    && rm -rf *.whl

COPY dist/*.whl /app/

#Install application
RUN pip install --no-cache-dir *.whl \
    && rm -rf *.whl
