Simple video index counting service based on docker-compose of indexer and api with shared sqlite database.

Usage
-----

Step 1) Build using `./build.sh`, this requires `poetry` to be installed.

Step 2) Start docker-compose: `docker-compose up --build`

Deployment
----------
Deploy using [deploy.sh](deploy.sh) and upgrade using the same script again after a `git pull`.
