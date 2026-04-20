## GEOS-CF Virtualizarr Data Pipeline

This pipeline creates and manages Virtualizarr/Icechunk stores for GEOS-CF (Goddard Earth Observing System Composition Forecasting) data on AWS. It is based on the [virtualizarr-data-pipelines](https://github.com/NASA-IMPACT/virtualizarr-data-pipelines) template and provides scalable infrastructure for processing and concatenating GEOS-CF archival files into virtual datasets.

[![Architecture](./docs/architecture.png)](./docs/architecture.png)

### Configuration :wrench:
The pipeline uses a strongly-typed [settings module](./cdk/settings.py) to configure deployment parameters like bucket names and SNS topics. Settings can be overridden using a `.env` file (see [.env.sample](.env.sample) for an example).

For preproduction/production routing:
- Lambdas use `ICECHUNK_BUCKET` and `ICECHUNK_PREFIX`
- A single SQS-driven processor routes each file key into one of two stores under the configured prefix:
	- `preprod/` for keys with dates before `2019-11-29T12:30:00Z`
	- `prod/` for keys at or after `2019-11-29T12:30:00Z`
- Routing is derived from the file key date (for example `.../Y2019/M11/D29/...`)
- Accepted message payloads can provide either `url` (single file) or `urls` (list of files)

### Development :hammer:
#### Set up the development environment
```
./scripts/setup.sh
```

#### Run tests
```
uv run pytest
```

#### Review infrastructure before deploying
```
uv run --env-file .env cdk synth
```

#### Deploy the CDK infrastructure
```
uv run --env-file .env cdk deploy
```
