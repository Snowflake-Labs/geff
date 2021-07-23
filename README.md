# GEFF

The Generic External Function Framework (GEFF) is extensible Python Lambda code which can be called using a [Snowflake External Function](https://docs.snowflake.com/en/sql-reference/external-functions-introduction.html) that allows Snowflake operators to perform generic invocations of Call Drivers (e.g. HTTP, SMTP, XML-RPC) and either return or write responses to generic Destination Drivers (e.g. S3). This empowers them to create new pipelines in Data Infrastructure while using reviewed and standardized RBAC and interaction with Cloud Infrastructure for secrets management.

## Instructions

### Setup

```bash
git clone git@github.com:Snowflake-Labs/geff.git
python3 -m venv ./venv
source ./venv/bin/activate

pip install -r requirements-dev.txt
```

### Test

```bash
# While in the venv
python -m pytest tests/*
```

## TODO

- [x] Lambda base code
- [x] Basic tests needs environment variables to be set
- [ ] Use mocks to simulate remote services and test all drivers
- [ ] Test async functionality
- [ ] Use moto for mocking boto3 and test s3 destination drivers
