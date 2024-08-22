[![Terraform](https://github.com/Snowflake-Labs/geff/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Snowflake-Labs/geff/actions/workflows/ci.yml)

# GEFF

The Generic External Function Framework (GEFF) is a generic backend for [Snowflake External Functions](https://docs.snowflake.com/en/sql-reference/external-functions-introduction.html) which allows Snowflake users to invoke RPC endpoints via Call Drivers (e.g. HTTP, SMTP, XML-RPC), either returning results to Snowflake, or storing them with Write Drivers (e.g. to S3).

GEFF empowers users to invoke a variety of external RPC's without changing infrastructure, allowing them to manage and threat model those RPC interfaces in Snowflake's Data Cloud using Snowflake RBAC in a single standardized interaction with CSP's.

We recommend deployed GEFF via the Terraform in [terraform-snowflake-api-integration-with-geff-aws](https://github.com/Snowflake-Labs/terraform-snowflake-api-integration-with-geff-aws) but you could also build it as an image on AWS ECR by invoking the bash script below or create a zip archive which can be uploaded into the AWS lambda UI using the `make` command. 

## Example

After deploying GEFF behind an [API Integration](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration.html), you can create external functions that specify a protocol and an authenticated endpoint, e.g. —

~~~sql
CREATE OR REPLACE EXTERNAL FUNCTION abuseipdb_check_ip(ip STRING, max_age_in_days NUMBER, verbose BOOL)
  RETURNS VARIANT
  VOLATILE
  COMMENT='https://docs.abuseipdb.com/#check-endpoint'
  API_INTEGRATION=SECENG
  HEADERS=(
    'auth'='arn:aws:secretsmanager:us-west-2:123456789012:secret:prod/seceng/abuseip-api-pmsbfa'
    'params'='ipAddress={0}&maxAgeInDays={1}&{2}'
    'url'='https://api.abuseipdb.com/api/v2/check'
  )
  AS 'https://r2vuxhftrg.execute-api.us-west-2.amazonaws.com/prod/https'
;

SELECT abuseipdb_check_ip('127.0.0.1', 365, TRUE);
~~~

GEFF will then retrieve the secret referenced in `auth`, e.g. `{"host": "api.abuseipdb.com", "headers": {"Key": "fbgzxukuci..."}}` and use that to authenticate the API call while maintaining a variety of [security, observability, and auditability committments](https://github.com/Snowflake-Labs/geff/wiki/I.-GEFF#security-guarantees).

## Dev Instructions

### Building and uploading GEFF Lambda image to ECR

```bash
# Clone repo
git clone git@github.com:Snowflake-Labs/geff.git

# Run ecr.sh
./ecr.sh 123556660 us-west-2 0.0.x-dev
```

### Deploying rest of Snowflake and AWS infra with Terraform

Below is an example as used in [`terraform-snowflake-aws-geff`](https://github.com/Snowflake-Labs/terraform-snowflake-aws-geff):

NOTE: The handler is `geff.lambda_function.lambda_handler` as opposed to the default `lambda_function.lambda_handler`. We're invoking GEFF as a package.

```hcl
resource "aws_lambda_function" "geff_lambda" {
  function_name = local.lambda_function_name
  role          = aws_iam_role.geff_lambda_assume_role.arn

  memory_size = "4096" # 4 GB
  timeout     = "900"  # 15 mins

  image_uri    = local.lambda_image_repo_version # this is the GEFF docker image uploaded using ecr.sh
  package_type = "Image"
}
```

### Setup

```bash
git clone git@github.com:Snowflake-Labs/geff.git
python3 -m venv ./venv
source ./venv/bin/activate

pip3 install -r requirements-dev.txt
```

### Test

```bash
# While in the venv
python -m pytest tests/*
```

### Creating a zip archive of the code

```bash
make pack
```

## TODO

- [x] Lambda base code
- [x] Basic tests needs environment variables to be set
- [ ] Use mocks to simulate remote services and test all drivers
- [ ] Test async functionality
- [ ] Use moto for mocking boto3 and test s3 destination drivers
