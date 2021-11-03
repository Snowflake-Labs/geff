[![Terraform](https://github.com/Snowflake-Labs/geff/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Snowflake-Labs/geff/actions/workflows/ci.yml)

# GEFF

The Generic External Function Framework (GEFF) is a generic backend for [Snowflake External Functions](https://docs.snowflake.com/en/sql-reference/external-functions-introduction.html) which allows Snowflake operators to perform generic invocations of Call Drivers (e.g. HTTP, SMTP, XML-RPC) and either return to Snowflake or write call responses using Destination Drivers (e.g. S3). This empowers them to create new pipelines in Snowflake's Data Cloud using a standardized RBAC and interactions with Cloud Infrastructure for management of authentication credentials and other secrets.

## Instructions

### Deploying with Terraform

Below is an example as used in [`terraform-snowflake-aws-geff`](https://github.com/Snowflake-Labs/terraform-snowflake-aws-geff):

NOTE: The handler is `geff.lambda_function.lambda_handler` as opposed to the default `lambda_function.lambda_handler`. We're invoking GEFF as a package.

```hcl
resource "aws_lambda_function" "geff_lambda" {
  function_name    = local.lambda_function_name
  role             = aws_iam_role.geff_lambda_assume_role.arn
  handler          = "geff.lambda_function.lambda_handler"
  memory_size      = "4096" # 4 GB
  runtime          = local.runtime
  timeout          = "900" # 15 mins
  publish          = null
  filename         = data.archive_file.lambda_code.output_path
  source_code_hash = data.archive_file.lambda_code.output_base64sha256
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

### Creating a zip archive
```bash
make pack
```

## TODO

- [x] Lambda base code
- [x] Basic tests needs environment variables to be set
- [ ] Use mocks to simulate remote services and test all drivers
- [ ] Test async functionality
- [ ] Use moto for mocking boto3 and test s3 destination drivers
