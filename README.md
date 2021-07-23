# GEFF

The Generic External Function Framework (GEFF) is extensible Python Lambda code which can be called using a [Snowflake External Function](https://docs.snowflake.com/en/sql-reference/external-functions-introduction.html) that allows Snowflake operators to perform generic invocations of Call Drivers (e.g. HTTP, SMTP, XML-RPC) and either return or write responses to generic Destination Drivers (e.g. S3). This empowers them to create new pipelines in Data Infrastructure while using reviewed and standardized RBAC and interaction with Cloud Infrastructure for secrets management.

## TODO

- [x] Lambda base code
- [x] Basic tests needs environment variables to be set
- [] Use mocks to simulate remote services and test all drivers
- [] Test async functionality
- [] Use moto for mocking boto3 and test s3 destination drivers
