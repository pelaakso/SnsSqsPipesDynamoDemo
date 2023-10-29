# Infra

Using:

* TypeScript
* Node 18.x (at the time of writing the version supported by AWS Lambda)

## Useful commands

* `npm run all`         run all steps
* `npm run watch`       watch for changes and compile
* `npm run test`        perform the jest unit tests
* `npm run snap -- -u`  accept and update snapshot tests
* `npx cdk --profile your-profile deploy` deploy this stack to AWS account/region
* `npx cdk --profile your-profile diff`   compare deployed stack with current state
* `npx cdk --profile your-profile synth`  emits the synthesized CloudFormation template
