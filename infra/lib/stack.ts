import { Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { SqsSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import {
  DefinitionBody,
  JsonPath,
  LogLevel,
  Map,
  Pass,
  StateMachine,
  StateMachineType,
} from 'aws-cdk-lib/aws-stepfunctions';
import { CfnPipe } from 'aws-cdk-lib/aws-pipes';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { DynamoAttributeValue, DynamoPutItem } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { AttributeType, Billing, TableClass, TableV2 } from 'aws-cdk-lib/aws-dynamodb';

export interface Props extends StackProps {
  /**
   * Is this stack being created as part of a unit test?
   * If so, a dummy code asset is used so that real code assets don't need to exist.
   *
   * @default - It is assumed that stack is not created as part of unit test and real asset path is used.
   */
  readonly isUnitTest?: boolean;
}

enum PipeState {
  RUNNING = 'RUNNING',
  STOPPED = 'STOPPED',
}

export class PipesTestStack extends Stack {
  constructor(scope: Construct, id: string, props?: Props) {
    super(scope, id, props);

    const tbl = new TableV2(this, 'PipesTestTable', {
      tableName: 'pipes-test-table',
      partitionKey: {
        name: 'pk',
        type: AttributeType.STRING,
      },
      sortKey: {
        name: 'sk',
        type: AttributeType.STRING,
      },
      billing: Billing.onDemand(),
      deletionProtection: false,
      pointInTimeRecovery: false,
      tableClass: TableClass.STANDARD,
      timeToLiveAttribute: 'ttl',
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const queue = new Queue(this, 'PipesTestQueue', {
      queueName: 'pipes-test-queue',
      visibilityTimeout: Duration.seconds(300),
      retentionPeriod: Duration.minutes(10),
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const snsTopic = new Topic(this, 'PipesTestSns', {
      topicName: 'pipes-test-sns',
      displayName: 'Topic fot EventBridge Pipes Test',
    });

    snsTopic.addSubscription(
      new SqsSubscription(queue, {
        rawMessageDelivery: true,
      }),
    );

    const stepFnLogGroup = new LogGroup(this, 'PipesTestStateMachineLogGroup', {
      logGroupName: 'pipes-test-state-machine',
      removalPolicy: RemovalPolicy.DESTROY,
      retention: RetentionDays.ONE_WEEK,
    });

    // Input be like:
    // {
    //   "city": "Tampere",
    //   "date": "yyyy-mm-dd",
    //   "inspectTime": "yyyy-mm-ddThh:mm:ss",
    //   "tempCelcius": 0.0,
    //   "humidityPercent": 0.0,
    // }

    // PK: WDATA#CITY#Tampere#yyyy-mm-dd
    const dynamoDbPutItem = new DynamoPutItem(this, 'DynamoPutItem', {
      comment: 'Put item into DynamoDB table',
      item: {
        pk: DynamoAttributeValue.fromString(JsonPath.stringAt("States.Format('WDATA#CITY#{}#{}', $.city, $.date)")),
        sk: DynamoAttributeValue.fromString(JsonPath.stringAt('$.inspectTime')),
        type: DynamoAttributeValue.fromString('WeatherData'),
        inspectTime: DynamoAttributeValue.fromString(JsonPath.stringAt('$.inspectTime')),
        tempCelcius: DynamoAttributeValue.fromNumber(JsonPath.numberAt('$.tempCelcius')),
        humidityPercent: DynamoAttributeValue.fromNumber(JsonPath.numberAt('$.humidityPercent')),
      },
      table: tbl,
    });
    const mapState = new Map(this, 'MapInputToDynamoDbPutItem', {
      comment: 'Map input to DynamoDB PutItem task',
      itemsPath: '$',
      maxConcurrency: 1,
    });
    mapState.iterator(dynamoDbPutItem);

    const passState = new Pass(this, 'SelectBodyFromInput', {
      comment: 'Select body from input message',
      inputPath: '$..body',
    }).next(mapState);

    const stateMachine = new StateMachine(this, 'PipesTestStateMachine', {
      definitionBody: DefinitionBody.fromChainable(passState),
      stateMachineName: 'pipes-test-state-machine',
      stateMachineType: StateMachineType.EXPRESS,
      timeout: Duration.minutes(1),
      logs: {
        destination: stepFnLogGroup,
        includeExecutionData: true,
        level: LogLevel.ALL,
      },
    });

    const pipeRole = new Role(this, 'PipesTestRole', {
      assumedBy: new ServicePrincipal('pipes.amazonaws.com'),
      roleName: 'EventBridgePipesTestRole',
      description: 'Role assumed by EventBridge Pipes pipe',
    });
    pipeRole.addToPolicy(
      new PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
        resources: [queue.queueArn],
      }),
    );
    pipeRole.addToPolicy(
      new PolicyStatement({
        actions: ['states:ListStateMachines'],
        resources: ['*'],
      }),
    );
    pipeRole.addToPolicy(
      new PolicyStatement({
        actions: ['states:Start*', 'states:Stop*'],
        resources: [stateMachine.stateMachineArn],
      }),
    );

    new CfnPipe(this, 'PipesTestPipe', {
      description: 'EventBridge Pipes Test Pipe',
      name: 'pipes-test-pipe',
      desiredState: PipeState.RUNNING,
      source: queue.queueArn,
      sourceParameters: {
        sqsQueueParameters: {
          batchSize: 10,
          maximumBatchingWindowInSeconds: 45,
        },
      },
      targetParameters: {
        inputTemplate: '{ "body": <$.body> }',
      },
      target: stateMachine.stateMachineArn,
      roleArn: pipeRole.roleArn,
    });
  }
}
