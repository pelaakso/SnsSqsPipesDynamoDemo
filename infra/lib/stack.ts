import { Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { SqsSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import { LogLevel, Pass, StateMachine, StateMachineType } from 'aws-cdk-lib/aws-stepfunctions';
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

    const passState = new Pass(this, 'SelectBodyFromInput', {
      comment: 'Select body from input message',
      inputPath: '$..body',
    }).next(
      new DynamoPutItem(this, 'DynamoPutItem', {
        item: {
          pk: DynamoAttributeValue.fromString('test'),
          sk: DynamoAttributeValue.fromString('test'),
          type: DynamoAttributeValue.fromString('Test'),
        },
        table: tbl,
      }),
    );

    const stateMachine = new StateMachine(this, 'PipesTestStateMachine', {
      definition: passState,
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
      target: stateMachine.stateMachineArn,
      roleArn: pipeRole.roleArn,
    });

    /*
    new lambda.Function(this, 'BlankJavaWithPowertoolsGradle', {
      code: props?.isUnitTest
        ? lambda.Code.fromAsset('./test/resources/dummy-code.zip')
        : lambda.Code.fromAsset(
            '../java/blank-java-with-powertools/build/distributions/blank-java-with-powertools-0.0.1-SNAPSHOT-package.zip'
          ),
      handler: 'be.petey952.blankjavapowertools.Handler',
      runtime: lambda.Runtime.JAVA_11,
      architecture: lambda.Architecture.ARM_64,
      memorySize: 512,
      timeout: Duration.seconds(15),
      environment: {
        JAVA_TOOL_OPTIONS: '-XX:+TieredCompilation -XX:TieredStopAtLevel=1',
        POWERTOOLS_SERVICE_NAME: 'BlankJavaTemplateWithPowertoolsGradle',
      },
      functionName: 'blank-java-template-with-powertools-gradle',
      description: 'Blank Lambda template using Java with Powertools, Gradle build',
    });
    */
  }
}
