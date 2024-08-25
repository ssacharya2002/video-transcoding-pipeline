import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand, Message } from "@aws-sdk/client-sqs";
import { S3Event } from "aws-lambda";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

const client = new SQSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
    }
});

const ecsClient = new ECSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!
    }
});

async function init() {
    const command = new ReceiveMessageCommand({
        QueueUrl: process.env.QUEUE_URL!,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20
    });

    while (true) {
        const { Messages } = await client.send(command);
        if (!Messages || Messages.length === 0) {
            console.log("no message in Queue");
            continue;
        }

        try {
            for (const message of Messages) {
                const { MessageId, Body, ReceiptHandle } = message;
                console.log("Message Received", { MessageId, Body });

                if (!Body) continue;

                // Validate & parse the event
                const event = JSON.parse(Body) as S3Event;

                // Ignore the test Event 
                if ("Service" in event && "Event" in event) {
                    if (event.Event === "s3:TestEvent") {
                        await client.send(new DeleteMessageCommand({
                            QueueUrl: process.env.QUEUE_URL!,
                            ReceiptHandle: ReceiptHandle!
                        }));
                        continue;
                    }
                }

                for (const record of event.Records) {
                    const { s3 } = record;
                    const { bucket, object: { key } } = s3;

                    // Spin the docker container 
                    const runTaskCommand = new RunTaskCommand({
                        taskDefinition: process.env.TASK_DEFINITION!,
                        cluster: process.env.CLUSTER!,
                        launchType: "FARGATE",
                        networkConfiguration: {
                            awsvpcConfiguration: {
                                assignPublicIp: "ENABLED",
                                securityGroups: [process.env.SECURITY_GROUP!],
                                subnets: process.env.SUBNETS!.split(',')
                            }
                        },
                        overrides: {
                            containerOverrides: [{
                                name: "video-transcoder",
                                environment: [
                                    { name: "BUCKET_NAME", value: bucket.name },
                                    { name: "KEY", value: key },
                                    { name: "AWS_ACCESS_KEY_ID", value: process.env.AWS_ACCESS_KEY_ID! },
                                    { name: "AWS_SECRET_ACCESS_KEY", value: process.env.AWS_SECRET_ACCESS_KEY! }
                                ]
                            }]
                        }
                    });
                    await ecsClient.send(runTaskCommand);

                    // Delete the message from the queue
                    await client.send(new DeleteMessageCommand({
                        QueueUrl: process.env.QUEUE_URL!,
                        ReceiptHandle: ReceiptHandle!
                    }));
                }
            }
        } catch (error) {
            console.error(error);
        }
    }
}

init();
