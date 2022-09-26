import * as kafkajs from "kafkajs";
import * as kafka_iam from "@jm18457/kafkajs-msk-iam-authentication-mechanism";

(async () => {


  const KAFKA_BROKER = process.env["AWS_MSK_BROKER_URL"];
  if(!KAFKA_BROKER) {
    throw new Error("Please set AWS_MSK_BROKER_URL env var");
  }
  const AWS_REGION = process.env["AWS_REGION"];
  if(!AWS_REGION) {
    throw new Error("Please set AWS_REGION env var");
  }

  const TOPIC_NAME = "testtopic";

  const kafka = new kafkajs.Kafka( {
    brokers: [KAFKA_BROKER],
    clientId: 'test',
    ssl: true,
    sasl: {
      mechanism: kafka_iam.Type,
      authenticationProvider: kafka_iam.awsIamAuthenticator(AWS_REGION)
    }
  });

  const admin = kafka.admin();
  await admin.connect();
  // Create topic if it does not exist
  if((await admin.listTopics()).filter(t => t == TOPIC_NAME).length == 0) {
    await admin.createTopics({
      topics: [{
        topic: TOPIC_NAME,
        numPartitions: 32,
      }]
    });
  }
  await admin.disconnect();

  // Start consumer
  const consumer = kafka.consumer({groupId: "test", allowAutoTopicCreation: false});
  await consumer.connect();
  await consumer.subscribe({topic: TOPIC_NAME} );

  await consumer.run({
    eachMessage: async (_) => {
      // ..
    }
  });

  await consumer.disconnect();

})();

