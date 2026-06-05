import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageRoot = join(__dirname, "..");
const schemaRegistryUrl =
  process.env.SCHEMA_REGISTRY_URL ?? "http://localhost:8081";

const schemas = [
  {
    subject: "order.order-events-OrderCreated-value",
    fileName: "order-created.v1.avsc"
  },
  {
    subject: "risk.risk-events-OrderRiskApproved-value",
    fileName: "order-risk-approved.v1.avsc"
  },
  {
    subject: "risk.risk-events-OrderRiskRejected-value",
    fileName: "order-risk-rejected.v1.avsc"
  },
  {
    subject: "payment.payment-events-PaymentAuthorized-value",
    fileName: "payment-authorized.v1.avsc"
  },
  {
    subject: "payment.payment-events-PaymentFailed-value",
    fileName: "payment-failed.v1.avsc"
  },
  {
    subject: "notification.notification-commands-NotificationCommand-value",
    fileName: "notification-command.v1.avsc"
  },
  {
    subject: "dead-letter.events-DeadLetterEvent-value",
    fileName: "dead-letter-event.v1.avsc"
  }
];

for (const schema of schemas) {
  const schemaPath = join(packageRoot, "schemas", "avro", schema.fileName);
  const schemaContent = await readFile(schemaPath, "utf8");

  await setCompatibility(schema.subject);
  const id = await registerSchema(schema.subject, schemaContent);

  console.log(`registered ${schema.subject} id=${id}`);
}

async function setCompatibility(subject) {
  const response = await fetch(`${schemaRegistryUrl}/config/${subject}`, {
    method: "PUT",
    headers: {
      "content-type": "application/vnd.schemaregistry.v1+json"
    },
    body: JSON.stringify({
      compatibility: "BACKWARD"
    })
  });

  if (!response.ok) {
    throw new Error(
      `failed to set compatibility for ${subject}: ${response.status} ${await response.text()}`
    );
  }
}

async function registerSchema(subject, schemaContent) {
  const response = await fetch(`${schemaRegistryUrl}/subjects/${subject}/versions`, {
    method: "POST",
    headers: {
      "content-type": "application/vnd.schemaregistry.v1+json"
    },
    body: JSON.stringify({
      schemaType: "AVRO",
      schema: schemaContent
    })
  });

  if (!response.ok) {
    throw new Error(
      `failed to register ${subject}: ${response.status} ${await response.text()}`
    );
  }

  const body = await response.json();

  return body.id;
}
