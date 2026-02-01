import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";
import dotenv from "dotenv";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 4004;

// Middleware
const allowedOrigins = (process.env.CORS_ORIGINS || "https://microservices-ecom.vercel.app,http://localhost:3000,http://localhost:3001")
  .split(",")
  .map((o) => o.trim())
  .filter(Boolean);

const corsOptions = {
  origin: allowedOrigins,
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
};

app.use(cors(corsOptions));
// Explicitly handle preflight requests for all routes
app.options("*", cors(corsOptions));

app.use(express.json());

// Kafka configuration - supports both local and cloud (Upstash/Confluent)
const kafkaBrokers = process.env.KAFKA_BROKERS 
  ? process.env.KAFKA_BROKERS.split(',').map(b => b.trim())
  : ["localhost:9094"];

const kafkaConfig = {
  clientId: "email-service",
  brokers: kafkaBrokers,
};

// Add SASL authentication if credentials are provided (for Upstash/Confluent)
if (process.env.KAFKA_USE_SASL === 'true' && process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD) {
  kafkaConfig.sasl = {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  };
  kafkaConfig.ssl = true; // Upstash/Confluent require SSL
}

const kafka = new Kafka(kafkaConfig);

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "email-service" });

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({
    status: "OK",
    service: "Email Service",
    port: PORT,
    timestamp: new Date().toISOString(),
  });
});

// Get service info
app.get("/", (req, res) => {
  res.json({
    service: "Email Service",
    version: "1.0.0",
    port: PORT,
    endpoints: {
      health: "/health",
      sendEmail: "/api/send-email (POST)",
    },
  });
});

// Send email endpoint
app.post("/api/send-email", async (req, res) => {
  try {
    const { to, subject, text, html } = req.body;

    // Validate input
    if (!to || !subject) {
      return res.status(400).json({ error: "To and subject are required" });
    }

    // For now, just log the email (mock implementation)
    console.log(`Email would be sent to: ${to}`);
    console.log(`Subject: ${subject}`);
    console.log(`Text: ${text || "No text content"}`);

    // In a real implementation, you would use nodemailer or similar
    // const transporter = nodemailer.createTransporter({...});
    // await transporter.sendMail({ to, subject, text, html });

    res.json({
      success: true,
      message: "Email sent successfully (mock)",
      to,
      subject,
    });
  } catch (error) {
    console.error("Error sending email:", error);
    res.status(500).json({
      success: false,
      error: "Failed to send email",
    });
  }
});

const run = async () => {
  try {
    // Connect to Kafka
    await producer.connect();
    await consumer.connect();

    // --- Nodemailer setup ---
    const nodemailer = await import("nodemailer");
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: process.env.SMTP_PORT,
      secure: false,
      auth: {
        user: process.env.SMTP_USER,
        pass: process.env.SMTP_PASS,
      },
    });

    async function sendMail(to, subject, text) {
      try {
        console.log(`Attempting to send email to: ${to}`);
        await transporter.sendMail({
          from: process.env.SMTP_FROM || process.env.SMTP_USER,
          to,
          subject,
          text,
        });
        console.log(`Email sent successfully to ${to}`);
        return true;
      } catch (e) {
        console.error("Failed to send email:", e);
        return false;
      }
    }

    // Subscribe to email-related topics
    await consumer.subscribe({ topic: "send-email", fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        let payload;
        try {
          payload = JSON.parse(value);
        } catch (e) {
          console.error("Invalid Kafka message", e);
          return;
        }

        const { to, subject, text, type } = payload;

        if (topic === "send-email") {
          if (!to || !subject) {
            console.warn("Email event missing required fields (to, subject)");
            return;
          }

          console.log(`Processing email: type=${type}, to=${to}`);
          const sent = await sendMail(to, subject, text || "");
          
          if (sent) {
            // Optionally publish email-sent confirmation event
            await producer.send({
              topic: "email-sent",
              messages: [
                {
                  value: JSON.stringify({
                    to,
                    type,
                    timestamp: new Date().toISOString(),
                  }),
                },
              ],
            });
          }
        }
      },
    });

    // Start Express server
    app.listen(PORT, () => {
      console.log(`🚀 Email Service running on port ${PORT}`);
      console.log(`📊 Health check: http://localhost:${PORT}/health`);
      console.log(`📋 API docs: http://localhost:${PORT}/`);
      console.log(`📧 Listening to Kafka topic: send-email`);
    });
  } catch (err) {
    console.error("Error starting email service:", err);
    process.exit(1);
  }
};

run();
