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

// --- Email sending function ---
// Supports: Brevo (recommended), Resend, and SMTP fallback
async function sendMail(to, subject, text, html) {
  // Option 1: Brevo API (recommended - can send to anyone on free tier)
  if (process.env.BREVO_API_KEY) {
    try {
      console.log(`Sending email via Brevo API to: ${to}`);
      const response = await fetch("https://api.brevo.com/v3/smtp/email", {
        method: "POST",
        headers: {
          "accept": "application/json",
          "api-key": process.env.BREVO_API_KEY,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          sender: {
            name: process.env.EMAIL_FROM_NAME || "ShopEase",
            email: process.env.EMAIL_FROM || "noreply@shopease.com",
          },
          to: [{ email: to }],
          subject: subject,
          htmlContent: html || `<p>${text}</p>`,
          textContent: text,
        }),
      });

      const data = await response.json();
      
      if (!response.ok) {
        console.error("Brevo API error:", data);
        return false;
      }
      
      console.log(`Email sent successfully via Brevo to ${to}. MessageId: ${data.messageId}`);
      return true;
    } catch (error) {
      console.error("Failed to send email via Brevo:", error);
      return false;
    }
  }

  // Option 2: Resend API (requires verified domain for sending to others)
  if (process.env.RESEND_API_KEY) {
    try {
      console.log(`Sending email via Resend API to: ${to}`);
      const response = await fetch("https://api.resend.com/emails", {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${process.env.RESEND_API_KEY}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          from: process.env.EMAIL_FROM || "onboarding@resend.dev",
          to: to,
          subject: subject,
          text: text,
          html: html || text,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        console.error("Resend API error:", errorData);
        return false;
      }

      const data = await response.json();
      console.log(`Email sent successfully via Resend. ID: ${data.id}`);
      return true;
    } catch (error) {
      console.error("Failed to send email via Resend:", error);
      return false;
    }
  }

  // Option 3: SMTP (for local development only - blocked on cloud platforms)
  if (process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS) {
    try {
      const nodemailer = await import("nodemailer");
      const transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST,
        port: parseInt(process.env.SMTP_PORT || "587"),
        secure: process.env.SMTP_SECURE === "true",
        auth: {
          user: process.env.SMTP_USER,
          pass: process.env.SMTP_PASS,
        },
        connectionTimeout: 10000,
      });

      console.log(`Sending email via SMTP to: ${to}`);
      await transporter.sendMail({
        from: process.env.SMTP_FROM || process.env.SMTP_USER,
        to,
        subject,
        text,
        html: html || text,
      });
      console.log(`Email sent successfully via SMTP to ${to}`);
      return true;
    } catch (error) {
      console.error("Failed to send email via SMTP:", error.message);
      return false;
    }
  }

  // No email provider configured - log only
  console.log(`[MOCK] Email would be sent to: ${to}`);
  console.log(`[MOCK] Subject: ${subject}`);
  console.log(`[MOCK] Text: ${text}`);
  return true;
}

// Health check endpoint
app.get("/health", (req, res) => {
  const emailProvider = process.env.BREVO_API_KEY ? "Brevo" : 
                        process.env.RESEND_API_KEY ? "Resend" : 
                        process.env.SMTP_HOST ? "SMTP" : "Mock";
  res.json({
    status: "OK",
    service: "Email Service",
    port: PORT,
    emailProvider,
    timestamp: new Date().toISOString(),
  });
});

// Get service info
app.get("/", (req, res) => {
  const emailProvider = process.env.BREVO_API_KEY ? "Brevo" : 
                        process.env.RESEND_API_KEY ? "Resend" : 
                        process.env.SMTP_HOST ? "SMTP" : "Mock";
  res.json({
    service: "Email Service",
    version: "1.0.0",
    port: PORT,
    emailProvider,
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

    const sent = await sendMail(to, subject, text, html);

    if (sent) {
      res.json({
        success: true,
        message: "Email sent successfully",
        to,
        subject,
      });
    } else {
      res.status(500).json({
        success: false,
        error: "Failed to send email",
      });
    }
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

        const { to, subject, text, html, type } = payload;

        if (topic === "send-email") {
          if (!to || !subject) {
            console.warn("Email event missing required fields (to, subject)");
            return;
          }

          console.log(`Processing email: type=${type}, to=${to}`);
          const sent = await sendMail(to, subject, text, html);
          
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

    // Log email provider status
    const emailProvider = process.env.RESEND_API_KEY ? "Resend API" : (process.env.SMTP_HOST ? "SMTP" : "Mock (no provider configured)");
    
    // Start Express server
    app.listen(PORT, () => {
      console.log(`🚀 Email Service running on port ${PORT}`);
      console.log(`📧 Email provider: ${emailProvider}`);
      console.log(`📊 Health check: http://localhost:${PORT}/health`);
      console.log(`📋 API docs: http://localhost:${PORT}/`);
      console.log(`📨 Listening to Kafka topic: send-email`);
    });
  } catch (err) {
    console.error("Error starting email service:", err);
    process.exit(1);
  }
};

run();
