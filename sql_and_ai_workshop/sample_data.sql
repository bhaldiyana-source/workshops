-- Databricks SQL and AI Functions Workshop
-- Sample Data Creation Script
-- 
-- This script creates sample datasets for use throughout the workshop exercises.
-- It includes customer reviews, support tickets, products, and documents for
-- practicing AI Functions including classification, extraction, embeddings, and more.

-- =============================================================================
-- SETUP: Configure Catalog and Schema
-- =============================================================================

USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS sql_ai_workshop;
USE SCHEMA sql_ai_workshop;

-- =============================================================================
-- TABLE 1: Customer Reviews
-- =============================================================================
-- Sample customer product reviews for sentiment analysis and text processing

CREATE OR REPLACE TABLE customer_reviews (
  review_id INT,
  customer_id STRING,
  product_id STRING,
  product_name STRING,
  product_category STRING,
  review_text STRING,
  review_date TIMESTAMP,
  rating INT,
  verified_purchase BOOLEAN
);

INSERT INTO customer_reviews VALUES
  (1, 'CUST001', 'PROD_LP_001', 'Professional Laptop 15"', 'Electronics', 
   'Outstanding laptop! The performance is exceptional, battery lasts all day, and the display is crystal clear. Perfect for my development work. Highly recommend!', 
   '2024-01-10 09:15:00', 5, TRUE),
   
  (2, 'CUST002', 'PROD_LP_001', 'Professional Laptop 15"', 'Electronics',
   'Very disappointed. The laptop died after just 3 weeks. Screen went black and won''t turn on. Terrible quality control. Waste of money!',
   '2024-01-12 14:30:00', 1, TRUE),
   
  (3, 'CUST003', 'PROD_MS_001', 'Wireless Mouse Pro', 'Accessories',
   'Comfortable mouse with great precision. The wireless connection is stable and battery life is excellent. Good value for the price.',
   '2024-01-13 10:45:00', 4, TRUE),
   
  (4, 'CUST004', 'PROD_KB_001', 'Mechanical Keyboard RGB', 'Accessories',
   'Keys are way too loud for an office environment. Some keys stick occasionally. Expected much better quality at this price point.',
   '2024-01-14 16:20:00', 2, TRUE),
   
  (5, 'CUST005', 'PROD_LP_001', 'Professional Laptop 15"', 'Electronics',
   'Good laptop overall. Performance is solid but it gets quite hot during video calls. Would be perfect if cooling was improved.',
   '2024-01-15 11:00:00', 4, TRUE),
   
  (6, 'CUST006', 'PROD_HP_001', 'Noise Canceling Headphones', 'Audio',
   'Absolutely amazing headphones! Sound quality is incredible and noise cancellation works perfectly. Best investment I''ve made!',
   '2024-01-16 09:30:00', 5, TRUE),
   
  (7, 'CUST007', 'PROD_WC_001', 'HD Webcam 1080p', 'Electronics',
   'Image quality is decent but the built-in microphone is terrible. Picks up every background noise. Had to buy a separate mic.',
   '2024-01-17 13:45:00', 3, TRUE),
   
  (8, 'CUST008', 'PROD_MN_001', '4K Monitor 27"', 'Electronics',
   'Beautiful display with accurate colors! Perfect for photo editing and design work. The 4K resolution is stunning.',
   '2024-01-18 10:15:00', 5, TRUE),
   
  (9, 'CUST009', 'PROD_MS_001', 'Wireless Mouse Pro', 'Accessories',
   'Scroll wheel broke after only one month. Connection drops randomly. Very poor quality control. Do not recommend.',
   '2024-01-19 15:00:00', 1, TRUE),
   
  (10, 'CUST010', 'PROD_LP_002', 'Budget Laptop 13"', 'Electronics',
   'Great laptop for students! Lightweight, good battery, and handles all my coursework easily. Excellent value for money.',
   '2024-01-20 12:30:00', 5, TRUE);

-- =============================================================================
-- TABLE 2: Support Tickets
-- =============================================================================
-- Customer support tickets for classification and priority assignment

CREATE OR REPLACE TABLE support_tickets (
  ticket_id INT,
  customer_id STRING,
  subject STRING,
  description STRING,
  created_at TIMESTAMP,
  channel STRING,
  status STRING
);

INSERT INTO support_tickets VALUES
  (1, 'CUST101', 'Cannot access my account - URGENT', 
   'I have been trying to log in for the past 2 hours but keep getting an authentication error. This is critical as I need to access my project files immediately for a client presentation!',
   '2024-01-15 08:30:00', 'Email', 'Open'),
   
  (2, 'CUST102', 'Billing question about duplicate charge',
   'I noticed I was charged twice for my monthly subscription. Can someone please review my account and process a refund for the duplicate charge?',
   '2024-01-15 10:15:00', 'Chat', 'Open'),
   
  (3, 'CUST103', 'Feature request for mobile app',
   'It would be really helpful if you could add dark mode to the mobile app. Many users in the community have been requesting this feature.',
   '2024-01-15 11:45:00', 'Email', 'Open'),
   
  (4, 'CUST104', 'Data export timing out',
   'When I try to export my data to CSV format, I get a timeout error after about 30 seconds. The file should be around 500MB. Is there a file size limit?',
   '2024-01-15 14:20:00', 'Email', 'Open'),
   
  (5, 'CUST105', 'Password reset email not arriving',
   'I requested a password reset but the email never arrives. I have checked my spam folder multiple times. Please help!',
   '2024-01-15 16:00:00', 'Phone', 'Open'),
   
  (6, 'CUST106', 'General inquiry about business hours',
   'What are your customer support business hours? I need to know when I can call for assistance.',
   '2024-01-16 09:00:00', 'Chat', 'Closed'),
   
  (7, 'CUST107', 'Application crashes - CRITICAL BUG',
   'The application crashes every single time I try to upload files larger than 10MB. This is completely blocking my work. URGENT FIX NEEDED!',
   '2024-01-16 11:30:00', 'Email', 'Open'),
   
  (8, 'CUST108', 'API integration documentation needed',
   'How do I integrate your REST API with Salesforce? I need documentation or code examples to get started.',
   '2024-01-17 10:00:00', 'Email', 'Open'),
   
  (9, 'CUST109', 'GDPR data deletion request',
   'I want to delete my account and all associated personal data per GDPR requirements. Please process this request.',
   '2024-01-17 13:15:00', 'Email', 'Open'),
   
  (10, 'CUST110', 'Dashboard loading very slowly',
   'The main dashboard takes over a minute to load since yesterday. This never happened before. The performance is terrible.',
   '2024-01-18 09:45:00', 'Chat', 'Open');

-- =============================================================================
-- TABLE 3: Product Catalog
-- =============================================================================
-- Product descriptions for semantic search and recommendations

CREATE OR REPLACE TABLE products (
  product_id STRING,
  product_name STRING,
  category STRING,
  description STRING,
  price DECIMAL(10,2),
  in_stock BOOLEAN
);

INSERT INTO products VALUES
  ('PROD_LP_001', 'Professional Laptop 15"', 'Electronics',
   'High-performance laptop with 15-inch 4K display, Intel Core i7 processor, 16GB RAM, 512GB SSD. Perfect for professionals, developers, and content creators. Exceptional battery life up to 12 hours.',
   1299.99, TRUE),
   
  ('PROD_LP_002', 'Budget Laptop 13"', 'Electronics',
   'Affordable 13-inch laptop with Intel Core i5 processor, 8GB RAM, 256GB SSD. Great for students and everyday computing. Lightweight and portable design.',
   599.99, TRUE),
   
  ('PROD_MS_001', 'Wireless Mouse Pro', 'Accessories',
   'Ergonomic wireless mouse with precision tracking, 6 programmable buttons, and rechargeable battery lasting up to 3 months. Compatible with Windows, Mac, and Linux.',
   49.99, TRUE),
   
  ('PROD_KB_001', 'Mechanical Keyboard RGB', 'Accessories',
   'Premium mechanical keyboard with customizable RGB backlighting, tactile switches, and wireless Bluetooth connectivity. Perfect for typing enthusiasts and gamers.',
   129.99, TRUE),
   
  ('PROD_MN_001', '4K Monitor 27"', 'Electronics',
   'Professional 27-inch 4K monitor with IPS panel, HDR support, USB-C connectivity, and 99% sRGB color accuracy. Ideal for photo editing, video production, and design work.',
   449.99, TRUE),
   
  ('PROD_HP_001', 'Noise Canceling Headphones', 'Audio',
   'Premium wireless headphones with active noise cancellation, 30-hour battery life, and exceptional sound quality. Perfect for travel, work, and music enjoyment.',
   299.99, TRUE),
   
  ('PROD_WC_001', 'HD Webcam 1080p', 'Electronics',
   'Full HD 1080p webcam with autofocus, built-in dual microphones, and wide-angle lens. Essential for video conferencing, streaming, and remote work.',
   79.99, TRUE),
   
  ('PROD_SSD_001', 'Portable SSD 1TB', 'Storage',
   'Ultra-fast portable solid-state drive with 1TB capacity, USB-C interface, and rugged design. Transfer speeds up to 1050 MB/s for quick file transfers.',
   149.99, TRUE),
   
  ('PROD_HUB_001', 'USB-C Hub 7-Port', 'Accessories',
   'Versatile 7-port USB-C hub with HDMI 4K output, USB 3.0 ports, SD card reader, and Ethernet port. Essential accessory for modern laptops.',
   59.99, TRUE),
   
  ('PROD_LAMP_001', 'LED Desk Lamp', 'Office',
   'Adjustable LED desk lamp with multiple brightness levels, color temperature control, USB charging port, and modern minimalist design. Energy-efficient and eye-friendly.',
   39.99, TRUE);

-- =============================================================================
-- TABLE 4: Knowledge Base Articles
-- =============================================================================
-- Documentation articles for semantic search exercises

CREATE OR REPLACE TABLE knowledge_base (
  article_id INT,
  title STRING,
  category STRING,
  content STRING,
  author STRING,
  created_date DATE,
  view_count INT
);

INSERT INTO knowledge_base VALUES
  (1, 'How to Reset Your Password', 'Account Management',
   'If you need to reset your password, follow these steps: 1) Go to the login page and click "Forgot Password". 2) Enter your email address and click Submit. 3) Check your email for a password reset link (check spam if needed). 4) Click the link and enter your new password. Your password must be at least 8 characters and include uppercase, lowercase, numbers, and special characters.',
   'Support Team', '2024-01-01', 1523),
   
  (2, 'Troubleshooting Login Issues', 'Account Management',
   'If you are experiencing login problems, try these solutions: First, verify your username and password are correct (passwords are case-sensitive). Second, check if Caps Lock is enabled. Third, clear your browser cache and cookies. Fourth, try a different web browser. Fifth, disable browser extensions temporarily. If problems persist, use the password reset feature or contact our support team.',
   'Support Team', '2024-01-01', 2104),
   
  (3, 'Setting Up Two-Factor Authentication', 'Security',
   'Two-factor authentication (2FA) adds an extra layer of security to your account. To enable 2FA: Navigate to Account Settings > Security > Two-Factor Authentication. Choose your preferred method: SMS text message or authenticator app (Google Authenticator, Authy). Follow the setup wizard to configure your device. Save your backup codes in a secure location. Once enabled, you will need to enter a verification code each time you log in.',
   'Security Team', '2024-01-02', 892),
   
  (4, 'Exporting Your Data', 'Data Management',
   'You can export your data at any time. To export: Go to Settings > Data Management > Export Data. Select the data types you want to export (contacts, messages, files, etc.). Choose your preferred export format: CSV for spreadsheets, JSON for developers, or XML. Click "Start Export" and wait for processing to complete. You will receive an email with a secure download link within 24 hours. Large exports may take longer.',
   'Data Team', '2024-01-03', 1456),
   
  (5, 'API Authentication Guide', 'Developer',
   'Our REST API provides programmatic access to your account. To authenticate API requests: Generate an API key from Settings > Developer > API Keys. Include your API key in the Authorization header: "Authorization: Bearer YOUR_API_KEY". All API requests must use HTTPS. Rate limits apply: 1,000 requests per hour for free accounts, 10,000 for premium accounts. See full API documentation at docs.example.com/api for endpoints and examples.',
   'Developer Team', '2024-01-04', 3201),
   
  (6, 'Understanding Your Bill', 'Billing',
   'Your monthly invoice includes several components: Base subscription fee for your plan tier, per-user charges for additional team members, storage overage fees if you exceed your plan limit, and API usage costs for premium features. Billing occurs on the first day of each month. Accepted payment methods: credit card (Visa, Mastercard, Amex), PayPal, and wire transfer for enterprise accounts. View detailed billing history in Account > Billing.',
   'Finance Team', '2024-01-05', 1789),
   
  (7, 'Mobile App Features', 'Mobile',
   'Download our mobile app from the App Store (iOS) or Google Play (Android). Key features include: Real-time dashboard viewing, push notifications for important updates, quick data entry and form submission, offline mode for working without internet, automatic sync when connection is restored, biometric login (fingerprint/face ID), and dark mode support. The app requires iOS 14+ or Android 10+.',
   'Product Team', '2024-01-06', 2341),
   
  (8, 'Data Privacy and Security', 'Security',
   'We take your privacy seriously and employ industry-leading security measures: All data is encrypted at rest using AES-256 encryption, data in transit is protected with TLS 1.3, we are compliant with GDPR, CCPA, and SOC 2 Type II standards, data is stored in geographically distributed data centers with automatic backups, we never sell your personal data to third parties, you can request data export or deletion at any time, and our security team conducts regular audits and penetration testing.',
   'Security Team', '2024-01-07', 1654),
   
  (9, 'Third-Party Integrations', 'Integrations',
   'Connect with your favorite tools and services: Supported integrations include Salesforce for CRM, Slack for team communication, Microsoft Teams for collaboration, Google Workspace for productivity, Zoom for video conferencing, Zapier for automation, and custom webhooks. Setup is simple: Go to Settings > Integrations, select your tool, click "Connect", and authorize access. Most integrations sync data in real-time. Premium plans support unlimited integrations and custom webhooks.',
   'Integration Team', '2024-01-08', 2789),
   
  (10, 'Performance Optimization Tips', 'Performance',
   'Improve system performance with these tips: Simplify complex dashboards by removing unnecessary widgets, use filters to limit data scope and improve query speed, enable caching on frequently accessed reports, archive old data that is no longer needed for active analysis, schedule large data exports and reports during off-peak hours (overnight), optimize custom queries by limiting date ranges, and contact our support team for database indexing recommendations specific to your usage patterns.',
   'Performance Team', '2024-01-09', 1123);

-- =============================================================================
-- TABLE 5: Email Messages
-- =============================================================================
-- Sample emails for text extraction and classification exercises

CREATE OR REPLACE TABLE email_messages (
  message_id INT,
  sender STRING,
  recipient STRING,
  subject STRING,
  body STRING,
  sent_date TIMESTAMP,
  has_attachments BOOLEAN
);

INSERT INTO email_messages VALUES
  (1, 'customer@example.com', 'support@company.com', 'Order Issue #12345',
   'Hello, I ordered a laptop (Order #12345) two weeks ago but it still hasn''t arrived. The tracking number shows it''s stuck in transit. Can you please help me locate my package or issue a refund? This is urgent as I need it for work. My phone number is 555-0123 if you need to call me.',
   '2024-01-15 09:30:00', FALSE),
   
  (2, 'manager@company.com', 'team@company.com', 'Q1 Sales Performance Review',
   'Team, great work this quarter! We exceeded our sales targets by 23%. Key wins: New enterprise client deals, successful product launch, improved customer retention rates. Focus areas for Q2: Expand into new markets, enhance customer support response times, develop strategic partnerships. Meeting scheduled for Friday at 2 PM to discuss detailed action plans.',
   '2024-01-16 11:00:00', TRUE),
   
  (3, 'vendor@supplier.com', 'procurement@company.com', 'Invoice #INV-2024-001',
   'Please find attached invoice #INV-2024-001 for $15,750.00 covering January supplies delivery. Payment terms: Net 30 days. Bank details: Account #12345678, Routing #987654321. Please confirm receipt and expected payment date. Contact me at vendor@supplier.com or 555-0199 for any questions.',
   '2024-01-17 14:20:00', TRUE),
   
  (4, 'hr@company.com', 'all-employees@company.com', 'New Company Policy Updates',
   'Important policy updates effective February 1st: Remote work policy now allows 3 days per week work from home, health insurance enrollment period extended through January 31st, new parental leave policy provides 16 weeks paid leave, professional development budget increased to $2000 per employee annually. Full details available on the HR portal.',
   '2024-01-18 10:00:00', FALSE),
   
  (5, 'angry-customer@email.com', 'complaints@company.com', 'TERRIBLE SERVICE - DEMAND REFUND',
   'This is absolutely unacceptable! I have called your support line 5 times and nobody has helped me. Your product is completely broken and doesn''t work as advertised. I want an immediate full refund of $499.99 to my credit card. If I don''t hear back within 24 hours, I will post negative reviews everywhere and file a complaint with consumer protection. Contact me immediately at angry-customer@email.com.',
   '2024-01-19 16:45:00', FALSE);

-- =============================================================================
-- TABLE 6: Social Media Posts
-- =============================================================================
-- Sample social media content for sentiment analysis

CREATE OR REPLACE TABLE social_media_posts (
  post_id INT,
  platform STRING,
  username STRING,
  content STRING,
  posted_at TIMESTAMP,
  likes INT,
  shares INT,
  comments INT
);

INSERT INTO social_media_posts VALUES
  (1, 'Twitter', '@techreviewer', 
   'Just got the new Professional Laptop 15" and WOW! 🚀 Best laptop I''ve ever used. Lightning fast, beautiful display, amazing battery life. Worth every penny! #TechReview #ProductivityBoost',
   '2024-01-15 14:30:00', 342, 78, 45),
   
  (2, 'Twitter', '@disappointed_user',
   'Extremely disappointed with @CompanyName customer service. Been waiting 3 days for a response to my support ticket. This is unacceptable for a premium service provider. Do better! 😤',
   '2024-01-16 09:15:00', 89, 23, 67),
   
  (3, 'Facebook', 'Sarah Johnson',
   'Highly recommend the Noise Canceling Headphones from Company! Perfect for my work-from-home setup. Crystal clear sound and the noise cancellation is incredible. Game changer for video calls!',
   '2024-01-16 16:45:00', 156, 34, 28),
   
  (4, 'LinkedIn', 'Michael Chen',
   'Impressed by Company''s new API documentation. Clean, comprehensive, and easy to integrate. As a developer, I appreciate the detailed examples and SDKs in multiple languages. Great work! 👏',
   '2024-01-17 11:00:00', 234, 45, 19),
   
  (5, 'Twitter', '@budget_shopper',
   'The Budget Laptop 13" is perfect for students! 💻 Lightweight, good performance, and actually affordable. My daughter uses it for all her college work without any issues. Great value!',
   '2024-01-18 13:20:00', 178, 52, 31);

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- Count records in each table
SELECT 'customer_reviews' as table_name, COUNT(*) as record_count FROM customer_reviews
UNION ALL
SELECT 'support_tickets', COUNT(*) FROM support_tickets
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'knowledge_base', COUNT(*) FROM knowledge_base
UNION ALL
SELECT 'email_messages', COUNT(*) FROM email_messages
UNION ALL
SELECT 'social_media_posts', COUNT(*) FROM social_media_posts;

-- =============================================================================
-- USAGE NOTES
-- =============================================================================

-- These sample datasets are ready to use for:
-- 1. Sentiment analysis with AI_CLASSIFY()
-- 2. Text summarization with AI_GENERATE_TEXT()
-- 3. Entity extraction with AI_EXTRACT()
-- 4. Semantic search with AI_EMBED() and AI_SIMILARITY()
-- 5. Custom AI Function development
-- 6. Production pipeline patterns and optimization

-- Run this script to populate your workshop environment with realistic data!
