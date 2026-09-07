CREATE SCHEMA IF NOT EXISTS `qwiklabs-gcp-00-38c5c3a6722c.demo_dataset`;

CREATE TABLE IF NOT EXISTS `qwiklabs-gcp-00-38c5c3a6722c.demo_dataset.customers`
(
  customer_id INT64,
  customer_name STRING,
  email STRING,
  city STRING,
  created_at TIMESTAMP
);
  
 
