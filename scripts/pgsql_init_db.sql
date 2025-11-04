/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'datawarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.

IMPORTANT:
    This script must be run in TWO parts:
    1. First, run Part 1 while connected to the 'postgres' database
    2. Then, run Part 2 while connected to the 'datawarehouse' database
*/

-- ============================================================
-- PART 1: Run this while connected to 'postgres' database
-- ============================================================

-- Terminate all active connections to the 'datawarehouse' database
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'datawarehouse'
  AND pid <> pg_backend_pid();

-- Drop the 'datawarehouse' database if it exists
DROP DATABASE IF EXISTS datawarehouse;

-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;


-- ============================================================
-- PART 2: Now connect to 'datawarehouse' database and run this
-- ============================================================

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

