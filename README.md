# PayU Financial Reconciliation & ETL Pipeline

## Overview
This project provides a professional-grade automated solution for Financial Reconciliation. It integrates directly with the PayU Latam API to request, generate, and process transactional reports, transforming raw financial data into structured insights stored in Microsoft SQL Server.

## Engineering Highlights
* Object-Oriented Design (OOP): Built using a modular class-based architecture for better maintainability.
* Robust API Orchestration: Implements JWT/OAuth2 flow and automated Merchant Switching.
* Intelligent Polling Mechanism: Retry logic to wait for asynchronous file generation.
* Data Integrity & Smart Upsert: Performs CDC logic to identify state updates and uses SQLAlchemy Transactions for atomicity.
* Security First: Separation of credentials into environment variables (.env).

## Tech Stack
* Language: Python 3.x
* Data Processing: Pandas, NumPy
* Database: SQLAlchemy (MSSQL / pyodbc)
* Integration: REST API (Requests Session)
* Environment: Python-Dotenv

## Prerequisites
* A valid PayU Merchant account.
* SQL Server instance.
* Environment variables configured (see .env.example).

## How to use

### Step 1: Clone the repo
git clone https://github.com/Rodo3128/payu-reconciliation-api.git

### Step 2: Install dependencies
pip install -r requirements.txt

### Step 3: Configure Environment
Create a .env file based on .env.example with your PayU and DB credentials.

### Step 4: Run the Pipeline
python main.py

## Impact
By implementing this pipeline, the manual reconciliation workload is reduced from hours to minutes, eliminating human error and providing a reliable Single Source of Truth for Finance and Operations teams.