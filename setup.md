# Global Transactions Assessment - Setup Guide

## Getting Started (15 minutes)

### 1. Clone the Repository

```bash
git clone [repository-url]
cd global_transactions
```

### 2. Connect to the Database

The SQLite database is already built and located at:
`target/global_transactions.db`

You can connect to it using any of these options:

#### Option A: DataGrip (Recommended)

1. Open DataGrip
2. File > New > Data Source > SQLite
3. Browse to `target/global_transactions.db`
4. Test Connection & Save

#### Option B: DBeaver

1. Open DBeaver
2. File > New > Database Connection
3. Select SQLite
4. Browse to `target/global_transactions.db`
5. Finish

#### Option C: VSCode with SQLite Viewer

1. Install SQLite Viewer extension in VSCode
2. Right-click on `target/global_transactions.db`
3. Select "Open With SQLite Viewer"

### 3. Verify Your Setup

All data for this assessment is in the seeds folder. An example staging model (stg_transactions.sql) has been provided to help you get started.

Run this query to check you can access the data:
```sql
SELECT 
    transaction_type,
    COUNT(*) as count
FROM transactions
GROUP BY transaction_type;
```

### 4. Start Development

1. Create a new branch:
```bash
git checkout -b solution-[your-full-name]
```

2. Project structure:

```
global_transactions/
├── README.md           # Project requirements
├── models/            
│   ├── staging/       # Create your staging models here
│   ├── intermediate/  # Add intermediate calculations
│   └── marts/         # Final output models
└── target/
    └── global_transactions.db  # Pre-built database
```

3. Submit your solution:
- Push your branch
- Create a Pull Request

## Available Tables

1. `transactions` - All platform transactions
2. `client_contracts` - Client discount agreements
3. `currency_rates` - Daily currency exchange rates
4. `transaction_resolutions` - Chargeback resolution statuses

## Need Help?

Contact lanre.nathaniel-ayodele@prolific.com if you have any technical issues.