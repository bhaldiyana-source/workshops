# Databricks Apps Workshop - Quick Start Guide

## 🚀 Get Started in 5 Minutes

### Prerequisites

- [ ] Databricks workspace access
- [ ] SQL Warehouse running
- [ ] Databricks CLI installed
- [ ] Unity Catalog with at least one table

### Step 1: Install Databricks CLI

```bash
pip install databricks-cli
```

### Step 2: Configure Authentication

```bash
databricks configure --token
```

Enter your workspace URL and personal access token when prompted.

### Step 3: Choose Your First Lab

#### Option A: Start with Basics (Recommended)

```bash
cd apps/lab1-hello-world
```

#### Option B: Jump to Data Exploration

```bash
cd apps/lab2-data-explorer
```

#### Option C: Try ML Models

```bash
cd apps/lab3-ml-model-interface
```

### Step 4: Configure

Edit `app.yaml` and replace:

```yaml
warehouse_id: "YOUR_WAREHOUSE_ID_HERE"
```

**Find your Warehouse ID:**
1. Go to Databricks workspace
2. Click **SQL Warehouses**
3. Select your warehouse
4. Copy the ID from **Connection Details**

### Step 5: Deploy

```bash
databricks apps deploy /Users/<your-email>/<lab-name>
```

Example:
```bash
databricks apps deploy /Users/john.doe@company.com/lab1-hello-world
```

### Step 6: Access Your App

The deployment command will output your app URL. Open it in your browser!

---

## 📚 Lab Selection Guide

### I want to...

**Learn the basics** → Lab 1 (Hello World)

**Explore data interactively** → Lab 2 (Data Explorer)

**Work with ML models** → Lab 3 (ML Model Interface)

**Build dashboards** → Lab 4 (Multi-User Dashboard)

**Create an API** → Lab 5 (RESTful API)

---

## 🎯 Recommended Learning Path

### Path 1: Complete Beginner (2-3 hours)
1. Lab 1: Hello World (30 min)
2. Lab 2: Data Explorer (60 min)
3. Lab 4: Multi-User Dashboard (90 min)

### Path 2: ML Engineer (2 hours)
1. Lab 1: Hello World (30 min)
2. Lab 3: ML Model Interface (45 min)
3. Lab 5: RESTful API (45 min)

### Path 3: Backend Developer (2 hours)
1. Lab 1: Hello World (30 min)
2. Lab 2: Data Explorer (45 min)
3. Lab 5: RESTful API (45 min)

### Path 4: Complete Workshop (5+ hours)
1. Lab 1 → Lab 2 → Lab 3 → Lab 4 → Lab 5

---

## 🔧 Common Configuration

### All Labs Need

**app.yaml:**
```yaml
resources:
  - name: warehouse_connection
    warehouse:
      warehouse_id: "abc123def456"  # Your warehouse ID
```

### Labs 2, 4, 5 Also Need

```yaml
env:
  - name: CATALOG_NAME
    value: "main"
  - name: SCHEMA_NAME
    value: "default"
  - name: SAMPLE_TABLE
    value: "your_table"
```

### Lab 3 Needs

```yaml
env:
  - name: MODEL_NAME
    value: "catalog.schema.model_name"
  - name: MODEL_VERSION
    value: "1"
```

---

## ⚡ Quick Commands

### Deploy an app
```bash
databricks apps deploy /Users/<email>/<app-name>
```

### View logs
```bash
databricks apps logs /Users/<email>/<app-name>
```

### Update an app
```bash
databricks apps update /Users/<email>/<app-name>
```

### Delete an app
```bash
databricks apps delete /Users/<email>/<app-name>
```

### List all apps
```bash
databricks apps list
```

---

## 🐛 Quick Troubleshooting

### App won't start?
```bash
# Check logs
databricks apps logs /Users/<email>/<app-name>

# Common fixes:
# 1. Verify warehouse_id in app.yaml
# 2. Check requirements.txt dependencies
# 3. Ensure warehouse is running
```

### Permission denied?
```sql
-- Grant warehouse access
GRANT USE WAREHOUSE ON WAREHOUSE <id> TO `user@example.com`;

-- Grant catalog access
GRANT USE CATALOG ON CATALOG main TO `user@example.com`;
GRANT USE SCHEMA ON SCHEMA main.default TO `user@example.com`;
GRANT SELECT ON TABLE main.default.table TO `user@example.com`;
```

### Can't find warehouse ID?
1. Navigate to **SQL Warehouses** in your workspace
2. Click on your warehouse name
3. Go to **Connection Details** tab
4. Copy the value after `/warehouses/` in the HTTP Path

---

## 📖 What's Next?

After deploying your first app:

1. **Customize**: Modify the code to fit your needs
2. **Explore**: Try the interactive features
3. **Share**: Grant access to team members
4. **Extend**: Add new features and capabilities

---

## 🆘 Need Help?

- **Check Logs**: `databricks apps logs /path/to/app`
- **Review README**: Each lab has detailed documentation
- **Troubleshooting**: See `WORKSHOP_SUMMARY.md`
- **Documentation**: Visit [docs.databricks.com](https://docs.databricks.com)

---

## 💡 Pro Tips

1. **Start Simple**: Begin with Lab 1, even if you're experienced
2. **Test Locally**: Export environment variables and test before deploying
3. **Use Git**: Version control your modifications
4. **Read READMEs**: Each lab README has valuable insights
5. **Check Examples**: All labs include working code examples

---

## 🎉 You're Ready!

Pick a lab, follow the steps above, and start building!

**Time to first deployed app: ~5 minutes** ⚡

---

*For detailed information, see [README.md](README.md) and [WORKSHOP_SUMMARY.md](WORKSHOP_SUMMARY.md)*
