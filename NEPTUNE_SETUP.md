# Neptune Database Setup Guide

This guide walks through setting up and deploying the Maria RAGnet graph to AWS Neptune.

## Prerequisites

1. **AWS Neptune Database** - Already provisioned in your AWS account
2. **S3 Bucket** - For bulk loading data into Neptune
3. **IAM Role** - With permissions for Neptune bulk load from S3
4. **AWS Credentials** - Configured locally via AWS CLI or environment variables

## Installation

### 1. Install Python Dependencies

```bash
cd data-build
pip install -r requirements.txt
```

This installs:
- `boto3` - AWS SDK for Python
- `gremlinpython` - Gremlin client for Neptune queries
- Other data processing libraries

### 2. Configure Environment

Copy the example environment file and fill in your Neptune details:

```bash
cp .env.example .env
```

Edit `.env` with your Neptune configuration:

```bash
# Neptune cluster endpoint (without protocol or port)
NEPTUNE_ENDPOINT=your-cluster.cluster-xxxxx.us-east-1.neptune.amazonaws.com

# S3 bucket for Neptune bulk loading
NEPTUNE_S3_BUCKET=your-neptune-bulk-load-bucket

# IAM role ARN with Neptune bulk load permissions
NEPTUNE_IAM_ROLE=arn:aws:iam::123456789012:role/NeptuneLoadFromS3

# AWS Region
AWS_REGION=us-east-1
```

### 3. AWS Credentials

Ensure AWS credentials are configured. Options:

**Option A: AWS CLI** (recommended)
```bash
aws configure
```

**Option B: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

**Option C: Add to .env file**
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

## Data Pipeline

### Step 1: Generate Neptune Bulk Load Files

The graph data files should already be generated in `out/neptune_load/`:

```bash
# If you need to regenerate:
cd data-build
python build_graph_artifacts.py
```

This creates:
- `out/neptune_load/nodes.csv` - All MP/UP nodes with properties
- `out/neptune_load/edges.csv` - NEAR and TIME_OVERLAP relationships

### Step 2: Upload to Neptune

Load the environment variables and run the bulk loader:

```bash
# Load environment variables
export $(cat ../.env | xargs)

# Upload to Neptune
python neptune_bulkload.py \
  --neptune-endpoint $NEPTUNE_ENDPOINT \
  --s3-bucket $NEPTUNE_S3_BUCKET \
  --iam-role-arn $NEPTUNE_IAM_ROLE \
  --region $AWS_REGION \
  --data-dir ../out/neptune_load
```

Or simply (using environment variables from .env):

```bash
source ../.env
python neptune_bulkload.py
```

The script will:
1. Upload `nodes.csv` and `edges.csv` to S3
2. Trigger Neptune bulk load job
3. Monitor progress until completion
4. Report success/failure

### Step 3: Verify the Load

Check graph statistics:

```bash
python run_cypher_jobs.py --neptune-endpoint $NEPTUNE_ENDPOINT stats
```

Expected output:
```json
{
  "missing_persons": 1234,
  "unidentified_persons": 567,
  "near_edges": 8910,
  "time_overlap_edges": 1234,
  "total_nodes": 2345,
  "total_edges": 10144
}
```

## Querying Neptune

### CLI Query Interface

The `run_cypher_jobs.py` script provides several query commands:

#### Get Graph Statistics
```bash
python run_cypher_jobs.py stats
```

#### Find Matches for a Missing Person
```bash
# Find top matches for MP-001
python run_cypher_jobs.py find-mp MP-001

# With custom threshold and limit
python run_cypher_jobs.py find-mp MP-001 --min-score 0.7 --limit 10
```

#### Find Matches for an Unidentified Person
```bash
python run_cypher_jobs.py find-uid UID-101 --min-score 0.6
```

#### Find Matches by State
```bash
# All matches in Arkansas within 200km
python run_cypher_jobs.py find-state AR

# Custom distance threshold
python run_cypher_jobs.py find-state AR --max-distance 100 --limit 50
```

#### Find Matches by Sex
```bash
python run_cypher_jobs.py find-sex F --limit 100
```

### Programmatic Access

Use the `NeptuneQueryClient` class in your Python code:

```python
from run_cypher_jobs import NeptuneQueryClient

# Initialize client
client = NeptuneQueryClient(
    neptune_endpoint="your-cluster.cluster-xxxxx.us-east-1.neptune.amazonaws.com"
)

# Find matches for a missing person
matches = client.find_matches_for_mp("MP-001", min_score=0.7, limit=20)

# Get stats
stats = client.get_graph_stats()

# Close connection
client.close()
```

## Neptune Network Access

Neptune clusters run in a VPC and are not publicly accessible. Options:

### Option 1: EC2 Instance in Same VPC
Run queries from an EC2 instance in the same VPC as Neptune.

### Option 2: VPN/Direct Connect
Set up VPN or Direct Connect to your VPC.

### Option 3: Neptune Workbench
Use Neptune Workbench (managed Jupyter notebook) for interactive queries.

### Option 4: Lambda Functions
Deploy Lambda functions in the same VPC for programmatic access.

## Data Schema

### Node Types

**MissingPerson** (label: `MissingPerson`)
- `id`: Unique identifier (e.g., "MP-001")
- `sex`: M/F/U
- `age_min`, `age_max`: Age range
- `height_in`: Height in inches
- `weight_lb`: Weight in pounds
- `date`: Date missing (ISO format)
- `lat`, `lon`: Last seen location
- `state`: Two-letter state code
- `eye_color`: Eye color
- `race`: Race/ethnicity
- `notes`: Case notes

**UnidentifiedPerson** (label: `UnidentifiedPerson`)
- Same schema as MissingPerson
- `date`: Date found

### Edge Types

**NEAR** (label: `NEAR`)
- Connects MissingPerson → UnidentifiedPerson
- `km`: Distance in kilometers
- `days_gap`: Temporal gap in days
- `score`: Match confidence score (0.0-1.0)

**TIME_OVERLAP** (label: `TIME_OVERLAP`)
- Temporal relationship between cases
- `days_gap`: Days between events

## Scoring Algorithm

Matches are scored using weighted similarity across multiple dimensions:

1. **Sex Match** (weight: 1.0) - Hard requirement
2. **Age Similarity** (weight: 1.2) - Exponential decay over age gaps
3. **Height Similarity** (weight: 0.8) - Exponential decay over height differences
4. **Weight Similarity** (weight: 0.4) - Exponential decay over weight differences
5. **Distance** (weight: 1.0) - Logarithmic decay with geographic distance
6. **Date Consistency** (weight: 1.0) - Temporal overlap constraint
7. **Eye Color** (weight: 0.5) - Categorical match
8. **Tattoo Signals** (weight: 1.4) - Shared tattoo descriptions
9. **Clothing Signals** (weight: 0.8) - Shared clothing items
10. **Distinctive Marks** (weight: 1.6) - Presence of distinctive features

Final score: Weighted average normalized to 0.0-1.0

## Troubleshooting

### Bulk Load Fails

**Check IAM Role Permissions:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket/*",
        "arn:aws:s3:::your-bucket"
      ]
    }
  ]
}
```

**Check Neptune Trust Relationship:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "rds.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

**View Load Status:**
```bash
python -c "
from neptune_bulkload import NeptuneBulkLoader
loader = NeptuneBulkLoader('$NEPTUNE_ENDPOINT', '$NEPTUNE_S3_BUCKET', '$NEPTUNE_IAM_ROLE')
status = loader.check_load_status('YOUR_LOAD_ID')
print(status)
"
```

### Connection Errors

**Cannot connect to Neptune:**
- Verify you're in the same VPC or have VPN access
- Check security group rules allow port 8182
- Verify Neptune endpoint is correct

**SSL/TLS Errors:**
- Neptune requires WebSocket Secure (wss://)
- Ensure port 8182 is open for WebSocket connections

## Next Steps: Adding True RAG Capabilities

Current implementation is rule-based graph matching. To add RAG:

1. **Vector Embeddings:**
   ```bash
   pip install sentence-transformers
   ```
   Generate embeddings from case descriptions and store as node properties.

2. **Semantic Search:**
   Use Neptune's vector similarity search (if available in your Neptune version) or integrate with OpenSearch.

3. **LLM Integration:**
   ```bash
   pip install anthropic  # or openai
   ```
   Use Claude/GPT to generate match explanations and refinement suggestions.

4. **Update Schema:**
   Add `embedding` property to nodes (vector type) and query using cosine similarity.

## References

- [Neptune Bulk Loader](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html)
- [Gremlin Query Language](https://tinkerpop.apache.org/docs/current/reference/#graph-traversal-steps)
- [Neptune Workbench](https://docs.aws.amazon.com/neptune/latest/userguide/notebooks.html)
