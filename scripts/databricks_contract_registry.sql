-- Databricks Contract Registry: table definition and useful queries

-- 1) Registry table (run once)
CREATE TABLE IF NOT EXISTS contracts.contract_registry (
  contract_id STRING NOT NULL,
  contract_name STRING NOT NULL,
  version STRING NOT NULL,
  domain STRING,
  contract_content STRING NOT NULL,
  published_at TIMESTAMP NOT NULL,
  publisher STRING NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (contract_id, version)
)
USING DELTA;

-- 2) Latest contract versions
SELECT
  contract_id,
  max(published_at) AS last_published_at,
  max(version) AS latest_version
FROM contracts.contract_registry
GROUP BY contract_id;

-- 3) Contracts published recently
SELECT
  contract_id,
  contract_name,
  version,
  domain,
  publisher,
  published_at
FROM contracts.contract_registry
ORDER BY published_at DESC
LIMIT 100;

-- 4) Publisher activity / operational stats
SELECT
  publisher,
  count(*) AS contracts_published,
  min(published_at) AS first_published,
  max(published_at) AS last_published
FROM contracts.contract_registry
GROUP BY publisher
ORDER BY contracts_published DESC;

-- 5) Schema diff by version (hash-based)
SELECT
  contract_id,
  version,
  sha2(contract_content, 256) AS contract_hash,
  published_at
FROM contracts.contract_registry
ORDER BY contract_id, published_at;
