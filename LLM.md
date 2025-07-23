# LLM.md

This file provides guidance to LLM agents when working with code in this repository.

## Project Overview

Data Universe is a Bittensor Subnet 13 for data scraping and sentiment analysis. It collects real-time data from social media sources (X/Twitter, Reddit, YouTube) and stores it in a distributed network of miners. Validators query miners and score them based on data quality, freshness, and diversity.

### Core Architecture

**Miners** (`neurons/miner.py`):
- Scrape data from configured sources using the scraping system
- Store data locally in SQLite databases via `SqliteMinerStorage`
- Upload data to HuggingFace or S3 for public access
- Serve data to validators through protocol requests

**Validators** (`neurons/validator.py`):
- Query miners for their data indexes
- Verify data quality and authenticity
- Score miners based on data value, freshness, and credibility
- Maintain a complete view of network data distribution

**Data Model** (`common/data.py`):
- `DataEntity`: Individual pieces of scraped data (tweets, posts, etc.)
- `DataEntityBucket`: Logical grouping by source, time, and label
- `DataEntityBucketId`: Unique identifier (DataSource + TimeBucket + DataLabel)
- `CompressedMinerIndex`: Summary of miner's data for efficient querying

**Protocol** (`common/protocol.py`):
- `GetMinerIndex`: Retrieve miner's data summary
- `GetDataEntityBucket`: Fetch specific data buckets
- `OnDemandRequest`: Custom scraping requests from validators

## Development Commands

### Setup
```bash
pip install -r requirements.txt
```

### Running Nodes
```bash
# Run miner
python neurons/miner.py --netuid 13 --subtensor.network <network> --wallet.name <wallet> --wallet.hotkey <hotkey>

# Run validator  
python neurons/validator.py --netuid 13 --subtensor.network <network> --wallet.name <wallet> --wallet.hotkey <hotkey>
```

### Testing
```bash
# Run all tests (note: currently has module import issues)
python tests/test_all.py

# Run specific test files
python -m pytest tests/common/test_protocol.py
python -m pytest tests/scraping/test_coordinator.py
python -m pytest tests/storage/miner/test_sqlite_miner_storage.py
```

## Key Directories

- `neurons/`: Main miner and validator implementations
- `common/`: Shared data models, protocol definitions, and utilities
- `scraping/`: Data scraping system with source-specific scrapers
- `storage/`: Storage abstractions for miners and validators
- `rewards/`: Scoring and reward calculation logic
- `upload_utils/`: HuggingFace and S3 uploading functionality
- `vali_utils/`: Validator-specific utilities and APIs
- `dynamic_desirability/`: Dynamic data valuation system
- `tests/`: Unit and integration tests

## Scraping System

The scraping system is modular and configurable:

- `ScraperCoordinator` (`scraping/coordinator.py`): Orchestrates multiple scrapers
- `ScraperProvider` (`scraping/provider.py`): Factory for creating scrapers
- Source-specific scrapers in `scraping/reddit/`, `scraping/x/`, `scraping/youtube/`
- Configuration via JSON files in `scraping/config/`

## Storage Systems

**Miner Storage**:
- `MinerStorage` interface (`storage/miner/miner_storage.py`)
- SQLite implementation (`storage/miner/sqlite_miner_storage.py`)

**Validator Storage**:
- Memory-based storage for miner indexes
- S3 integration for large-scale data access

## Data Upload and Access

**HuggingFace Integration**:
- Dual uploader for metadata and datasets
- Encoding system for privacy and data integrity

**S3 Storage**:
- Partitioned storage by miner hotkey
- Efficient validator access to all miner data
- Blockchain-based authentication

## Configuration

- Miner configuration via command-line args and scraping config JSON
- Validator configuration for scoring, timeouts, and data validation
- Dynamic desirability updates from external systems

## Important Notes

- Data older than 30 days is not scored
- Miners are rewarded based on data value, freshness, and diversity
- All data is anonymized before public upload
- The system supports both organic (validator-initiated) and on-demand scraping