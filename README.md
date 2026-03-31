# Notion Game Backlog Sync

A Notion-backed game backlog metadata sync tool that makes your backlog smarter with API-driven metadata and cover posters.

## Features
- API ingestion from HLTB and Metacritic.
- Steam cover/poster lookup for enhanced game artwork.
- Local metadata collection and poster lookup.
- Optional Notion row update workflow for backlog enrichment.
- CSV/JSON export for archive and analysis.

## Setup
1. Create a virtual environment:
   python -m venv .venv
2. Activate:
   PowerShell: .\.venv\Scripts\Activate.ps1
3. Install dependencies:
   pip install -r requirements.txt
4. Copy .env.example to .env and configure keys.

## Usage
- python populate_game_metadata.py
- python populate_hltb.py
- python populate_metacritic.py
- python populate_posters.py

## My Notion Setup
https://clever-addition-8ec.notion.site/f420b94ee5884f4fb52b16de5f962ea3?v=c8355725fb2d45d3a565018b11aee313
