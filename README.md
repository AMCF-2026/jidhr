# 🌳 Jidhr (جذر) - AMCF Operations Assistant

**Jidhr** ("The Root" in Arabic) is an AI-powered operations assistant for the American Muslim Community Foundation. It provides a natural language interface to query and interact with both HubSpot (CRM/Marketing) and CSuite (Fund Accounting).

## Features

- **Natural Language Queries** - Ask questions in plain English
- **HubSpot Integration** - Contacts, forms, social media, campaigns, events
- **CSuite Integration** - Funds, donations, grants, profiles, vouchers
- **Smart Context** - Automatically gathers relevant data based on your question
- **Conversation Memory** - Maintains context across messages

## Project Structure

```
jidhr/
├── app.py              # Flask routes (slim!)
├── config.py           # Settings & environment variables
├── assistant.py        # Main AI brain
├── clients/
│   ├── __init__.py
│   ├── openrouter.py   # Claude API client
│   ├── hubspot.py      # HubSpot API client
│   └── csuite.py       # CSuite API client
├── templates/
│   └── chat.html       # Web interface
├── static/
│   └── style.css       # Styles
├── requirements.txt
├── railway.toml
├── Procfile
└── .env.example
```

## Quick Start

### Local Development

1. Clone the repo:
   ```bash
   git clone https://github.com/your-org/jidhr.git
   cd jidhr
   ```

2. Create virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # or venv\Scripts\activate on Windows
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment:
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

5. Run:
   ```bash
   python app.py
   ```

6. Visit http://localhost:5000

### Deploy to Railway

1. Push to GitHub
2. Connect repo to Railway
3. Add environment variables in Railway dashboard:
   - `OPENROUTER_API_KEY`
   - `HUBSPOT_ACCESS_TOKEN`
   - `CSUITE_API_KEY`
   - `CSUITE_API_SECRET`
4. Deploy!

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENROUTER_API_KEY` | Yes | OpenRouter API key for Claude access |
| `HUBSPOT_ACCESS_TOKEN` | Yes | HubSpot Personal Access Token |
| `CSUITE_API_KEY` | Yes | CSuite API key |
| `CSUITE_API_SECRET` | Yes | CSuite API secret |
| `CSUITE_BASE_URL` | No | CSuite API URL (defaults to AMCF) |
| `SECRET_KEY` | No | Flask secret key |
| `DEBUG` | No | Enable debug mode (default: False) |
| `PORT` | No | Port to run on (default: 5000) |

## Example Queries

**CSuite (Fund Accounting):**
- "What's the balance in the Tanvir Fund?"
- "Show me recent donations"
- "List all DAF funds"
- "Who are our largest donors?"

**HubSpot (Marketing/CRM):**
- "Who submitted the endowment form?"
- "Show me our social channels"
- "List recent contacts"
- "What marketing events do we have?"

**Cross-System:**
- "Draft a thank you email for the Ahmeds"
- "What's the status of the Women's Giving Circle?"

## Roadmap

- **v1.0** 🌱 Query HubSpot + CSuite (current)
- **v1.5** 🌿 Slack integration + bi-directional sync
- **v2.0** 🌲 Google Trends → auto-draft social content
- **v3.0** 🌳 Donor-facing assistant
- **v4.0** 🌴 iOS app

## License

Private - American Muslim Community Foundation
