# Fred Scraper

## Environment Variables
* variables
    `MODE` -  `prod` or `dev`(default)
* dotenv files
    * production
        * `.env.local.production`
        * `.env.production`
        * `.env.local`
        * `.env`
    * development
        * `.env.local.development`
        * `.env.development`
        * `.env.local`
        * `.env`
    * required variables
        *
        * CUSTOM_STORAGE_DIR: directory to store the custom data for fred 
        * FRED_API_KEY: api key for fredapi

## Tradefi - Fred

### Execution

Development
```bash
MODE=dev python main.py
```

Production
```bash
MODE=prod python main.py
```

### Structure
* main.py
* Fred engine
    * packages/fred/engine.py
    * Handler
        * packages/fred/handler.py
    * Runs once a day
