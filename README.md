# Scalable Web Scraper and Company Data Matching API

This project is a complete, distributed data pipeline built in Python. It takes a list of websites, scrapes them using a resilient multi-tiered fetching strategy, extracts structured company contact information, stores it in Elasticsearch, and exposes a smart API to find the best-matching company profile for a given input.

The entire system is orchestrated with Docker and uses Kafka for robust, asynchronous communication between services.

## Core Features

-   **Distributed Architecture:** Services are decoupled using Kafka, allowing for independent scaling and resilience.
-   **Multi-Tiered Fetching Strategy:**
    -   **Simple Fetcher:** A lightweight, high-performance asynchronous fetcher using `aiohttp`.
    -   **Advanced Fetcher:** A powerful, browser-based fetcher using `selenium-base` to render JavaScript and handle Single-Page Applications (SPAs).
-   **Intelligent Escalation Logic:** The system automatically detects if a page is a likely SPA (based on content analysis) and escalates it from the simple to the advanced fetcher.
-   **Robust Data Storage & Search:** Uses Elasticsearch to store company records and provide powerful full-text search and matching capabilities.
-   **Data Normalization:** Enriches the stored data with normalized fields (E.164 for phone numbers, clean domains, social media profile IDs) to achieve a high match rate.
-   **Smart Matching API:** A FastAPI endpoint that uses a weighted, multi-field query to find the single best-matching company profile.
-   **Fully Containerized:** The entire application stack is defined and managed by Docker Compose for easy setup and deployment.
-   **Observability:** Includes a dedicated logging service and an analytics service to monitor pipeline health and performance.

## System Architecture

The pipeline is composed of several microservices communicating through Kafka topics. The `Extractor Service` acts as the central brain for routing and escalation.

```mermaid
graph TD
    subgraph Input
        A[Input CSV file] --> B[URL Producer];
    end

    B --> C[t: urls_to_fetch];

    subgraph Fetching Tier 1 (Fast)
        C --> D[Fetcher Simple];
    end

    D --> E[t: htmls_to_process];

    subgraph Processing & Routing
        E --> F[Extractor Service];
    end

    subgraph Data & Storage
        F -- Success --> G[t: extracted_data];
        G --> H[Storage Service];
        H --> I[Elasticsearch];
    end

    subgraph Escalation & Failure Handling
        F -- SPA Detected --> J[t: urls_to_fetch_advanced];
        J --> K[Fetcher Advanced];
        K -- Success --> E;
        K -- Network Error --> L[t: failed_urls_dlq];
        F -- Advanced Fetch Failed --> L;
    end
    
    subgraph API
        M[API Service] <--> I;
        N[User] --> M;
    end

    subgraph Observability
        subgraph All Services
            B; D; F; H; K; M;
        end -- Logs --> O[t: log_events];
        G --> P[Analytics Service];
        O --> P;
    end

    style C fill:#f9f,stroke:#333,stroke-width:2px
    style E fill:#f9f,stroke:#333,stroke-width:2px
    style G fill:#f9f,stroke:#333,stroke-width:2px
    style J fill:#f9f,stroke:#333,stroke-width:2px
    style L fill:#f9f,stroke:#333,stroke-width:2px
    style O fill:#f9f,stroke:#333,stroke-width:2px
```

## Architectural Decisions

The architecture of this project was deliberately chosen to prioritize scalability, resilience, and maintainability.

-   **Why Kafka? (Decoupling and Asynchronicity)**
    -   **Resilience:** Kafka acts as a durable buffer between services. If the `ExtractorService` crashes, the `FetcherService` can continue to produce messages without data loss. Once the `ExtractorService` restarts, it will resume processing from where it left off. This is crucial for a long-running scraping job.
    -   **Independent Scaling:** Each service is a separate consumer group. If fetching becomes a bottleneck, we can scale up the number of `fetcher` service containers without touching any other part of the system. The same applies to the `extractor`, `storage`, or any other service.
    -   **Backpressure Handling:** Kafka naturally handles backpressure. If the `ExtractorService` is slower than the `FetcherService`, messages will simply accumulate in the `htmls_to_process` topic without overwhelming the extractor service's memory.

-   **Why a Multi-Tiered Fetching Strategy? (Efficiency and Capability)**
    -   **Cost/Speed Optimization:** A simple, asynchronous `aiohttp` fetch is extremely fast and resource-efficient. A full browser fetch with Selenium is slow, CPU-intensive, and memory-heavy. By using the simple fetcher as the first line of attack, we process the vast majority of "easy" websites cheaply.
    -   **Targeted Power:** We only use the expensive "advanced" fetcher when absolutely necessary, as determined by the `ExtractorService`. This targeted approach provides the power to handle complex JavaScript-driven sites without paying the performance penalty for every single URL.

-   **Why is the Extractor the "Brain"? (Single Responsibility & Centralized Logic)**
    -   By moving the escalation logic from the fetcher to the extractor, we adhere to the Single Responsibility Principle. The fetcher's job is just to fetch; the extractor's job is to understand HTML.
    -   The `ExtractorService` is the only service that can definitively say, "This HTML is empty and contains no useful data." Placing the re-queueing logic here is more accurate and centralizes the complex decision-making process.

-   **Why Elasticsearch? (Beyond Storage to Search)**
    -   For the final goal of matching company profiles, a standard relational database is insufficient. Simple `LIKE` queries are slow and cannot handle the "fuzzy" nature of matching names or differently formatted phone numbers.
    -   Elasticsearch's powerful text analysis, inverted index, and relevance scoring are purpose-built for this kind of search and matching problem, providing both the speed and the capabilities required for a high match rate.

---

## Technology Stack

-   **Backend:** Python 3.10+
-   **Messaging Queue:** Apache Kafka
-   **Database/Search Index:** Elasticsearch
-   **API Framework:** FastAPI
-   **Web Fetching:** `aiohttp` (simple), `selenium-base` (advanced)
-   **Orchestration:** Docker & Docker Compose
-   **Key Python Libraries:** `confluent-kafka`, `elasticsearch-py`, `phonenumbers`, `beautifulsoup4`, `uvicorn`.

## Setup and Installation

### Prerequisites

-   [Docker](https://docs.docker.com/get-docker/)
-   [Docker Compose](https://docs.docker.com/compose/install/)
-   Python 3.10+ (for running local test scripts)

### Installation Steps

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd <your-repo-directory>
    ```

2.  **Prepare Input Data:**
    Place the list of websites to be scraped in the `data/sample-websites.csv` file. It must have a header row with at least a `domain` column.
    Place the list company names matched to domains in the `data/sample-websites-company-names.csv` file. The header row contains `domain`, `company_commercial_name`,`company_legal_name` and `company_all_available_names` columns.

3.  **Install Python dependencies:**
    These are required for running the local API testing script.
    ```bash
    pip install -r requirements.txt
    ```

## How to Run the Pipeline

1.  **Start the entire application stack:**
    From the root directory, run the following command. The `--build` flag is necessary the first time or after changing dependencies.
    ```bash
    docker-compose up --build
    ```
    This will start all services, including Kafka, Elasticsearch, and the application microservices, in the foreground.

2.  **To run in detached mode (in the background):**
    ```bash
    docker-compose up --build -d
    ```

3.  **To shut down the entire stack:**
    ```bash
    docker-compose down -v
    ```

## Usage

### Kicking off the Scraping Process

The `url_producer` service is configured to run once and then exit. It reads the URLs from `data/sample-websites.csv` and seeds the pipeline. The scraping and processing will begin automatically as soon as the services are up.

### Using the Matching API

The API service is exposed on your local machine at port `8000`.

#### Interactive API Documentation

FastAPI provides automatic, interactive API documentation. Once the services are running, navigate to:

**[http://localhost:8000/docs](http://localhost:8000/docs)**

You can see the endpoint, its required schema, and test it directly from your browser.

#### `POST /match` Endpoint

This endpoint accepts a JSON object with one or more fields and returns the single best-matching company profile.

-   **URL:** `http://localhost:8000/match`
-   **Method:** `POST`
-   **Request Body:**

    ```json
    {
      "name": "Company Name",
      "phone": "+15551234567",
      "website": "example.com",
      "input_facebook": "https://facebook.com/example"
    }
    ```

#### `curl` Example

```bash
curl -X POST "http://localhost:8000/match" \
-H "Content-Type: application/json" \
-d '{
  "website": "steppir.com"
}'
```

-   **Success Response (`200 OK`):** Returns the JSON object of the matched company profile.
-   **Not Found Response (`404 Not Found`):** Returned if no suitable match is found in the database.

## Testing the API

A script is provided to test the match rate of the API against a sample input file.

1.  **Prepare Test Data:** Ensure the `data/api_test_data.csv` file exists.
2.  **Run the Test Script:** Make sure the Docker stack is running, then execute the script in a separate terminal.
    ```bash
    python test_api_script.py
    ```
    The script will log its progress and output a final match rate report. This is the key metric for evaluating and tuning the matching algorithm's performance.
3.  **Expected match rate report at time of writing:**
    ```bash
    2025-06-09 12:55:07 - INFO - Match Rate Report
    2025-06-09 12:55:07 - INFO - Total Test Cases:      32
    2025-06-09 12:55:07 - INFO - Successful Matches:    31
    2025-06-09 12:55:07 - INFO - Failed/No Matches:     1
    2025-06-09 12:55:07 - INFO - Overall Match Rate:  96.88%
    ```

## Future Work & Potential Improvements

This project provides a solid foundation, but there are many ways it could be extended and improved:

-   **Proxy Integration & Rotation:** The Fetchers could integrate a proxy rotation service (e.g., using `rotating-proxies` library). This would make the system far more robust against IP blocks.

-   **Kubernetes:** Solid choice to make the project truly production ready.

-   **Enhanced Data Extraction:** Current data extraction regex patterns still have bugs. It is very hard to create a perfect heuristic here, so enhancements in this area are plenty.

-   **Unit Tests:** The project has no unit tests. This is deliberate, as the focus was mostly on functionality.

-   **CI/CD Pipeline:** Implement a continuous integration and deployment pipeline (e.g., using GitHub Actions) to automate testing, building, and deploying Docker images to a container registry.

-   **Advanced Monitoring and Alerting:** While the `AnalyticsService` provides basic metrics, integrating with a monitoring stack like Prometheus and Grafana would provide deeper insights into pipeline performance.

## Project Structure

```
.
├── data/
│   └── sample-websites.csv   # Input for the URL producer
├── services/
│   ├── api_service.py
│   ├── analytics_service.py
│   ├── extractor_service.py
│   ├── fetcher_service.py
│   ├── logging_service.py
│   └── storage_service.py
├── src/
│   ├── components/
│   │   ├── html_data_extractor.py
│   │   ├── pipeline_metrics_tracker.py
│   │   └── web_fetcher.py
│   ├── models/
│   │   └── company_record.py
│   └── utils/
│       ├── config.py
│       ├── elastic_search_utils.py
│       ├── kafka_log_handler.py
│       ├── kafka_utils.py
│       └── logging_setup.py
├── api_test_data.csv         # Input for the test script
├── docker-compose.yml        # Main orchestration file
├── Dockerfile                # Docker build instructions
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── run_url_producer.py       # Script to start the pipeline
└── test_api_script.py        # Script to test the API match rate
```