# Crypto Dagster Pipeline 🚀

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Dagster](https://img.shields.io/badge/Dagster-Orchestrated-green)
![License](https://img.shields.io/badge/License-MIT-green)
![Last Commit](https://img.shields.io/github/last-commit/Athirath-1304/crypto-dagster-pipeline)

---

A modern data engineering pipeline built with **Dagster** that ingests cryptocurrency data from the [CoinGecko API](https://www.coingecko.com/), processes it through a robust ETL pipeline, and provides real-time insights. This project demonstrates best practices in data orchestration, asset management, and modular data engineering.

---

## 📌 Key Features

- ⛽ **Data Ingestion:** Live cryptocurrency data from CoinGecko API
- 🔄 **Dagster Orchestration:** Modern data orchestration with assets and jobs
- 📊 **Data Processing:** Transform and validate crypto market data
- 🗄️ **Data Storage:** DuckDB for fast analytical queries
- 📈 **Real-time Insights:** Live market data analysis
- 🧪 **Testing:** Comprehensive unit tests
- 🔧 **Development Ready:** Hot reloading and development tools

---

## 🛠️ Tech Stack

- **Python 3.10+**
- **Dagster** for data orchestration
- **DuckDB** for analytical data storage
- **pandas** for data manipulation
- **requests** for API calls
- **pytest** for testing

---

## 🚀 Quick Start

### Prerequisites

- Python 3.10 or higher
- pip package manager

### Installation

1. **Clone the repository:**
```bash
git clone https://github.com/Athirath-1304/crypto-dagster-pipeline.git
cd crypto-dagster-pipeline
```

2. **Install the project in editable mode:**
```bash
pip install -e ".[dev]"
```

3. **Start the Dagster UI:**
```bash
dagster dev
```

4. **Open your browser:**
Navigate to [http://localhost:3000](http://localhost:3000) to access the Dagster UI.

---

## 📂 Project Structure

```
crypto-dagster-pipeline/
├── crypto_pipeline_project/
│   ├── __init__.py
│   ├── assets.py              # Dagster assets
│   ├── definitions.py          # Dagster definitions
│   ├── models.py              # Data models
│   └── schedules.py           # Dagster schedules
├── crypto_pipeline_project_tests/
│   ├── __init__.py
│   └── test_assets.py         # Unit tests
├── data/
│   ├── crypto_data.duckdb     # DuckDB database
│   └── crypto_data_fixed.duckdb
├── pyproject.toml             # Project configuration
├── setup.py                   # Package setup
├── requirements.txt           # Dependencies
└── README.md
```

---

## 🔧 Development

### Adding New Assets

Assets are automatically loaded into the Dagster code location as you define them in `crypto_pipeline_project/assets.py`.

Example asset:
```python
@asset
def crypto_data():
    """Fetch cryptocurrency data from CoinGecko API."""
    # Your asset logic here
    return data
```

### Adding Dependencies

You can specify new Python dependencies in `setup.py`:

```python
install_requires=[
    "dagster",
    "pandas",
    "requests",
    "duckdb",
    # Add your dependencies here
]
```

### Running Tests

Tests are in the `crypto_pipeline_project_tests` directory:

```bash
pytest crypto_pipeline_project_tests
```

### Schedules and Sensors

To enable Dagster Schedules or Sensors for your jobs, the Dagster Daemon process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

---

## 📊 Data Pipeline Flow

1. **Data Ingestion:** Fetch live cryptocurrency data from CoinGecko API
2. **Data Validation:** Ensure data quality and schema compliance
3. **Data Processing:** Transform and enrich the data
4. **Data Storage:** Store processed data in DuckDB for analytical queries
5. **Monitoring:** Track pipeline performance and data quality

---

## 🧪 Testing

The project includes comprehensive unit tests:

```bash
# Run all tests
pytest crypto_pipeline_project_tests

# Run with coverage
pytest crypto_pipeline_project_tests --cov=crypto_pipeline_project
```

---

## 🚀 Deployment

### Local Development

```bash
# Start Dagster UI
dagster dev

# Run specific jobs
dagster job execute -f crypto_pipeline_project/definitions.py -j your_job_name
```

### Production Deployment

The easiest way to deploy your Dagster project is to use **Dagster Cloud**:

1. Sign up for [Dagster Cloud](https://cloud.dagster.io/)
2. Connect your GitHub repository
3. Deploy with automatic CI/CD

For self-hosted deployment, refer to the [Dagster documentation](https://docs.dagster.io/deployment).

---

## 📈 Monitoring

- **Dagster UI:** Monitor pipeline runs, assets, and schedules
- **Asset Lineage:** Track data dependencies and transformations
- **Error Handling:** Comprehensive error tracking and alerting
- **Performance Metrics:** Monitor pipeline execution times and resource usage

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📜 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---

## 🙌 Connect with Me

Built by **Athirath Bommerla** — Data Engineer & ML Enthusiast 🚀

- 📫 **LinkedIn:** [linkedin.com/in/athirath-bommerla](https://www.linkedin.com/in/athirath-bommerla-7a1076237/)
- 💻 **GitHub:** [github.com/Athirath-1304](https://github.com/Athirath-1304)
- 📧 **Email:** athirathbommerla7@gmail.com

---

## ⭐ Support

If you find this project helpful, please give it a star! It helps others discover the project and motivates further development.

---

## 🔮 Roadmap

- [ ] Add more cryptocurrency data sources
- [ ] Implement real-time streaming with Kafka
- [ ] Add machine learning models for price prediction
- [ ] Create advanced analytics dashboard
- [ ] Add data quality monitoring
- [ ] Implement automated testing pipeline
- [ ] Add Docker containerization
- [ ] Create Kubernetes deployment manifests

---

*Built with ❤️ using Dagster*
