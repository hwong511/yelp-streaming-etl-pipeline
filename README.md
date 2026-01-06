# Yelp Streaming ETL Pipeline

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

A short description of the project.

## Project Organization

```
├── LICENSE            <- Open-source license if one is chosen
├── Makefile           <- Makefile with convenience commands like `make data` or `make train`
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external       <- Data from third party sources (e.g., yelp_reviews.json)
│   ├── interim        <- Intermediate data that has been transformed
│   ├── processed      <- The final, canonical data sets for modeling
│   └── raw            <- The original, immutable data dump
│
├── docs               <- A default mkdocs project; see www.mkdocs.org for details
│
├── models             <- Trained and serialized models, model predictions, or model summaries
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-jqp-initial-data-exploration`
│
├── pyproject.toml     <- Project configuration file with package metadata for
│                         yelp_streaming_etl_pipeline and configuration for tools like black
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials
│
├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures        <- Generated graphics and figures to be used in reporting
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment
│
├── streaming          <- Main streaming ETL pipeline code
│   ├── __init__.py           <- Makes streaming a Python module
│   ├── docker-compose.yml    <- Docker Compose configuration for Kafka, Postgres, etc.
│   ├── faust_app.py          <- Main Faust streaming application
│   ├── models.py             <- Faust models for data serialization
│   ├── producer.py           <- Kafka producer for ingesting Yelp reviews
│   ├── quality_engine.py     <- Data quality validation engine
│   ├── postgres_sink.py      <- Postgres sink connector for persisting data
│   ├── requirements.txt      <- Requirements for the streaming application
│   └── test_quality.py       <- Unit tests for quality engine
│
├── tests              <- Unit tests for the project
│   └── test_data.py
│
└── yelp_streaming_etl_pipeline   <- Source code for use in this project
    │
    ├── __init__.py             <- Makes yelp_streaming_etl_pipeline a Python module
    │
    ├── config.py               <- Store useful variables and configuration
    │
    ├── dataset.py              <- Scripts to download or generate data
    │
    ├── features.py             <- Code to create features for modeling
    │
    ├── modeling
    │   ├── __init__.py
    │   ├── predict.py          <- Code to run model inference with trained models
    │   └── train.py            <- Code to train models
    │
    └── plots.py                <- Code to create visualizations
```

--------

