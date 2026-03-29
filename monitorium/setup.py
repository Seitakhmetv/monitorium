from setuptools import setup, find_packages

setup(
    name="monitorium",
    version="0.1.0",
    package_dir={"": "."},
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "python-dotenv",
        "yfinance",
        "requests",
        "beautifulsoup4",
        "google-cloud-storage",
        "google-cloud-bigquery",
    ],
)