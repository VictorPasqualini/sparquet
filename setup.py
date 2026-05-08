from setuptools import find_packages, setup

setup(
    name="spark-framework",
    version="0.1.0",
    description="JSON-driven Spark pipeline framework for ingestion, transformation, and data quality",
    packages=find_packages(exclude=["tests*", "examples*"]),
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.4.0",
    ],
    entry_points={
        "console_scripts": [
            "spark-framework=main:main",
        ],
    },
)
