from setuptools import find_packages, setup

setup(
    name="spark-framework",
    version="0.2.0",
    description="JSON-driven Spark pipeline framework for ingestion, transformation, and data quality",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="STech",
    python_requires=">=3.9",
    packages=find_packages(exclude=["tests*", "examples*", "docs*"]),
    install_requires=[
        "pyspark>=3.4.0",
    ],
    extras_require={
        # pip install spark-framework[delta]  →  OSS Delta fora do Databricks
        "delta": ["delta-spark>=3.0.0"],
        # pip install spark-framework[kafka]  →  integração Kafka
        "kafka": ["pyspark[sql]>=3.4.0"],
        "all": ["delta-spark>=3.0.0"],
    },
    entry_points={
        "console_scripts": [
            "spark-framework=main:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
)
