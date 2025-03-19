from setuptools import setup, find_packages

setup(
    name="metadata_etl_framework",
    version="0.1.0",
    description="Metadata-driven ETL framework for data pipelines",
    author="Data Engineering Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.5.0",
        "pyyaml>=6.0",
    ],
    entry_points={
        'console_scripts': [
            'run-etl=scripts.run_demo:run_etl_pipeline',
        ],
    },
) 