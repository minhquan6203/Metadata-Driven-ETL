@echo off
:: Windows Batch script to run the complete ETL demo from data generation to processing

echo ===== Metadata-Driven ETL Framework Demo =====
echo This script will generate sample data and run the ETL pipeline to process it.

:: Step 1: Build and start Docker container with docker-compose
echo.
echo [1/5] Building and starting Docker container...
docker-compose up -d --build
if %ERRORLEVEL% neq 0 (
    echo Error building/starting Docker container. Exiting.
    exit /b 1
)

:: Step 2: Ensure container is running
echo.
echo [2/5] Verifying container is running...
docker-compose ps | find "metadata-etl" > nul
if %ERRORLEVEL% neq 0 (
    echo Container failed to start. Exiting.
    exit /b 1
) else (
    echo Container running successfully.
)

:: Step 3: Generate sample data
echo.
echo [3/5] Generating sample data...
docker-compose exec etl-framework python scripts/generate_sample_data.py
if %ERRORLEVEL% neq 0 (
    echo Error generating sample data. Exiting.
    exit /b 1
)

:: Step 4: Initialize metadata tables
echo.
echo [4/5] Initializing metadata tables...
docker-compose exec etl-framework python scripts/init_metadata_tables.py
if %ERRORLEVEL% neq 0 (
    echo Error initializing metadata tables. Exiting.
    exit /b 1
)

:: Step 5: Run the ETL pipeline
echo.
echo [5/5] Running ETL pipeline to process data through all layers...
docker-compose exec etl-framework python scripts/run_etl_pipeline.py
if %ERRORLEVEL% neq 0 (
    echo Warning: ETL pipeline completed with some errors. Check logs for details.
)

:: Step 6: View all tables
echo.
echo Viewing tables in all layers...
docker-compose exec etl-framework python scripts/query_delta_tables.py
if %ERRORLEVEL% neq 0 (
    echo Warning: Error viewing tables. Check the script for issues.
)

echo.
echo ===== ETL Demo Completed =====
echo All tables have been processed and displayed.

pause 