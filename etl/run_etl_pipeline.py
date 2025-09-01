#!/usr/bin/env python3
"""
ETL Pipeline for Infrastructure Climate Analytics
Uses DuckDB for efficient data processing
"""

import pandas as pd
import duckdb
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InfrastructureETL:
    """ETL Pipeline for Infrastructure and Climate Data"""
    
    def __init__(self):
        self.base_path = Path.cwd()
        self.raw_path = self.base_path / "data" / "raw"
        self.processed_path = self.base_path / "data" / "processed"
        self.final_path = self.base_path / "data" / "final"
        
        # Create directories
        self.processed_path.mkdir(parents=True, exist_ok=True)
        self.final_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize DuckDB
        self.db_path = self.base_path / "data" / "infrastructure.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        logger.info(f"Connected to DuckDB at {self.db_path}")
    
    def extract(self):
        """Extract data from raw files"""
        logger.info("Starting EXTRACT phase...")
        
        # Load infrastructure data
        infra_file = self.raw_path / "infrastructure_resilience_scores.csv"
        if infra_file.exists():
            # Use DuckDB to read CSV directly
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_infrastructure AS
                SELECT * FROM read_csv_auto('{infra_file}')
            """)
            
            count = self.conn.execute("SELECT COUNT(*) FROM raw_infrastructure").fetchone()[0]
            logger.info(f"✅ Loaded {count} infrastructure records")
        
        # Load any World Bank data if available
        for csv_file in self.raw_path.glob("worldbank_*.csv"):
            table_name = f"raw_{csv_file.stem}"
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{csv_file}')
            """)
            logger.info(f"✅ Loaded {csv_file.name}")
        
        return True
    
    def transform(self):
        """Transform and clean data"""
        logger.info("Starting TRANSFORM phase...")
        
        # Create cleaned infrastructure table
        self.conn.execute("""
            CREATE OR REPLACE TABLE clean_infrastructure AS
            SELECT 
                country,
                year,
                infrastructure_score,
                transport_resilience,
                energy_resilience,
                water_resilience,
                digital_resilience,
                -- Add calculated fields
                (transport_resilience + energy_resilience + water_resilience + digital_resilience) / 4 as avg_resilience,
                infrastructure_score - LAG(infrastructure_score, 1) OVER (PARTITION BY country ORDER BY year) as score_change,
                RANK() OVER (PARTITION BY year ORDER BY infrastructure_score DESC) as yearly_rank
            FROM raw_infrastructure
            WHERE year >= 2010
            ORDER BY country, year
        """)
        
        # Create country summary
        self.conn.execute("""
            CREATE OR REPLACE TABLE country_summary AS
            SELECT 
                country,
                MIN(year) as first_year,
                MAX(year) as last_year,
                COUNT(*) as num_years,
                AVG(infrastructure_score) as avg_score,
                MIN(infrastructure_score) as min_score,
                MAX(infrastructure_score) as max_score,
                MAX(infrastructure_score) - MIN(infrastructure_score) as score_improvement,
                AVG(score_change) as avg_yearly_change
            FROM clean_infrastructure
            GROUP BY country
        """)
        
        # Create yearly trends
        self.conn.execute("""
            CREATE OR REPLACE TABLE yearly_trends AS
            SELECT 
                year,
                AVG(infrastructure_score) as global_avg_score,
                STDDEV(infrastructure_score) as score_std_dev,
                MIN(infrastructure_score) as min_score,
                MAX(infrastructure_score) as max_score,
                COUNT(DISTINCT country) as num_countries
            FROM clean_infrastructure
            GROUP BY year
            ORDER BY year
        """)
        
        logger.info("✅ Data transformation complete")
        return True
    
    def load(self):
        """Load processed data to final storage"""
        logger.info("Starting LOAD phase...")
        
        # Export to Parquet for efficient storage
        tables_to_export = [
            'clean_infrastructure',
            'country_summary',
            'yearly_trends'
        ]
        
        for table in tables_to_export:
            # Export to Parquet
            output_file = self.final_path / f"{table}.parquet"
            self.conn.execute(f"""
                COPY {table} TO '{output_file}' (FORMAT PARQUET)
            """)
            logger.info(f"✅ Exported {table} to {output_file.name}")
            
            # Also export to CSV for easy viewing
            csv_file = self.final_path / f"{table}.csv"
            self.conn.execute(f"""
                COPY {table} TO '{csv_file}' (FORMAT CSV, HEADER)
            """)
        
        # Create metadata
        metadata = {
            'pipeline_run': datetime.now().isoformat(),
            'tables_created': tables_to_export,
            'record_counts': {}
        }
        
        for table in tables_to_export:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            metadata['record_counts'][table] = count
        
        # Save metadata
        metadata_file = self.final_path / "pipeline_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"✅ Pipeline metadata saved to {metadata_file.name}")
        return True
    
    def create_analytics_views(self):
        """Create analytical views for dashboard"""
        logger.info("Creating analytics views...")
        
        # Top performers view
        self.conn.execute("""
            CREATE OR REPLACE VIEW top_performers AS
            SELECT 
                c.country,
                c.avg_score,
                c.score_improvement,
                ci.infrastructure_score as latest_score,
                ci.yearly_rank as latest_rank
            FROM country_summary c
            JOIN clean_infrastructure ci ON c.country = ci.country
            WHERE ci.year = (SELECT MAX(year) FROM clean_infrastructure)
            ORDER BY ci.infrastructure_score DESC
            LIMIT 10
        """)
        
        # Export view for dashboard
        self.conn.execute(f"""
            COPY top_performers TO '{self.final_path}/top_performers.csv' (FORMAT CSV, HEADER)
        """)
        
        logger.info("✅ Analytics views created")
    
    def run_quality_checks(self):
        """Run data quality checks"""
        logger.info("Running quality checks...")
        
        checks = []
        
        # Check for nulls
        null_check = self.conn.execute("""
            SELECT COUNT(*) as nulls
            FROM clean_infrastructure
            WHERE infrastructure_score IS NULL
        """).fetchone()[0]
        
        checks.append({
            'check': 'null_values',
            'passed': null_check == 0,
            'details': f"Found {null_check} null values"
        })
        
        # Check for duplicates
        dup_check = self.conn.execute("""
            SELECT COUNT(*) as duplicates
            FROM (
                SELECT country, year, COUNT(*) as cnt
                FROM clean_infrastructure
                GROUP BY country, year
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        
        checks.append({
            'check': 'duplicates',
            'passed': dup_check == 0,
            'details': f"Found {dup_check} duplicate records"
        })
        
        # Save quality report
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'checks': checks,
            'all_passed': all(c['passed'] for c in checks)
        }
        
        report_file = self.final_path / "quality_report.json"
        with open(report_file, 'w') as f:
            json.dump(quality_report, f, indent=2)
        
        logger.info(f"✅ Quality checks complete. All passed: {quality_report['all_passed']}")
        return quality_report['all_passed']
    
    def close(self):
        """Close database connection"""
        self.conn.close()
        logger.info("Database connection closed")
    
    def run_pipeline(self):
        """Run complete ETL pipeline"""
        logger.info("="*60)
        logger.info("🚀 STARTING ETL PIPELINE")
        logger.info("="*60)
        
        try:
            # Run ETL phases
            self.extract()
            self.transform()
            self.load()
            self.create_analytics_views()
            self.run_quality_checks()
            
            logger.info("="*60)
            logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("="*60)
            
            # Print summary
            print("\n📊 Pipeline Summary:")
            print(f"  - Database: {self.db_path}")
            print(f"  - Processed data: {self.final_path}")
            print(f"  - Files created: {len(list(self.final_path.glob('*')))}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return False
        finally:
            self.close()

def main():
    """Main execution"""
    pipeline = InfrastructureETL()
    success = pipeline.run_pipeline()
    
    if success:
        print("\n🎉 ETL Pipeline executed successfully!")
        print("📁 Check the data/final folder for processed datasets")
        print("📊 Ready to build dashboard with the processed data!")
    else:
        print("\n❌ Pipeline encountered errors. Check logs above.")
    
    return success

if __name__ == "__main__":
    main()
