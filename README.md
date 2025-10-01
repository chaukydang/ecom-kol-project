# E-commerce + KOL Data Platform (Mini Project)

## Structure
- `pipeline/ingest.py`: simulate daily ingestion (Bronze layer)
- `pipeline/transform.py`: clean & aggregate to Silver/Gold
- `pipeline/kol_build.py`: assign synthetic KOLs & produce performance table
- `dashboard/app.py`: Streamlit dashboard for funnel + KOL performance
- `scheduler/prefect_flow.py`: Prefect orchestration
- `scheduler/airflow_dag.py`: Airflow DAG
- `requirements.txt`: dependencies

## Usage
1. Install requirements: `pip install -r requirements.txt`
2. Place RetailRocket dataset in `data/raw_master/` (events.csv, item_properties...)
3. Run ingestion (nạp từng ngày): `python pipeline/ingest.py`
4. Run transform: `python pipeline/transform.py`
5. Build KOL performance: `python pipeline/kol_build.py`
6. Launch dashboard: `streamlit run dashboard/app.py`
7. Automate with Prefect/Airflow under `scheduler/`
