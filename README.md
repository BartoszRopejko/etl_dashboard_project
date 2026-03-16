Instructions

# 0. Prerequisites
- Snowflake account
- OMDb API key

# 1. Clone repo
git clone https://github.com/BartoszRopejko/etl_dashboard_project.git
cd etl_dashboard_project

# 2. Install dependencies
pip install -r requirements.txt

# 3. Extract data from .csv and API
python extract_and_transform.py

# 4. Load data to Snowflake
python load.py

# 4. Run dashboard
streamlit run dashboard.py
