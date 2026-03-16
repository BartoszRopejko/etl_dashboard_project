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


# DASHBOARD EXAMPLES

TOP 10 MOVIES BASED ON REVENUE
<img width="1177" height="860" alt="obraz" src="https://github.com/user-attachments/assets/1a49ffb4-3147-4aab-be1d-d90812956344" />

REVENUE TREND IN YEARS
<img width="1161" height="702" alt="obraz" src="https://github.com/user-attachments/assets/a751ee4e-ff49-4671-b7bc-dfeddf7de442" />

MOVIE CARDS
<img width="1062" height="1158" alt="obraz" src="https://github.com/user-attachments/assets/a41ba721-3f7f-4889-816d-6ef9880cab56" />

