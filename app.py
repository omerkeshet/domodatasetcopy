"""
DOMO Dataset Copy Tool
Copies datasets from Production (keshet-tv) to Development (keshet-tv-dev)
Uses OAuth authentication with the public DOMO API
"""

import streamlit as st
import requests
import pandas as pd
import base64
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from io import StringIO
import time

# =============================================================================
# CONFIGURATION
# =============================================================================

APP_NAME = "Dataset Copy Tool"
PROD_INSTANCE = "keshet-tv"
DEV_INSTANCE = "keshet-tv-dev"

# =============================================================================
# STYLING
# =============================================================================

def apply_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    :root {
        --primary: #0f172a;
        --primary-light: #1e293b;
        --accent: #3b82f6;
        --accent-hover: #2563eb;
        --success: #10b981;
        --success-bg: #ecfdf5;
        --success-border: #a7f3d0;
        --warning: #f59e0b;
        --warning-bg: #fffbeb;
        --warning-border: #fde68a;
        --error: #ef4444;
        --error-bg: #fef2f2;
        --error-border: #fecaca;
        --info: #3b82f6;
        --info-bg: #eff6ff;
        --info-border: #bfdbfe;
        --bg-primary: #f8fafc;
        --bg-secondary: #ffffff;
        --bg-tertiary: #f1f5f9;
        --text-primary: #0f172a;
        --text-secondary: #475569;
        --text-muted: #94a3b8;
        --border-color: #e2e8f0;
        --border-radius: 12px;
        --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
        --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
        --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
    }
    
    .stApp {
        background: var(--bg-primary);
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .app-header {
        background: var(--primary);
        padding: 2.5rem 3rem;
        border-radius: var(--border-radius);
        margin-bottom: 2rem;
        box-shadow: var(--shadow-lg);
        position: relative;
        overflow: hidden;
    }
    
    .app-header::before {
        content: '';
        position: absolute;
        top: 0;
        right: 0;
        width: 40%;
        height: 100%;
        background: linear-gradient(135deg, transparent 0%, rgba(59, 130, 246, 0.1) 100%);
    }
    
    .app-title {
        color: #ffffff !important;
        font-size: 1.75rem;
        font-weight: 600;
        margin: 0;
        letter-spacing: -0.025em;
    }
    
    .app-subtitle {
        color: rgba(255,255,255,0.7) !important;
        font-size: 0.9rem;
        margin-top: 0.5rem;
        font-weight: 400;
    }
    
    .metric-box {
        background: var(--bg-secondary);
        border-radius: var(--border-radius);
        padding: 1.5rem;
        border: 1px solid var(--border-color);
        text-align: center;
        transition: all 0.2s ease;
        box-shadow: var(--shadow-sm);
    }
    
    .metric-box:hover {
        box-shadow: var(--shadow-md);
        border-color: var(--accent);
    }
    
    .metric-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 0.25rem;
        letter-spacing: -0.025em;
    }
    
    .metric-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.1em;
        font-weight: 500;
    }
    
    .status-indicator {
        display: inline-flex;
        align-items: center;
        padding: 0.375rem 0.875rem;
        border-radius: 9999px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .status-exists {
        background: var(--warning-bg);
        color: #b45309;
        border: 1px solid var(--warning-border);
    }
    
    .status-new {
        background: var(--success-bg);
        color: #047857;
        border: 1px solid var(--success-border);
    }
    
    .alert {
        border-radius: var(--border-radius);
        padding: 1rem 1.25rem;
        margin: 0.75rem 0;
        font-size: 0.875rem;
        line-height: 1.5;
    }
    
    .alert-info {
        background: var(--info-bg);
        border: 1px solid var(--info-border);
        color: #1e40af;
    }
    
    .alert-warning {
        background: var(--warning-bg);
        border: 1px solid var(--warning-border);
        color: #92400e;
    }
    
    .alert-success {
        background: var(--success-bg);
        border: 1px solid var(--success-border);
        color: #065f46;
    }
    
    .alert-error {
        background: var(--error-bg);
        border: 1px solid var(--error-border);
        color: #991b1b;
    }
    
    .alert-title {
        font-weight: 600;
        margin-bottom: 0.375rem;
        display: block;
    }
    
    .dataset-card {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: var(--border-radius);
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: var(--shadow-sm);
        transition: all 0.2s ease;
    }
    
    .dataset-card:hover {
        box-shadow: var(--shadow-md);
    }
    
    .dataset-card-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 1.25rem;
        padding-bottom: 1rem;
        border-bottom: 1px solid var(--border-color);
    }
    
    .dataset-card-title {
        font-size: 1.125rem;
        font-weight: 600;
        color: var(--text-primary);
        letter-spacing: -0.025em;
    }
    
    .dataset-card-id {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-top: 0.25rem;
    }
    
    .dataset-card-details {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 1.5rem;
    }
    
    .dataset-card-detail {
        text-align: center;
        padding: 0.75rem;
        background: var(--bg-tertiary);
        border-radius: 8px;
    }
    
    .dataset-card-detail-value {
        font-size: 1.125rem;
        font-weight: 700;
        color: var(--text-primary);
    }
    
    .dataset-card-detail-label {
        font-size: 0.65rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-top: 0.25rem;
        font-weight: 500;
    }
    
    .section-title {
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 1rem;
        padding-bottom: 0.75rem;
        border-bottom: 1px solid var(--border-color);
    }
    
    .column-item {
        background: var(--bg-tertiary);
        border-radius: 8px;
        padding: 0.625rem 0.875rem;
        margin: 0.375rem 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
        transition: all 0.15s ease;
    }
    
    .column-item:hover {
        background: var(--border-color);
    }
    
    .column-name { 
        color: var(--text-primary); 
        font-weight: 500;
    }
    
    .column-type { 
        color: var(--text-muted); 
        font-size: 0.7rem;
        background: var(--bg-secondary);
        padding: 0.125rem 0.5rem;
        border-radius: 4px;
    }
    
    .instance-badge {
        display: inline-flex;
        padding: 0.25rem 0.625rem;
        border-radius: 6px;
        font-size: 0.65rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .instance-prod { 
        background: #fef2f2; 
        color: #dc2626; 
    }
    
    .instance-dev { 
        background: #ecfdf5; 
        color: #059669; 
    }
    
    .divider {
        height: 1px;
        background: var(--border-color);
        margin: 2rem 0;
    }
    
    /* Button styling */
    .stButton > button {
        background: var(--accent) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.625rem 1.25rem !important;
        font-weight: 500 !important;
        font-size: 0.875rem !important;
        transition: all 0.2s ease !important;
        box-shadow: var(--shadow-sm) !important;
    }
    
    .stButton > button:hover {
        background: var(--accent-hover) !important;
        box-shadow: var(--shadow-md) !important;
        transform: translateY(-1px);
    }
    
    .stButton > button:active {
        transform: translateY(0);
    }
    
    /* Input styling */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div {
        border-radius: 8px !important;
        border-color: var(--border-color) !important;
        font-size: 0.875rem !important;
    }
    
    .stTextInput > div > div > input:focus,
    .stSelectbox > div > div > div:focus {
        border-color: var(--accent) !important;
        box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1) !important;
    }
    
    /* Progress bar - fix to show actual progress */
    .stProgress > div > div {
        background-color: var(--bg-tertiary) !important;
        border-radius: 10px !important;
    }
    
    .stProgress > div > div > div {
        background: linear-gradient(90deg, var(--accent) 0%, #93fab4 100%) !important;
        border-radius: 10px !important;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    header {visibility: hidden;}
    
    /* Hide the stale/duplicate UI that appears during script reruns */
    [data-stale="true"] {
        display: none !important;
    }
    
    .stale-element {
        display: none !important;
    }
    
    /* Alternative: make stale elements invisible instead of taking space */
    div[data-stale="true"] {
        visibility: hidden !important;
        height: 0 !important;
        overflow: hidden !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    
    /* Checkbox styling */
    .stCheckbox > label {
        font-size: 0.875rem !important;
        color: var(--text-secondary) !important;
    }
    
    /* Date input styling */
    .stDateInput > div > div > input {
        border-radius: 8px !important;
    }
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# OAUTH AUTHENTICATION
# =============================================================================

def get_oauth_token(instance: str) -> Optional[str]:
    """Get OAuth access token for an instance."""
    if instance == PROD_INSTANCE:
        client_id = st.secrets["domo"]["prod_client_id"]
        client_secret = st.secrets["domo"]["prod_client_secret"]
    else:
        client_id = st.secrets["domo"]["dev_client_id"]
        client_secret = st.secrets["domo"]["dev_client_secret"]
    
    auth_url = "https://api.domo.com/oauth/token"
    
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    data = {
        'grant_type': 'client_credentials',
        'scope': 'data'
    }
    
    response = requests.post(auth_url, headers=headers, data=data, timeout=30)
    
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f"OAuth authentication failed: {response.text}")


def get_oauth_headers(token: str) -> Dict[str, str]:
    """Get headers with OAuth token."""
    return {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }


# =============================================================================
# DOMO API FUNCTIONS
# =============================================================================

@st.cache_data(show_spinner=False)
def list_datasets(instance: str) -> List[Dict]:
    """List all datasets from a DOMO instance."""
    token = get_oauth_token(instance)
    
    url = "https://api.domo.com/v1/datasets"
    all_datasets = []
    offset = 0
    limit = 50
    
    while True:
        params = {'offset': offset, 'limit': limit}
        response = requests.get(url, headers=get_oauth_headers(token), params=params, timeout=60)
        response.raise_for_status()
        
        batch = response.json()
        
        if not batch:
            break
        
        all_datasets.extend(batch)
        
        if len(batch) < limit:
            break
            
        offset += limit
    
    return all_datasets


def get_dataset_info(instance: str, dataset_id: str) -> Dict:
    """Get detailed information about a specific dataset."""
    token = get_oauth_token(instance)
    
    url = f"https://api.domo.com/v1/datasets/{dataset_id}"
    response = requests.get(url, headers=get_oauth_headers(token), timeout=60)
    response.raise_for_status()
    return response.json()


def export_dataset_data(instance: str, dataset_id: str, date_column: str = None, 
                         start_date=None, end_date=None, progress_callback=None) -> pd.DataFrame:
    """Export dataset data as DataFrame with support for large datasets.
    
    For large datasets, applies date filter server-side via SQL to reduce data transfer.
    """
    token = get_oauth_token(instance)
    
    # First get dataset info to know the size
    dataset_info = get_dataset_info(instance, dataset_id)
    schema = dataset_info.get('schema', {}).get('columns', [])
    column_names = [col['name'] for col in schema]
    total_rows = dataset_info.get('rows', 0)
    
    # Build WHERE clause for date filtering (applied server-side for efficiency)
    where_clause = ""
    if date_column and start_date and end_date:
        where_clause = f"WHERE `{date_column}` >= '{start_date}' AND `{date_column}` <= '{end_date}'"
    
    # For smaller datasets (under 100k rows) without date filter, use direct export
    if total_rows < 100000 and not where_clause:
        url = f"https://api.domo.com/v1/datasets/{dataset_id}/data"
        headers = get_oauth_headers(token)
        headers['Accept'] = 'text/csv'
        
        response = requests.get(url, headers=headers, timeout=300)
        response.raise_for_status()
        
        csv_text = response.text
        
        # Detect if CSV has headers
        first_line = csv_text.split('\n')[0] if csv_text else ""
        first_line_values = first_line.split(',') if first_line else []
        
        has_header = len(first_line_values) == len(column_names) and any(
            val.strip().strip('"') in column_names for val in first_line_values[:3]
        )
        
        if has_header:
            df = pd.read_csv(StringIO(csv_text))
        else:
            df = pd.read_csv(StringIO(csv_text), header=None, names=column_names)
        
        return df
    
    # For large datasets or when filtering, use SQL query with pagination
    all_data = []
    chunk_size = 100000  # 100k rows per chunk (reduced for memory efficiency)
    offset = 0
    max_rows = 10000000  # Safety limit: 10M rows max
    
    # First, get count of filtered data
    count_sql = f"SELECT COUNT(*) as cnt FROM table {where_clause}"
    url = f"https://api.domo.com/v1/datasets/query/execute/{dataset_id}"
    headers = get_oauth_headers(token)
    
    try:
        response = requests.post(url, headers=headers, json={"sql": count_sql}, timeout=120)
        if response.status_code == 200:
            result = response.json()
            if result.get('rows') and result['rows'][0]:
                total_rows = int(result['rows'][0][0])
    except:
        pass  # Use original total_rows if count fails
    
    while offset < min(total_rows, max_rows):
        if progress_callback:
            progress_callback(offset, total_rows)
        
        sql = f"SELECT * FROM table {where_clause} LIMIT {chunk_size} OFFSET {offset}"
        
        payload = {"sql": sql}
        
        response = requests.post(url, headers=headers, json=payload, timeout=300)
        response.raise_for_status()
        
        result = response.json()
        columns = result.get('columns', [])
        rows = result.get('rows', [])
        
        if not rows:
            break
        
        chunk_df = pd.DataFrame(rows, columns=columns)
        all_data.append(chunk_df)
        
        offset += len(rows)
        
        # If we got fewer rows than requested, we're done
        if len(rows) < chunk_size:
            break
        
        # Memory safety: if we've collected too much data, stop
        if offset >= max_rows:
            break
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame(columns=column_names)


def create_dataset(instance: str, name: str, schema: List[Dict]) -> Dict:
    """Create a new dataset in the target instance."""
    token = get_oauth_token(instance)
    
    url = "https://api.domo.com/v1/datasets"
    
    payload = {
        "name": name,
        "description": f"Copied from {PROD_INSTANCE} on {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "schema": {
            "columns": schema
        }
    }
    
    response = requests.post(url, headers=get_oauth_headers(token), json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def stream_copy_dataset(
    source_instance: str, 
    source_dataset_id: str,
    target_instance: str,
    target_dataset_id: str,
    date_column: str = None,
    start_date=None,
    end_date=None,
    progress_callback=None,
    status_callback=None
) -> int:
    """
    Stream data directly from source to target without loading all into memory.
    Fetches chunks from source, writes to temp file, then uploads to target.
    Returns total rows copied.
    """
    import tempfile
    import os
    
    source_token = get_oauth_token(source_instance)
    target_token = get_oauth_token(target_instance)
    
    # Get source dataset info
    source_info = get_dataset_info(source_instance, source_dataset_id)
    schema = source_info.get('schema', {}).get('columns', [])
    column_names = [col['name'] for col in schema]
    total_rows = source_info.get('rows', 0)
    
    # Build WHERE clause for date filtering
    where_clause = ""
    if date_column and start_date and end_date:
        where_clause = f"WHERE `{date_column}` >= '{start_date}' AND `{date_column}` <= '{end_date}'"
    
    # Get count of rows to copy
    if where_clause:
        count_sql = f"SELECT COUNT(*) as cnt FROM table {where_clause}"
        url = f"https://api.domo.com/v1/datasets/query/execute/{source_dataset_id}"
        headers = {'Authorization': f'Bearer {source_token}', 'Content-Type': 'application/json'}
        
        try:
            response = requests.post(url, headers=headers, json={"sql": count_sql}, timeout=120)
            if response.status_code == 200:
                result = response.json()
                if result.get('rows') and result['rows'][0]:
                    total_rows = int(result['rows'][0][0])
        except:
            pass
    
    if status_callback:
        status_callback(f"Total rows to copy: {total_rows:,}")
    
    # Create temp file to store CSV data
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8')
    temp_path = temp_file.name
    
    try:
        # Stream data in chunks to temp file
        chunk_size = 100000  # 100k rows per chunk
        offset = 0
        chunk_num = 1
        total_copied = 0
        header_written = False
        
        source_headers = {'Authorization': f'Bearer {source_token}', 'Content-Type': 'application/json'}
        
        while offset < total_rows:
            if progress_callback:
                progress_callback(offset, total_rows)
            
            if status_callback:
                status_callback(f"Fetching chunk {chunk_num} (rows {offset:,} - {min(offset + chunk_size, total_rows):,})...")
            
            # Fetch chunk from source
            sql = f"SELECT * FROM table {where_clause} LIMIT {chunk_size} OFFSET {offset}"
            url = f"https://api.domo.com/v1/datasets/query/execute/{source_dataset_id}"
            
            response = requests.post(url, headers=source_headers, json={"sql": sql}, timeout=300)
            response.raise_for_status()
            
            result = response.json()
            columns = result.get('columns', [])
            rows = result.get('rows', [])
            
            if not rows:
                break
            
            # Convert to CSV and write to temp file
            chunk_df = pd.DataFrame(rows, columns=columns)
            chunk_df.to_csv(temp_file, index=False, header=not header_written, mode='a')
            header_written = True
            
            rows_in_chunk = len(rows)
            total_copied += rows_in_chunk
            
            # Free memory
            del chunk_df
            del rows
            del result
            
            offset += chunk_size
            chunk_num += 1
            
            # Check if we got fewer rows than requested (end of data)
            if rows_in_chunk < chunk_size:
                break
        
        # Close temp file
        temp_file.close()
        
        if status_callback:
            status_callback(f"Uploading {total_copied:,} rows to target...")
        
        if progress_callback:
            progress_callback(total_rows, total_rows)
        
        # Upload the temp file to target
        target_headers = {'Authorization': f'Bearer {target_token}', 'Content-Type': 'text/csv'}
        upload_url = f"https://api.domo.com/v1/datasets/{target_dataset_id}/data"
        
        # Read and upload in streaming fashion
        with open(temp_path, 'rb') as f:
            response = requests.put(upload_url, headers=target_headers, data=f, timeout=600)
            response.raise_for_status()
        
        if status_callback:
            status_callback(f"Upload complete ({total_copied:,} rows)")
        
        return total_copied
        
    finally:
        # Clean up temp file
        try:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        except:
            pass
    """Upload data to a dataset with support for large datasets."""
    token = get_oauth_token(instance)
    
    total_rows = len(df)
    
    # For smaller datasets, upload directly
    if total_rows < 100000:
        url = f"https://api.domo.com/v1/datasets/{dataset_id}/data"
        headers = get_oauth_headers(token)
        headers['Content-Type'] = 'text/csv'
        
        csv_data = df.to_csv(index=False)
        
        response = requests.put(url, headers=headers, data=csv_data.encode('utf-8'), timeout=300)
        response.raise_for_status()
        return True
    
    # For large datasets, use stream API with parts
    headers = get_oauth_headers(token)
    
    # Get or create stream for this dataset
    stream_url = "https://api.domo.com/v1/streams"
    
    # Search for existing stream
    stream_id = None
    params = {'limit': 500}
    response = requests.get(stream_url, headers=headers, params=params, timeout=60)
    
    if response.status_code == 200:
        streams = response.json()
        for stream in streams:
            if stream.get('dataSet', {}).get('id') == dataset_id:
                stream_id = stream.get('id')
                break
    
    # If no stream exists, create one
    if not stream_id:
        payload = {
            "dataSet": {"id": dataset_id},
            "updateMethod": "REPLACE"
        }
        response = requests.post(stream_url, headers=headers, json=payload, timeout=60)
        if response.status_code in [200, 201]:
            stream_id = response.json().get('id')
    
    if stream_id:
        # Use stream-based upload
        return upload_via_stream(instance, stream_id, df, progress_callback)
    else:
        # Fallback: try direct upload anyway
        url = f"https://api.domo.com/v1/datasets/{dataset_id}/data"
        headers = get_oauth_headers(token)
        headers['Content-Type'] = 'text/csv'
        
        csv_data = df.to_csv(index=False)
        
        response = requests.put(url, headers=headers, data=csv_data.encode('utf-8'), timeout=600)
        response.raise_for_status()
        return True


def upload_data_to_dataset(instance: str, dataset_id: str, df: pd.DataFrame, progress_callback=None) -> bool:
    """Upload data to a dataset with support for large datasets."""
    token = get_oauth_token(instance)
    
    total_rows = len(df)
    
    # For smaller datasets, upload directly
    if total_rows < 100000:
        url = f"https://api.domo.com/v1/datasets/{dataset_id}/data"
        headers = get_oauth_headers(token)
        headers['Content-Type'] = 'text/csv'
        
        csv_data = df.to_csv(index=False)
        
        response = requests.put(url, headers=headers, data=csv_data.encode('utf-8'), timeout=300)
        response.raise_for_status()
        return True
    
    # For large datasets, use stream API with parts
    headers = get_oauth_headers(token)
    
    # Get or create stream for this dataset
    stream_url = "https://api.domo.com/v1/streams"
    
    # Search for existing stream
    stream_id = None
    params = {'limit': 500}
    response = requests.get(stream_url, headers=headers, params=params, timeout=60)
    
    if response.status_code == 200:
        streams = response.json()
        for stream in streams:
            if stream.get('dataSet', {}).get('id') == dataset_id:
                stream_id = stream.get('id')
                break
    
    # If no stream exists, create one
    if not stream_id:
        payload = {
            "dataSet": {"id": dataset_id},
            "updateMethod": "REPLACE"
        }
        response = requests.post(stream_url, headers=headers, json=payload, timeout=60)
        if response.status_code in [200, 201]:
            stream_id = response.json().get('id')
    
    if stream_id:
        # Use stream-based upload
        return upload_via_stream(instance, stream_id, df, progress_callback)
    else:
        # Fallback: try direct upload anyway
        url = f"https://api.domo.com/v1/datasets/{dataset_id}/data"
        headers = get_oauth_headers(token)
        headers['Content-Type'] = 'text/csv'
        
        csv_data = df.to_csv(index=False)
        
        response = requests.put(url, headers=headers, data=csv_data.encode('utf-8'), timeout=600)
        response.raise_for_status()
        return True


def upload_via_stream(instance: str, stream_id: int, df: pd.DataFrame, progress_callback=None) -> bool:
    """Upload data via stream API with chunked parts."""
    token = get_oauth_token(instance)
    headers = get_oauth_headers(token)
    
    # Create execution
    exec_url = f"https://api.domo.com/v1/streams/{stream_id}/executions"
    response = requests.post(exec_url, headers=headers, timeout=60)
    response.raise_for_status()
    
    execution_id = response.json().get('id')
    
    # Upload in chunks (100k rows per chunk for memory efficiency)
    chunk_size = 100000
    total_rows = len(df)
    part_num = 1
    
    try:
        for start_idx in range(0, total_rows, chunk_size):
            if progress_callback:
                progress_callback(start_idx, total_rows)
            
            end_idx = min(start_idx + chunk_size, total_rows)
            chunk_df = df.iloc[start_idx:end_idx]
            
            # Only include header in first part
            csv_data = chunk_df.to_csv(index=False, header=(part_num == 1))
            
            part_url = f"https://api.domo.com/v1/streams/{stream_id}/executions/{execution_id}/part/{part_num}"
            headers_csv = get_oauth_headers(token)
            headers_csv['Content-Type'] = 'text/csv'
            
            response = requests.put(part_url, headers=headers_csv, data=csv_data.encode('utf-8'), timeout=300)
            response.raise_for_status()
            
            part_num += 1
        
        # Commit execution
        commit_url = f"https://api.domo.com/v1/streams/{stream_id}/executions/{execution_id}/commit"
        response = requests.put(commit_url, headers=headers, timeout=120)
        response.raise_for_status()
        
        return True
        
    except Exception as e:
        # Try to abort execution on failure
        try:
            abort_url = f"https://api.domo.com/v1/streams/{stream_id}/executions/{execution_id}/abort"
            requests.put(abort_url, headers=headers, timeout=30)
        except:
            pass
        raise e


def check_dataset_exists_in_dev(dataset_name: str, dev_datasets: List[Dict]) -> Optional[Dict]:
    """Check if a dataset with the same name exists in dev."""
    for ds in dev_datasets:
        if ds.get('name', '').lower() == dataset_name.lower():
            return ds
    return None


def get_date_columns(schema: List[Dict]) -> List[str]:
    """Extract date/datetime columns from schema."""
    date_types = ['DATE', 'DATETIME', 'TIMESTAMP']
    return [col['name'] for col in schema if col.get('type', '').upper() in date_types]


def format_row_count(count) -> str:
    """Format row count with commas."""
    if count is None:
        return "N/A"
    return f"{count:,}"


# =============================================================================
# UI COMPONENTS
# =============================================================================

def render_header():
    st.markdown(f"""
    <div class="app-header">
        <h1 class="app-title">{APP_NAME}</h1>
        <p class="app-subtitle">Transfer datasets from Production to Development environment</p>
    </div>
    """, unsafe_allow_html=True)


def render_instance_metrics(prod_count: int, dev_count: int):
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{prod_count:,}</div>
            <div class="metric-label">Production Datasets</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{dev_count:,}</div>
            <div class="metric-label">Dev Datasets</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        diff = prod_count - dev_count if prod_count > dev_count else 0
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{diff}</div>
            <div class="metric-label">Missing in Dev</div>
        </div>
        """, unsafe_allow_html=True)


def render_dataset_info(dataset: Dict, schema: List[Dict], exists_in_dev: bool, dev_dataset: Optional[Dict] = None):
    row_count = dataset.get('rows', 0) or 0
    col_count = len(schema) if schema else dataset.get('columns', 0)
    
    status_class = "status-exists" if exists_in_dev else "status-new"
    status_text = "EXISTS IN DEV" if exists_in_dev else "NEW TO DEV"
    
    st.markdown(f"""
    <div class="dataset-card">
        <div class="dataset-card-header">
            <div>
                <div class="dataset-card-title">{dataset.get('name', 'Unknown')}</div>
                <div class="dataset-card-id">ID: {dataset.get('id', 'N/A')}</div>
            </div>
            <span class="status-indicator {status_class}">{status_text}</span>
        </div>
        <div class="dataset-card-details">
            <div class="dataset-card-detail">
                <div class="dataset-card-detail-value">{format_row_count(row_count)}</div>
                <div class="dataset-card-detail-label">Rows</div>
            </div>
            <div class="dataset-card-detail">
                <div class="dataset-card-detail-value">{col_count}</div>
                <div class="dataset-card-detail-label">Columns</div>
            </div>
            <div class="dataset-card-detail">
                <div class="dataset-card-detail-value"><span class="instance-badge instance-prod">PROD</span></div>
                <div class="dataset-card-detail-label">Source</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    if exists_in_dev and dev_dataset:
        st.markdown(f"""
        <div class="alert alert-warning">
            <span class="alert-title">Dataset Already Exists</span><br/>
            A dataset with this name already exists in the dev instance (ID: {dev_dataset.get('id', 'N/A')}).<br/>
            Proceeding will <strong>replace the data</strong> in the existing dataset.
        </div>
        """, unsafe_allow_html=True)


def render_schema_preview(schema: List[Dict], date_columns: List[str]):
    st.markdown('<div class="section-title">Schema Preview</div>', unsafe_allow_html=True)
    
    if not schema:
        st.warning("No schema information available")
        return
    
    for col in schema[:10]:
        is_date = col['name'] in date_columns
        date_indicator = " " if is_date else ""
        st.markdown(f"""
        <div class="column-item">
            <span class="column-name">{col['name']}{date_indicator}</span>
            <span class="column-type">{col.get('type', 'UNKNOWN')}</span>
        </div>
        """, unsafe_allow_html=True)
    
    if len(schema) > 10:
        st.markdown(f"""
        <div class="alert alert-info" style="margin-top: 0.5rem;">
            ... and {len(schema) - 10} more columns
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    st.set_page_config(
        page_title=APP_NAME,
        page_icon="◈",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    apply_custom_css()
    render_header()
    
    # Load datasets from both instances
    try:
        with st.spinner("Loading datasets from Production..."):
            prod_datasets = list_datasets(PROD_INSTANCE)
        with st.spinner("Loading datasets from Development..."):
            dev_datasets = list_datasets(DEV_INSTANCE)
    except Exception as e:
        st.error(f"Failed to load datasets: {e}")
        st.exception(e)
        return
    
    # Render metrics with refresh button
    metric_col1, metric_col2, metric_col3, refresh_col = st.columns([1, 1, 1, 0.5])
    
    with metric_col1:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{len(prod_datasets):,}</div>
            <div class="metric-label">Production Datasets</div>
        </div>
        """, unsafe_allow_html=True)
    
    with metric_col2:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{len(dev_datasets):,}</div>
            <div class="metric-label">Dev Datasets</div>
        </div>
        """, unsafe_allow_html=True)
    
    with metric_col3:
        diff = len(prod_datasets) - len(dev_datasets) if len(prod_datasets) > len(dev_datasets) else 0
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-value">{diff}</div>
            <div class="metric-label">Missing in Dev</div>
        </div>
        """, unsafe_allow_html=True)
    
    with refresh_col:
        st.markdown("<div style='height: 0.5rem'></div>", unsafe_allow_html=True)
        if st.button("Refresh", use_container_width=True, help="Refresh dataset lists from DOMO"):
            list_datasets.clear()
            st.rerun()
    
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    # Check if we have datasets
    if not prod_datasets:
        st.warning("No datasets found in Production instance")
        return
    
    # Main layout
    col_select, col_preview = st.columns([1, 2])
    
    with col_select:
        st.markdown('<div class="section-title">Select Dataset</div>', unsafe_allow_html=True)
        
        # Create options for selectbox
        dataset_options = {f"{ds['name']} ({ds['id'][:8]}...)": ds['id'] for ds in prod_datasets}
        
        # Search filter
        st.markdown("**Search Production Datasets**")
        search_term = st.text_input("Search", "", placeholder="Filter by name or ID", key="ds_search", label_visibility="collapsed")
        
        if search_term:
            filtered_options = [opt for opt in dataset_options.keys() if search_term.lower() in opt.lower()]
        else:
            filtered_options = list(dataset_options.keys())
        
        if not filtered_options:
            st.warning("No matching datasets found")
            return
        
        selected = st.selectbox("Dataset", filtered_options, label_visibility="collapsed", key="ds_select")
        selected_ds_id = dataset_options[selected]
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        # Get dataset details
        try:
            dataset_info = get_dataset_info(PROD_INSTANCE, selected_ds_id)
            schema = dataset_info.get('schema', {}).get('columns', [])
            date_columns = get_date_columns(schema)
        except Exception as e:
            st.error(f"Failed to load dataset details: {e}")
            return
        
        # Check if exists in dev
        exists_in_dev = check_dataset_exists_in_dev(dataset_info.get('name', ''), dev_datasets)
        
        # Dataset name configuration
        st.markdown('<div class="section-title">Dataset Name</div>', unsafe_allow_html=True)
        
        default_name = dataset_info.get('name', '')
        
        use_custom_name = st.checkbox("Use different name in Dev", value=False, key="use_custom_name")
        
        if use_custom_name:
            target_dataset_name = st.text_input(
                "New Dataset Name",
                value=default_name,
                key="custom_name"
            )
            if not target_dataset_name.strip():
                st.warning("Dataset name cannot be empty")
                target_dataset_name = default_name
        else:
            target_dataset_name = default_name
            st.markdown(f"""
            <div class="alert alert-info">
                <span class="alert-title">Dataset Name</span><br/>
                <code>{default_name}</code>
            </div>
            """, unsafe_allow_html=True)
        
        # Check if the target name exists in dev
        target_exists_in_dev = check_dataset_exists_in_dev(target_dataset_name, dev_datasets)
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        # Date filter configuration
        st.markdown('<div class="section-title">Date Filter</div>', unsafe_allow_html=True)
        
        if date_columns:
            selected_date_column = st.selectbox(
                "Date Column",
                options=date_columns,
                key="date_col_select"
            )
            
            st.markdown("**Date Range**")
            date_col1, date_col2 = st.columns(2)
            
            default_start = datetime.now() - timedelta(days=7)
            default_end = datetime.now()
            
            with date_col1:
                start_date = st.date_input("Start", value=default_start, key="start_date")
            with date_col2:
                end_date = st.date_input("End", value=default_end, key="end_date")
            
            st.markdown(f"""
            <div class="alert alert-info">
                <span class="alert-title">Date Filter Active</span><br/>
                Only rows where <code>{selected_date_column}</code> is between <strong>{start_date}</strong> and <strong>{end_date}</strong> will be copied.
            </div>
            """, unsafe_allow_html=True)
        else:
            selected_date_column = None
            start_date = None
            end_date = None
            st.markdown("""
            <div class="alert alert-warning">
                <span class="alert-title">No Date Columns Found</span><br/>
                This dataset has no DATE/DATETIME columns. All data will be copied.
            </div>
            """, unsafe_allow_html=True)
    
    with col_preview:
        st.markdown('<div class="section-title">Dataset Preview</div>', unsafe_allow_html=True)
        
        # Render dataset info
        render_dataset_info(
            dataset_info, 
            schema, 
            target_exists_in_dev is not None,
            target_exists_in_dev
        )
        
        # Show target name if different
        if use_custom_name and target_dataset_name != dataset_info.get('name', ''):
            st.markdown(f"""
            <div class="alert alert-info">
                <span class="alert-title">New Name in Dev</span><br/>
                <code>{target_dataset_name}</code>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        # Render schema
        render_schema_preview(schema, date_columns)
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        # Copy action
        st.markdown('<div class="section-title">Copy to Development</div>', unsafe_allow_html=True)
        
        if target_exists_in_dev:
            st.markdown(f"""
            <div class="alert alert-warning">
                <span class="alert-title">Dataset Already Exists</span><br/>
                A dataset named "<strong>{target_dataset_name}</strong>" already exists in dev (ID: {target_exists_in_dev.get('id', 'N/A')}).<br/>
                Proceeding will <strong>replace the data</strong> in the existing dataset.
            </div>
            """, unsafe_allow_html=True)
        
        copy_button = st.button("Copy to Development", type="primary", use_container_width=True)
        
        if copy_button:
            progress_placeholder = st.empty()
            status_placeholder = st.empty()
            
            try:
                # Get row count to decide on copy method
                row_count = dataset_info.get('rows', 0)
                
                # For large datasets (> 500k rows), use streaming
                if row_count > 500000:
                    progress_placeholder.progress(0.05, "Preparing streaming copy...")
                    status_placeholder.info(f"Large dataset detected ({row_count:,} rows). Using streaming mode...")
                    
                    # Step 1: Create or get target dataset
                    if target_exists_in_dev:
                        new_dataset_id = target_exists_in_dev.get('id')
                        status_placeholder.info(f"Using existing dataset: {new_dataset_id}")
                    else:
                        status_placeholder.info("Creating new dataset in development instance...")
                        new_dataset = create_dataset(DEV_INSTANCE, target_dataset_name, schema)
                        new_dataset_id = new_dataset.get('id')
                    
                    # Step 2: Stream copy
                    def stream_progress(current, total):
                        pct = min(0.1 + (current / total) * 0.85, 0.95)
                        progress_placeholder.progress(pct, f"Streaming: {current:,} / {total:,} rows...")
                    
                    def stream_status(msg):
                        status_placeholder.info(msg)
                    
                    total_copied = stream_copy_dataset(
                        source_instance=PROD_INSTANCE,
                        source_dataset_id=selected_ds_id,
                        target_instance=DEV_INSTANCE,
                        target_dataset_id=new_dataset_id,
                        date_column=selected_date_column,
                        start_date=start_date,
                        end_date=end_date,
                        progress_callback=stream_progress,
                        status_callback=stream_status
                    )
                    
                    # Done!
                    progress_placeholder.progress(1.0, "Complete!")
                    status_placeholder.empty()
                    
                    action_text = "Data Replaced" if target_exists_in_dev else "Dataset Created"
                    
                    st.markdown(f"""
                    <div class="alert alert-success">
                        <span class="alert-title">{action_text} Successfully!</span><br/>
                        <strong>Name:</strong> {target_dataset_name}<br/>
                        <strong>Dataset ID:</strong> {new_dataset_id}<br/>
                        <strong>Rows Copied:</strong> {total_copied:,}<br/>
                        <strong>Mode:</strong> Streaming (memory efficient)<br/>
                        <strong>Target:</strong> <span class="instance-badge instance-dev">DEV</span> {DEV_INSTANCE}
                    </div>
                    """, unsafe_allow_html=True)
                
                else:
                    # For smaller datasets, use the original method
                    # Step 1: Export data from prod
                    progress_placeholder.progress(0.1, "Exporting data from Production...")
                    status_placeholder.info(" Downloading data from production instance...")
                    
                    def export_progress(current, total):
                        pct = min(0.1 + (current / total) * 0.4, 0.5)
                        progress_placeholder.progress(pct, f"Exporting: {current:,} / {total:,} rows...")
                    
                    # Pass date filter to export function for server-side filtering
                    df = export_dataset_data(
                        PROD_INSTANCE, 
                        selected_ds_id, 
                        date_column=selected_date_column,
                        start_date=start_date,
                        end_date=end_date,
                        progress_callback=export_progress if row_count > 100000 else None
                    )
                    original_count = len(df)
                    
                    status_placeholder.info(f"Exported {original_count:,} rows")
                    
                    time.sleep(0.5)
                    
                    # Step 2: Create dataset in dev OR use existing
                    if target_exists_in_dev:
                        progress_placeholder.progress(0.5, "Using existing dataset in Development...")
                        status_placeholder.info(f"Found existing dataset: {target_exists_in_dev.get('id')}")
                        new_dataset_id = target_exists_in_dev.get('id')
                    else:
                        progress_placeholder.progress(0.5, "Creating dataset in Development...")
                        status_placeholder.info("Creating new dataset in development instance...")
                        
                        new_dataset = create_dataset(DEV_INSTANCE, target_dataset_name, schema)
                        new_dataset_id = new_dataset.get('id')
                    
                    time.sleep(0.5)
                    
                    # Step 3: Upload data
                    progress_placeholder.progress(0.6, "Uploading data to Development...")
                    status_placeholder.info(f"Uploading {len(df):,} rows to dev instance...")
                    
                    def upload_progress(current, total):
                        pct = min(0.6 + (current / total) * 0.35, 0.95)
                        progress_placeholder.progress(pct, f"Uploading: {current:,} / {total:,} rows...")
                    
                    upload_data_to_dataset(DEV_INSTANCE, new_dataset_id, df, progress_callback=upload_progress if len(df) > 100000 else None)
                    
                    # Done!
                    progress_placeholder.progress(1.0, "Complete!")
                    status_placeholder.empty()
                    
                    action_text = "Data Replaced" if target_exists_in_dev else "Dataset Created"
                    
                    st.markdown(f"""
                    <div class="alert alert-success">
                        <span class="alert-title">{action_text} Successfully!</span><br/>
                        <strong>Name:</strong> {target_dataset_name}<br/>
                        <strong>Dataset ID:</strong> {new_dataset_id}<br/>
                        <strong>Rows Copied:</strong> {len(df):,}<br/>
                        <strong>Target:</strong> <span class="instance-badge instance-dev">DEV</span> {DEV_INSTANCE}
                    </div>
                    """, unsafe_allow_html=True)
                
            except Exception as e:
                progress_placeholder.empty()
                status_placeholder.empty()
                st.markdown(f"""
                <div class="alert alert-error">
                    <span class="alert-title">Copy Failed</span><br/>
                    Error: {str(e)}
                </div>
                """, unsafe_allow_html=True)
                st.exception(e)


if __name__ == "__main__":
    main()
