"""
DOMO Dataset Copy Tool
Copies datasets from Production (keshet-tv) to Development (keshet-tv-dev)
"""

import streamlit as st
import requests
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timedelta
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
        --primary: #4a5568;
        --primary-dark: #2d3748;
        --accent: #5a67d8;
        --success: #48bb78;
        --success-bg: #f0fff4;
        --success-border: #9ae6b4;
        --warning: #ed8936;
        --warning-bg: #fffaf0;
        --warning-border: #fbd38d;
        --error: #e53e3e;
        --error-bg: #fff5f5;
        --error-border: #feb2b2;
        --info: #4299e1;
        --info-bg: #ebf8ff;
        --info-border: #90cdf4;
        --bg-primary: #f7fafc;
        --bg-secondary: #ffffff;
        --bg-tertiary: #edf2f7;
        --text-primary: #1a202c;
        --text-secondary: #718096;
        --text-muted: #a0aec0;
        --border-color: #e2e8f0;
    }
    
    .stApp {
        background: var(--bg-primary);
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .app-header {
        background: linear-gradient(135deg, var(--primary-dark) 0%, var(--primary) 100%);
        padding: 2rem 2.5rem;
        border-radius: 8px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    
    .app-title {
        color: #ffffff !important;
        font-size: 1.5rem;
        font-weight: 600;
        margin: 0;
    }
    
    .app-subtitle {
        color: rgba(255,255,255,0.85) !important;
        font-size: 0.875rem;
        margin-top: 0.5rem;
    }
    
    .metric-box {
        background: var(--bg-secondary);
        border-radius: 6px;
        padding: 1rem 1.25rem;
        border: 1px solid var(--border-color);
        text-align: center;
    }
    
    .metric-value {
        font-size: 1.125rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 0.25rem;
    }
    
    .metric-label {
        font-size: 0.75rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .status-indicator {
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.75rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 500;
        text-transform: uppercase;
    }
    
    .status-exists {
        background: var(--warning-bg);
        color: #c05621;
        border: 1px solid var(--warning-border);
    }
    
    .status-new {
        background: var(--success-bg);
        color: #276749;
        border: 1px solid var(--success-border);
    }
    
    .alert {
        border-radius: 6px;
        padding: 1rem;
        margin: 0.75rem 0;
        font-size: 0.875rem;
    }
    
    .alert-info {
        background: var(--info-bg);
        border: 1px solid var(--info-border);
        color: #2c5282;
    }
    
    .alert-warning {
        background: var(--warning-bg);
        border: 1px solid var(--warning-border);
        color: #744210;
    }
    
    .alert-success {
        background: var(--success-bg);
        border: 1px solid var(--success-border);
        color: #22543d;
    }
    
    .alert-error {
        background: var(--error-bg);
        border: 1px solid var(--error-border);
        color: #742a2a;
    }
    
    .alert-title {
        font-weight: 600;
        margin-bottom: 0.25rem;
    }
    
    .dataset-card {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 1.25rem;
        margin-bottom: 1rem;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
    }
    
    .dataset-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    .dataset-card-title {
        font-size: 1rem;
        font-weight: 600;
        color: var(--text-primary);
    }
    
    .dataset-card-id {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        color: var(--text-muted);
    }
    
    .dataset-card-details {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 1rem;
    }
    
    .dataset-card-detail {
        text-align: center;
    }
    
    .dataset-card-detail-value {
        font-size: 1rem;
        font-weight: 600;
        color: var(--text-primary);
    }
    
    .dataset-card-detail-label {
        font-size: 0.7rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .section-title {
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--text-primary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid var(--border-color);
    }
    
    .column-item {
        background: var(--bg-tertiary);
        border-radius: 4px;
        padding: 0.5rem 0.75rem;
        margin: 0.25rem 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    
    .column-name { color: var(--text-primary); }
    .column-type { color: var(--text-muted); font-size: 0.7rem; }
    
    .instance-badge {
        display: inline-flex;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .instance-prod { background: #fed7d7; color: #c53030; }
    .instance-dev { background: #c6f6d5; color: #276749; }
    
    .divider {
        height: 1px;
        background: var(--border-color);
        margin: 1.5rem 0;
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# DOMO API FUNCTIONS
# =============================================================================

def get_domo_headers(instance: str) -> Dict[str, str]:
    """Get headers for DOMO API requests based on instance."""
    token_key = "prod_developer_token" if instance == PROD_INSTANCE else "dev_developer_token"
    return {
        'Content-Type': 'application/json',
        'X-DOMO-Developer-Token': st.secrets["domo"][token_key]
    }


def get_domo_base_url(instance: str) -> str:
    """Get base URL for DOMO instance."""
    return f"https://{instance}.domo.com"


@st.cache_data(ttl=300)
def list_datasets(instance: str) -> List[Dict]:
    """List all datasets from a DOMO instance."""
    url = f"{get_domo_base_url(instance)}/api/data/v3/datasources"
    all_datasets = []
    
    # First request without parameters to get initial batch
    headers = get_domo_headers(instance)
    
    try:
        # Try without any query parameters first (works reliably)
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        # API returns {'dataSources': [...], '_metaData': {...}}
        batch = data.get('dataSources', [])
        all_datasets.extend(batch)
        
        # Check metadata for total count
        metadata = data.get('_metaData', {})
        total_count = metadata.get('totalCount', len(batch))
        
        # If there are more datasets, paginate using limit only
        offset = len(batch)
        while offset < total_count:
            try:
                # Use limit parameter only for pagination
                paginated_url = f"{url}?limit=50&offset={offset}"
                response = requests.get(paginated_url, headers=headers, timeout=60)
                response.raise_for_status()
                data = response.json()
                batch = data.get('dataSources', [])
                
                if not batch:
                    break
                    
                all_datasets.extend(batch)
                offset += len(batch)
            except:
                # If pagination fails, just return what we have
                break
        
        return all_datasets
        
    except Exception as e:
        # Fallback: try the UI search endpoint
        try:
            search_url = f"{get_domo_base_url(instance)}/api/data/ui/v3/datasources/search"
            payload = {
                "filters": [],
                "combineResults": True,
                "count": 500,
                "offset": 0,
                "sort": {"field": "name", "order": "ASC"}
            }
            response = requests.post(search_url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()
            return data.get('dataSources', [])
        except:
            raise e


def get_dataset_info(instance: str, dataset_id: str) -> Dict:
    """Get detailed information about a specific dataset."""
    url = f"{get_domo_base_url(instance)}/api/data/v3/datasources/{dataset_id}"
    response = requests.get(url, headers=get_domo_headers(instance), timeout=60)
    response.raise_for_status()
    return response.json()


def get_dataset_schema(instance: str, dataset_id: str) -> List[Dict]:
    """Get schema (columns) for a dataset."""
    dataset_info = get_dataset_info(instance, dataset_id)
    return dataset_info.get('schemas', {}).get('columns', [])


def create_dataset(instance: str, name: str, schema: List[Dict]) -> Dict:
    """Create a new dataset in the target instance."""
    url = f"{get_domo_base_url(instance)}/api/data/v3/datasources"
    
    payload = {
        'name': name,
        'schemas': {
            'columns': schema
        }
    }
    
    response = requests.post(url, headers=get_domo_headers(instance), json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def export_dataset_csv(instance: str, dataset_id: str) -> pd.DataFrame:
    """Export dataset data using Domo Query API."""
    url = f"{get_domo_base_url(instance)}/api/query/v1/execute/{dataset_id}"
    headers = get_domo_headers(instance)
    
    # Query all data from the dataset
    payload = {
        "sql": "SELECT * FROM table"
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=300)
    response.raise_for_status()
    
    data = response.json()
    
    # Parse the response - it contains columns and rows
    columns = data.get('columns', [])
    rows = data.get('rows', [])
    
    # Create DataFrame
    df = pd.DataFrame(rows, columns=columns)
    
    return df


def upload_data_to_dataset(instance: str, dataset_id: str, csv_data: str) -> bool:
    """Upload CSV data to a dataset."""
    url = f"{get_domo_base_url(instance)}/api/data/v3/datasources/{dataset_id}/data"
    
    headers = get_domo_headers(instance)
    headers['Content-Type'] = 'text/csv'
    
    response = requests.put(url, headers=headers, data=csv_data.encode('utf-8'), timeout=300)
    response.raise_for_status()
    return True


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
        <h1 class="app-title">📦 {APP_NAME}</h1>
        <p class="app-subtitle">Copy datasets from Production ({PROD_INSTANCE}) to Development ({DEV_INSTANCE})</p>
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
    row_count = dataset.get('rowCount', 0) or 0
    col_count = len(schema) if schema else dataset.get('columnCount', 0)
    
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
            <span class="alert-title">⚠️ Dataset Already Exists in Dev</span><br/>
            A dataset with this name already exists in the dev instance (ID: {dev_dataset.get('id', 'N/A')}).<br/>
            Copying will create a <strong>new dataset</strong> with the same name.
        </div>
        """, unsafe_allow_html=True)


def render_schema_preview(schema: List[Dict], date_columns: List[str]):
    st.markdown('<div class="section-title">Schema Preview</div>', unsafe_allow_html=True)
    
    if not schema:
        st.warning("No schema information available")
        return
    
    for col in schema[:10]:
        is_date = col['name'] in date_columns
        date_indicator = " 📅" if is_date else ""
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
        page_icon="📦",
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
    
    # Render metrics
    render_instance_metrics(len(prod_datasets), len(dev_datasets))
    
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
            schema = dataset_info.get('schemas', {}).get('columns', [])
            date_columns = get_date_columns(schema)
        except Exception as e:
            st.error(f"Failed to load dataset details: {e}")
            return
        
        # Check if exists in dev
        exists_in_dev = check_dataset_exists_in_dev(dataset_info.get('name', ''), dev_datasets)
        
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
            exists_in_dev is not None,
            exists_in_dev
        )
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        # Render schema
        render_schema_preview(schema, date_columns)
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        # Copy action
        st.markdown('<div class="section-title">Copy to Development</div>', unsafe_allow_html=True)
        
        if exists_in_dev:
            st.markdown(f"""
            <div class="alert alert-warning">
                <span class="alert-title">⚠️ Duplicate Warning</span><br/>
                A dataset named "<strong>{dataset_info.get('name')}</strong>" already exists in dev.<br/>
                Proceeding will create a new dataset with the same name.
            </div>
            """, unsafe_allow_html=True)
        
        copy_button = st.button("🚀 Copy Dataset to Dev", type="primary", use_container_width=True)
        
        if copy_button:
            progress_placeholder = st.empty()
            status_placeholder = st.empty()
            
            try:
                # Step 1: Export data from prod
                progress_placeholder.progress(0.2, "Exporting data from Production...")
                status_placeholder.info("📥 Downloading data from production instance...")
                
                df = export_dataset_csv(PROD_INSTANCE, selected_ds_id)
                original_count = len(df)
                
                # Apply date filter if specified
                if selected_date_column and start_date and end_date:
                    df[selected_date_column] = pd.to_datetime(df[selected_date_column], errors='coerce')
                    df = df[
                        (df[selected_date_column] >= pd.Timestamp(start_date)) & 
                        (df[selected_date_column] <= pd.Timestamp(end_date))
                    ]
                    filtered_count = len(df)
                    status_placeholder.info(f"📊 Filtered {original_count:,} rows → {filtered_count:,} rows")
                else:
                    status_placeholder.info(f"📊 Exported {original_count:,} rows")
                
                time.sleep(0.5)
                
                # Step 2: Create dataset in dev
                progress_placeholder.progress(0.5, "Creating dataset in Development...")
                status_placeholder.info("🏗️ Creating new dataset in development instance...")
                
                new_dataset = create_dataset(
                    DEV_INSTANCE,
                    dataset_info.get('name'),
                    schema
                )
                new_dataset_id = new_dataset.get('id')
                
                time.sleep(0.5)
                
                # Step 3: Upload data
                progress_placeholder.progress(0.8, "Uploading data to Development...")
                status_placeholder.info(f"📤 Uploading {len(df):,} rows to dev instance...")
                
                # Convert back to CSV for upload
                csv_to_upload = df.to_csv(index=False)
                upload_data_to_dataset(DEV_INSTANCE, new_dataset_id, csv_to_upload)
                
                # Done!
                progress_placeholder.progress(1.0, "Complete!")
                status_placeholder.empty()
                
                st.markdown(f"""
                <div class="alert alert-success">
                    <span class="alert-title">✅ Dataset Copied Successfully!</span><br/>
                    <strong>Name:</strong> {dataset_info.get('name')}<br/>
                    <strong>New Dataset ID:</strong> {new_dataset_id}<br/>
                    <strong>Rows Copied:</strong> {len(df):,}<br/>
                    <strong>Target:</strong> <span class="instance-badge instance-dev">DEV</span> {DEV_INSTANCE}
                </div>
                """, unsafe_allow_html=True)
                
                # Clear cache to refresh dev datasets list
                list_datasets.clear()
                
            except Exception as e:
                progress_placeholder.empty()
                status_placeholder.empty()
                st.markdown(f"""
                <div class="alert alert-error">
                    <span class="alert-title">❌ Copy Failed</span><br/>
                    Error: {str(e)}
                </div>
                """, unsafe_allow_html=True)
                st.exception(e)


if __name__ == "__main__":
    main()
