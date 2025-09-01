#!/usr/bin/env python3
"""
Infrastructure Climate Analytics Dashboard - Production Version
Works with or without local data files
"""

import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from pathlib import Path
import json
import numpy as np

# Initialize Dash app with Bootstrap theme
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="Infrastructure Climate Analytics"
)

# For deployment
server = app.server

def generate_sample_data():
    """Generate sample data for demo purposes"""
    countries = ['United States', 'China', 'Germany', 'Japan', 'India', 
                 'United Kingdom', 'France', 'Italy', 'Brazil', 'Canada',
                 'South Korea', 'Spain', 'Australia', 'Mexico', 'Indonesia']
    years = list(range(2010, 2024))
    
    data = []
    for country in countries:
        for year in years:
            base_score = 50 + (countries.index(country) * 2)
            year_improvement = (year - 2010) * 0.5
            
            data.append({
                'country': country,
                'year': year,
                'infrastructure_score': base_score + year_improvement + np.random.uniform(-5, 5),
                'transport_resilience': base_score + year_improvement + 5 + np.random.uniform(-3, 3),
                'energy_resilience': base_score + year_improvement - 5 + np.random.uniform(-3, 3),
                'water_resilience': base_score + year_improvement + 2 + np.random.uniform(-3, 3),
                'digital_resilience': base_score + year_improvement + 10 + np.random.uniform(-3, 3),
                'avg_resilience': base_score + year_improvement + 3,
                'score_change': np.random.uniform(-2, 5),
                'yearly_rank': np.random.randint(1, 16)
            })
    
    infrastructure_df = pd.DataFrame(data)
    
    # Create summary data
    country_summary = infrastructure_df.groupby('country').agg({
        'infrastructure_score': ['mean', 'min', 'max'],
        'year': ['min', 'max']
    }).reset_index()
    country_summary.columns = ['country', 'avg_score', 'min_score', 'max_score', 'first_year', 'last_year']
    country_summary['score_improvement'] = country_summary['max_score'] - country_summary['min_score']
    
    # Create yearly trends
    yearly_trends = infrastructure_df.groupby('year').agg({
        'infrastructure_score': ['mean', 'std', 'min', 'max']
    }).reset_index()
    yearly_trends.columns = ['year', 'global_avg_score', 'score_std_dev', 'min_score', 'max_score']
    
    # Top performers
    latest_year = infrastructure_df['year'].max()
    top_performers = infrastructure_df[infrastructure_df['year'] == latest_year].nlargest(10, 'infrastructure_score')[
        ['country', 'infrastructure_score', 'yearly_rank']
    ]
    top_performers.columns = ['country', 'latest_score', 'latest_rank']
    
    return {
        'infrastructure': infrastructure_df,
        'country_summary': country_summary,
        'yearly_trends': yearly_trends,
        'top_performers': top_performers,
        'metadata': {
            'pipeline_run': '2024-12-28',
            'record_counts': {'infrastructure': len(infrastructure_df)}
        }
    }

def load_data():
    """Load data - try local files first, then generate sample data"""
    DATA_PATH = Path.cwd() / "data" / "final"
    
    try:
        # Try to load actual data
        data = {}
        data['infrastructure'] = pd.read_csv(DATA_PATH / "clean_infrastructure.csv")
        data['country_summary'] = pd.read_csv(DATA_PATH / "country_summary.csv")
        data['yearly_trends'] = pd.read_csv(DATA_PATH / "yearly_trends.csv")
        data['top_performers'] = pd.read_csv(DATA_PATH / "top_performers.csv")
        with open(DATA_PATH / "pipeline_metadata.json", 'r') as f:
            data['metadata'] = json.load(f)
        print("Loaded actual data files")
        return data
    except:
        # Generate sample data if files don't exist
        print("Generating sample data for demo")
        return generate_sample_data()

# Load data
datasets = load_data()

# Define color scheme
colors = {
    'background': '#f8f9fa',
    'text': '#212529',
    'primary': '#0066cc',
    'success': '#28a745',
    'danger': '#dc3545',
    'warning': '#ffc107',
    'info': '#17a2b8'
}

# Create header
def create_header():
    return dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Overview", href="#overview")),
            dbc.NavItem(dbc.NavLink("Trends", href="#trends")),
            dbc.NavItem(dbc.NavLink("Rankings", href="#rankings")),
            dbc.NavItem(dbc.NavLink("Analysis", href="#analysis")),
        ],
        brand="🌍 Infrastructure Climate Analytics",
        brand_href="#",
        color="primary",
        dark=True,
        className="mb-4"
    )

# Create KPI cards
def create_kpi_cards():
    """Create key performance indicator cards"""
    
    # Calculate KPIs
    total_countries = datasets['infrastructure']['country'].nunique()
    avg_score = datasets['infrastructure']['infrastructure_score'].mean()
    best_performer = datasets['top_performers'].iloc[0]['country']
    total_records = len(datasets['infrastructure'])
    
    kpis = [
        {
            'title': 'Countries Analyzed',
            'value': str(total_countries),
            'icon': '🌍',
            'color': 'primary'
        },
        {
            'title': 'Avg Infrastructure Score',
            'value': f"{avg_score:.1f}",
            'icon': '📊',
            'color': 'success'
        },
        {
            'title': 'Top Performer',
            'value': best_performer,
            'icon': '🏆',
            'color': 'warning'
        },
        {
            'title': 'Data Points',
            'value': f"{total_records:,}",
            'icon': '📈',
            'color': 'info'
        }
    ]
    
    cards = []
    for kpi in kpis:
        card = dbc.Card([
            dbc.CardBody([
                html.H1(kpi['icon'], className="text-center"),
                html.H6(kpi['title'], className="text-center text-muted"),
                html.H3(kpi['value'], className="text-center"),
            ])
        ], color=kpi['color'], outline=True)
        cards.append(dbc.Col(card, xs=12, sm=6, md=3))
    
    return dbc.Row(cards, className="mb-4")

# Create filters
def create_filters():
    """Create filter controls"""
    
    countries = sorted(datasets['infrastructure']['country'].unique())
    years = sorted(datasets['infrastructure']['year'].unique())
    
    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Select Countries:", className="fw-bold"),
                    dcc.Dropdown(
                        id='country-filter',
                        options=[{'label': c, 'value': c} for c in countries],
                        value=datasets['top_performers']['country'].head(5).tolist(),
                        multi=True
                    )
                ], md=6),
                dbc.Col([
                    html.Label("Year Range:", className="fw-bold"),
                    dcc.RangeSlider(
                        id='year-slider',
                        min=min(years),
                        max=max(years),
                        value=[min(years), max(years)],
                        marks={str(y): str(y) for y in years[::2]},
                        tooltip={"placement": "bottom", "always_visible": True}
                    )
                ], md=6)
            ])
        ])
    ], className="mb-4")

# Layout
app.layout = dbc.Container([
    create_header(),
    
    # Overview Section
    html.Div(id="overview"),
    dbc.Container([
        html.H2("Executive Dashboard", className="mb-4"),
        create_kpi_cards(),
    ]),
    
    # Filters
    dbc.Container([
        html.H3("Interactive Analysis", className="mb-3"),
        create_filters()
    ]),
    
    # Visualizations
    dbc.Container([
        dbc.Row([
            # Time series chart
            dbc.Col([
                dcc.Graph(id='time-series-chart')
            ], md=6),
            
            # Ranking chart
            dbc.Col([
                dcc.Graph(id='ranking-chart')
            ], md=6)
        ], className="mb-4"),
        
        dbc.Row([
            # Heatmap
            dbc.Col([
                dcc.Graph(id='heatmap-chart')
            ], md=12)
        ], className="mb-4"),
        
        dbc.Row([
            # Improvement chart
            dbc.Col([
                dcc.Graph(id='improvement-chart')
            ], md=6),
            
            # Distribution chart
            dbc.Col([
                dcc.Graph(id='distribution-chart')
            ], md=6)
        ])
    ]),
    
    # Footer
    html.Hr(),
    dbc.Container([
        html.P([
            "Data processed: ", 
            datasets['metadata']['pipeline_run'],
            " | Created by: Wendy Lim",
            " | ",
            html.A("GitHub", href="https://github.com/wendy-gaiden/infrastructure-climate-analytics")
        ], className="text-center text-muted")
    ])
], fluid=True)

# Callbacks
@app.callback(
    Output('time-series-chart', 'figure'),
    [Input('country-filter', 'value'),
     Input('year-slider', 'value')]
)
def update_time_series(countries, year_range):
    """Update time series chart"""
    
    filtered = datasets['infrastructure'][
        (datasets['infrastructure']['country'].isin(countries)) &
        (datasets['infrastructure']['year'] >= year_range[0]) &
        (datasets['infrastructure']['year'] <= year_range[1])
    ]
    
    fig = px.line(
        filtered,
        x='year',
        y='infrastructure_score',
        color='country',
        title='Infrastructure Score Over Time',
        labels={'infrastructure_score': 'Score', 'year': 'Year'},
        line_shape='spline',
        render_mode='svg'
    )
    
    fig.update_layout(
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return fig

@app.callback(
    Output('ranking-chart', 'figure'),
    [Input('year-slider', 'value')]
)
def update_ranking(year_range):
    """Update ranking chart"""
    
    latest_year = year_range[1]
    year_data = datasets['infrastructure'][
        datasets['infrastructure']['year'] == latest_year
    ].nlargest(10, 'infrastructure_score')
    
    fig = px.bar(
        year_data,
        x='infrastructure_score',
        y='country',
        orientation='h',
        title=f'Top 10 Countries ({latest_year})',
        labels={'infrastructure_score': 'Score', 'country': ''},
        color='infrastructure_score',
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(
        template='plotly_white',
        height=400,
        showlegend=False
    )
    
    return fig

@app.callback(
    Output('heatmap-chart', 'figure'),
    [Input('country-filter', 'value')]
)
def update_heatmap(countries):
    """Update resilience heatmap"""
    
    filtered = datasets['infrastructure'][
        datasets['infrastructure']['country'].isin(countries)
    ]
    
    # Pivot for heatmap
    pivot_data = filtered.pivot_table(
        values=['transport_resilience', 'energy_resilience', 
                'water_resilience', 'digital_resilience'],
        index='country',
        aggfunc='mean'
    )
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values.T,
        x=pivot_data.index,
        y=['Transport', 'Energy', 'Water', 'Digital'],
        colorscale='RdYlGn',
        text=pivot_data.values.T,
        texttemplate='%{text:.1f}',
        textfont={"size": 10},
        colorbar=dict(title="Score")
    ))
    
    fig.update_layout(
        title='Resilience Scores by Category',
        template='plotly_white',
        height=300
    )
    
    return fig

@app.callback(
    Output('improvement-chart', 'figure'),
    [Input('country-filter', 'value')]
)
def update_improvement(countries):
    """Update improvement chart"""
    
    summary = datasets['country_summary'][
        datasets['country_summary']['country'].isin(countries)
    ].sort_values('score_improvement', ascending=True)
    
    fig = px.bar(
        summary,
        x='score_improvement',
        y='country',
        orientation='h',
        title='Infrastructure Score Improvement',
        labels={'score_improvement': 'Improvement', 'country': ''},
        color='score_improvement',
        color_continuous_scale='RdYlGn'
    )
    
    fig.update_layout(
        template='plotly_white',
        height=400,
        showlegend=False
    )
    
    return fig

@app.callback(
    Output('distribution-chart', 'figure'),
    [Input('year-slider', 'value')]
)
def update_distribution(year_range):
    """Update distribution chart"""
    
    latest_year = year_range[1]
    year_data = datasets['infrastructure'][
        datasets['infrastructure']['year'] == latest_year
    ]
    
    fig = px.box(
        year_data,
        y=['transport_resilience', 'energy_resilience', 
           'water_resilience', 'digital_resilience'],
        title=f'Resilience Distribution ({latest_year})',
        labels={'value': 'Score', 'variable': 'Category'}
    )
    
    fig.update_layout(
        template='plotly_white',
        height=400,
        showlegend=False
    )
    
    return fig

# Run the app
if __name__ == '__main__':
    print("🚀 Starting dashboard...")
    print("📊 Open your browser to: http://127.0.0.1:8050")
    app.run(debug=True, host='0.0.0.0', port=8050)
