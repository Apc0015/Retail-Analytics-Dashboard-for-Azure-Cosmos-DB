"""
Retail Analytics Dashboard for Azure Cosmos DB
Ayush Chhoker - U00363568
"""

import streamlit as st
import pymongo
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import os

# Page configuration
st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="�",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Azure Cosmos DB Connection
# Set COSMOS_CONNECTION_STRING environment variable before running
COSMOS_CONNECTION_STRING = os.environ.get('COSMOS_CONNECTION_STRING')
if not COSMOS_CONNECTION_STRING:
    st.error("⚠️ Please set COSMOS_CONNECTION_STRING environment variable")
    st.info("See README.md for setup instructions")
    st.stop()
    
DATABASE_NAME = "retail_analytics"

# Initialize connection
@st.cache_resource
def init_connection():
    """Initialize Azure Cosmos DB connection"""
    try:
        client = MongoClient(COSMOS_CONNECTION_STRING, serverSelectionTimeoutMS=10000)
        # Test connection
        client.server_info()
        return client
    except Exception as e:
        st.error(f"Failed to connect to Azure Cosmos DB: {str(e)}")
        return None

# Data loading functions with caching
@st.cache_data(ttl=300)
def load_products():
    """Load products data"""
    try:
        client = init_connection()
        if client is None:
            return pd.DataFrame()
        db = client[DATABASE_NAME]
        data = list(db.products.find({}, {'_id': 0}))
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error loading products: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_customers():
    """Load customers data"""
    try:
        client = init_connection()
        if client is None:
            return pd.DataFrame()
        db = client[DATABASE_NAME]
        data = list(db.customers.find({}, {'_id': 0}))
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error loading customers: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_orders():
    """Load orders data"""
    try:
        client = init_connection()
        if client is None:
            return pd.DataFrame()
        db = client[DATABASE_NAME]
        data = list(db.orders.find({}, {'_id': 0}))
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error loading orders: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_reviews():
    """Load reviews data"""
    try:
        client = init_connection()
        if client is None:
            return pd.DataFrame()
        db = client[DATABASE_NAME]
        data = list(db.reviews.find({}, {'_id': 0}))
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error loading reviews: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_category_stats():
    """Get product category statistics"""
    try:
        client = init_connection()
        if client is None:
            return pd.DataFrame()
        db = client[DATABASE_NAME]
        pipeline = [
            {
                '$group': {
                    '_id': '$category',
                    'total_products': {'$sum': 1},
                    'avg_price': {'$avg': '$price'},
                    'avg_rating': {'$avg': '$rating'},
                    'total_stock': {'$sum': '$stock_quantity'}
                }
            },
            {'$sort': {'total_products': -1}}
        ]
        data = list(db.products.aggregate(pipeline))
        if data:
            df = pd.DataFrame(data)
            df.columns = ['category', 'total_products', 'avg_price', 'avg_rating', 'total_stock']
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading category stats: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_sales_by_state():
    """Get sales statistics by state"""
    try:
        client = init_connection()
        if client is None:
            return pd.DataFrame()
        db = client[DATABASE_NAME]
        pipeline = [
            {
                '$group': {
                    '_id': '$customer_state',
                    'total_orders': {'$sum': 1},
                    'total_revenue': {'$sum': '$total_amount'},
                    'avg_order_value': {'$avg': '$total_amount'}
                }
            },
            {'$sort': {'total_revenue': -1}}
        ]
        data = list(db.orders.aggregate(pipeline))
        if data:
            df = pd.DataFrame(data)
            df.columns = ['state', 'total_orders', 'total_revenue', 'avg_order_value']
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading sales by state: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_top_products():
    """Get top products by reviews"""
    try:
        client = init_connection()
        if client is None:
            return pd.DataFrame()
        db = client[DATABASE_NAME]
        pipeline = [
            {
                '$group': {
                    '_id': '$product_id',
                    'product_name': {'$first': '$product_name'},
                    'avg_rating': {'$avg': '$rating'},
                    'review_count': {'$sum': 1}
                }
            },
            {'$match': {'review_count': {'$gte': 2}}},
            {'$sort': {'avg_rating': -1, 'review_count': -1}},
            {'$limit': 15}
        ]
        data = list(db.reviews.aggregate(pipeline))
        if data:
            df = pd.DataFrame(data)
            df.columns = ['product_id', 'product_name', 'avg_rating', 'review_count']
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading top products: {str(e)}")
        return pd.DataFrame()

def main():
    st.title("Retail Analytics Dashboard")
    st.markdown("### Retail Analytics Dashboard for Azure Cosmos DB\nAyush Chhoker - U00363568")

    # Sidebar
    st.sidebar.title("Dashboard Controls")
    st.sidebar.markdown("---")

    # Navigation
    page = st.sidebar.radio(
        "Select View",
        ["About", "Overview", "Products", "Customers", "Orders", "Reviews"]
    )

    # Load data
    with st.spinner("Loading data from Azure Cosmos DB..."):
        if page == "About":
            show_about()
        elif page == "Overview":
            show_overview()
        elif page == "Products":
            show_products()
        elif page == "Customers":
            show_customers()
        elif page == "Orders":
            show_orders()
        elif page == "Reviews":
            show_reviews()

def show_about():
    """Display about section with business narrative"""
    st.header("About This Dashboard")
    
    st.markdown("""
    ### Business Context
    
    This dashboard provides retail analytics for managers, business analysts, and decision-makers who need 
    visibility into product performance, customer behavior, sales patterns, and customer feedback. The retail 
    industry requires fast access to accurate data to stay competitive, manage inventory efficiently, and 
    respond to changing customer preferences.
    
    ### Analytical Value
    
    The dashboard consolidates data from four key areas:
    
    **Product Analysis** helps identify which items sell well, which categories drive revenue, and where 
    inventory levels need adjustment. Managers can spot slow-moving products and make informed decisions 
    about promotions or discontinuations.
    
    **Customer Insights** reveal demographic patterns and loyalty distribution across regions. Understanding 
    where customers are located and their purchasing power helps target marketing campaigns and plan regional 
    expansion strategies.
    
    **Order Analytics** track transaction volume, revenue trends, and payment preferences. This information 
    guides financial forecasting, helps identify peak sales periods, and informs decisions about payment 
    options and shipping strategies.
    
    **Review Monitoring** captures customer sentiment and product satisfaction levels. Feedback analysis 
    identifies quality issues early, highlights products that exceed expectations, and provides direction 
    for customer service improvements.
    
    ### Practical Applications
    
    A merchandising manager uses product and review data to decide which items to reorder, which to discount, 
    and which new products to introduce. Regional patterns in customer and order data inform where to focus 
    advertising spend and whether to adjust pricing by location.
    
    An operations director reviews order status and fulfillment metrics to improve logistics and reduce 
    delivery times. Customer loyalty tiers guide retention programs and help allocate resources toward 
    high-value segments.
    
    Marketing teams analyze review sentiment alongside sales data to craft messaging that resonates with 
    customers and addresses common concerns. Inventory planners use real-time stock levels and sales velocity 
    to minimize overstock and prevent stockouts.
    
    ### Data Foundation
    
    The dashboard connects to an Azure Cosmos DB database containing structured retail transaction data. 
    This cloud-based approach ensures data is accessible from any location, supports real-time updates, and 
    scales as the business grows.
    
    By bringing together product, customer, order, and review information in one place, this tool eliminates 
    the need to manually compile reports from multiple systems. Decisions become faster and more data-driven, 
    reducing guesswork and improving operational efficiency.
    """)

def show_overview():
    """Display overview dashboard"""
    st.header("Business Overview")

    # Load all data
    products_df = load_products()
    customers_df = load_customers()
    orders_df = load_orders()
    reviews_df = load_reviews()

    if products_df.empty or customers_df.empty or orders_df.empty or reviews_df.empty:
        st.warning("Some data is missing. Please check your database connection.")
        return

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Products", f"{len(products_df):,}")
        st.metric("Product Categories", f"{products_df['category'].nunique()}")

    with col2:
        st.metric("Total Customers", f"{len(customers_df):,}")
        st.metric("Unique States", f"{customers_df['state'].nunique()}")

    with col3:
        st.metric("Total Orders", f"{len(orders_df):,}")
        total_revenue = orders_df['total_amount'].sum()
        st.metric("Total Revenue", f"${total_revenue:,.2f}")

    with col4:
        st.metric("Total Reviews", f"{len(reviews_df):,}")
        avg_rating = reviews_df['rating'].mean()
        st.metric("Avg Rating", f"{avg_rating:.2f}⭐")

    st.markdown("---")

    # Visualizations
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by State")
        sales_by_state = get_sales_by_state()
        if not sales_by_state.empty:
            fig = px.bar(sales_by_state.head(10), 
                        x='state', 
                        y='total_revenue',
                        title="Top 10 States by Revenue",
                        color='total_revenue',
                        color_continuous_scale='Blues')
            fig.update_layout(xaxis_title="State", yaxis_title="Revenue ($)")
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Product Categories")
        category_stats = get_category_stats()
        if not category_stats.empty:
            fig = px.pie(category_stats, 
                        values='total_products', 
                        names='category',
                        title="Products by Category")
            st.plotly_chart(fig, use_container_width=True)

    # Order status distribution
    st.subheader("Order Status Distribution")
    col1, col2 = st.columns(2)

    with col1:
        status_counts = orders_df['status'].value_counts()
        fig = px.bar(x=status_counts.index, 
                    y=status_counts.values,
                    title="Orders by Status",
                    labels={'x': 'Status', 'y': 'Count'},
                    color=status_counts.values,
                    color_continuous_scale='Greens')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        payment_counts = orders_df['payment_method'].value_counts()
        fig = px.pie(values=payment_counts.values, 
                    names=payment_counts.index,
                    title="Payment Methods")
        st.plotly_chart(fig, use_container_width=True)

def show_products():
    """Display products analysis"""
    st.header("Product Analysis")

    products_df = load_products()

    if products_df.empty:
        st.warning("No product data available")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Products", f"{len(products_df):,}")
    with col2:
        avg_price = products_df['price'].mean()
        st.metric("Average Price", f"${avg_price:.2f}")
    with col3:
        total_stock = products_df['stock_quantity'].sum()
        st.metric("Total Stock", f"{total_stock:,}")
    with col4:
        avg_rating = products_df['rating'].mean()
        st.metric("Avg Product Rating", f"{avg_rating:.2f}/5.0")

    st.markdown("---")

    # Category analysis
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Average Price by Category")
        category_stats = get_category_stats()
        if not category_stats.empty:
            fig = px.bar(category_stats.sort_values('avg_price', ascending=False),
                        x='category',
                        y='avg_price',
                        title="Average Product Price by Category",
                        color='avg_price',
                        color_continuous_scale='Viridis')
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Stock Levels by Category")
        if not category_stats.empty:
            fig = px.bar(category_stats.sort_values('total_stock', ascending=False),
                        x='category',
                        y='total_stock',
                        title="Total Stock by Category",
                        color='total_stock',
                        color_continuous_scale='Oranges')
            st.plotly_chart(fig, use_container_width=True)

    # Price distribution
    st.subheader("Price Distribution")
    fig = px.histogram(products_df, 
                      x='price', 
                      nbins=30,
                      title="Distribution of Product Prices")
    fig.update_layout(xaxis_title="Price ($)", yaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

    # Products table
    st.subheader("Product Catalog")
    display_df = products_df[['product_id', 'name', 'category', 'brand', 'price', 'stock_quantity', 'rating']].copy()
    display_df = display_df.sort_values('price', ascending=False)
    st.dataframe(display_df, use_container_width=True, height=400)

def show_customers():
    """Display customer analysis"""
    st.header("Customer Analysis")

    customers_df = load_customers()

    if customers_df.empty:
        st.warning("No customer data available")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Customers", f"{len(customers_df):,}")
    with col2:
        st.metric("Unique Cities", f"{customers_df['city'].nunique()}")
    with col3:
        st.metric("Unique States", f"{customers_df['state'].nunique()}")
    with col4:
        loyalty_dist = customers_df['loyalty_tier'].value_counts()
        top_tier = loyalty_dist.index[0] if len(loyalty_dist) > 0 else "N/A"
        st.metric("Top Loyalty Tier", top_tier)

    st.markdown("---")

    # Visualizations
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Customers by State")
        state_counts = customers_df['state'].value_counts().head(10)
        fig = px.bar(x=state_counts.index, 
                    y=state_counts.values,
                    title="Top 10 States by Customer Count",
                    labels={'x': 'State', 'y': 'Customers'},
                    color=state_counts.values,
                    color_continuous_scale='Blues')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Loyalty Tier Distribution")
        tier_counts = customers_df['loyalty_tier'].value_counts()
        fig = px.pie(values=tier_counts.values, 
                    names=tier_counts.index,
                    title="Customer Loyalty Tiers")
        st.plotly_chart(fig, use_container_width=True)

    # City distribution
    st.subheader("Top Cities by Customer Count")
    city_counts = customers_df['city'].value_counts().head(10)
    fig = px.bar(x=city_counts.index, 
                y=city_counts.values,
                title="Top 10 Cities",
                labels={'x': 'City', 'y': 'Customers'},
                color=city_counts.values,
                color_continuous_scale='Greens')
    st.plotly_chart(fig, use_container_width=True)

    # Customers table
    st.subheader("Customer Directory")
    display_df = customers_df[['customer_id', 'name', 'email', 'city', 'state', 'loyalty_tier']].copy()
    st.dataframe(display_df, use_container_width=True, height=400)

def show_orders():
    """Display orders analysis"""
    st.header("Orders Analysis")

    orders_df = load_orders()

    if orders_df.empty:
        st.warning("No order data available")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Orders", f"{len(orders_df):,}")
    with col2:
        total_revenue = orders_df['total_amount'].sum()
        st.metric("Total Revenue", f"${total_revenue:,.2f}")
    with col3:
        avg_order = orders_df['total_amount'].mean()
        st.metric("Avg Order Value", f"${avg_order:.2f}")
    with col4:
        completed = len(orders_df[orders_df['status'] == 'Completed'])
        st.metric("Completed Orders", f"{completed:,}")

    st.markdown("---")

    # Visualizations
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by State")
        sales_by_state = get_sales_by_state()
        if not sales_by_state.empty:
            fig = px.bar(sales_by_state.head(10),
                        x='state',
                        y='total_revenue',
                        title="Top 10 States by Revenue",
                        color='total_revenue',
                        color_continuous_scale='Viridis')
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Order Status Breakdown")
        status_counts = orders_df['status'].value_counts()
        fig = px.pie(values=status_counts.values, 
                    names=status_counts.index,
                    title="Orders by Status")
        st.plotly_chart(fig, use_container_width=True)

    # Order value distribution
    st.subheader("Order Value Distribution")
    fig = px.histogram(orders_df, 
                      x='total_amount', 
                      nbins=30,
                      title="Distribution of Order Values")
    fig.update_layout(xaxis_title="Order Amount ($)", yaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

    # Recent orders table
    st.subheader("Recent Orders")
    display_df = orders_df[['order_id', 'customer_name', 'customer_state', 'total_amount', 'status', 'payment_method']].copy()
    display_df = display_df.sort_values('order_id', ascending=False)
    st.dataframe(display_df, use_container_width=True, height=400)

def show_reviews():
    """Display reviews analysis"""
    st.header("Reviews Analysis")

    reviews_df = load_reviews()

    if reviews_df.empty:
        st.warning("No review data available")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Reviews", f"{len(reviews_df):,}")
    with col2:
        avg_rating = reviews_df['rating'].mean()
        st.metric("Average Rating", f"{avg_rating:.2f}⭐")
    with col3:
        positive_reviews = len(reviews_df[reviews_df['rating'] >= 4])
        st.metric("Positive Reviews", f"{positive_reviews:,}")
    with col4:
        avg_helpful = reviews_df['helpful_votes'].mean()
        st.metric("Avg Helpful Votes", f"{avg_helpful:.1f}")

    st.markdown("---")

    # Visualizations
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Rating Distribution")
        rating_counts = reviews_df['rating'].value_counts().sort_index()
        fig = px.bar(x=rating_counts.index, 
                    y=rating_counts.values,
                    title="Reviews by Rating",
                    labels={'x': 'Rating', 'y': 'Count'},
                    color=rating_counts.values,
                    color_continuous_scale='RdYlGn')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Rated Products")
        top_products = get_top_products()
        if not top_products.empty:
            fig = px.bar(top_products.head(10),
                        x='avg_rating',
                        y='product_name',
                        orientation='h',
                        title="Top 10 Products by Rating",
                        color='review_count',
                        color_continuous_scale='Blues')
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

    # Helpful votes distribution
    st.subheader("Review Helpfulness")
    fig = px.histogram(reviews_df, 
                      x='helpful_votes', 
                      nbins=20,
                      title="Distribution of Helpful Votes")
    fig.update_layout(xaxis_title="Helpful Votes", yaxis_title="Count")
    st.plotly_chart(fig, use_container_width=True)

    # Reviews table
    st.subheader("Recent Reviews")
    display_df = reviews_df[['review_id', 'product_name', 'customer_name', 'rating', 'review_text', 'helpful_votes']].copy()
    display_df = display_df.sort_values('review_id', ascending=False)
    st.dataframe(display_df, use_container_width=True, height=400)

if __name__ == "__main__":
    main()
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray; padding: 20px;'>"
        "Made by Ayush Chhoker"
        "</div>",
        unsafe_allow_html=True
    )
