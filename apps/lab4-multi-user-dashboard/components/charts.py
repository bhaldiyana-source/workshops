"""Chart building utilities for Dash dashboard."""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Optional, List, Dict, Any


class ChartBuilder:
    """Utility class for building Plotly charts."""
    
    @staticmethod
    def create_line_chart(
        df: pd.DataFrame,
        x: str,
        y: str,
        title: str = "",
        color: Optional[str] = None,
        template: str = "plotly_white"
    ) -> go.Figure:
        """
        Create a line chart.
        
        Args:
            df: DataFrame with data
            x: Column name for x-axis
            y: Column name for y-axis
            title: Chart title
            color: Column for color grouping
            template: Plotly template
            
        Returns:
            Plotly Figure
        """
        if df.empty:
            return ChartBuilder._empty_chart(title)
        
        fig = px.line(df, x=x, y=y, color=color, title=title, template=template)
        fig.update_traces(line_width=2)
        return fig
    
    @staticmethod
    def create_bar_chart(
        df: pd.DataFrame,
        x: str,
        y: str,
        title: str = "",
        color: Optional[str] = None,
        template: str = "plotly_white"
    ) -> go.Figure:
        """
        Create a bar chart.
        
        Args:
            df: DataFrame with data
            x: Column name for x-axis
            y: Column name for y-axis
            title: Chart title
            color: Column for color values
            template: Plotly template
            
        Returns:
            Plotly Figure
        """
        if df.empty:
            return ChartBuilder._empty_chart(title)
        
        fig = px.bar(df, x=x, y=y, color=color, title=title, template=template)
        return fig
    
    @staticmethod
    def create_pie_chart(
        df: pd.DataFrame,
        values: str,
        names: str,
        title: str = "",
        hole: float = 0.0,
        template: str = "plotly_white"
    ) -> go.Figure:
        """
        Create a pie chart.
        
        Args:
            df: DataFrame with data
            values: Column name for values
            names: Column name for labels
            title: Chart title
            hole: Size of hole (0 = pie, 0.4 = donut)
            template: Plotly template
            
        Returns:
            Plotly Figure
        """
        if df.empty:
            return ChartBuilder._empty_chart(title)
        
        fig = px.pie(df, values=values, names=names, title=title, hole=hole, template=template)
        return fig
    
    @staticmethod
    def create_scatter_chart(
        df: pd.DataFrame,
        x: str,
        y: str,
        title: str = "",
        color: Optional[str] = None,
        size: Optional[str] = None,
        template: str = "plotly_white"
    ) -> go.Figure:
        """
        Create a scatter chart.
        
        Args:
            df: DataFrame with data
            x: Column name for x-axis
            y: Column name for y-axis
            title: Chart title
            color: Column for color grouping
            size: Column for bubble size
            template: Plotly template
            
        Returns:
            Plotly Figure
        """
        if df.empty:
            return ChartBuilder._empty_chart(title)
        
        fig = px.scatter(df, x=x, y=y, color=color, size=size, title=title, template=template)
        return fig
    
    @staticmethod
    def _empty_chart(title: str = "No Data") -> go.Figure:
        """Create an empty chart with a message."""
        fig = go.Figure()
        fig.add_annotation(
            text="No data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title=title,
            template="plotly_white",
            height=300
        )
        return fig
