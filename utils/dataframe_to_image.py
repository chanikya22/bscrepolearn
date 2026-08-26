import os
import uuid
import numpy as np
import pandas as pd
from datetime import datetime
import dataframe_image as dfi
from typing import Dict, List, Union, Callable, Any, Optional, Tuple


class DataFrameImage:
    """
    A flexible toolkit to convert pandas DataFrames into beautifully styled images
    with rich formatting and visual enhancements.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the DataFrameImage with a pandas DataFrame

        Args:
            df: Input pandas DataFrame
        """
        self.original_df = df.copy()
        self.working_df = df.copy()
        self.selected_columns = list(df.columns)
        self.column_mapping = {col: col for col in df.columns}
        self.reverse_mapping = {col: col for col in df.columns}
        self.formatting = {}
        self.progress_bars = []
        self.highlighting_rules = []
        self.total_row_config = {}
        self.include_total_row = False
        self.total_row_label = "Grand Total"
        self.title = None
        self.logo_url = None
        self.color_theme = {
            'header_bg': 'linear-gradient(to bottom, #2563EB, #1E3A8A)',
            'header_text': 'white',
            'id_column_bg': '#E1EFFE',
            'id_column_text': '#1F2937',
            'id_column_border': '#2563EB',
            'total_row_bg': 'linear-gradient(to bottom, #1E3A8A, #2563EB)',
            'total_row_text': 'white',
            'cell_bg': 'white',
            'cell_text': '#1F2937',
            'progress_bar_color': '#0EA5E9',
            'progress_bar_darker': '#0284C7',
            'highlight_color': '#059669',
            'highlight_text': 'white',
            'positive_value': '#059669',
            'negative_value': '#DC2626',
            'row_hover': 'rgba(243, 244, 246, 0.2)',
            'alternate_row': 'rgba(249, 250, 251, 0.5)'
        }
        self.font_size = {
            'header': '22px',
            'cell': '18px',
            'total_row': '20px'
        }
        self.image_dpi = 400

    def set_columns(self, columns: Dict[str, str] = None):
        """
        Select specific columns and optionally rename them in the output

        Args:
            columns: Dictionary mapping original column names to display names
                     e.g., {'sales': 'Sales (₹)', 'growth': 'Growth (%)'}

        Returns:
            self for method chaining
        """
        if columns is None:
            # Reset to all columns with original names
            self.selected_columns = list(self.original_df.columns)
            self.column_mapping = {col: col for col in self.original_df.columns}
            self.reverse_mapping = {col: col for col in self.original_df.columns}
            return self

        # Filter the working DataFrame to only include selected columns
        orig_cols = list(columns.keys())
        self.working_df = self.original_df[orig_cols].copy()

        # Store column mapping (original -> display name)
        self.column_mapping = columns

        # Create reverse mapping (display name -> original)
        self.reverse_mapping = {v: k for k, v in columns.items()}

        # Update selected columns list
        self.selected_columns = orig_cols

        return self

    def set_number_formatting(self, formatting: Dict[str, str]):
        """
        Set number formatting for specific columns

        Args:
            formatting: Dictionary mapping column names to format types
                        Valid formats: 'comma', 'lakh', 'crore', 'percent', 'currency'
                        Can include additional options like 'decimals:2'
                        e.g., {'sales': 'lakh', 'growth': 'percent:1'}

        Returns:
            self for method chaining
        """
        self.formatting = formatting
        return self

    def add_progress_bar(self, columns: List[str], min_value: float = 0, max_value: float = 100):
        """
        Add progress bar visualization to specific columns

        Args:
            columns: List of column names to visualize as progress bars
            min_value: Minimum value (0% filled bar)
            max_value: Maximum value (100% filled bar)

        Returns:
            self for method chaining
        """
        for col in columns:
            if col in self.selected_columns:
                self.progress_bars.append({
                    'column': col,
                    'min': min_value,
                    'max': max_value
                })
        return self


    def highlight_rows(self, top_n: int = None, column: str = None,
                       condition: Callable = None, color: str = None,
                       text_color: str = None, type: str = None,
                       threshold: float = 0):
        """
        Add row highlighting based on conditions or top N values, or color coding for columns

        Args:
            top_n: Number of top rows to highlight
            column: Column to sort by for top_n highlighting or column to color code
            condition: Lambda function that returns True for rows to highlight
                e.g., lambda row: row['market_share'] > 30
            color: Background color for highlighted rows (hex or name) or color for positive values in color_code
            text_color: Text color for highlighted rows (hex or name)
            type: Type of highlighting ('top_n', 'condition', 'color_code', or None for entire row)
            threshold: Threshold value for color coding (default 0)

        Returns:
            self for method chaining
        """
        # Determine the type of highlighting
        if type is not None:
            highlight_type = type
        elif top_n is not None:
            highlight_type = 'top_n'
        elif condition is not None:
            highlight_type = 'condition'

        highlight_config = {
            'type': highlight_type,
            'column': column,
            'top_n': top_n,
            'condition': condition,
            'color': color or self.color_theme['highlight_color'],
            'text_color': text_color or self.color_theme['highlight_text'],
            'threshold': threshold  # Add threshold for color_code type
        }

        self.highlighting_rules.append(highlight_config)
        return self

    def add_grand_total_row(self, calculations: Dict[str, str] = None, row_label: str = "Grand Total"):
        """
        Add a grand total/summary row to the DataFrame

        Args:
            calculations: Dictionary of column calculations
                          Valid options: 'sum', 'mean', 'median', 'min', 'max', or custom formula
                          e.g., {'sales': 'sum', 'growth': 'mean',
                                 'ratio': "df['col1'].sum() / df['col2'].sum() * 100"}
            row_label: Label for the total row

        Returns:
            self for method chaining
        """
        self.include_total_row = True
        self.total_row_label = row_label

        if calculations is None:
            # Default to sum for numeric columns
            calculations = {}
            for col in self.selected_columns:
                try:
                    # Check if column is numeric
                    pd.to_numeric(self.working_df[col], errors='raise')
                    calculations[col] = 'sum'
                except:
                    # Skip non-numeric columns
                    pass

        self.total_row_config = calculations
        return self

    def set_header(self, title: str = None, logo_url: str = None, title_align: str = 'center'):
        """
        Set header content to appear at the top of the image
        """
        print(f"Setting header: title='{title}', logo_url='{logo_url}'")
        self.header_title = title
        self.header_logo_url = logo_url
        self.header_title_align = title_align
        self.use_header = True
        return self

    def set_footer(self, title: str = None, logo_url: str = None, title_align: str = 'center'):
        """
        Set footer content (title, logo, timestamp) to appear at the bottom of the image

        Args:
            title: Title text
            logo_url: URL for logo image
            title_align: Alignment of the title ('left', 'center', or 'right')

        Returns:
            self for method chaining
        """
        self.footer_title = title
        self.footer_logo_url = logo_url
        self.footer_title_align = title_align
        return self

    def set_font_size(self, header: str = None, cell: str = None, total_row: str = None):
        """
        Set font sizes for different parts of the table

        Args:
            header: Font size for header (e.g., '22px')
            cell: Font size for data cells (e.g., '18px')
            total_row: Font size for total row (e.g., '20px')

        Returns:
            self for method chaining
        """
        if header:
            self.font_size['header'] = header
        if cell:
            self.font_size['cell'] = cell
        if total_row:
            self.font_size['total_row'] = total_row
        return self

    def set_dpi(self, dpi: int):
        """
        Set resolution (DPI) for the output image

        Args:
            dpi: DPI value (e.g., 300, 400, 600)

        Returns:
            self for method chaining
        """
        self.image_dpi = dpi
        return self

    def _apply_number_formatting(self):
        """
        Apply formatting to the DataFrame before styling

        Returns:
            Formatted DataFrame copy
        """
        df_formatted = self.working_df.copy()

        # Handle column renaming for display
        df_formatted.columns = [self.column_mapping.get(col, col) for col in df_formatted.columns]

        # Helper function to convert string with commas to float
        def clean_number(x):
            if isinstance(x, str):
                # Remove both commas and percentage symbols
                x = x.replace(',', '').replace('%', '')
            return float(x)

        # Apply number formatting
        for orig_col, format_spec in self.formatting.items():
            if orig_col in self.selected_columns:
                display_col = self.column_mapping.get(orig_col, orig_col)

                # Parse format specification
                format_parts = format_spec.split(':')
                format_type = format_parts[0]
                decimals = 2  # Default
                if len(format_parts) > 1:
                    try:
                        decimals = int(format_parts[1])
                    except:
                        pass

                # Apply the formatting with handling for comma-formatted numbers
                if format_type == 'comma':
                    df_formatted[display_col] = df_formatted[display_col].apply(
                        lambda x: f"{clean_number(x):,.{decimals}f}" if pd.notnull(x) else ""
                    )
                elif format_type == 'lakh':
                    df_formatted[display_col] = df_formatted[display_col].apply(
                        lambda x: f"{clean_number(x) / 100000:.{decimals}f}L" if pd.notnull(x) else ""
                    )
                elif format_type == 'crore':
                    df_formatted[display_col] = df_formatted[display_col].apply(
                        lambda x: f"{clean_number(x) / 10000000:.{decimals}f}Cr" if pd.notnull(x) else ""
                    )
                elif format_type == 'percent':
                    df_formatted[display_col] = df_formatted[display_col].apply(
                        lambda x: f"{clean_number(x):.{decimals}f}%" if pd.notnull(x) else ""
                    )
                elif format_type == 'currency':
                    df_formatted[display_col] = df_formatted[display_col].apply(
                        lambda x: f"₹{clean_number(x):,.{decimals}f}" if pd.notnull(x) else ""
                    )

        # Add grand total row if configured
        if self.include_total_row:
            # Create a total row
            total_row = {col: "" for col in df_formatted.columns}
            first_col = df_formatted.columns[0]
            total_row[first_col] = self.total_row_label

            # Calculate values for each column based on configuration
            for orig_col, calc_type in self.total_row_config.items():
                if orig_col in self.selected_columns:
                    display_col = self.column_mapping.get(orig_col, orig_col)

                    # Handle different calculation types
                    if calc_type == 'sum':
                        try:
                            total_row[display_col] = self.working_df[orig_col].sum()
                        except:
                            total_row[display_col] = "—"
                    elif calc_type == 'mean':
                        try:
                            total_row[display_col] = self.working_df[orig_col].mean()
                        except:
                            total_row[display_col] = "—"
                    elif calc_type == 'median':
                        try:
                            total_row[display_col] = self.working_df[orig_col].median()
                        except:
                            total_row[display_col] = "—"
                    elif calc_type == 'min':
                        try:
                            total_row[display_col] = self.working_df[orig_col].min()
                        except:
                            total_row[display_col] = "—"
                    elif calc_type == 'max':
                        try:
                            total_row[display_col] = self.working_df[orig_col].max()
                        except:
                            total_row[display_col] = "—"
                    elif calc_type.startswith("("):
                        # Custom formula
                        try:
                            # Use eval to evaluate the formula
                            df = self.working_df  # This makes df available in the eval context
                            result = eval(calc_type)
                            total_row[display_col] = result
                        except Exception as e:
                            print(f"Error calculating {display_col}: {e}")
                            total_row[display_col] = "Error"

            # Apply the same formatting to the total row
            for orig_col, format_spec in self.formatting.items():
                if orig_col in self.selected_columns:
                    display_col = self.column_mapping.get(orig_col, orig_col)

                    if display_col in total_row and total_row[display_col] not in ["", "—", "Error"]:
                        # Parse format specification
                        format_parts = format_spec.split(':')
                        format_type = format_parts[0]
                        decimals = 2  # Default
                        if len(format_parts) > 1:
                            try:
                                decimals = int(format_parts[1])
                            except:
                                pass

                        # Apply the formatting
                        if format_type == 'comma':
                            total_row[display_col] = f"{float(total_row[display_col]):,.{decimals}f}"
                        elif format_type == 'lakh':
                            total_row[display_col] = f"{float(total_row[display_col]) / 100000:.{decimals}f}L"
                        elif format_type == 'crore':
                            total_row[display_col] = f"{float(total_row[display_col]) / 10000000:.{decimals}f}Cr"
                        elif format_type == 'percent':
                            total_row[display_col] = f"{float(total_row[display_col]):.{decimals}f}%"
                        elif format_type == 'currency':
                            total_row[display_col] = f"₹{float(total_row[display_col]):,.{decimals}f}"

            # Add total row to dataframe
            df_formatted = pd.concat([df_formatted, pd.DataFrame([total_row])], ignore_index=True)

        return df_formatted

    def _highlight_cells(self, x):
        """
        Apply styling to cells based on configuration

        Args:
            x: DataFrame to style

        Returns:
            DataFrame of CSS styles
        """
        # Create an empty DataFrame of styles
        styles = pd.DataFrame('', index=x.index, columns=x.columns)

        # Base cell styling for all cells
        base_style = f"""
            padding: 12px 15px;
            text-align: center;
            border: 1px solid #ddd; 
            font-weight: bold;
            font-size: {self.font_size['cell']};
            letter-spacing: 0.5px;
        """

        # Get the last row index (Grand Total row if present)
        last_row_idx = x.index[-1]
        has_total_row = self.include_total_row

        # First column (typically the ID/name column)
        first_col = x.columns[0]

        # Precompute highlight indices for top_n rules
        top_n_indices = {}
        for rule in self.highlighting_rules:
            if rule['type'] == 'top_n' and rule['column'] in self.selected_columns:
                orig_col = rule['column']
                display_col = self.column_mapping.get(orig_col, orig_col)
                n = rule['top_n']

                # Get values for this column, excluding the total row if present
                end_idx = -1 if has_total_row else None
                try:
                    # For percentage columns, remove % sign and convert to float
                    if self.formatting.get(orig_col, '').startswith('percent'):
                        values = [
                            float(str(x).replace('%', ''))
                            if isinstance(x, str) and '%' in x
                            else float(x) if pd.notnull(x) else float('-inf')
                            for x in self.working_df[orig_col][:end_idx]
                        ]
                    else:
                        # For regular numeric columns
                        values = [
                            float(x) if pd.notnull(x) else float('-inf')
                            for x in self.working_df[orig_col][:end_idx]
                        ]

                    # Find indices of top n values
                    indices = np.argsort(values)[-n:]
                    top_n_indices[orig_col] = indices
                except:
                    # If conversion fails, skip this column
                    top_n_indices[orig_col] = []

        # Apply styles based on conditions
        for idx in x.index:
            # Is this the total row?
            is_total_row = has_total_row and idx == last_row_idx

            # Special styling for the Total row
            if is_total_row:
                for col in x.columns:
                    styles.loc[idx, col] = f"""
                        background: {self.color_theme['total_row_bg']};
                        color: {self.color_theme['total_row_text']};
                        font-weight: bold;
                        font-size: {self.font_size['total_row']};
                        text-align: {'left' if col == first_col else 'center'};
                        padding: 18px 22px;
                        border-bottom: 2px solid #1E3A8A;
                        text-shadow: 0px 1px 1px rgba(0,0,0,0.4);
                        letter-spacing: 0.8px;
                    """
                continue

            # Get underlying data row for condition-based rules
            row_data = x.loc[idx]

            # Regular row styling
            for col in x.columns:
                # Find the original column name
                orig_col = self.reverse_mapping.get(col, col)

                # Default styling for this cell
                cell_style = f"""
                    background: {self.color_theme['cell_bg']};
                    color: {self.color_theme['cell_text']};
                    text-align: {'left' if col == first_col else 'center'};
                    {base_style}
                """

                # Special styling for first column (ID/name column)
                if col == first_col:
                    cell_style = f"""
                        background: {self.color_theme['id_column_bg']};
                        color: {self.color_theme['id_column_text']};
                        text-align: left;
                        border-left: 4px solid {self.color_theme['id_column_border']};
                        {base_style}
                    """

                # Progress bar styling if configured
                pb_config = next((pb for pb in self.progress_bars if pb['column'] == orig_col), None)
                if pb_config:
                    try:
                        # Get value and convert to float for calculation
                        val_str = str(x.loc[idx, col])
                        if '%' in val_str:
                            val = float(val_str.replace('%', ''))
                        else:
                            val = float(val_str)

                        # Get min/max for scaling
                        min_val = pb_config.get('min', 0)
                        max_val = pb_config.get('max', 100)

                        # Calculate width for progress bar (scaled between min and max)
                        width_pct = max(0, min(100, ((val - min_val) / (max_val - min_val)) * 100))

                        # Get colors for progress bar
                        bar_color = self.color_theme['progress_bar_color']
                        darker_color = self.color_theme['progress_bar_darker']

                        # Progress bar style with wave effect
                        cell_style = f"""
                            background: linear-gradient(to right, 
                                {bar_color} {width_pct}%, 
                                #F3F4F6 {width_pct}%
                            );
                            background-image: 
                                linear-gradient(to right, 
                                    {bar_color} {width_pct}%, 
                                    #F3F4F6 {width_pct}%
                                ),
                                /* First wave layer */
                                radial-gradient(
                                    circle at 20% 50%,
                                    rgba(255, 255, 255, 0.2) 0%,
                                    rgba(255, 255, 255, 0.2) 2%,
                                    transparent 2.5%
                                ),
                                radial-gradient(
                                    circle at 40% 40%,
                                    rgba(255, 255, 255, 0.2) 0%,
                                    rgba(255, 255, 255, 0.2) 2%,
                                    transparent 2.5%
                                ),
                                radial-gradient(
                                    circle at 60% 60%,
                                    rgba(255, 255, 255, 0.2) 0%,
                                    rgba(255, 255, 255, 0.2) 2%,
                                    transparent 2.5%
                                ),
                                radial-gradient(
                                    circle at 80% 45%,
                                    rgba(255, 255, 255, 0.2) 0%,
                                    rgba(255, 255, 255, 0.2) 2.5%,
                                    transparent 3%
                                ),
                                /* Second wave layer - darker color */
                                linear-gradient(to right,
                                    transparent 0%,
                                    transparent 2%,
                                    {darker_color} 2%,
                                    {darker_color} 3%,
                                    transparent 3%,
                                    transparent 13%,
                                    {darker_color} 13%,
                                    {darker_color} 14%,
                                    transparent 14%,
                                    transparent 33%,
                                    {darker_color} 33%,
                                    {darker_color} 34%,
                                    transparent 34%,
                                    transparent 55%,
                                    {darker_color} 55%,
                                    {darker_color} 56%,
                                    transparent 56%,
                                    transparent 78%,
                                    {darker_color} 78%,
                                    {darker_color} 79%,
                                    transparent 79%
                                ),
                                /* Third wave layer - animated wave effect */
                                repeating-linear-gradient(
                                    45deg,
                                    rgba(255, 255, 255, 0.15) 0px,
                                    rgba(255, 255, 255, 0.15) 10px,
                                    rgba(255, 255, 255, 0) 10px,
                                    rgba(255, 255, 255, 0) 20px
                                );
                            color: black;
                            font-weight: bold;
                            border-radius: 6px;
                            position: relative;
                            overflow: hidden;
                            box-shadow: inset 0 -2px 5px rgba(0, 0, 0, 0.1);
                            text-shadow: 0px 1px 1px rgba(255, 255, 255, 0.5);
                            {base_style}
                        """
                    except:
                        # Fallback for invalid values
                        pass

                # Apply highlighting rules
                for rule in self.highlighting_rules:
                    if rule['type'] == 'top_n' and rule['column'] == orig_col:
                        # Is this row's value in the top N?
                        if idx in top_n_indices.get(orig_col, []):
                            bg_color = rule.get('color', self.color_theme['highlight_color'])
                            txt_color = rule.get('text_color', self.color_theme['highlight_text'])

                            cell_style = f"""
                                background: {bg_color};
                                color: {txt_color};
                                border-radius: 4px;
                                text-shadow: 0px 1px 2px rgba(0,0,0,0.3);
                                {base_style}
                            """

                    elif rule['type'] == 'color_code' and rule['column'] == orig_col:
                        # Color coding based on value (positive/negative)
                        try:
                            val_str = str(x.loc[idx, col])
                            if '%' in val_str:
                                val = float(val_str.replace('%', ''))
                            else:
                                val = float(val_str)

                            threshold = rule.get('threshold', 0)

                            if val > threshold:
                                bg_color = rule.get('color', self.color_theme['positive_value'])
                                text_color = rule.get('text_color')
                                cell_style = f"""
                                    background: {bg_color};
                                    color: {text_color};
                                    border-radius: 4px;
                                    text-shadow: 0px 1px 2px rgba(0,0,0,0.3);
                                    {base_style}
                                """
                            elif val < threshold:

                                cell_style = f"""
                                    background: #FFFFFF;
                                    color: white;
                                    border-radius: 4px;
                                    text-shadow: 0px 1px 2px rgba(0,0,0,0.3);
                                    {base_style}
                                """
                            else:
                                # val == threshold, use default styling
                                cell_style = base_style

                        except:
                            # Skip if value can't be processed, use default styling
                            cell_style = base_style

                    elif rule['type'] == 'condition' and rule['column'] == orig_col:
                        # Apply condition-based highlighting
                        try:
                            # CREATE A TEMPORARY DICTIONARY WITH THE COLUMN NAME TO VALUE MAPPING
                            # This simulates a row object for lambda functions like: lambda row: row['D2C'] > 0
                            temp_row = {}

                            # Include the target column value
                            cell_value = x.loc[idx, col]
                            temp_row[orig_col] = cell_value

                            # For conditions that might reference other columns, include all columns
                            for other_col in self.working_df.columns:
                                # Get the display name for this column
                                display_col = self.column_mapping.get(other_col, other_col)
                                # If it's in the styled dataframe, get its value
                                if display_col in x.columns:
                                    temp_row[other_col] = x.loc[idx, display_col]

                            # Now evaluate the condition with our temp_row
                            if rule['condition'](temp_row):
                                bg_color = rule.get('color', self.color_theme['highlight_color'])
                                txt_color = rule.get('text_color', self.color_theme['highlight_text'])

                                cell_style = f"""
                                    background: {bg_color};
                                    color: {txt_color};
                                    border-radius: 4px;
                                    text-shadow: 0px 1px 2px rgba(0,0,0,0.3);
                                    {base_style}
                                """
                        except Exception as e:
                            # More detailed error handling for debugging
                            print(f"Error evaluating condition for {orig_col}: {str(e)}")
                            # Skip if condition can't be evaluated
                            pass

                # Apply the calculated style to this cell
                styles.loc[idx, col] = cell_style

        return styles

    def export(self, filepath: str = None, filename: str = None, format: str = 'png', dpi: int = None):
        """
        Generate and save the styled DataFrame as an image

        Args:
            filepath: Full path where the file should be saved
            filename: Output filename without path
            format: Output format ('png', 'jpg', or 'svg')
            dpi: Optional DPI setting for this export

        Returns:
            Dict with information about the generated image
        """
        # Apply all formatting
        df_formatted = self._apply_number_formatting()

        # Determine output path...
        if filepath:
            output_path = filepath
            filename = os.path.basename(filepath)
        else:
            temp_folder = os.path.join(os.getcwd(), "table_images")
            os.makedirs(temp_folder, exist_ok=True)

            if filename is None:
                now = datetime.now().strftime("%Y%m%d_%H%M%S")
                unique_id = str(uuid.uuid4())[:8]
                filename = f'table_image_{now}_{unique_id}'

            if not any(filename.lower().endswith(f'.{ext}') for ext in ['png', 'jpg', 'jpeg', 'svg']):
                filename = f"{filename}.{format}"

            output_path = os.path.join(temp_folder, filename)

        # Determine if we're using a header or footer
        use_header = hasattr(self, 'use_header') and self.use_header

        # Timestamp for both header and footer
        timestamp = datetime.now().strftime('%b %d, %Y at %H:%M')

        if use_header:
            # For header, we'll add an HTML row at the top
            # First, create a styled dataframe without the header
            styled_table = df_formatted.style.apply(self._highlight_cells, axis=None)

            # Apply table styling
            styled_table = styled_table.set_table_styles([
                # Your existing table styles...
                # Header styling
                {'selector': 'thead th', 'props': [
                    ('background', self.color_theme['header_bg']),
                    ('color', self.color_theme['header_text']),
                    ('font-weight', 'bold'),
                    ('text-align', 'center'),
                    ('padding', '12px 15px'),
                    ('font-size', self.font_size['header']),
                    ('border-bottom', '2px solid #1E3A8A'),
                    ('text-shadow', '0px 1px 1px rgba(0,0,0,0.4)'),
                    ('box-shadow', '0px 2px 3px rgba(0,0,0,0.2)'),
                    ('letter-spacing', '0.5px'),
                    ('text-transform', 'uppercase')
                ]},
                # Your other style elements...
            ])

            # Hide index
            styled_table = styled_table.hide(axis='index')

            # Export table to HTML
            table_html = styled_table.to_html()

            # Get header content
            header_title = self.header_title if hasattr(self, 'header_title') else ""
            logo_url = self.header_logo_url if hasattr(self, 'header_logo_url') else ""
            # Create header HTML with larger logo and shifted more to the left
            # Create header HTML with larger logo, everything aligned in one row, and added top padding
            logo_html = f'<img src="{logo_url}" alt="Logo" style="height: 70px; margin: 0; vertical-align: middle;">' if logo_url else ""

            header_html = f"""
                            <div style="display: flex; justify-content: space-between; align-items: center; margin: 0; padding: 45px 0 20px 0; width: 100%; position: relative;">
                                <div style="position: absolute; left: 0; top: 50%; transform: translateY(-50%);">
                                    <img src="{logo_url}" alt="Logo" style="height: 70px; display: block; margin: 0; padding: 0;">
                                </div>
                                <div style="flex: 1; visibility: hidden;">
                                    <!-- Spacer for logo -->
                                </div>
                                 <div style="position: absolute; left: 50%; top: 50%; transform: translate(-50%, -50%); width: auto;">
                               <span style="font-size: 24px; font-weight: bold; color: #1E3A8A;">{header_title}</span>
                                 </div>         
                                <div style="position: absolute; top: 5px; right: 5px; font-style: italic;">
                                    Generated on {timestamp}
                                </div>
                            </div>
                            """

            # Create full HTML document with more top padding
            full_html = f"""
                            <!DOCTYPE html>
                            <html>
                            <head>
                                <style>
                                    body {{
                                        font-family: 'Segoe UI', Roboto, sans-serif;
                                        padding: 0;
                                        margin: 0;
                                        overflow-x: hidden;
                                    }}
                                    @page {{
                                        margin: 0;
                                        padding: 0;
                                    }}
                                    .container {{
                                        padding: 40px 0 0 0;
                                        position: relative;
                                    }}
                                </style>
                            </head>
                            <body>
                                <div class="container">
                                    {header_html}
                                    {table_html}
                                </div>
                            </body>
                            </html>
                            """
            # Write HTML to temporary file
            temp_html = "temp_table_with_header.html"
            with open(temp_html, "w", encoding="utf-8") as f:
                f.write(full_html)

            # Use dataframe_image to convert HTML to image
            from dataframe_image import export
            export_dpi = dpi if dpi is not None else self.image_dpi

            # Convert HTML to image
            # Unfortunately, we need another approach since dfi.export expects a styled dataframe
            # Let's try a simpler solution: use webbrowser to open the HTML file
            # and then take a screenshot

            # For now, let's just export the table without header and note the limitation
            print("Note: Header will be placed at the bottom as a caption due to limitations.")
            print("To place a header at the top, additional packages are required.")

            # Fall back to using caption at the bottom
            styled_table = styled_table.set_caption(header_html)
            dfi.export(styled_table, output_path, dpi=export_dpi)

            # Clean up temporary file
            try:
                os.remove(temp_html)
            except:
                pass

        else:
            # Standard footer approach
            styled = df_formatted.style.apply(self._highlight_cells, axis=None)

            # Apply table styling
            styled = styled.set_table_styles([
                # Your existing table styles...
            ])

            # Hide index
            styled = styled.hide(axis='index')

            # Add footer caption
            if hasattr(self, 'title') and self.title:
                logo_html = ""
                if hasattr(self, 'logo_url') and self.logo_url:
                    logo_html = f'<img src="{self.logo_url}" alt="Logo" style="height: 50px; margin-right: 10px;">'

                title_html = f'<span style="font-size: 24px; font-weight: bold; color: #1E3A8A;">{self.title}</span>'

                caption_html = f"""
                <div style="display: flex; justify-content: space-between; width: 100%; align-items: center; padding: 10px 0;">
                    <div style="flex: 1; text-align: left;">{logo_html}</div>
                    <div style="flex: 2; text-align: center;">{title_html}</div>
                    <div style="flex: 1; text-align: right; font-style: italic;">Generated on {timestamp}</div>
                </div>
                """
                styled = styled.set_caption(caption_html)

            # Export using dataframe_image
            export_dpi = dpi if dpi is not None else self.image_dpi
            dfi.export(styled, output_path, dpi=export_dpi)

        result = {
            "file_name": filename,
            "file_path": output_path,
            "message": f"Table image generated successfully: {filename}"
        }

        return result