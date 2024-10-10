import sys
import logging
from dotenv import load_dotenv
import os
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox,
    QLineEdit, QPushButton, QTableWidget, QTableWidgetItem, QMessageBox, QScrollArea,
    QSplitter, QFormLayout, QSizePolicy, QStatusBar, QListWidget, QListWidgetItem, QTabWidget,
    QGroupBox, QTextEdit
)
from PySide6.QtCore import Qt, QThread, Signal, QSize
from PySide6.QtGui import QFont, QPalette, QColor, QAction
from sqlalchemy import create_engine, exc, text
import pandas as pd
from queries import queries  # Importing the queries from queries.py
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import seaborn as sns

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename='app.log',  # Log file
    filemode='a',        # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

class QueryThread(QThread):
    """
    QThread subclass to handle database queries asynchronously.
    Emits signals when results are ready or an error occurs.
    """
    results_ready = Signal(pd.DataFrame)  # Signal emitted when query results are ready
    error_occurred = Signal(str)           # Signal emitted when an error occurs

    def __init__(self, engine, sql, params):
        """
        Initializes the thread with the database engine, SQL query, and parameters.

        Args:
            engine: SQLAlchemy engine instance.
            sql: SQL query string.
            params: Dictionary of parameters for the SQL query.
        """
        super().__init__()
        self.engine = engine
        self.sql = sql
        self.params = params

    def run(self):
        """
        Executes the SQL query in a separate thread.
        Emits the results_ready signal with a DataFrame or error_occurred signal with an error message.
        """
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql_query(self.sql, connection, params=self.params)
            self.results_ready.emit(df)
        except Exception as e:
            self.error_occurred.emit(str(e))

class DataAnalysisThread(QThread):
    """
    QThread subclass to handle data analysis queries asynchronously.
    Emits signals when results are ready or an error occurs.
    """
    analysis_ready = Signal(pd.DataFrame)  # Signal emitted when analysis results are ready
    error_occurred = Signal(str)            # Signal emitted when an error occurs

    def __init__(self, engine, sql):
        """
        Initializes the thread with the database engine and SQL query.

        Args:
            engine: SQLAlchemy engine instance.
            sql: SQL query string.
        """
        super().__init__()
        self.engine = engine
        self.sql = sql

    def run(self):
        """
        Executes the SQL query in a separate thread.
        Emits the analysis_ready signal with a DataFrame or error_occurred signal with an error message.
        """
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql_query(self.sql, connection)
            self.analysis_ready.emit(df)
        except Exception as e:
            self.error_occurred.emit(str(e))

class MplCanvas(FigureCanvas):
    """
    Matplotlib Canvas to embed plots into the PySide6 application.
    """
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        """
        Initializes the Matplotlib figure and axes.

        Args:
            parent: Parent widget.
            width: Width of the figure.
            height: Height of the figure.
            dpi: Dots per inch.
        """
        fig, self.ax = plt.subplots(figsize=(width, height), dpi=dpi)
        super().__init__(fig)

class MainWindow(QMainWindow):
    """
    Main application window class.
    Handles UI setup, user interactions, query executions, and data visualizations.
    """

    def __init__(self):
        """
        Initializes the main window, sets up the database connection, and initializes the UI.
        """
        super().__init__()

        # Load database credentials from environment variables
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_HOST = os.getenv('DB_HOST', 'localhost')  # Default to 'localhost' if not set
        DB_NAME = os.getenv('DB_NAME')

        # Validate environment variables
        if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
            logging.error("One or more database environment variables are missing.")
            QMessageBox.critical(self, 'Configuration Error', 'Database configuration is incomplete. Please check your .env file.')
            sys.exit(1)

        # Set up the database connection
        try:
            self.engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')
            # Test the connection
            with self.engine.connect() as connection:
                pass
            logging.info("Database connection established successfully.")
        except exc.SQLAlchemyError as e:
            logging.error(f'Database connection failed: {e}')
            QMessageBox.critical(self, 'Database Connection Error', f'Failed to connect to the database:\n{e}')
            sys.exit(1)

        self.current_theme = 'light'  # Default theme

        # Initialize thread references to prevent garbage collection
        self.query_thread = None
        self.analysis_thread = None

        self.initUI()  # Initialize the User Interface

    def initUI(self):
        """
        Sets up the User Interface components.
        """
        self.setWindowTitle('Airbnb Data Analysis')
        self.setMinimumSize(1400, 900)  # Increased size to accommodate visualizations and data analysis

        # Central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Menu bar setup for theme switching
        self.setupMenuBar()

        # Create a TabWidget to separate Query and Data Analysis
        self.main_tabs = QTabWidget()
        main_layout.addWidget(self.main_tabs)

        # Setup Query Tab
        self.setupQueryTab()

        # Setup Data Analysis Tab
        self.setupDataAnalysisTab()

        # Status bar
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)

    def setupMenuBar(self):
        """
        Sets up the menu bar with options like theme switching.
        """
        menu_bar = self.menuBar()
        view_menu = menu_bar.addMenu('View')

        # Theme switch action
        self.theme_action = QAction('Switch to Dark Theme', self)
        self.theme_action.triggered.connect(self.toggleTheme)
        view_menu.addAction(self.theme_action)

    def setupQueryTab(self):
        """
        Sets up the Query tab with query selection, parameters, execution, and results display.
        """
        self.query_tab = QWidget()
        query_layout = QVBoxLayout(self.query_tab)
        query_layout.setSpacing(10)
        query_layout.setContentsMargins(10, 10, 10, 10)

        # Create a splitter to split the Query tab horizontally
        splitter = QSplitter(Qt.Horizontal)

        # Left widget: parameters and query selection area
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setSpacing(10)
        left_layout.setContentsMargins(10, 10, 10, 10)

        # Add query selection to left layout
        self.setupQuerySelection(left_layout)

        # Parameters label
        parameters_label = QLabel('Parameters:')
        parameters_label.setFont(QFont('Arial', 12))
        left_layout.addWidget(parameters_label)

        # Add parameters input to left layout
        self.setupParametersInput(left_layout)

        # Sorting and filtering options
        self.setupSortingFilteringOptions(left_layout)

        # Stretch to push the run button to the bottom
        left_layout.addStretch()

        # Add run button to left layout
        self.setupRunButton(left_layout)

        # Add the left widget to the splitter
        splitter.addWidget(left_widget)

        # Right widget: Tabbed interface for Table and Visualization
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        self.setupResultsTabs()
        right_layout.addWidget(self.results_tabs)
        splitter.addWidget(right_widget)

        # Set the splitter sizes
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 3)
        splitter.setSizes([400, 1000])

        # Add the splitter to the Query tab layout
        query_layout.addWidget(splitter)

        # Add the Query tab to the main TabWidget
        self.main_tabs.addTab(self.query_tab, "Query")

    def setupDataAnalysisTab(self):
        """
        Sets up the Data Analysis tab with various analysis and visualization options.
        """
        self.data_analysis_tab = QWidget()
        analysis_layout = QVBoxLayout(self.data_analysis_tab)
        analysis_layout.setSpacing(10)
        analysis_layout.setContentsMargins(10, 10, 10, 10)

        # Group box for selecting analysis type
        analysis_group = QGroupBox("Select Analysis Type")
        analysis_group.setFont(QFont('Arial', 12))
        analysis_layout.addWidget(analysis_group)

        analysis_group_layout = QVBoxLayout(analysis_group)

        self.analysis_combo = QComboBox()
        self.analysis_combo.setFont(QFont('Arial', 12))
        # Updated predefined analysis options: Removed 'Top 10 Neighbourhoods by Listings' and 'Price Distribution'
        # Added 'Host Listings Count Distribution' and 'Average Review Scores by Room Type'
        self.analysis_combo.addItems([
            'None',
            'Average Price by Neighbourhood',
            'Distribution of Accommodations',
            'Reviews vs Price',
            'Host Listings Count Distribution',
            'Average Review Scores by Room Type'
        ])
        analysis_group_layout.addWidget(QLabel("Choose an Analysis:"))
        analysis_group_layout.addWidget(self.analysis_combo)

        # Run Analysis Button
        self.run_analysis_button = QPushButton('Run Analysis')
        self.run_analysis_button.setFont(QFont('Arial', 12))
        self.run_analysis_button.clicked.connect(self.runDataAnalysis)
        analysis_group_layout.addWidget(self.run_analysis_button)

        # Visualization Canvas
        self.analysis_canvas = MplCanvas(self, width=8, height=6, dpi=100)
        self.analysis_canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        analysis_layout.addWidget(self.analysis_canvas)

        # Description or Information Box
        self.analysis_info = QTextEdit()
        self.analysis_info.setReadOnly(True)
        self.analysis_info.setFont(QFont('Arial', 11))
        self.analysis_info.setPlaceholderText("Analysis details and insights will appear here.")
        analysis_layout.addWidget(self.analysis_info)

        # Add the Data Analysis tab to the main TabWidget
        self.main_tabs.addTab(self.data_analysis_tab, "Data Analysis")

    def setupQuerySelection(self, parent_layout):
        """
        Sets up the query selection list.

        Args:
            parent_layout: The layout to add the query selection widgets.
        """
        query_label = QLabel('Select Query:')
        query_label.setFont(QFont('Arial', 12))

        self.query_list = QListWidget()
        self.query_list.setFont(QFont('Arial', 12))
        self.query_list.setToolTip('Select the query to execute')
        self.query_list.setWordWrap(True)
        self.query_list.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.query_list.setSelectionMode(QListWidget.SingleSelection)

        # Remove any conflicting stylesheets to allow theming to work correctly
        # Only add minimal styles if necessary
        self.query_list.setStyleSheet("""
            QListWidget::item {
                padding: 10px;
                margin: 5px;
            }
        """)

        # Populate the list widget with queries
        for index in sorted(queries.keys()):
            query = queries[index]
            item = QListWidgetItem(query['description'])
            item.setData(Qt.UserRole, index)  # Store the index in the item
            item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
            item.setSizeHint(QSize(item.sizeHint().width(), 60))  # Increased height for better spacing
            self.query_list.addItem(item)

        # Connect the selection change to update parameters
        self.query_list.currentRowChanged.connect(self.updateParameters)

        # Create a scroll area for the query list
        query_scroll_area = QScrollArea()
        query_scroll_area.setWidgetResizable(True)
        query_container = QWidget()
        query_layout = QVBoxLayout(query_container)
        query_layout.addWidget(query_label)
        query_layout.addWidget(self.query_list)
        query_scroll_area.setWidget(query_container)

        # Add the scroll area to the parent layout
        parent_layout.addWidget(query_scroll_area)

    def setupParametersInput(self, parent_layout):
        """
        Sets up the parameters input area within a scrollable area.

        Args:
            parent_layout: The layout to add the parameters input widgets.
        """
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        self.parameters_container = QWidget()
        # Use QFormLayout for a more professional look
        self.parameters_layout = QFormLayout(self.parameters_container)
        scroll_area.setWidget(self.parameters_container)

        parent_layout.addWidget(scroll_area)

        # Dictionary to hold parameter input widgets
        self.parameter_inputs = {}

    def setupSortingFilteringOptions(self, parent_layout):
        """
        Sets up the sorting and filtering options for the selected query.

        Args:
            parent_layout: The layout to add the sorting and filtering widgets.
        """
        # Sorting options
        sorting_group_box = QWidget()
        sorting_layout = QFormLayout(sorting_group_box)
        sorting_label = QLabel('Sort by:')
        sorting_label.setFont(QFont('Arial', 12))
        sorting_layout.addRow(sorting_label)

        self.sort_column_combo = QComboBox()
        self.sort_column_combo.setFont(QFont('Arial', 12))
        self.sort_order_combo = QComboBox()
        self.sort_order_combo.setFont(QFont('Arial', 12))
        self.sort_order_combo.addItems(['Ascending', 'Descending'])

        sorting_layout.addRow('Column:', self.sort_column_combo)
        sorting_layout.addRow('Order:', self.sort_order_combo)

        parent_layout.addWidget(sorting_group_box)

    def setupVisualizationOptions(self, parent_layout):
        """
        Sets up the visualization type selection options.

        Args:
            parent_layout: The layout to add the visualization widgets.
        """
        # This method can be removed or commented out since we are eliminating the visualization in the Query Tab
        # To ensure only the Data Analysis Tab handles visualizations, we'll remove the visualization options here.
        # However, to ensure that the Data Analysis tab retains its own visualization options, no changes are needed there.

        # Remove or comment out the following lines if you don't want visualization options in the Query tab:
        # self.setupVisualizationOptions(left_layout)

        # Commenting out to remove visualization options from Query Tab
        """
        visualization_group_box = QWidget()
        visualization_layout = QFormLayout(visualization_group_box)
        visualization_label = QLabel('Visualization Type:')
        visualization_label.setFont(QFont('Arial', 12))
        visualization_layout.addRow(visualization_label)

        self.visualization_combo = QComboBox()
        self.visualization_combo.setFont(QFont('Arial', 12))
        # Add common visualization types
        self.visualization_combo.addItems(['None', 'Bar Chart', 'Pie Chart', 'Scatter Plot', 'Line Chart', 'Histogram'])

        visualization_layout.addRow('Type:', self.visualization_combo)

        parent_layout.addWidget(visualization_group_box)
        """

    def setupRunButton(self, parent_layout):
        """
        Sets up the 'Run Query' button.

        Args:
            parent_layout: The layout to add the run button.
        """
        run_layout = QHBoxLayout()
        self.run_button = QPushButton('Run Query')
        self.run_button.setFont(QFont('Arial', 12))
        self.run_button.clicked.connect(self.runQuery)

        run_layout.addWidget(self.run_button)
        run_layout.setAlignment(Qt.AlignCenter)

        parent_layout.addLayout(run_layout)

    def setupResultsTabs(self):
        """
        Sets up a tabbed interface for displaying results in table and visualization forms.
        """
        self.results_tabs = QTabWidget()

        # Tab for Table View
        self.table_tab = QWidget()
        table_layout = QVBoxLayout(self.table_tab)
        self.setupResultsTable()
        table_layout.addWidget(self.results_table)
        self.results_tabs.addTab(self.table_tab, "Table View")

        # Remove Visualization Tab from Query Tab
        # If you want to retain visualization in the Query Tab, keep the following code
        # However, as per the user's request, we'll remove it.

        # Commenting out the Visualization tab in Query Tab
        """
        # Tab for Visualization
        self.visualization_tab = QWidget()
        visualization_layout = QVBoxLayout(self.visualization_tab)
        self.setupVisualizationCanvas()
        visualization_layout.addWidget(self.canvas)
        self.results_tabs.addTab(self.visualization_tab, "Visualization")
        """

        # Since we are removing the first visualization part, you may choose to delete the above or keep it commented.

    def setupResultsTable(self):
        """
        Sets up the table widget to display query results.
        """
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(0)
        self.results_table.setRowCount(0)
        self.results_table.setFont(QFont('Arial', 10))
        self.results_table.setEditTriggers(QTableWidget.NoEditTriggers)  # Make table read-only
        self.results_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.results_table.setSelectionMode(QTableWidget.SingleSelection)
        self.results_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

    def setupVisualizationCanvas(self):
        """
        Sets up the Matplotlib canvas for displaying visualizations.
        """
        # Since we are removing the first visualization part, we can omit this method or keep it for future use.
        self.canvas = MplCanvas(self, width=8, height=6, dpi=100)
        self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

    def updateParameters(self, current_row):
        """
        Updates the parameters input fields based on the selected query.
        Clears existing parameters and loads new ones.

        Args:
            current_row: The current row index of the selected query.
        """
        # Get the selected query index
        item = self.query_list.currentItem()
        if item:
            index = item.data(Qt.UserRole)
        else:
            index = None

        # Clear existing parameter inputs
        self.clearParameters()
        self.parameters_container.update()  # Update the container

        # Fetch the selected query information
        query_info = queries.get(index)
        if query_info:
            params = query_info.get('params', [])
            for param_name, label_text, param_type in params:
                input_field = self.createParameterInput(label_text)
                self.parameter_inputs[param_name] = (input_field, param_type)

            # Update sortable columns
            self.sort_column_combo.clear()
            sortable_columns = query_info.get('sortable_columns', [])
            self.sort_column_combo.addItems(sortable_columns)
        else:
            self.sort_column_combo.clear()

        # Reset visualization type to 'None' when query changes
        # Commented out since we removed visualization in Query Tab
        # self.visualization_combo.setCurrentText('None')

    def clearParameters(self):
        """
        Clears all existing parameter input widgets from the parameters layout.
        """
        while self.parameters_layout.count():
            item = self.parameters_layout.takeAt(0)
            if item is not None:
                widget = item.widget()
                if widget is not None:
                    widget.deleteLater()
        self.parameter_inputs.clear()

    def createParameterInput(self, label_text):
        """
        Creates an input field for a parameter and adds it to the parameters layout.

        Args:
            label_text: The text for the label describing the parameter.

        Returns:
            The QLineEdit input field widget.
        """
        label = QLabel(label_text)
        label.setFont(QFont('Arial', 12))
        input_field = QLineEdit()
        input_field.setFont(QFont('Arial', 12))
        input_field.setPlaceholderText('Enter value')
        # Add to form layout
        self.parameters_layout.addRow(label, input_field)
        return input_field

    def runQuery(self):
        """
        Gathers input parameters, constructs the SQL query with dynamic filtering and sorting,
        executes the query in a separate thread, and handles the results or errors.
        """
        item = self.query_list.currentItem()
        if item:
            index = item.data(Qt.UserRole)
        else:
            QMessageBox.warning(self, 'No Query Selected', 'Please select a query to run.')
            return

        query_info = queries.get(index)

        if not query_info:
            QMessageBox.warning(self, 'Not Implemented', 'This query is not implemented yet.')
            return

        base_sql = query_info['sql']
        params = {}
        where_clauses = []
        order_by_clause = ''

        try:
            # Gather and validate parameters
            for param_name, (input_field, param_type) in self.parameter_inputs.items():
                param_value = input_field.text().strip()
                if param_value != '':
                    # Convert parameter to the required type
                    if param_type == int:
                        try:
                            param_value = int(param_value)
                        except ValueError:
                            raise ValueError(f'Invalid integer value for "{param_name.replace("_", " ")}".')
                    elif param_type == float:
                        try:
                            param_value = float(param_value)
                        except ValueError:
                            raise ValueError(f'Invalid float value for "{param_name.replace("_", " ")}".')
                    # Add more type conversions if needed

                    # Build WHERE clause
                    if param_name.startswith('min_'):
                        column = param_name.replace('min_', '')
                        where_clauses.append(f'"{column}" >= :{param_name}')
                    elif param_name.startswith('max_'):
                        column = param_name.replace('max_', '')
                        where_clauses.append(f'"{column}" <= :{param_name}')
                    else:
                        where_clauses.append(f'"{param_name}" = :{param_name}')

                    params[param_name] = param_value

            # Build WHERE clause
            where_clause = ''
            if where_clauses:
                where_clause = 'WHERE ' + ' AND '.join(where_clauses)
            else:
                where_clause = ''

            # Build ORDER BY clause
            sort_column = self.sort_column_combo.currentText()
            sort_order = self.sort_order_combo.currentText()

            # Validate sort_column against sortable_columns
            sortable_columns = query_info.get('sortable_columns', [])
            if sort_column and sort_column in sortable_columns:
                order_by_clause = f'ORDER BY "{sort_column}" {"ASC" if sort_order == "Ascending" else "DESC"}'
            else:
                order_by_clause = ''

            # Replace placeholders in SQL
            sql = base_sql.format(where_clause=where_clause, order_by_clause=order_by_clause)
            sql = text(sql)

            logging.info(f'Executing query: {query_info["description"]} with params {params}')
            self.statusBar.showMessage('Executing query...', 5000)

            # Execute the query in a separate thread
            self.query_thread = QueryThread(self.engine, sql, params)  # Keep a reference
            self.query_thread.results_ready.connect(self.displayResults)
            self.query_thread.error_occurred.connect(self.handleThreadError)
            self.query_thread.finished.connect(self.query_thread.deleteLater)  # Clean up after thread finishes
            self.query_thread.start()

        except ValueError as ve:
            logging.warning(f'Input validation error: {ve}')
            QMessageBox.warning(self, 'Input Error', str(ve))
        except exc.SQLAlchemyError as sae:
            logging.error(f'Database error: {sae}')
            QMessageBox.critical(self, 'Database Error', f'An error occurred while executing the query:\n{sae}')
        except Exception as e:
            logging.error(f'Unexpected error: {e}')
            QMessageBox.critical(self, 'Error', f'An unexpected error occurred:\n{e}')

    def displayResults(self, df):
        """
        Displays the query results in the table widget and generates visualizations.

        Args:
            df: Pandas DataFrame containing the query results.
        """
        self.results_table.clear()
        self.results_table.setColumnCount(len(df.columns))
        self.results_table.setRowCount(len(df.index))
        self.results_table.setHorizontalHeaderLabels(df.columns)

        # Populate the table with data
        for row in range(len(df.index)):
            for col in range(len(df.columns)):
                value = df.iat[row, col]
                item = QTableWidgetItem(str(value))
                self.results_table.setItem(row, col, item)

        self.results_table.resizeColumnsToContents()
        self.results_table.resizeRowsToContents()

        logging.info(f'Query executed successfully with {len(df)} records returned.')
        self.statusBar.showMessage(f'Query executed successfully. {len(df)} records returned.', 5000)

        # Scroll to the top of the table
        self.results_table.scrollToTop()

        # Generate visualization based on selected type
        # Since we're removing the first visualization part, ensure it's not called
        # Commenting out the following line:
        # self.generateVisualization(df)

    def handleThreadError(self, error_message):
        """
        Handles errors emitted from the QueryThread.

        Args:
            error_message: The error message string.
        """
        logging.error(f'Query thread error: {error_message}')
        QMessageBox.critical(self, 'Query Error', f'An error occurred while executing the query:\n{error_message}')
        self.statusBar.showMessage('Error occurred during query execution.', 5000)

    def toggleTheme(self):
        """
        Toggles between light and dark themes.
        """
        if self.current_theme == 'light':
            self.setDarkTheme()
            self.current_theme = 'dark'
            self.theme_action.setText('Switch to Light Theme')
        else:
            self.setLightTheme()
            self.current_theme = 'light'
            self.theme_action.setText('Switch to Dark Theme')

    def setLightTheme(self):
        """
        Applies the light theme to the application.
        """
        palette = QPalette()
        palette.setColor(QPalette.Window, Qt.white)
        palette.setColor(QPalette.WindowText, Qt.black)
        palette.setColor(QPalette.Base, Qt.white)
        palette.setColor(QPalette.AlternateBase, QColor(245, 245, 245))
        palette.setColor(QPalette.ToolTipBase, Qt.white)
        palette.setColor(QPalette.ToolTipText, Qt.black)
        palette.setColor(QPalette.Text, Qt.black)
        palette.setColor(QPalette.Button, QColor(225, 225, 225))
        palette.setColor(QPalette.ButtonText, Qt.black)
        palette.setColor(QPalette.BrightText, Qt.red)
        palette.setColor(QPalette.Highlight, QColor(0, 120, 215))
        palette.setColor(QPalette.HighlightedText, Qt.white)
        QApplication.instance().setPalette(palette)

    def setDarkTheme(self):
        """
        Applies the dark theme to the application.
        """
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor(53, 53, 53))
        palette.setColor(QPalette.WindowText, Qt.white)
        palette.setColor(QPalette.Base, QColor(35, 35, 35))
        palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
        palette.setColor(QPalette.ToolTipBase, Qt.white)
        palette.setColor(QPalette.ToolTipText, Qt.white)
        palette.setColor(QPalette.Text, Qt.white)
        palette.setColor(QPalette.Button, QColor(53, 53, 53))
        palette.setColor(QPalette.ButtonText, Qt.white)
        palette.setColor(QPalette.BrightText, Qt.red)
        palette.setColor(QPalette.Highlight, QColor(142, 45, 197))
        palette.setColor(QPalette.HighlightedText, Qt.black)
        QApplication.instance().setPalette(palette)

    def runDataAnalysis(self):
        """
        Executes the selected data analysis and generates corresponding visualizations.
        """
        analysis_type = self.analysis_combo.currentText()

        if analysis_type == 'None':
            self.analysis_canvas.ax.clear()
            self.analysis_info.clear()
            self.analysis_canvas.draw()
            return

        try:
            if analysis_type == 'Average Price by Neighbourhood':
                sql = """
                    SELECT 
                        neighbourhood,
                        ROUND(AVG(price), 2) AS average_price
                    FROM 
                        Listings
                    WHERE 
                        price IS NOT NULL
                    GROUP BY 
                        neighbourhood
                    ORDER BY 
                        average_price DESC
                    LIMIT 20;
                """
                query_description = "Average Price by Neighbourhood"

            elif analysis_type == 'Distribution of Accommodations':
                sql = """
                    SELECT 
                        accommodates, 
                        COUNT(*) AS count
                    FROM 
                        Listings
                    WHERE 
                        accommodates IS NOT NULL
                    GROUP BY 
                        accommodates
                    ORDER BY 
                        accommodates ASC;
                """
                query_description = "Distribution of Accommodations"

            elif analysis_type == 'Reviews vs Price':
                sql = """
                    SELECT 
                        review_scores_rating, 
                        price
                    FROM 
                        Listings
                    WHERE 
                        review_scores_rating IS NOT NULL
                        AND price IS NOT NULL;
                """
                query_description = "Reviews vs Price"

            # New Analysis 1: Host Listings Count Distribution
            elif analysis_type == 'Host Listings Count Distribution':
                sql = """
                    SELECT 
                        calculated_host_listings_count AS host_listings_count, 
                        COUNT(*) AS host_count
                    FROM 
                        Listings
                    GROUP BY 
                        calculated_host_listings_count
                    ORDER BY 
                        calculated_host_listings_count ASC;
                """
                query_description = "Host Listings Count Distribution"

            # New Analysis 2: Average Review Scores by Room Type
            elif analysis_type == 'Average Review Scores by Room Type':
                sql = """
                    SELECT 
                        room_type, 
                        ROUND(AVG(review_scores_rating), 2) AS average_review_score
                    FROM 
                        Listings
                    WHERE 
                        room_type IS NOT NULL
                        AND review_scores_rating IS NOT NULL
                    GROUP BY 
                        room_type
                    ORDER BY 
                        average_review_score DESC;
                """
                query_description = "Average Review Scores by Room Type"

            else:
                QMessageBox.warning(self, 'Invalid Selection', f'The selected analysis "{analysis_type}" is not recognized.')
                return

            logging.info(f'Running data analysis: {analysis_type}')
            self.statusBar.showMessage(f'Running data analysis: {analysis_type}', 5000)

            # Execute the analysis query in a separate thread
            self.analysis_thread = DataAnalysisThread(self.engine, text(sql))  # Keep a reference
            self.analysis_thread.analysis_ready.connect(lambda df: self.displayDataAnalysisResults(df, analysis_type))
            self.analysis_thread.error_occurred.connect(self.handleDataAnalysisError)
            self.analysis_thread.finished.connect(self.analysis_thread.deleteLater)  # Clean up after thread finishes
            self.analysis_thread.start()

        except Exception as e:
            logging.error(f'Data Analysis error: {e}')
            QMessageBox.critical(self, 'Data Analysis Error', f'An error occurred while setting up the analysis:\n{e}')
            self.statusBar.showMessage('Error occurred during data analysis setup.', 5000)

    def displayDataAnalysisResults(self, df, analysis_type):
        """
        Displays the data analysis results in the visualization canvas and info box.

        Args:
            df: Pandas DataFrame containing the analysis results.
            analysis_type: The type of analysis performed.
        """
        # Clear previous plot and info
        self.analysis_canvas.ax.clear()
        self.analysis_info.clear()

        try:
            if analysis_type == 'Average Price by Neighbourhood':
                sns.barplot(x='average_price', y='neighbourhood', data=df, ax=self.analysis_canvas.ax, palette='viridis')
                self.analysis_canvas.ax.set_title('Average Price by Neighbourhood')
                self.analysis_canvas.ax.set_xlabel('Average Price (ZAR)')
                self.analysis_canvas.ax.set_ylabel('Neighbourhood')
                self.analysis_info.setText("This bar chart displays the average price of Airbnb listings across different neighbourhoods. Higher bars indicate more expensive areas.")

            elif analysis_type == 'Distribution of Accommodations':
                sns.barplot(x='accommodates', y='count', data=df, ax=self.analysis_canvas.ax, palette='coolwarm')
                self.analysis_canvas.ax.set_title('Distribution of Accommodations')
                self.analysis_canvas.ax.set_xlabel('Number of Accommodates')
                self.analysis_canvas.ax.set_ylabel('Number of Listings')
                self.analysis_info.setText("This bar chart shows how Airbnb listings are distributed based on their accommodation capacity. It highlights the popularity of different accommodation sizes.")

            elif analysis_type == 'Reviews vs Price':
                sns.scatterplot(x='price', y='review_scores_rating', data=df, ax=self.analysis_canvas.ax, alpha=0.6)
                self.analysis_canvas.ax.set_title('Reviews vs Price')
                self.analysis_canvas.ax.set_xlabel('Price (ZAR)')
                self.analysis_canvas.ax.set_ylabel('Review Scores Rating')
                self.analysis_info.setText("This scatter plot explores the relationship between listing prices and their review scores. It helps identify if higher-priced listings tend to have better reviews.")

            # Visualization for Host Listings Count Distribution
            elif analysis_type == 'Host Listings Count Distribution':
                sns.barplot(x='host_listings_count', y='host_count', data=df, ax=self.analysis_canvas.ax, palette='plasma')
                self.analysis_canvas.ax.set_title('Host Listings Count Distribution')
                self.analysis_canvas.ax.set_xlabel('Number of Listings per Host')
                self.analysis_canvas.ax.set_ylabel('Number of Hosts')
                self.analysis_info.setText("This bar chart illustrates how many hosts have a certain number of listings. It provides insight into host activity and concentration of listings among hosts.")

            # Visualization for Average Review Scores by Room Type
            elif analysis_type == 'Average Review Scores by Room Type':
                sns.barplot(x='average_review_score', y='room_type', data=df, ax=self.analysis_canvas.ax, palette='Spectral')
                self.analysis_canvas.ax.set_title('Average Review Scores by Room Type')
                self.analysis_canvas.ax.set_xlabel('Average Review Score')
                self.analysis_canvas.ax.set_ylabel('Room Type')
                self.analysis_info.setText("This bar chart compares the average review scores across different room types. It highlights which room types tend to receive higher ratings from guests.")

            else:
                QMessageBox.warning(self, 'Unsupported Analysis', f'The analysis type "{analysis_type}" is not supported.')
                self.analysis_canvas.ax.clear()
                self.analysis_canvas.draw()
                return

            self.analysis_canvas.ax.figure.tight_layout()
            self.analysis_canvas.draw()

            logging.info(f'Data analysis "{analysis_type}" executed successfully with {len(df)} records returned.')
            self.statusBar.showMessage(f'Data analysis "{analysis_type}" executed successfully.', 5000)

        except Exception as e:
            logging.error(f'Data Analysis Visualization error: {e}')
            QMessageBox.critical(self, 'Visualization Error', f'An error occurred while generating the analysis visualization:\n{e}')
            self.analysis_canvas.ax.clear()
            self.analysis_canvas.draw()
            self.analysis_info.clear()

    def handleDataAnalysisError(self, error_message):
        """
        Handles errors emitted from the Data Analysis QueryThread.

        Args:
            error_message: The error message string.
        """
        logging.error(f'Data Analysis thread error: {error_message}')
        QMessageBox.critical(self, 'Data Analysis Error', f'An error occurred while executing the data analysis:\n{error_message}')
        self.statusBar.showMessage('Error occurred during data analysis execution.', 5000)

if __name__ == '__main__':
    # Create the application instance
    app = QApplication(sys.argv)

    # Set application style for consistency across platforms
    app.setStyle('Fusion')

    # Set the default font
    app.setFont(QFont('Arial', 10))

    # Instantiate and display the main window
    window = MainWindow()
    window.show()

    # Apply the initial theme
    window.setLightTheme()  # Start with the light theme

    logging.info("Application started.")

    # Execute the application's main loop
    sys.exit(app.exec())
