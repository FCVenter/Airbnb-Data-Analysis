# main.py

import sys
import logging
from dotenv import load_dotenv
import os
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox,
    QLineEdit, QPushButton, QTableWidget, QTableWidgetItem, QMessageBox, QScrollArea
)
from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QFont, QPalette, QColor, QAction
from sqlalchemy import create_engine, exc, text
import pandas as pd
from queries import queries  # Importing the queries from queries.py

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

class MainWindow(QMainWindow):
    """
    Main application window class.
    Handles UI setup, user interactions, and query executions.
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

        self.initUI()  # Initialize the User Interface

    def initUI(self):
        """
        Sets up the User Interface components.
        """
        self.setWindowTitle('Airbnb Data Analysis')
        self.setMinimumSize(900, 700)

        # Central widget and main vertical layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # Menu bar setup for theme switching
        self.setupMenuBar()

        # Query selection layout (dropdown)
        self.setupQuerySelection(main_layout)

        # Parameters input area within a scrollable area
        self.setupParametersInput(main_layout)

        # Run button layout
        self.setupRunButton(main_layout)

        # Results table setup
        self.setupResultsTable(main_layout)

        # Initialize parameters for the first query
        self.updateParameters(0)

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

    def setupQuerySelection(self, main_layout):
        """
        Sets up the query selection dropdown.

        Args:
            main_layout: The main vertical layout to add the query selection widgets.
        """
        query_layout = QHBoxLayout()
        query_label = QLabel('Select Query:')
        query_label.setFont(QFont('Arial', 12))

        self.query_combo = QComboBox()
        self.query_combo.setFont(QFont('Arial', 12))

        # Populate the combo box with queries
        for index in sorted(queries.keys()):
            self.query_combo.addItem(queries[index]['description'])

        # Connect the selection change to update parameters
        self.query_combo.currentIndexChanged.connect(self.updateParameters)

        # Add widgets to the query layout
        query_layout.addWidget(query_label)
        query_layout.addWidget(self.query_combo)
        query_layout.addStretch()  # Pushes widgets to the left

        # Add the query layout to the main layout
        main_layout.addLayout(query_layout)

    def setupParametersInput(self, main_layout):
        """
        Sets up the parameters input area within a scrollable area.

        Args:
            main_layout: The main vertical layout to add the parameters input widgets.
        """
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)

        self.parameters_container = QWidget()
        self.parameters_layout = QVBoxLayout(self.parameters_container)
        scroll_area.setWidget(self.parameters_container)

        main_layout.addWidget(scroll_area)

        # Dictionary to hold parameter input widgets
        self.parameter_inputs = {}

    def setupRunButton(self, main_layout):
        """
        Sets up the 'Run Query' button.

        Args:
            main_layout: The main vertical layout to add the run button.
        """
        run_layout = QHBoxLayout()
        self.run_button = QPushButton('Run Query')
        self.run_button.setFont(QFont('Arial', 12))
        self.run_button.clicked.connect(self.runQuery)

        run_layout.addStretch()  # Pushes the button to the right
        run_layout.addWidget(self.run_button)

        main_layout.addLayout(run_layout)

    def setupResultsTable(self, main_layout):
        """
        Sets up the table widget to display query results.

        Args:
            main_layout: The main vertical layout to add the results table.
        """
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(0)
        self.results_table.setRowCount(0)
        self.results_table.setFont(QFont('Arial', 10))
        self.results_table.setEditTriggers(QTableWidget.NoEditTriggers)  # Make table read-only
        self.results_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.results_table.setSelectionMode(QTableWidget.SingleSelection)

        main_layout.addWidget(self.results_table)

    def updateParameters(self, index):
        """
        Updates the parameters input fields based on the selected query.
        Clears existing parameters and loads new ones.

        Args:
            index: The index of the selected query in the combo box.
        """
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

    def clearParameters(self):
        """
        Clears all existing parameter input widgets from the parameters layout.
        """
        def clearLayout(layout):
            if layout is not None:
                while layout.count():
                    item = layout.takeAt(0)
                    widget = item.widget()
                    if widget is not None:
                        widget.deleteLater()
                    else:
                        sub_layout = item.layout()
                        if sub_layout is not None:
                            clearLayout(sub_layout)
                            sub_layout.deleteLater()
        clearLayout(self.parameters_layout)
        self.parameter_inputs.clear()

    def createParameterInput(self, label_text):
        """
        Creates a horizontal layout with a label and input field for a parameter.

        Args:
            label_text: The text for the label describing the parameter.

        Returns:
            The QLineEdit input field widget.
        """
        layout = QHBoxLayout()
        label = QLabel(label_text)
        label.setFont(QFont('Arial', 12))
        input_field = QLineEdit()
        input_field.setFont(QFont('Arial', 12))
        input_field.setPlaceholderText('Enter value')
        layout.addWidget(label)
        layout.addWidget(input_field)
        self.parameters_layout.addLayout(layout)
        return input_field

    def runQuery(self):
        """
        Gathers input parameters, executes the selected query in a separate thread,
        and handles the results or errors.
        """
        index = self.query_combo.currentIndex()
        query_info = queries.get(index)

        if not query_info:
            QMessageBox.warning(self, 'Not Implemented', 'This query is not implemented yet.')
            return

        sql = text(query_info['sql'])  # Wrap the SQL with SQLAlchemy's text()
        params = {}

        try:
            # Gather and validate parameters
            for param_name, (input_field, param_type) in self.parameter_inputs.items():
                param_value = input_field.text().strip()
                if param_value == '':
                    raise ValueError(f'Please enter a value for "{param_name.replace("_", " ")}".')

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

                params[param_name] = param_value

            logging.info(f'Executing query: {query_info["description"]} with params {params}')

            # Execute the query in a separate thread
            self.thread = QueryThread(self.engine, sql, params)
            self.thread.results_ready.connect(self.displayResults)
            self.thread.error_occurred.connect(self.handleThreadError)
            self.thread.start()

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
        Displays the query results in the table widget.

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

    def handleThreadError(self, error_message):
        """
        Handles errors emitted from the QueryThread.

        Args:
            error_message: The error message string.
        """
        logging.error(f'Query thread error: {error_message}')
        QMessageBox.critical(self, 'Query Error', f'An error occurred while executing the query:\n{error_message}')

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
        self.setPalette(palette)

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
        self.setPalette(palette)

if __name__ == '__main__':
    # Create the application instance
    app = QApplication(sys.argv)

    # Set application style for consistency across platforms
    app.setStyle('Fusion')

    # Instantiate and display the main window
    window = MainWindow()
    window.setLightTheme()  # Start with the light theme
    window.show()

    logging.info("Application started.")

    # Execute the application's main loop
    sys.exit(app.exec())
