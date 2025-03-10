from flask import Flask, request, jsonify
import os
import csv
import re
import datetime
from decimal import Decimal
from io import StringIO
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from dateutil import parser  # More flexible date parsing

# Initialize Flask App
app = Flask(__name__)

# SQLite Database Configuration
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(BASE_DIR, 'database.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize Database
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Define Model
class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    transaction_date = db.Column(db.String(20), nullable=False)
    description = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(10), nullable=False)
    status = db.Column(db.String(20), nullable=False)

# Create tables inside app context
with app.app_context():
    db.create_all()

# Utility Functions
def detect_delimiter(sample_data: str) -> str:
    delimiters = [',', ';', '|']
    counts = {delim: sample_data.count(delim) for delim in delimiters}
    return max(counts, key=counts.get)

def normalize_column_name(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]', '_', name).lower()

def parse_amount(value: str) -> Decimal:
    """Handles both '1,234.56' and '1234,56' formats correctly."""
    value = value.replace(',', '').replace('.', '')
    if not value.isdigit():
        raise ValueError(f"Invalid amount: {value}")
    return Decimal(value) / 100

def parse_date(value: str) -> str:
    """Parses multiple date formats and returns a standard 'YYYY-MM-DD' format."""
    return parser.parse(value).strftime('%Y-%m-%d')

def process_csv(file_content: str, has_header=True):
    delimiter = detect_delimiter(file_content)
    csv_reader = csv.reader(StringIO(file_content), delimiter=delimiter, quotechar='"')

    rows = list(csv_reader)
    
    if not rows:
        return {'error': 'CSV file contains no data'}

    if has_header:
        headers = [normalize_column_name(h) for h in rows[0]]
        data_rows = rows[1:]  # Skip the header row
    else:
        headers = ['transaction_date', 'description', 'amount', 'currency', 'status']
        data_rows = rows

    if not data_rows:  # Ensure there's at least one row
        return {'error': 'No valid data rows found'}

    processed_data = []

    with app.app_context():
        for row in data_rows:
            if len(row) < 5:  # Ensure all columns exist
                return {'error': f'Malformed row: {row}'}

            transaction_data = {
                'transaction_date': parse_date(row[0]),  # 'YYYY-MM-DD' format
                'description': row[1],
                'amount': str(parse_amount(row[2])),  # Convert Decimal to string
                'currency': row[3],
                'status': row[4].lower(),
            }
            processed_data.append(transaction_data)

            # Save to DB
            transaction = Transaction(**transaction_data)
            db.session.add(transaction)

        db.session.commit()

    return {'message': 'Data processed successfully', 'data': processed_data}

# API Route to Upload CSV
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    try:
        file_content = file.read().decode('utf-8').strip()  # Strip whitespace
        
        if not file_content:
            return jsonify({'error': 'Empty file'}), 400  # Ensure file is not empty
        
        csv_reader = csv.reader(StringIO(file_content), delimiter=detect_delimiter(file_content), quotechar='"')
        rows = list(csv_reader)
        
        if not rows:
            return jsonify({'error': 'CSV file contains no data'}), 400  # Empty CSV check
        
        # Check if there's at least one data row after header
        has_header = len(rows) > 1 and all(normalize_column_name(col) in {'transaction_date', 'description', 'amount', 'currency', 'status'} for col in rows[0])

        if not has_header and len(rows) < 1:
            return jsonify({'error': 'No valid data rows in CSV'}), 400  # No data rows found

        result = process_csv(file_content, has_header)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
