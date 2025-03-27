This project gets Receipts, Receipts Payroll, Customers and Employees data from LAE API, Webquotes from Webquotes API and different AGI reports from the Payroll API and store it into database tables.

## Considerations

You need to create .env file in the root of the project and with this variables:

    SECRET_KEY = "secret_key"
    API_KEY = "api_key"
    S2_USER = "username"
    S2_PASS = "password"

You need to create .ini files in /db/config/ with this db credentials for the main_data,  compliance and main_data (flask) database:

    [postgresql]
    host=host
    database=dbname
    user=username
    password=password

## Instalation
1. Clone the repo to a local directory on your machine.
2. Install Python 3.12.3. Make sure to add Python and pip to PATH (Windows).
3. Open the project on VSCode, open a new Terminal and enter this commands:    

**Install virtualenv (venv)**

    python -m venv .venv

**Update pip**

    python -m pip install --upgrade pip

**Install external packages**

    pip install -r requirements.txt

**Activate virtualenv**

*For Windows*

    .\.venv\Scripts\activate

*For Linux*

    source .venv/bin/activate

4. Reload the VSCode window

5. Enter this command to run de API o click the start icon on the top right corner:

*For Windows*

    python .\api.py

*For Linux*

    python3 api.py
