This project gets data from different endpoints of the LAE API and store it into database tables.

Steps to install and run the project:
1. Clone the repo to a local directory on your machine.
2. Install Python 3.12.3.
3. Open the project on VSCode, open a new Terminal and enter this commands:

    python -m venv .venv

    python -m pip install --upgrade pip

    .\.venv\Scripts\activate (*for Windows*)

    source .venv/bin/activate (*for Linux*)

    pip install -r requirements.txt

4. Reload the VSCode window
5. Enter this command to run de API o click the start icon on the top right corner:

    python .\api.py (for Windows)

    python3 api.py (for Linux)
