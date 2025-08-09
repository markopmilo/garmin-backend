from __future__ import annotations
from flask import Flask
from flask_cors import CORS

from config import create_dirs_if_needed
from routes import api

def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)

    # make sure base dirs/config exist (no-ops if already present)
    create_dirs_if_needed()

    # register API blueprint
    app.register_blueprint(api)
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
