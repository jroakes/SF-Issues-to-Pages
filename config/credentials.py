from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT = 'config/locomotive-jupyterhub-service.json'
SERVICE_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
SERVICE_USER = "clients@locomotive.agency"

credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT,
    scopes=SERVICE_SCOPES,
    subject=SERVICE_USER
)