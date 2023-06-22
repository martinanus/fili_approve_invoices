import functions_framework
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "credentials.json"


def get_client():
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    return client;


def update_big_query(id, dataset):
    client = get_client();

    query = """
    DECLARE
      today DATE DEFAULT CURRENT_DATE();
    UPDATE
      `fili-377220."""+str(dataset)+""".invoices`
    SET
      status = 'aprobada',
      approved_date = today,
      pay_delay = DATE_DIFF(today, due_date, DAY),
      days_to_pay = null
    WHERE
      unique_key="""+str(id);

    query_job = client.query(query)  # Make an API request.


@functions_framework.http
def main(request):
    request_args = request.args

    exp_args = ['id', 'invoiceid', 'installment', 'dataset']
    act_args = list(request_args.keys())

    if request_args and set(exp_args).issubset(act_args):
       id             = request_args['id']
       invoiceid      = request_args['invoiceid']
       installment      = request_args['installment']
       dataset      = request_args['dataset']
       update_big_query(id, dataset);
       return ('La cuota {} de la factura con ID {} fue marcada como paga!'.format(installment, invoiceid)
                + '\n\n Puede cerrar esta ventana.')
    else:
       return ('Error en la identificación de la factura seleccionada. '
              + '\n\n Por favor, contacte al administrador para resolver el problema.')

