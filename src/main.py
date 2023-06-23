import functions_framework
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

key_path              = "credentials.json"
project_name          = "fili-377220"
invoices_table_name   = "invoices"
db_request            = {'args' : {
                            'id'          : '1234541',
                            'invoiceid'   : '1234541',
                            'installment' : '1',
                            'dataset'     : 'fili_staging',
                            'datetime'    : 'UTC-3'
                            }
                        }


@functions_framework.http
def main(request):

    if request == 'db':
        request_args = db_request["args"]
        print("Debug mode")
    else:
        request_args = request.args
        print("Http request")

    if validate_args(request_args) is False:
      print("invalid args")
      return ('Error en la aprobación de la factura seleccionada. '
              + '\n\n Por favor, contacte al administrador para resolver el problema.')


    id             = request_args['id']
    invoiceid      = request_args['invoiceid']
    installment    = request_args['installment']
    dataset        = request_args['dataset']
    datetime       = request_args['datetime']

    print("\
          id            : {0} \n\
          invoiceid     : {1} \n\
          installment   : {2} \n\
          dataset       : {3} \n\
          datetime      : {4} \n"
          .format(id, invoiceid, installment, dataset, datetime))


    update_big_query(id, dataset, datetime)

    print("OK: invoice approved successfully")
    return ('¡La cuota {} de la factura con ID {} fue marcada como paga!'.format(installment, invoiceid)
              + '\n\n Puede cerrar esta ventana.')


def validate_args(request_args):
    exp_args = ['id', 'invoiceid', 'installment', 'dataset', 'datetime']
    act_args = list(request_args.keys())

    if request_args and set(exp_args).issubset(act_args):
        return True

    return False


def get_client():
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    return client


def update_big_query(id, dataset, datetime):
    client = get_client()

    table_id = "{0}.{1}.{2}".format(project_name, dataset, invoices_table_name)

    query = """
    DECLARE
      today DATE DEFAULT CURRENT_DATE('"""+str(datetime)+"""');
    UPDATE
    `"""+ table_id + """`
    SET
      status        = 'aprobada',
      approved_date = today,
      pay_delay     = DATE_DIFF(today, due_date, DAY),
      days_to_pay   = null
    WHERE
      unique_key='""" +str(id) + """'"""

    query_job = client.query(query)  # Make an API request.

    query_job.result()


# # ------------------------------------------------------------------------
# #   Trigger main function execution when this file is run
# # ------------------------------------------------------------------------
if __name__ == "__main__":
    main('db')