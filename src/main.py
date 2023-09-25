import functions_framework
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

key_path              = "credentials.json"
project_name          = "fili-377220"
payment_table_name    = "p_01_payments_t"
invoices_table_name   = "i_06_invoices_t"
db_request            = {'args' : {
                            'id'          : '9878911',
                            'invoiceid'   : '4122670',
                            'installment' : '1',
                            'dataset'     : 'fili_sandbox',
                            'datetime'    : 'UTC-3',
                            'dbt_service' : 'dbt-dev'
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
    dbt_service    = request_args['dbt_service']

    print("\
          id            : {0} \n\
          invoiceid     : {1} \n\
          installment   : {2} \n\
          dataset       : {3} \n\
          datetime      : {4} \n\
          dbt_service   : {5} \n"
          .format(id, invoiceid, installment, dataset, datetime, dbt_service))


    update_big_query(id, dataset, datetime)

    run_dbt(dbt_service)


    print("OK: invoice approved successfully")
    return ('¡La cuota {} de la factura con ID {} fue marcada como paga!'.format(installment, invoiceid)
              + '\n\n Puede cerrar esta ventana.')


def validate_args(request_args):
    exp_args = ['id', 'invoiceid', 'installment', 'dataset', 'datetime', 'dbt_service']
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

    invoice_table_id = "{0}.{1}.{2}".format(project_name, dataset, invoices_table_name)
    payment_table_id = "{0}.{1}.{2}".format(project_name, dataset, payment_table_name)

    query = """
    INSERT INTO
        `"""+ payment_table_id + """`
            (timestamp,
            counterpart,
            contact_email,
            relation,
            is_income,
            id,
            invoice_key,
            date,
            currency,
            name_concept_1,
            amount_concept_1,
            name_concept_2,
            amount_concept_2,
            name_concept_3,
            amount_concept_3,
            name_concept_4,
            amount_concept_4,
            name_concept_5,
            amount_concept_5,
            name_concept_6,
            amount_concept_6,
            name_concept_7,
            amount_concept_7,
            name_concept_8,
            amount_concept_8,
            name_concept_9,
            amount_concept_9,
            name_concept_10,
            amount_concept_10,
            amount,
            peso_amount,
            dollar_official_amount,
            dollar_blue_amount,
            dollar_mep_amount,
            payment_method,
            payment_group_1,
            payment_group_2,
            payment_group_3,
            payment_group_4,
            payment_group_5,
            payment_documents_url,
            payment_url_source_reference)
    WITH
        new_id AS(
        SELECT
          IFNULL(CAST(CAST(MAX(id) AS FLOAT64) + 1 AS STRING), "890001") pay_id
        FROM
            `"""+ payment_table_id + """`
        WHERE
            NOT REGEXP_CONTAINS(id, '-')
            AND REGEXP_CONTAINS(id, '^89') )
    SELECT
        CURRENT_TIMESTAMP(),
        counterpart,
        CAST(NULL AS STRING),
        relation,
        is_income,
        new_id.pay_id,
        unique_key,
        CURRENT_DATE('"""+str(datetime)+"""'),
        currency,
        "Pago Factura",
        amount,
        CAST(NULL AS STRING),
        NULL,
        CAST(NULL AS STRING),
        NULL,
        CAST(NULL AS STRING),
        NULL,
        CAST(NULL AS STRING),
        NULL,
        CAST(NULL AS STRING),
        NULL,
        CAST(NULL AS STRING),
        NULL,
        CAST(NULL AS STRING),
        NULL,
        CAST(NULL AS STRING),
        NULL,
        CAST(NULL AS STRING),
        NULL,
        amount,
        peso_amount,
        dollar_official_amount,
        dollar_blue_amount,
        dollar_mep_amount,
        "platform",
        invoice_group_1,
        invoice_group_2,
        invoice_group_3,
        invoice_group_4,
        invoice_group_5,
        url_invoice,
        url_source_reference
    FROM
        `"""+ invoice_table_id + """`
    CROSS JOIN
        new_id
    WHERE
        unique_key='""" +str(id) + """'"""

    query_job = client.query(query)  # Make an API request.

    query_job.result()

def run_dbt(dbt_service):
    dbt_service_url = "https://{dbt_service}-7txkfbm3yq-uc.a.run.app" .format(dbt_service=dbt_service)

    try:
        requests.post(url=dbt_service_url, data={},timeout=1)

    except requests.exceptions.ReadTimeout:
        print("Wait for dbt skipped")
        pass


# # ------------------------------------------------------------------------
# #   Trigger main function execution when this file is run
# # ------------------------------------------------------------------------
if __name__ == "__main__":
    main('db')